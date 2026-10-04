import { serve } from "https://deno.land/std/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

/**
 * Shopify inventory webhook (optimized for speed):
 * - CryptoKeys pre-imported at module load (avoids expensive importKey per request)
 * - Parallel HMAC verification across all secrets via crypto.subtle.verify
 * - Pre-built constants (URL, headers, query, encoder/decoder)
 * - Native res.json() for Shopify response parsing
 * - Reduced console.log on happy path
 * - Routing by X-Shopify-Shop-Domain: shops listed in SHOPIFY_CLIENT_CREDENTIALS use a cached
 *   client-credentials token and their own HMAC secret; every other shop (or no header) follows
 *   the original path (shared secrets, static URL + token)
 *
 * Required Supabase secrets:
 * - SHOPIFY_APP_SECRETS="secret1,secret2,secret3"
 * - SHOPIFY_STORE_DOMAIN="adam-lippes.myshopify.com"
 * - SHOPIFY_API_VERSION="2024-10"
 * - SHOPIFY_ADMIN_ACCESS_TOKEN="shpat_...."
 * - SB_URL="https://nybxcfjjnkgxzgaeitlk.supabase.co"
 * - SB_SERVICE_ROLE_KEY="sb_secret_...."
 *
 * Optional Supabase secrets:
 * - SHOPIFY_CLIENT_CREDENTIALS='{"adam-lippes-uk.myshopify.com":{"client_id":"...","client_secret":"..."}}'
 *   (Shopify CLI apps: Admin token obtained via the client credentials grant, ~24h validity)
 *   Do NOT also list these client secrets in SHOPIFY_APP_SECRETS. The shop header is not covered
 *   by the HMAC, so a secret present there would let a webhook signed for that shop pass the
 *   default-shop path (US header or no header) and be enriched against, and enqueued as, the
 *   default store. Keeping it only in SHOPIFY_CLIENT_CREDENTIALS binds it to its own shop.
 *   If such an overlap is found at startup, the shared copy is ignored (fail closed).
 */

// ── Module-level singletons (allocated once, reused across requests) ──

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const SECRETS_RAW = (Deno.env.get("SHOPIFY_APP_SECRETS") ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const SHOPIFY_STORE_DOMAIN = Deno.env.get("SHOPIFY_STORE_DOMAIN")!;
const SHOPIFY_API_VERSION = Deno.env.get("SHOPIFY_API_VERSION") ?? "2024-10";
const SHOPIFY_ADMIN_ACCESS_TOKEN = Deno.env.get("SHOPIFY_ADMIN_ACCESS_TOKEN")!;

const SB_URL = Deno.env.get("SB_URL")!;
const SB_SERVICE_ROLE_KEY = Deno.env.get("SB_SERVICE_ROLE_KEY")!;

// Pre-built URL & headers (avoid re-allocating on every request)
const GRAPHQL_URL = `https://${SHOPIFY_STORE_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
const SHOPIFY_HEADERS: HeadersInit = {
  "Content-Type": "application/json",
  "X-Shopify-Access-Token": SHOPIFY_ADMIN_ACCESS_TOKEN,
};

const sb = createClient(SB_URL, SB_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ── Client-credentials shops (e.g. UK Shopify CLI app) ──

// Every shop domain must match this before a secret or token is sent to it.
const SHOP_DOMAIN_RE = /^[a-z0-9][a-z0-9-]*\.myshopify\.com$/;

// Parsed once at module load; absent or invalid => {} (logged once, without values).
const CLIENT_CREDENTIALS = new Map<
  string,
  { client_id: string; client_secret: string }
>();
{
  const raw = Deno.env.get("SHOPIFY_CLIENT_CREDENTIALS");
  if (raw) {
    try {
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error("not an object");
      }
      for (const [shop, c] of Object.entries<any>(parsed)) {
        const domain = shop.trim().toLowerCase();
        if (
          !SHOP_DOMAIN_RE.test(domain) ||
          typeof c?.client_id !== "string" || !c.client_id ||
          typeof c?.client_secret !== "string" || !c.client_secret
        ) {
          console.log("Ignoring invalid SHOPIFY_CLIENT_CREDENTIALS entry");
          continue;
        }
        CLIENT_CREDENTIALS.set(domain, {
          client_id: c.client_id,
          client_secret: c.client_secret,
        });
      }
    } catch {
      console.log("Invalid SHOPIFY_CLIENT_CREDENTIALS, ignoring it");
      CLIENT_CREDENTIALS.clear();
    }
  }
}

// A client secret must never also verify as a shared secret (see header): drop it from the
// shared key list but keep its slot (null) so the other indexes stay stable.
const CLIENT_SECRETS = new Set(
  [...CLIENT_CREDENTIALS.values()].map((c) => c.client_secret),
);
if (SECRETS_RAW.some((s) => CLIENT_SECRETS.has(s))) {
  console.log("Overlap detected, ignoring shared copy");
}

// Pre-import CryptoKeys at module load — importKey is expensive (~5-10ms per key).
// By doing it once at startup we save that cost on every incoming webhook.
const cryptoKeysPromise: Promise<(CryptoKey | null)[]> = Promise.all(
  SECRETS_RAW.map((secret) =>
    CLIENT_SECRETS.has(secret) ? null : crypto.subtle.importKey(
      "raw",
      encoder.encode(secret),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign", "verify"],
    )
  ),
);

// Per-shop HMAC keys, pre-imported at module load like the keys above.
const CLIENT_CREDENTIAL_KEYS = new Map<string, Promise<CryptoKey>>(
  [...CLIENT_CREDENTIALS].map(([shop, c]) => [
    shop,
    crypto.subtle.importKey(
      "raw",
      encoder.encode(c.client_secret),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign", "verify"],
    ),
  ]),
);

// Token cache per shop + one in-flight refresh per shop (dedupes concurrent refreshes).
// refreshAt = expiresAt - min(5 min, ttl/2). After a failed grant, fail fast for 60 s per shop.
const TOKEN_REFRESH_MARGIN_MS = 5 * 60 * 1000;
const TOKEN_FAILURE_BACKOFF_MS = 60 * 1000;
const tokenCache = new Map<
  string,
  { token: string; expiresAt: number; refreshAt: number }
>();
const tokenInFlight = new Map<string, Promise<string>>();
const tokenFailedUntil = new Map<string, number>();

// Minified GraphQL query (allocated once, no whitespace overhead)
const INVENTORY_LEVEL_QUERY =
  'query($id:ID!){inventoryLevel(id:$id){id updatedAt location{id name}item{id}quantities(names:["available","on_hand","incoming","committed","reserved"]){name quantity}}}';

// ── Helpers ──

/** Decode base64 to Uint8Array (for the received HMAC header). */
function b64ToBytes(b64: string): Uint8Array {
  let bin: string;
  try {
    bin = atob(b64);
  } catch {
    return new Uint8Array(0); // malformed header: never matches, caller answers 401
  }
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

/**
 * Verify HMAC with all pre-imported keys **in parallel**.
 * Uses crypto.subtle.verify which performs a native timing-safe comparison
 * (no need for manual base64 encode + JS-level byte comparison).
 * Returns the index of the matching key, or null.
 */
async function verifyHmac(
  rawBytes: Uint8Array,
  receivedB64: string,
): Promise<number | null> {
  const keys = await cryptoKeysPromise;
  if (keys.length === 0) return null;

  const receivedBytes = b64ToBytes(receivedB64);

  // All verify operations dispatched to native threads in parallel
  const results = await Promise.all(
    keys.map((key) => key && crypto.subtle.verify("HMAC", key, receivedBytes, rawBytes)),
  );

  const idx = results.indexOf(true);
  return idx === -1 ? null : idx;
}

/** Verify HMAC against the secret of ONE client-credentials shop (not the shared secrets). */
async function verifyHmacForShop(
  shop: string,
  rawBytes: Uint8Array,
  receivedB64: string,
): Promise<boolean> {
  const key = await CLIENT_CREDENTIAL_KEYS.get(shop);
  if (!key) return false;
  return crypto.subtle.verify("HMAC", key, b64ToBytes(receivedB64), rawBytes);
}

/** Client-credentials grant. Errors carry the HTTP status only (never the body). */
async function requestShopToken(
  shop: string,
): Promise<{ token: string; expiresAt: number; refreshAt: number }> {
  const creds = CLIENT_CREDENTIALS.get(shop);
  if (!creds || !SHOP_DOMAIN_RE.test(shop)) {
    throw new Error("Shopify token: shop not configured");
  }

  const res = await fetch(`https://${shop}/admin/oauth/access_token`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Accept": "application/json",
    },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: creds.client_id,
      client_secret: creds.client_secret,
    }),
    redirect: "error", // never replay the secret to another location
  });

  if (!res.ok) {
    await res.body?.cancel();
    throw new Error(`Shopify token HTTP ${res.status}`);
  }

  let json: any;
  try {
    json = await res.json();
  } catch {
    throw new Error("Shopify token: malformed response");
  }
  if (typeof json?.access_token !== "string" || !json.access_token) {
    throw new Error("Shopify token: malformed response");
  }
  const expiresIn = Number(json.expires_in);
  const ttlMs = (expiresIn > 0 ? expiresIn : 3600) * 1000;
  const expiresAt = Date.now() + ttlMs;
  const refreshAt = expiresAt - Math.min(TOKEN_REFRESH_MARGIN_MS, ttlMs / 2);
  return { token: json.access_token, expiresAt, refreshAt };
}

/**
 * Cached token for a shop; refreshed when < min(5 min, ttl/2) left, concurrent refreshes share
 * one promise. After a failed grant, fails fast for 60 s (a still-valid cached token is reused).
 */
function getShopToken(shop: string): Promise<string> {
  const cached = tokenCache.get(shop);
  const now = Date.now();
  if (cached && now < cached.refreshAt) return Promise.resolve(cached.token);

  if ((tokenFailedUntil.get(shop) ?? 0) > now) {
    return cached && now < cached.expiresAt
      ? Promise.resolve(cached.token)
      : Promise.reject(new Error("Shopify token: backing off after failure"));
  }

  let inFlight = tokenInFlight.get(shop);
  if (!inFlight) {
    inFlight = requestShopToken(shop)
      .then((entry) => {
        tokenCache.set(shop, entry);
        tokenFailedUntil.delete(shop);
        return entry.token;
      }, (e) => {
        tokenFailedUntil.set(shop, Date.now() + TOKEN_FAILURE_BACKOFF_MS);
        const stale = tokenCache.get(shop);
        if (stale && Date.now() < stale.expiresAt) return stale.token;
        throw e;
      })
      .finally(() => tokenInFlight.delete(shop));
    tokenInFlight.set(shop, inFlight);
  }
  return inFlight;
}

/** Drop the cached token only if it is still the one that was rejected. */
function invalidateShopToken(shop: string, rejectedToken: string) {
  if (tokenCache.get(shop)?.token === rejectedToken) tokenCache.delete(shop);
}

/**
 * Query the inventory level. Default store: static URL + token.
 * `ccShop`: client-credentials shop, queried with its own cached token (one retry on 401).
 */
async function fetchInventoryQuantitiesByLevelId(
  inventoryLevelGid: string,
  ccShop: string | null = null,
) {
  const body = JSON.stringify({
    query: INVENTORY_LEVEL_QUERY,
    variables: { id: inventoryLevelGid },
  });

  let res: Response;
  if (ccShop === null) {
    res = await fetch(GRAPHQL_URL, {
      method: "POST",
      headers: SHOPIFY_HEADERS,
      body,
    });
  } else {
    const url = `https://${ccShop}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
    const post = (token: string) =>
      fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Shopify-Access-Token": token,
        },
        body,
        redirect: "error", // never replay the token to another location
      });

    let token = await getShopToken(ccShop);
    res = await post(token);
    if (res.status === 401) {
      await res.body?.cancel();
      invalidateShopToken(ccShop, token);
      token = await getShopToken(ccShop);
      res = await post(token);
    }
  }

  if (!res.ok) {
    if (ccShop !== null) {
      await res.body?.cancel();
      throw new Error(`Shopify GraphQL HTTP ${res.status}`);
    }
    const text = await res.text();
    throw new Error(`Shopify GraphQL HTTP ${res.status}: ${text.slice(0, 500)}`);
  }

  // res.json() is implemented natively — faster than res.text() + JSON.parse()
  const json = ccShop === null ? await res.json() : await res.json().catch(() => {
    throw new Error("Shopify GraphQL: malformed response");
  });
  if (json.errors?.length) {
    if (ccShop !== null) {
      const codes = json.errors.map((e: any) => e?.extensions?.code).filter(Boolean);
      throw new Error(`Shopify GraphQL errors: ${codes.join(",") || "unknown"}`);
    }
    throw new Error(
      `Shopify GraphQL errors: ${JSON.stringify(json.errors).slice(0, 800)}`,
    );
  }

  return json.data?.inventoryLevel ?? null;
}

function normalizeQuantities(invLevel: any): Record<string, number> {
  const out: Record<string, number> = {};
  for (const q of invLevel?.quantities ?? []) {
    if (q?.name) out[q.name] = q.quantity;
  }
  return out;
}

// ── Main handler ──

serve(async (req) => {
  // Cheapest check first
  if (req.method !== "POST") {
    return new Response("Method Not Allowed", { status: 405 });
  }

  const headers = req.headers;
  const received = (headers.get("X-Shopify-Hmac-Sha256") ?? "").trim();

  if (!received) {
    return new Response("ok", { status: 200 });
  }

  if (SECRETS_RAW.length === 0) {
    console.log("Missing SHOPIFY_APP_SECRETS");
    return new Response("Server misconfigured", { status: 500 });
  }

  const topic = headers.get("X-Shopify-Topic");
  const shop = headers.get("X-Shopify-Shop-Domain");
  const webhookId = headers.get("X-Shopify-Webhook-Id");

  const rawBytes = new Uint8Array(await req.arrayBuffer());

  // Route by shop. The shop header is not covered by the HMAC, so a client-credentials
  // shop is verified against ITS OWN secret only; everything else uses the shared secrets.
  const shopKey = (shop ?? "").trim().toLowerCase();
  const ccShop = CLIENT_CREDENTIALS.has(shopKey) ? shopKey : null;

  // Parallel HMAC verification across all secrets (native threads)
  const verified = ccShop !== null
    ? await verifyHmacForShop(ccShop, rawBytes, received)
    : (await verifyHmac(rawBytes, received)) !== null;

  if (!verified) {
    console.log("❌ Invalid HMAC", { webhookId, topic, shop });
    return new Response("Invalid HMAC", { status: 401 });
  }

  // Parse payload
  let payload: any;
  try {
    payload = JSON.parse(decoder.decode(rawBytes));
  } catch {
    return new Response("ok", { status: 200 });
  }

  const inventory_item_id = Number(payload.inventory_item_id);
  const location_id = Number(payload.location_id);
  const inventoryLevelId = String(payload.admin_graphql_api_id || "");

  if (!inventoryLevelId.startsWith("gid://shopify/InventoryLevel/")) {
    return new Response("ok", { status: 200 });
  }

  // Fetch snapshot from Shopify and insert into queue
  try {
    const invLevel = await fetchInventoryQuantitiesByLevelId(inventoryLevelId, ccShop);
    if (!invLevel) {
      return new Response("ok", { status: 200 });
    }

    const quantities = normalizeQuantities(invLevel);

    const { error } = await sb.from("inventory_snapshot_queue").insert({
      shop: ccShop ?? shop ?? SHOPIFY_STORE_DOMAIN,
      webhook_id: webhookId,
      topic,
      inventory_item_id,
      location_id,
      inventory_level_id: invLevel.id,
      location_name: invLevel.location?.name ?? null,
      shopify_updated_at: invLevel.updatedAt ?? null,
      quantities,
      raw_payload: payload,
      status: "pending",
    });

    if (error) {
      const msg = (error.message || "").toLowerCase();
      if (!msg.includes("duplicate") && !msg.includes("unique")) {
        console.log("❌ Queue insert failed", { error: error.message });
      }
    }
  } catch (e) {
    console.log("❌ Snapshot fetch/enqueue failed", { error: String(e) });
  }

  return new Response("ok", { status: 200 });
});
