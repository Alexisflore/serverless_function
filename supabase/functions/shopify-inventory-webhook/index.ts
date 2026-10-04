import { serve } from "https://deno.land/std/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

/**
 * Shopify inventory webhook (optimized for speed):
 * - CryptoKeys pre-imported at module load (avoids expensive importKey per request)
 * - Parallel HMAC verification across all secrets via crypto.subtle.verify
 * - Pre-built constants (URL, headers, query, encoder/decoder)
 * - Native res.json() for Shopify response parsing
 * - Reduced console.log on happy path
 *
 * Required Supabase secrets:
 * - SHOPIFY_APP_SECRETS="secret1,secret2,secret3"
 * - SHOPIFY_STORE_DOMAIN="adam-lippes.myshopify.com"
 * - SHOPIFY_API_VERSION="2024-10"
 * - SHOPIFY_ADMIN_ACCESS_TOKEN="shpss_...."
 * - SB_URL="https://nybxcfjjnkgxzgaeitlk.supabase.co"
 * - SB_SERVICE_ROLE_KEY="sb_secret_...."
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

// Pre-import CryptoKeys at module load — importKey is expensive (~5-10ms per key).
// By doing it once at startup we save that cost on every incoming webhook.
const cryptoKeysPromise: Promise<CryptoKey[]> = Promise.all(
  SECRETS_RAW.map((secret) =>
    crypto.subtle.importKey(
      "raw",
      encoder.encode(secret),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign", "verify"],
    )
  ),
);

// Minified GraphQL query (allocated once, no whitespace overhead)
const INVENTORY_LEVEL_QUERY =
  'query($id:ID!){inventoryLevel(id:$id){id updatedAt location{id name}item{id}quantities(names:["available","on_hand","incoming","committed","reserved"]){name quantity}}}';

// ── Helpers ──

/** Decode base64 to Uint8Array (for the received HMAC header). */
function b64ToBytes(b64: string): Uint8Array {
  const bin = atob(b64);
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
    keys.map((key) => crypto.subtle.verify("HMAC", key, receivedBytes, rawBytes)),
  );

  const idx = results.indexOf(true);
  return idx === -1 ? null : idx;
}

async function fetchInventoryQuantitiesByLevelId(inventoryLevelGid: string) {
  const res = await fetch(GRAPHQL_URL, {
    method: "POST",
    headers: SHOPIFY_HEADERS,
    body: JSON.stringify({
      query: INVENTORY_LEVEL_QUERY,
      variables: { id: inventoryLevelGid },
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Shopify GraphQL HTTP ${res.status}: ${text.slice(0, 500)}`);
  }

  // res.json() is implemented natively — faster than res.text() + JSON.parse()
  const json = await res.json();
  if (json.errors?.length) {
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

  // Parallel HMAC verification across all secrets (native threads)
  const matchedIndex = await verifyHmac(rawBytes, received);

  if (matchedIndex === null) {
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
    const invLevel = await fetchInventoryQuantitiesByLevelId(inventoryLevelId);
    if (!invLevel) {
      return new Response("ok", { status: 200 });
    }

    const quantities = normalizeQuantities(invLevel);

    const { error } = await sb.from("inventory_snapshot_queue").insert({
      shop: shop ?? SHOPIFY_STORE_DOMAIN,
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
