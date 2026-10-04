import { serve } from "https://deno.land/std/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

/**
 * Shopify draft_orders/delete webhook.
 *
 * Required Supabase secrets:
 * - SHOPIFY_APP_SECRETS="secret1,secret2,secret3"
 * - SB_URL="https://nybxcfjjnkgxzgaeitlk.supabase.co"
 * - SB_SERVICE_ROLE_KEY="sb_secret_...."
 *
 * Optional Supabase secrets:
 * - SHOPIFY_CLIENT_CREDENTIALS='{"adam-lippes-uk.myshopify.com":{"client_id":"...","client_secret":"..."}}'
 *   Shops listed here are verified against THEIR OWN client_secret only (matched_index = -1).
 *   Do NOT also list these client secrets in SHOPIFY_APP_SECRETS: the shop header is not
 *   covered by the HMAC, so a secret present there would let a webhook signed for that shop
 *   pass with another shop's header. If such an overlap is found at startup, the shared copy
 *   is ignored (fail closed; the other shared secrets keep their matched_index).
 */

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const SECRETS_RAW = (Deno.env.get("SHOPIFY_APP_SECRETS") ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const SB_URL = Deno.env.get("SB_URL")!;
const SB_SERVICE_ROLE_KEY = Deno.env.get("SB_SERVICE_ROLE_KEY")!;

const sb = createClient(SB_URL, SB_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// Every shop domain must match this before its secret is used.
const SHOP_DOMAIN_RE = /^[a-z0-9][a-z0-9-]*\.myshopify\.com$/;

// Parsed once at module load; absent or invalid => empty (logged once, without values).
const CLIENT_CREDENTIALS = new Map<string, { client_secret: string }>();
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
        CLIENT_CREDENTIALS.set(domain, { client_secret: c.client_secret });
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

async function verifyHmac(rawBytes: Uint8Array, receivedB64: string): Promise<number | null> {
  const keys = await cryptoKeysPromise;
  if (keys.length === 0) return null;

  const receivedBytes = b64ToBytes(receivedB64);

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

serve(async (req) => {
  if (req.method !== "POST") return new Response("Method Not Allowed", { status: 405 });

  const headers = req.headers;
  const ua = headers.get("user-agent");
  const received = (headers.get("X-Shopify-Hmac-Sha256") ?? "").trim();

  // ✅ IMPORTANT: accept probes/unsigned checks
  if (!received) {
    console.log("Probe/unsigned request -> 200", {
      ua,
      len: headers.get("content-length"),
    });
    return new Response("ok", { status: 200 });
  }

  if (SECRETS_RAW.length === 0) {
    console.log("Missing SHOPIFY_APP_SECRETS");
    return new Response("Server misconfigured", { status: 500 });
  }

  const topic = headers.get("X-Shopify-Topic");
  const shop = headers.get("X-Shopify-Shop-Domain");
  const webhookId = headers.get("X-Shopify-Webhook-Id");
  const apiVersion = headers.get("X-Shopify-API-Version");

  const rawBytes = new Uint8Array(await req.arrayBuffer());

  // Client-credentials shops are bound to their own secret (header is not signed);
  // matched_index = -1 for them. Other shops keep the SHOPIFY_APP_SECRETS verification.
  const shopKey = (shop ?? "").trim().toLowerCase();
  const matchedIndex = CLIENT_CREDENTIALS.has(shopKey)
    ? ((await verifyHmacForShop(shopKey, rawBytes, received)) ? -1 : null)
    : await verifyHmac(rawBytes, received);
  if (matchedIndex === null) {
    console.log("❌ Invalid HMAC", { webhookId, topic, shop });
    return new Response("Invalid HMAC", { status: 401 });
  }

  let payload: any;
  try {
    payload = JSON.parse(decoder.decode(rawBytes));
  } catch {
    // still ACK
    return new Response("ok", { status: 200 });
  }

  // Insert into queue table (idempotent by unique webhook_id index)
  try {
    const row = {
      shop: matchedIndex === -1 ? shopKey : (shop ?? null),
      webhook_id: webhookId ?? null,
      topic: topic ?? null,
      api_version: apiVersion ?? null,
      matched_index: matchedIndex,
      draft_order_id: payload?.id ?? null,
      raw_payload: payload,
      status: "pending",
    };

    const { error } = await sb.from("draft_orders_delete_queue").insert(row);

    if (error) {
      const msg = (error.message || "").toLowerCase();
      if (!msg.includes("duplicate") && !msg.includes("unique")) {
        console.log("❌ Queue insert failed", { error: error.message, webhookId });
      }
    }
  } catch (e) {
    console.log("❌ Queue insert exception", { error: String(e), webhookId });
  }

  console.log("✅ draft_orders/delete received", { webhookId, topic, shop, matchedIndex });
  return new Response("ok", { status: 200 });
});