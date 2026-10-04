import { serve } from "https://deno.land/std/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

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

function b64ToBytes(b64: string): Uint8Array {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

async function verifyHmac(rawBytes: Uint8Array, receivedB64: string): Promise<number | null> {
  const keys = await cryptoKeysPromise;
  if (keys.length === 0) return null;

  const receivedBytes = b64ToBytes(receivedB64);

  const results = await Promise.all(
    keys.map((key) => crypto.subtle.verify("HMAC", key, receivedBytes, rawBytes)),
  );

  const idx = results.indexOf(true);
  return idx === -1 ? null : idx;
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

  const matchedIndex = await verifyHmac(rawBytes, received);
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
      shop: shop ?? null,
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