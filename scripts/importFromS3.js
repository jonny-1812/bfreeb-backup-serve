// scripts/importFromS3.js
import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";
import { createClient } from "@supabase/supabase-js";
import zlib from "zlib";
import { createHash } from "crypto";

const {
  AWS_REGION,
  AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,
  S3_BUCKET,
  S3_PREFIX,              // e.g. "backups/"
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  DEBUG_LIST,             // optional: "1" to print listed keys
  DEBUG_SUMMARY           // optional: "1" to print summarized keys
} = process.env;

// ---- Sanity checks ----
if (!AWS_REGION || !S3_BUCKET) {
  console.error("Missing AWS_REGION or S3_BUCKET env vars");
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE");
  process.exit(1);
}

// ---- AWS S3 client ----
const s3 = new S3Client({
  region: AWS_REGION,
  credentials: AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY
    ? { accessKeyId: AWS_ACCESS_KEY_ID, secretAccessKey: AWS_SECRET_ACCESS_KEY }
    : undefined
});

// ---- Supabase client ----
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

// ---- Helpers ----
function normalizePrefix(p = "") {
  p = p.replace(/^\/+/, "");        // remove leading slashes
  if (p && !p.endsWith("/")) p += "/";
  return p;
}
function isBackupKey(key = "") {
  return key.endsWith(".json") || key.endsWith(".json.gz");
}
async function listAllObjects(bucket, prefix) {
  let all = [];
  let token;
  do {
    const out = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      ContinuationToken: token
    }));
    all.push(...(out.Contents || []));
    token = out.IsTruncated ? out.NextContinuationToken : undefined;
  } while (token);
  return all;
}
async function latestKey() {
  const prefix = normalizePrefix(S3_PREFIX || "");
  const objects = await listAllObjects(S3_BUCKET, prefix);
  const files = objects.filter(o => o?.Key && isBackupKey(o.Key));
  if (!files.length) throw new Error("No backup files under prefix");
  if (DEBUG_LIST === "1") console.log("[S3] Keys under prefix", prefix, files.map(f => f.Key));
  files.sort((a, b) => (b.LastModified?.getTime?.() || 0) - (a.LastModified?.getTime?.() || 0));
  return files[0].Key;
}
async function readJson(key) {
  const obj = await s3.send(new GetObjectCommand({ Bucket: S3_BUCKET, Key: key }));
  const isGz = key.endsWith(".gz");
  const chunks = [];
  const stream = isGz ? obj.Body.pipe(zlib.createGunzip()) : obj.Body;
  for await (const chunk of stream) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

// ---------- ID helpers: deterministic UUID v5 from any string ----------
const NAMESPACE = "bfreeb-backup-namespace"; // קבוע דטרמיניסטי. אפשר לשנות, רק להשאיר קבוע לאורך זמן.

function uuidFromString(input) {
  // v5(uuid) approximately: SHA-1(namespace + input) with version/variant bits set.
  // We make a 16-byte buffer from sha1 and format as UUID.
  const hash = createHash("sha1").update(NAMESPACE).update(String(input)).digest();
  // Use first 16 bytes
  const bytes = Buffer.from(hash.subarray(0, 16));
  // set version 5 (0101) in byte 6
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  // set RFC 4122 variant in byte 8
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  const hex = bytes.toString("hex");
  return [
    hex.substring(0, 8),
    hex.substring(8, 12),
    hex.substring(12, 16),
    hex.substring(16, 20),
    hex.substring(20)
  ].join("-");
}

function mapId(anyId) {
  if (!anyId) return null;
  const s = String(anyId);
  // If it's already a canonical UUID, use as-is
  if (/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/.test(s)) {
    return s.toLowerCase();
  }
  // Otherwise, derive deterministic UUID v5 from the string
  return uuidFromString(s);
}

// ---------- Small utils ----------
function pickArray(root, candidates) {
  for (const path of candidates) {
    const parts = path.split(".");
    let cur = root;
    for (const p of parts) {
      if (cur && Object.prototype.hasOwnProperty.call(cur, p)) cur = cur[p];
      else { cur = undefined; break; }
    }
    if (Array.isArray(cur)) return cur;
  }
  return [];
}
async function upsert(table, rows, conflict = "id") {
  if (!rows?.length) return { count: 0 };
  const size = 1000;
  let total = 0;
  for (let i = 0; i < rows.length; i += size) {
    const batch = rows.slice(i, i + size);
    const { error } = await supabase.from(table).upsert(batch, { onConflict: conflict });
    if (error) throw error;
    total += batch.length;
  }
  return { count: total };
}

// ---------- Mappers ----------
function mapCustomer(c) {
  const composed = [c.first_name, c.last_name].filter(Boolean).join(" ");
  const name = c.full_name ?? c.name ?? composed;           // only ?? chain
  const safeName = (name && name.trim()) ? name : "Unknown";

  return {
    id:         mapId(c.id ?? c._id ?? c.uuid ?? c.customer_id),
    full_name:  safeName,
    email:      c.email ?? c.mail ?? null,
    phone:      c.phone ?? c.mobile ?? c.tel ?? null,
    created_at: c.created_at ?? c.createdAt ?? null
  };
}
function mapDocument(d) {
  const customerRef =
    d.customer_id ?? d.client_id ?? d.customerId ?? d.clientId ??
    d.client?.id ?? d.customer?.id ?? null;

  const title =
    d.title ?? d.reference_number ?? d.reference ?? d.number ?? d.name ?? "Document";

  const url =
    d.public_url ?? d.share_url ??
    (d.share_token ? `https://bfree-b-52180fe7.base44.app/PublicDocumentView?token=${d.share_token}` : null);

  // 1) קח את הסטטוס המקורי אם יש
  const rawStatus =
    d.status ?? (d.missing === true ? "missing" : null);

  // 2) הגדר את הסטטוסים המותרים בפועל בטבלה שלך
  // אם אינך יודע עדיין, השאר ['present','missing'] — זה יעבור את האילוץ שראינו.
  const ALLOWED = new Set(["present", "missing"]);

  // 3) נרמל: אם לא מותר/ריק → 'present'
  const status = ALLOWED.has(String(rawStatus || "").toLowerCase())
    ? String(rawStatus).toLowerCase()
    : "present";

  return {
    id:          mapId(d.id ?? d._id ?? d.uuid ?? d.document_id),
    customer_id: mapId(customerRef),
    title,
    url,
    status,
    created_at:  d.created_at ?? d.createdAt ?? null
  };
}

function mapOrder(o) {
  const customerRef =
    o.customer_id ?? o.client_id ?? o.customerId ?? o.clientId ??
    o.client?.id ?? o.customer?.id ?? null;

  const total    = o.total ?? o.amount ?? o.sum ?? 0;
  const currency = o.currency ?? o.curr ?? "ILS";
  const paid     = o.paid ?? o.is_paid ?? (o.status === "paid");

  return {
    id:          mapId(o.id ?? o._id ?? o.uuid ?? o.order_id),
    customer_id: mapId(customerRef),
    total,
    currency,
    paid,
    created_at:  o.created_at ?? o.createdAt ?? null
  };
}

(async () => {
  const key = await latestKey();
  console.log("Importing:", key);

  const data = await readJson(key);

  // ---- HARD SUMMARY (always print so we see what's inside) ----
  try {
    const keys = Object.keys(data || {});
    console.log("[SUMMARY/HARD] Top-level keys:", keys);
    for (const k of keys) {
      const v = data[k];
      if (Array.isArray(v)) console.log(`[SUMMARY/HARD] ${k}: array(${v.length})`);
      else console.log(`[SUMMARY/HARD] ${k}: ${typeof v}`);
    }
  } catch (e) {
    console.log("[SUMMARY/HARD] failed to print keys:", e?.message || e);
  }
  if (DEBUG_SUMMARY === "1") {
    const entries = Object.entries(data).map(([k, v]) => {
      const t = Array.isArray(v) ? `array(${v.length})` : typeof v;
      return `${k}: ${t}`;
    });
    console.log("[SUMMARY] Top-level keys:", entries);
  }

  // Print what's inside data.entities
  if (data.entities && typeof data.entities === "object") {
    const ekeys = Object.keys(data.entities);
    console.log("[ENTITIES] keys:", ekeys);
    for (const k of ekeys) {
      const v = data.entities[k];
      if (Array.isArray(v)) console.log(`[ENTITIES] ${k}: array(${v.length})`);
      else console.log(`[ENTITIES] ${k}: ${typeof v}`);
    }
  } else {
    console.log("[ENTITIES] Missing or not an object");
  }

  // Save raw JSON for traceability (requires table raw_backups)
  try {
    await supabase.from("raw_backups").insert({ s3_key: key, payload: data });
  } catch (e) {
    console.warn("raw_backups insert warning:", e.message || e);
  }

  // ---------- Pick arrays (under entities.*) ----------
  const rawCustomers = pickArray(data, [
    "entities.customers","entities.Customers","entities.Customer",
    "customers","Customers","Customer",
    "data.customers","data.Customer"
  ]);

  const rawDocuments = pickArray(data, [
    "entities.documents","entities.Documents","entities.Document",
    "documents","Documents","Document",
    "data.documents","data.Document"
  ]);

  const rawOrders = pickArray(data, [
    "entities.orders","entities.Orders","entities.Order",
    "orders","Orders","Order",
    "data.orders","data.Order"
  ]);

  const customers = rawCustomers.map(mapCustomer).filter(r => r.id);
  const documents = rawDocuments.map(mapDocument).filter(r => r.id);
  const orders    = rawOrders.map(mapOrder).filter(r => r.id);

  console.log(`[MAP] customers=${customers.length}, documents=${documents.length}, orders=${orders.length}`);

  const r1 = await upsert("customers", customers);
  const r2 = await upsert("documents", documents);
  const r3 = await upsert("orders", orders);

  console.log(`[UPSERT] customers=${r1.count || 0}, documents=${r2.count || 0}, orders=${r3.count || 0}`);
  console.log("Import completed");
  process.exit(0);
})().catch(e => {
  console.error(e);
  process.exit(1);
});
