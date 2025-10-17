// scripts/importFromS3.js
import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";
import { createClient } from "@supabase/supabase-js";
import zlib from "zlib";

const {
  AWS_REGION,
  AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,
  S3_BUCKET,
  S3_PREFIX,              // e.g. "backups/"
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  DEBUG_LIST              // optional: set to "1" to print listed keys
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

// ---- AWS S3 client (explicit credentials to avoid surprises) ----
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
  // no leading slash, ensure trailing slash only once
  p = p.replace(/^\/+/, "");
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
      Prefix: prefix,                // already normalized
      ContinuationToken: token
    }));

    const contents = out.Contents || [];
    all.push(...contents);

    token = out.IsTruncated ? out.NextContinuationToken : undefined;
  } while (token);

  return all;
}

async function latestKey() {
  const prefix = normalizePrefix(S3_PREFIX || "");   // e.g. "backups/"

  const objects = await listAllObjects(S3_BUCKET, prefix);
  const files = objects
    .filter(o => o && o.Key && isBackupKey(o.Key));

  if (DEBUG_LIST === "1") {
    console.log("[S3] Listed keys under prefix:", prefix, files.map(f => f.Key));
  }

  if (!files.length) {
    throw new Error("No backup files under prefix");
  }

  // Sort by LastModified (desc) and pick the newest
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

async function upsert(table, rows, conflict = "id") {
  if (!rows?.length) return;
  const size = 1000;
  for (let i = 0; i < rows.length; i += size) {
    const batch = rows.slice(i, i + size);
    const { error } = await supabase.from(table).upsert(batch, { onConflict: conflict });
    if (error) throw error;
  }
}

(async () => {
  const key = await latestKey();
  console.log("Importing:", key);

  const data = await readJson(key);
  // --- (A) Optional summary so we see what's inside the backup ---
if (process.env.DEBUG_SUMMARY === "1") {
  const entries = Object.entries(data).map(([k, v]) => {
    const t = Array.isArray(v) ? `array(${v.length})` : typeof v;
    return `${k}: ${t}`;
  });
  console.log("[SUMMARY] Top-level keys:", entries);
}

// --- (B) Store raw backup for traceability (if you created raw_backups) ---
try {
  await supabase.from("raw_backups").insert({ s3_key: key, payload: data });
} catch (e) {
  console.warn("raw_backups insert warning:", e.message || e);
}

// --- (C) Flexible extractors: try multiple common names/paths ---
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

// Try common Base44 shapes (adjust later if needed)
const rawCustomers = pickArray(data, ["customers","Customers","Customer","data.customers","data.Customer"]);
const rawDocuments = pickArray(data, ["documents","Documents","Document","data.documents","data.Document"]);
const rawOrders    = pickArray(data, ["orders","Orders","Order","data.orders","data.Order"]);

// --- (D) Map objects to your Supabase columns (tweak as needed) ---
function idLike(v) { return v?.id ?? v?._id ?? v?.uuid ?? v?.customer_id ?? v?.document_id ?? v?.order_id; }

function mapCustomer(c) {
  return {
    id:         idLike(c),
    full_name:  c.full_name ?? c.name ?? [c.first_name, c.last_name].filter(Boolean).join(" ") || "Unknown",
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

  const status = d.status ?? (d.missing === true ? "missing" : "present");

  return {
    id:          idLike(d),
    customer_id: customerRef || null,
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
    id:          idLike(o),
    customer_id: customerRef || null,
    total,
    currency,
    paid,
    created_at:  o.created_at ?? o.createdAt ?? null
  };
}

const customers = rawCustomers.map(mapCustomer).filter(r => r.id);
const documents = rawDocuments.map(mapDocument).filter(r => r.id);
const orders    = rawOrders.map(mapOrder).filter(r => r.id);

console.log(`[MAP] customers=${customers.length}, documents=${documents.length}, orders=${orders.length]`);

// Upsert in chunks
await upsert("customers", customers);
await upsert("documents", documents);
await upsert("orders", orders);

console.log("[UPSERT] done");


  // ⚠️ התאמה לשמות המפתחות בדיוק כפי שנמצאים בקבצי ה-JSON שלך ב-S3
  await upsert("customers",  data.customers);
  await upsert("documents",  data.documents);
  await upsert("orders",     data.orders);

  console.log("Import completed");
  process.exit(0);
})().catch(e => {
  console.error(e);
  process.exit(1);
});
