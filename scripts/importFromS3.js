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
