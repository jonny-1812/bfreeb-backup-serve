import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";
import { createClient } from "@supabase/supabase-js";
import zlib from "zlib";

const {
  AWS_REGION, S3_BUCKET, S3_PREFIX,
  SUPABASE_URL, SUPABASE_SERVICE_ROLE
} = process.env;

if (!AWS_REGION || !S3_BUCKET || !S3_PREFIX) {
  console.error("Missing AWS_REGION / S3_BUCKET / S3_PREFIX env vars");
  process.exit(1);
}
const s3 = new S3Client({ region: AWS_REGION });
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

async function latestKey() {
  const out = await s3.send(new ListObjectsV2Command({ Bucket: S3_BUCKET, Prefix: `${S3_PREFIX}/` }));
  const items = (out.Contents || []).filter(o =>
    o.Key.endsWith(".json") || o.Key.endsWith(".json.gz")
  );
  if (!items.length) throw new Error("No backup files under prefix");
  items.sort((a,b)=> b.LastModified - a.LastModified);
  return items[0].Key;
}
async function readJson(key) {
  const obj = await s3.send(new GetObjectCommand({ Bucket: S3_BUCKET, Key: key }));
  const isGz = key.endsWith(".gz");
  const chunks = [];
  const stream = isGz ? obj.Body.pipe(zlib.createGunzip()) : obj.Body;
  for await (const chunk of stream) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}
async function upsert(table, rows, conflict="id") {
  if (!rows?.length) return;
  const size = 1000;
  for (let i = 0; i < rows.length; i += size) {
    const batch = rows.slice(i, i+size);
    const { error } = await supabase.from(table).upsert(batch, { onConflict: conflict });
    if (error) throw error;
  }
}

(async () => {
  const key = await latestKey();
  console.log("Importing:", key);
  const data = await readJson(key);

  // התאם מפתחות לשמות בקבצי ה-JSON שלך בס3:
  await upsert("customers", data.customers);
  await upsert("documents", data.documents);
  await upsert("orders", data.orders);

  console.log("Import completed");
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
