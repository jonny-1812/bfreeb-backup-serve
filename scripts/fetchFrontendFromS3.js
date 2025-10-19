הנה הגרסה המתוקנת של `scripts/fetchFrontendFromS3.js` עם הגדרת endpoinט מפורש לאזור ה־S3 (מונע שגיאת `PermanentRedirect`) ותמיכה ב־`AWS_SESSION_TOKEN` אם יש לך אישורים זמניים. שמתי גם לוג קצר שתראה את ה־region/bucket בזמן ריצה.

```js
// scripts/fetchFrontendFromS3.js
import fs from "fs";
import path from "path";
import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";
import unzipper from "unzipper";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const {
  AWS_REGION,
  AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,
  AWS_SESSION_TOKEN,        // אופציונלי – אם יש אישורים זמניים
  FRONTEND_S3_BUCKET,
  FRONTEND_S3_PREFIX
} = process.env;

function normalizePrefix(p = "") {
  p = p.replace(/^\/+/, "");
  if (p && !p.endsWith("/")) p += "/";
  return p;
}

function ensureEmptyDir(dir) {
  if (fs.existsSync(dir)) fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
}

async function listAllObjects(s3, bucket, prefix) {
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

export default async function fetchFrontendFromS3() {
  if (!AWS_REGION || !FRONTEND_S3_BUCKET) {
    throw new Error("Missing AWS_REGION or FRONTEND_S3_BUCKET");
  }

  // לוג דיאגנוסטי
  console.log("[fetchFrontendFromS3] REGION=", AWS_REGION, "BUCKET=", FRONTEND_S3_BUCKET);

  // קונפיגורציה מפורשת לאזור (מונע PermanentRedirect)
  const s3 = new S3Client({
    region: AWS_REGION,                                  // לדוגמה: "eu-north-1"
    endpoint: `https://s3.${AWS_REGION}.amazonaws.com`,  // מצביע ישירות לאנדפוינט של האזור
    forcePathStyle: false,                                // bucket רגיל (לא דורש path-style)
    credentials:
      (AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY)
        ? {
            accessKeyId: AWS_ACCESS_KEY_ID,
            secretAccessKey: AWS_SECRET_ACCESS_KEY,
            sessionToken: AWS_SESSION_TOKEN || undefined,
          }
        : undefined,
  });

  const prefix = normalizePrefix(FRONTEND_S3_PREFIX || "");
  console.log(`[fetchFrontendFromS3] Searching s3://${FRONTEND_S3_BUCKET}/${prefix} for latest .zip`);

  const objects = await listAllObjects(s3, FRONTEND_S3_BUCKET, prefix);
  const zips = objects.filter(o => o.Key && o.Key.endsWith(".zip"));
  if (!zips.length) {
    console.warn("[fetchFrontendFromS3] No .zip builds found — skipping.");
    return { ok: false, reason: "no-zip-found" };
  }

  // הכי חדש לפי תאריך שינוי
  zips.sort((a, b) => (b.LastModified?.getTime?.() || 0) - (a.LastModified?.getTime?.() || 0));
  const latest = zips[0].Key;
  console.log(`[fetchFrontendFromS3] Latest: ${latest}`);

  const buildDir = path.join(process.cwd(), "build");
  ensureEmptyDir(buildDir);

  const obj = await s3.send(new GetObjectCommand({ Bucket: FRONTEND_S3_BUCKET, Key: latest }));
  console.log("[fetchFrontendFromS3] Downloaded zip; extracting to build/...");

  await new Promise((resolve, reject) => {
    const extract = unzipper.Extract({ path: buildDir });
    obj.Body.pipe(extract);
    extract.on("close", resolve);
    extract.on("error", reject);
  });

  // אם יש תיקיית-בן אחת (למשל build/build/), נרים את התכולה לשורש
  const entries = fs.readdirSync(buildDir);
  if (entries.length === 1) {
    const only = path.join(buildDir, entries[0]);
    if (fs.lstatSync(only).isDirectory()) {
      for (const f of fs.readdirSync(only)) {
        fs.renameSync(path.join(only, f), path.join(buildDir, f));
      }
      fs.rmSync(only, { recursive: true, force: true });
    }
  }

  const indexPath = path.join(buildDir, "index.html");
  const ok = fs.existsSync(indexPath);
  console.log(ok ? "[fetchFrontendFromS3] build/index.html ready ✅" : "[fetchFrontendFromS3] index.html NOT found ❌");
  return { ok, latestKey: latest };
}

// הרצה ישירה (prestart)
if (import.meta.url === `file://${__filename}`) {
  fetchFrontendFromS3().then(
    () => process.exit(0),
    (e) => { console.error(e); process.exit(1); }
  );
}
```
