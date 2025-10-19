import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import { createClient } from "@supabase/supabase-js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json());

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  BACKUP_SECRET,
  TZ = "Asia/Jerusalem"
} = process.env;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE");
  process.exit(1);
}
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

// --- Auth Header for secure routes ---
function auth(req, res, next) {
  const given = req.header("x-backup-secret");
  if (!given || given !== BACKUP_SECRET) return res.status(401).send("Unauthorized");
  next();
}

// --- Health check for Cloudflare ---
app.get("/api/healthz", (_req, res) => res.status(200).send("ok"));

// --- Manual trigger for DB backup ---
app.post("/api/backupDatabaseScheduler", auth, async (_req, res) => {
  res.json({ ok: true, ranAt: new Date().toISOString(), tz: TZ });
});

// --- Serve frontend from /build ---
const buildDir = path.join(__dirname, "build");
app.use(express.static(buildDir));

// For any non-API route, return index.html (SPA)
app.get(/^\/(?!api\/).*$/, (req, res) => {
  res.sendFile(path.join(buildDir, "index.html"));
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log("DR backup server listening on", port));
