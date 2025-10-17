import express from "express";
import { createClient } from "@supabase/supabase-js";

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

// Cron auth via header
function auth(req, res, next) {
  const given = req.header("x-backup-secret");
  if (!given || given !== BACKUP_SECRET) return res.status(401).send("Unauthorized");
  next();
}

// Health check (ל-Cloudflare Load Balancer)
app.get("/api/healthz", (_req, res) => res.status(200).send("ok"));

// טריגר ידני מרנדר/קרון
app.post("/api/backupDatabaseScheduler", auth, async (_req, res) => {
  res.json({ ok: true, ranAt: new Date().toISOString(), tz: TZ });
});

// אפשר להוסיף כאן בהמשך endpoints תואמים ל-Base44 לפי הצורך

const port = process.env.PORT || 3000;
app.listen(port, () => console.log("DR backup server listening on", port));
