import fs from "node:fs";
import pg from "pg";

const { Pool } = pg;
const backupPath = process.argv[2];

if (!backupPath) {
  console.error("Usage: DATABASE_URL='postgresql://…' node scripts/restore-open-interest-backup.js <backup.json>");
  process.exit(2);
}
if (!process.env.DATABASE_URL) {
  console.error("DATABASE_URL is required. Do not put database credentials in the backup file or commit them to Git.");
  process.exit(2);
}

function parseRecords(parsed) {
  const candidates = [
    parsed?.records,
    parsed?.data?.records,
    parsed?.payload?.records,
    parsed?.backup?.records,
  ];
  return candidates.find(Array.isArray) || null;
}

const parsed = JSON.parse(fs.readFileSync(backupPath, "utf8"));
const records = parseRecords(parsed);
if (!records) {
  console.error("Backup format not recognized: expected a records array.");
  process.exit(2);
}
if (parsed?.backupType && parsed.backupType !== "full_open_interest_history") {
  console.error("Unexpected backupType:", parsed.backupType);
  process.exit(2);
}

const cleaned = records.map((row, index) => {
  const exchange = String(row.exchange || "").trim();
  const symbol = String(row.symbol || "").trim();
  const metric = String(row.metric || "open_interest_native").trim();
  const value = String(row.value ?? "").trim();
  const collectedAt = new Date(row.collected_at ?? row.collectedAt);
  if (!exchange || !symbol || !metric || !value || !Number.isFinite(Number(value)) || Number(value) <= 0 || !Number.isFinite(collectedAt.getTime())) {
    throw new Error(`Invalid backup row at index ${index}`);
  }
  return {
    exchange,
    symbol,
    metric,
    value,
    collectedAt: collectedAt.toISOString(),
    sourceEndpoint: row.source_endpoint ?? row.sourceEndpoint ?? null,
  };
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSLMODE === "require" ? { rejectUnauthorized: false } : undefined,
  max: 2,
  connectionTimeoutMillis: 10000,
});

try {
  await pool.query("SELECT 1");
  await pool.query(`
    CREATE TABLE IF NOT EXISTS oi_snapshots (
      id BIGSERIAL PRIMARY KEY,
      exchange TEXT NOT NULL,
      symbol TEXT NOT NULL,
      metric TEXT NOT NULL DEFAULT 'open_interest',
      value NUMERIC NOT NULL CHECK (value > 0),
      collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      source_endpoint TEXT,
      UNIQUE (exchange, symbol, metric, collected_at)
    )
  `);
  await pool.query("CREATE INDEX IF NOT EXISTS oi_snapshots_exchange_time_idx ON oi_snapshots (exchange, collected_at DESC)");

  await pool.query("BEGIN");
  let insertedOrUpdated = 0;
  const batchSize = 250;
  for (let offset = 0; offset < cleaned.length; offset += batchSize) {
    const batch = cleaned.slice(offset, offset + batchSize);
    const values = [];
    const tuples = batch.map((row, i) => {
      const p = i * 6;
      values.push(row.exchange, row.symbol, row.metric, row.value, row.collectedAt, row.sourceEndpoint);
      return `(${p + 1}, ${p + 2}, ${p + 3}, ${p + 4}, ${p + 5}, ${p + 6})`;
    });
    const result = await pool.query(
      `INSERT INTO oi_snapshots (exchange, symbol, metric, value, collected_at, source_endpoint)
       VALUES ${tuples.join(",")}
       ON CONFLICT (exchange, symbol, metric, collected_at)
       DO UPDATE SET value = EXCLUDED.value, source_endpoint = EXCLUDED.source_endpoint`,
      values,
    );
    insertedOrUpdated += result.rowCount;
  }
  await pool.query("COMMIT");

  const verification = await pool.query(
    "SELECT exchange, COUNT(*)::int AS snapshots, MAX(collected_at) AS last_collected_at FROM oi_snapshots GROUP BY exchange ORDER BY exchange",
  );
  console.log(JSON.stringify({
    ok: true,
    backupPath,
    backupRecords: cleaned.length,
    insertedOrUpdated,
    coverage: verification.rows,
  }, null, 2));
} catch (error) {
  try { await pool.query("ROLLBACK"); } catch {}
  console.error("Restore failed:", error.message || error);
  process.exitCode = 1;
} finally {
  await pool.end();
}
