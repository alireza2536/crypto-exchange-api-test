import fs from "node:fs";
import pg from "pg";

const { Pool } = pg;
const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const preserveIds = args.includes("--preserve-ids") || process.env.PRESERVE_ARCHIVE_IDS === "true";
const backupPath = args.find((arg) => !arg.startsWith("--"));

if (!backupPath) {
  console.error("Usage: node scripts/restore-open-interest-backup.js <backup.json> [--dry-run] [--preserve-ids]");
  process.exit(2);
}
if (!dryRun && !process.env.DATABASE_URL) {
  console.error("DATABASE_URL is required unless --dry-run is used. Do not put database credentials in the backup file or commit them to Git.");
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

const acceptedBackupTypes = new Set(["full_open_interest_history", "open_interest_archive_chunk"]);
if (parsed?.backupType && !acceptedBackupTypes.has(parsed.backupType)) {
  console.error("Unexpected backupType:", parsed.backupType);
  process.exit(2);
}
const isArchiveChunk = parsed?.backupType === "open_interest_archive_chunk";

if (parsed?.recordCount !== undefined && Number(parsed.recordCount) !== records.length) {
  throw new Error(`Backup recordCount mismatch: metadata says ${parsed.recordCount}, file contains ${records.length}`);
}

if (isArchiveChunk) {
  const firstId = Number(parsed.firstId);
  const lastId = Number(parsed.lastId);
  const ids = records.map((row) => Number(row?.id));
  if (!Number.isSafeInteger(firstId) || !Number.isSafeInteger(lastId) ||
      firstId < 1 || lastId < firstId || records.length !== lastId - firstId + 1) {
    throw new Error("Archive chunk has an invalid ID range or record count.");
  }
  if (ids.some((id, index) => !Number.isSafeInteger(id) || id !== firstId + index)) {
    throw new Error("Archive chunk IDs are missing, duplicated, out of order, or non-contiguous.");
  }
}
if (preserveIds && !isArchiveChunk) {
  console.error("--preserve-ids can only be used with open_interest_archive_chunk files.");
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
    id: row.id == null ? null : Number(row.id),
    exchange,
    symbol,
    metric,
    value,
    collectedAt: collectedAt.toISOString(),
    sourceEndpoint: row.source_endpoint ?? row.sourceEndpoint ?? null,
  };
});

const exchangeCounts = Object.fromEntries(
  [...new Set(cleaned.map((row) => row.exchange))].sort().map((exchange) => [
    exchange,
    cleaned.filter((row) => row.exchange === exchange).length,
  ]),
);
const timestamps = cleaned.map((row) => Date.parse(row.collectedAt)).sort((a, b) => a - b);
const validationSummary = {
  ok: true,
  dryRun,
  backupPath,
  backupType: parsed?.backupType ?? "unspecified",
  backupRecords: cleaned.length,
  firstId: isArchiveChunk ? Number(parsed.firstId) : null,
  lastId: isArchiveChunk ? Number(parsed.lastId) : null,
  uniqueSymbols: new Set(cleaned.map((row) => row.exchange + "|" + row.symbol)).size,
  exchangeCounts,
  firstCollectedAt: timestamps.length ? new Date(timestamps[0]).toISOString() : null,
  lastCollectedAt: timestamps.length ? new Date(timestamps[timestamps.length - 1]).toISOString() : null,
  preservesIds: preserveIds,
};
if (dryRun) {
  console.log(JSON.stringify(validationSummary, null, 2));
  process.exit(0);
}

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
      if (preserveIds) {
        const p = i * 7;
        values.push(row.id, row.exchange, row.symbol, row.metric, row.value, row.collectedAt, row.sourceEndpoint);
        return `(${p + 1}, ${p + 2}, ${p + 3}, ${p + 4}, ${p + 5}, ${p + 6}, ${p + 7})`;
      }
      const p = i * 6;
      values.push(row.exchange, row.symbol, row.metric, row.value, row.collectedAt, row.sourceEndpoint);
      return `(${p + 1}, ${p + 2}, ${p + 3}, ${p + 4}, ${p + 5}, ${p + 6})`;
    });
    const sql = preserveIds
      ? `INSERT INTO oi_snapshots (id, exchange, symbol, metric, value, collected_at, source_endpoint)
         VALUES ${tuples.join(",")}
         ON CONFLICT (exchange, symbol, metric, collected_at)
         DO UPDATE SET value = EXCLUDED.value, source_endpoint = EXCLUDED.source_endpoint`
      : `INSERT INTO oi_snapshots (exchange, symbol, metric, value, collected_at, source_endpoint)
         VALUES ${tuples.join(",")}
         ON CONFLICT (exchange, symbol, metric, collected_at)
         DO UPDATE SET value = EXCLUDED.value, source_endpoint = EXCLUDED.source_endpoint`;
    const result = await pool.query(sql, values);
    insertedOrUpdated += result.rowCount;
  }
  if (preserveIds) {
    await pool.query("SELECT setval(pg_get_serial_sequence('oi_snapshots', 'id'), GREATEST(COALESCE((SELECT MAX(id) FROM oi_snapshots), 1), 1), true)");
  }
  await pool.query("COMMIT");

  const verification = await pool.query(
    "SELECT exchange, COUNT(*)::int AS snapshots, MAX(collected_at) AS last_collected_at FROM oi_snapshots GROUP BY exchange ORDER BY exchange",
  );
  console.log(JSON.stringify({
    ...validationSummary,
    dryRun: false,
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
