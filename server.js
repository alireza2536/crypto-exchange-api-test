import http from "node:http";
import pg from "pg";
const { Pool } = pg;

const PORT = Number(process.env.PORT || 3000);
const TIMEOUT_MS = 15000;

const OI_SNAPSHOT_INTERVAL_MS = 5 * 60 * 1000;
const GOOGLE_DRIVE_BACKUP_INTERVAL_MS = 6 * 60 * 60 * 1000;
let oiDriveBackupLastAt = null;
let oiDriveBackupLastResult = null;
const oiSnapshotPool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.PGSSLMODE === "require" ? { rejectUnauthorized: false } : undefined,
      connectionTimeoutMillis: 5000,
      idleTimeoutMillis: 30000,
      max: 3,
    })
  : null;
let oiSnapshotDbReady = false;
let oiSnapshotDbError = null;
let oiSnapshotCollectorRunning = false;
let oiSnapshotLastRunAt = null;
let oiSnapshotLastSaved = 0;
let oiSnapshotLastFailures = [];

async function initOiSnapshotDb() {
  if (!oiSnapshotPool) return false;
  try {
    await oiSnapshotPool.query(`
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
    await oiSnapshotPool.query("CREATE INDEX IF NOT EXISTS oi_snapshots_exchange_time_idx ON oi_snapshots (exchange, collected_at DESC)");
    oiSnapshotDbReady = true;
    oiSnapshotDbError = null;
    return true;
  } catch (error) {
    oiSnapshotDbReady = false;
    oiSnapshotDbError = String(error.message || error);
    return false;
  }
}

function extractOpenInterestValue(exchange, json) {
  let value = null;
  if (exchange === "MEXC") {
    const row = Array.isArray(json?.data) ? json.data.find(x => x?.symbol === "BTC_USDT") : json?.data;
    value = row?.holdVol;
  } else if (exchange === "BingX") value = json?.data?.openInterest;
  else if (exchange === "OKX") value = json?.data?.find(x => x?.instId === "BTC-USDT-SWAP")?.oi ?? json?.data?.[0]?.oi;
  else if (exchange === "WEEX") value = json?.openInterest;
  else if (exchange === "HTX") {
    const rows = Array.isArray(json?.data) ? json.data : json?.data ? [json.data] : [];
    const row = rows.find(x => Number(x?.volume ?? x?.amount ?? x?.open_interest) > 0);
    value = row?.volume ?? row?.amount ?? row?.open_interest;
  } else if (exchange === "KuCoin") value = Array.isArray(json?.data) ? json.data[0]?.openInterest : json?.data?.openInterest;
  else if (exchange === "XT") value = json?.result?.openInterest ?? json?.result?.openInterestUsd;
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

async function collectOpenInterestSnapshots() {
  if (!oiSnapshotPool || !oiSnapshotDbReady || oiSnapshotCollectorRunning) return;
  oiSnapshotCollectorRunning = true;
  oiSnapshotLastRunAt = new Date().toISOString();
  oiSnapshotLastSaved = 0;
  oiSnapshotLastFailures = [];
  try {
    const probes = dataProbes.filter(p => p.kind === "open_interest" || p.kind === "open_interest_15m");
    const outcomes = await Promise.all(probes.map(async probe => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
      try {
        const response = await fetch(probe.url, { headers: { accept: "application/json", "user-agent": "exchange-api-connectivity-test/1.0" }, signal: controller.signal });
        const raw = await response.text();
        let json;
        try { json = JSON.parse(raw); } catch { json = null; }
        if (!response.ok || !json || !probe.check(json)) throw new Error(!response.ok ? `HTTP ${response.status}` : "API schema/value check failed");
        const value = extractOpenInterestValue(probe.exchange, json);
        if (value === null) throw new Error("Open Interest value could not be extracted");
        const symbol = probe.exchange === "MEXC" ? "BTC_USDT" : probe.exchange === "OKX" ? "BTC-USDT-SWAP" : probe.exchange === "HTX" ? "BTC-USDT" : probe.exchange === "KuCoin" ? "XBTUSDTM" : probe.exchange === "XT" ? "btc_usdt" : "BTCUSDT";
        await oiSnapshotPool.query(
          "INSERT INTO oi_snapshots (exchange, symbol, metric, value, source_endpoint) VALUES ($1,$2,$3,$4,$5)",
          [probe.exchange, symbol, "open_interest_native", value, probe.url]
        );
        return { exchange: probe.exchange, status: "SAVED" };
      } catch (error) {
        return { exchange: probe.exchange, status: "FAIL", error: String(error.message || error) };
      } finally { clearTimeout(timer); }
    }));
    oiSnapshotLastSaved = outcomes.filter(x => x.status === "SAVED").length;
    oiSnapshotLastFailures = outcomes.filter(x => x.status === "FAIL");
    console.log(JSON.stringify({ event: "open_interest_snapshot_collection", finishedAt: new Date().toISOString(), saved: oiSnapshotLastSaved, total: outcomes.length, failures: oiSnapshotLastFailures }));
  } finally {
    oiSnapshotCollectorRunning = false;
  }
}

async function backupOpenInterestToGoogleDrive() {
  const endpoint = process.env.GOOGLE_DRIVE_BACKUP_URL;
  const secret = process.env.GOOGLE_DRIVE_BACKUP_SECRET;
  if (!endpoint || !secret || !oiSnapshotPool || !oiSnapshotDbReady) {
    oiDriveBackupLastResult = { ok: false, error: "Google Drive backup or database is not configured" };
    return oiDriveBackupLastResult;
  }
  try {
    const result = await oiSnapshotPool.query(
      "SELECT exchange, symbol, metric, value::text AS value, collected_at, source_endpoint FROM oi_snapshots ORDER BY collected_at ASC"
    );
    const payload = {
      secret,
      backupType: "full_open_interest_history",
      sourceService: "exchange-api-connectivity-test",
      generatedAt: new Date().toISOString(),
      recordCount: result.rowCount,
      records: result.rows
    };
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json", "accept": "application/json" },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(60000)
    });
    const raw = await response.text();
    let body;
    try { body = JSON.parse(raw); } catch { body = { raw: raw.slice(0, 500) }; }
    if (!response.ok || body?.ok !== true) {
      throw new Error("Google Drive backup failed: HTTP " + response.status + " " + JSON.stringify(body).slice(0, 500));
    }
    oiDriveBackupLastAt = new Date().toISOString();
    oiDriveBackupLastResult = { ok: true, backedUpRecords: result.rowCount, file: body.file || null, completedAt: oiDriveBackupLastAt };
    console.log(JSON.stringify({ event: "google_drive_open_interest_backup", ...oiDriveBackupLastResult }));
    return oiDriveBackupLastResult;
  } catch (error) {
    oiDriveBackupLastResult = { ok: false, error: String(error.message || error), attemptedAt: new Date().toISOString() };
    console.log(JSON.stringify({ event: "google_drive_open_interest_backup", ...oiDriveBackupLastResult }));
    return oiDriveBackupLastResult;
  }
}

async function getOiSnapshotStatus() {
  const status = {
    configured: Boolean(oiSnapshotPool),
    databaseReady: oiSnapshotDbReady,
    collectorIntervalMinutes: OI_SNAPSHOT_INTERVAL_MS / 60000,
    lastRunAt: oiSnapshotLastRunAt,
    lastSaved: oiSnapshotLastSaved,
    lastFailures: oiSnapshotLastFailures,
    note: oiSnapshotPool ? (oiSnapshotDbReady ? "Open Interest snapshots are being stored; values use each exchange's native units and should not be compared directly across exchanges." : "DATABASE_URL is set but database initialization has not succeeded.") : "Persistent history is not enabled. Configure DATABASE_URL with a PostgreSQL database to enable periodic snapshots.",
  };
  if (oiSnapshotPool && oiSnapshotDbReady) {
    try {
      const result = await oiSnapshotPool.query("SELECT exchange, COUNT(*)::int AS snapshots, MAX(collected_at) AS last_collected_at FROM oi_snapshots GROUP BY exchange ORDER BY exchange");
      status.coverage = result.rows;
    } catch (error) { status.queryError = String(error.message || error); }
  }
  return status;
}

const probes = [
  {
    exchange: "MEXC",
    url: "https://api.mexc.com/api/v1/contract/ticker",
    benchmark: "BTC_USDT",
    check: (j) => j && (j.success === true || j.code === 0) && (Array.isArray(j.data) ? j.data.some((x) => x?.symbol === "BTC_USDT" && Number(x.lastPrice) > 0) : (j.data && j.data.symbol === "BTC_USDT" && Number(j.data.lastPrice) > 0)),
  },
  {
    exchange: "BingX",
    url: "https://open-api.bingx.com/openApi/swap/v2/quote/ticker",
    benchmark: "BTC-USDT",
    check: (j) => j && (j.code === 0 || j.code === "0") && Array.isArray(j.data) && j.data.some((x) => x?.symbol === "BTC-USDT" && Number(x.lastPrice) > 0),
  },
  {
    exchange: "OKX",
    url: "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
    benchmark: "BTC-USDT-SWAP",
    check: (j) => j && String(j.code) === "0" && Array.isArray(j.data) && j.data.some((x) => x?.instId === "BTC-USDT-SWAP" && Number(x.last) > 0),
  },
  {
    exchange: "WEEX",
    url: "https://api-contract.weex.com/capi/v3/market/ticker/24hr?symbol=BTCUSDT",
    check: (j) => Array.isArray(j) && j.some((x) => x && x.symbol === "BTCUSDT" && (typeof x.lastPrice === "string" || typeof x.lastPrice === "number")),
  },
  {
    exchange: "HTX",
    url: "https://api.hbdm.com/linear-swap-ex/market/detail/merged?contract_code=BTC-USDT",
    check: (j) => j && j.status === "ok" && j.tick && (typeof j.tick.close === "number" || typeof j.tick.close === "string"),
  },
  {
    exchange: "KuCoin",
    url: "https://api-futures.kucoin.com/api/v1/allTickers",
    benchmark: "XBTUSDTM/BTCUSDTM",
    check: (j) => j && String(j.code) === "200000" && Array.isArray(j.data) && j.data.some((x) => ["XBTUSDTM", "BTCUSDTM"].includes(x?.symbol) && Number(x.price) > 0),
  },
  {
    exchange: "XT",
    url: "https://fapi.xt.com/future/market/v1/public/q/tickers",
    benchmark: "btc_usdt",
    check: (j) => j && j.returnCode === 0 && Array.isArray(j.result) && j.result.some((x) => x?.s === "btc_usdt" && Number(x.c) > 0),
  },
];


const dataProbes = [
  { exchange:"MEXC", kind:"candles_15m", url:"https://api.mexc.com/api/v1/contract/kline/BTC_USDT?interval=Min15", check:j=>j?.success===true && j?.code===0 && (Array.isArray(j?.data?.time) ? j.data.time.length>0 && Array.isArray(j.data.close) && Number(j.data.close[j.data.close.length-1])>0 : Array.isArray(j?.data) && j.data.length>0) },
  { exchange:"MEXC", kind:"open_interest", url:"https://api.mexc.com/api/v1/contract/ticker?symbol=BTC_USDT", check:j=>j?.success===true && j?.code===0 && (Array.isArray(j?.data)?j.data.some(x=>x?.symbol==="BTC_USDT"&&Number(x?.holdVol)>0):j?.data?.symbol==="BTC_USDT"&&Number(j.data.holdVol)>0) },
  { exchange:"BingX", kind:"candles_15m", url:"https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol=BTC-USDT&interval=15m&limit=3", check:j=>(j?.code===0||j?.code==="0") && Array.isArray(j?.data) && j.data.length>0 && Number(j.data[0]?.close)>0 },
  { exchange:"BingX", kind:"open_interest", url:"https://open-api.bingx.com/openApi/swap/v2/quote/openInterest?symbol=BTC-USDT", check:j=>(j?.code===0||j?.code==="0") && j?.data && Number(j.data.openInterest)>0 },
  { exchange:"OKX", kind:"candles_15m", url:"https://www.okx.com/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=15m&limit=3", check:j=>String(j?.code)==="0" && Array.isArray(j?.data) && j.data.length>0 && Number(j.data[0]?.[4])>0 },
  { exchange:"OKX", kind:"open_interest", url:"https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=BTC-USDT-SWAP", check:j=>String(j?.code)==="0" && Array.isArray(j?.data) && j.data.length>0 && Number(j.data[0]?.oi)>0 },
  { exchange:"WEEX", kind:"candles_15m", url:"https://api-contract.weex.com/capi/v3/market/klines?symbol=BTCUSDT&interval=15m&limit=3", check:j=>Array.isArray(j) && j.length>0 && Number(j[0]?.[4])>0 },
  { exchange:"WEEX", kind:"open_interest", url:"https://api-contract.weex.com/capi/v3/market/openInterest?symbol=BTCUSDT", check:j=>j?.symbol==="BTCUSDT" && Number(j.openInterest)>0 },
  { exchange:"HTX", kind:"candles_15m", url:"https://api.hbdm.com/linear-swap-ex/market/history/kline?contract_code=BTC-USDT&period=15min&size=3", check:j=>j?.status==="ok" && Array.isArray(j.data) && j.data.length>0 && Number(j.data[0]?.close)>0 },
  { exchange:"HTX", kind:"open_interest", url:"https://api.hbdm.com/linear-swap-api/v1/swap_open_interest?contract_code=BTC-USDT", check:j=>j?.status==="ok" && j?.data && (Array.isArray(j.data) ? j.data.some(x=>Number(x?.volume??x?.amount??x?.open_interest)>0) : Number(j.data.volume??j.data.amount??j.data.open_interest)>0) },
  { exchange:"KuCoin", kind:"candles_15m", url:"https://api.kucoin.com/api/ua/v2/market/kline?symbol=XBTUSDTM&tradeType=FUTURES&klineType=TRADE&interval=15min", check:j=>String(j?.code)==="200000" && Array.isArray(j?.data?.list) && j.data.list.length>0 && Number(j.data.list[0]?.[4])>0 },
  { exchange:"KuCoin", kind:"open_interest", url:"https://api.kucoin.com/api/ua/v1/market/open-interest?symbol=XBTUSDTM", check:j=>String(j?.code)==="200000" && (Array.isArray(j?.data) ? j.data.some(x=>Number(x?.openInterest)>0) : Number(j?.data?.openInterest)>0) },
  { exchange:"XT", kind:"candles_15m", url:"https://fapi.xt.com/future/market/v1/public/q/kline?symbol=btc_usdt&interval=15m&limit=3", check:j=>j?.returnCode===0 && (Array.isArray(j?.result) ? j.result.length>0 : Array.isArray(j?.result?.items) && j.result.items.length>0) },
  { exchange:"XT", kind:"open_interest", url:"https://fapi.xt.com/future/market/v1/public/contract/open-interest?symbol=btc_usdt", check:j=>j?.returnCode===0 && j?.result && Number(j.result.openInterest??j.result.openInterestUsd)>0 },
];

const timeframeMinutes = {"15m":15,"1h":60,"2h":120,"4h":240,"1d":1440};
function candleTimestamp(exchange,j) {
  let values=[];
  if(exchange==="MEXC") values=j?.data?.time||[];
  else if(exchange==="BingX") values=(j?.data||[]).map(x=>x?.time);
  else if(exchange==="OKX") values=(j?.data||[]).map(x=>x?.[0]);
  else if(exchange==="WEEX") values=(Array.isArray(j)?j:[]).map(x=>x?.[0]);
  else if(exchange==="HTX") values=(j?.data||[]).map(x=>x?.id);
  else if(exchange==="KuCoin") values=(j?.data?.list||[]).map(x=>x?.[0]);
  else if(exchange==="XT") values=(j?.result||[]).map(x=>x?.t);
  const nums=values.map(Number).filter(Number.isFinite);
  if(!nums.length) return null;
  const latest=Math.max(...nums);
  return latest < 100000000000 ? latest*1000 : latest;
}
function candleShapeOk(exchange,j) {
  if(exchange==="MEXC") return j?.success===true && j?.code===0 && Array.isArray(j?.data?.time) && j.data.time.length>0 && Array.isArray(j.data.close) && j.data.close.length===j.data.time.length && Number(j.data.close[j.data.close.length-1])>0;
  if(exchange==="BingX") return (j?.code===0||j?.code==="0") && Array.isArray(j?.data) && j.data.length>0 && Number(j.data[0]?.close)>0;
  if(exchange==="OKX") return String(j?.code)==="0" && Array.isArray(j?.data) && j.data.length>0 && Number(j.data[0]?.[4])>0;
  if(exchange==="WEEX") return Array.isArray(j) && j.length>0 && Number(j[0]?.[4])>0;
  if(exchange==="HTX") return j?.status==="ok" && Array.isArray(j?.data) && j.data.length>0 && Number(j.data[0]?.close)>0;
  if(exchange==="KuCoin") return String(j?.code)==="200000" && Array.isArray(j?.data?.list) && j.data.list.length>0 && Number(j.data.list[0]?.[4])>0;
  if(exchange==="XT") return j?.returnCode===0 && Array.isArray(j?.result) && j.result.length>0 && Number(j.result[0]?.c)>0;
  return false;
}
const deepProbes = [];
const timeframeSpecs = [
  {exchange:"MEXC", values:{"1h":"Min60","4h":"Hour4","1d":"Day1"}, url:v=>`https://api.mexc.com/api/v1/contract/kline/BTC_USDT?interval=${v}`},
  {exchange:"BingX", values:{"1h":"1h","2h":"2h","4h":"4h","1d":"1d"}, url:v=>`https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol=BTC-USDT&interval=${v}&limit=3`},
  {exchange:"OKX", values:{"1h":"1H","2h":"2H","4h":"4H","1d":"1Dutc"}, url:v=>`https://www.okx.com/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=${v}&limit=3`},
  {exchange:"WEEX", values:{"1h":"1h","4h":"4h","1d":"1d"}, url:v=>`https://api-contract.weex.com/capi/v3/market/klines?symbol=BTCUSDT&interval=${v}&limit=3`},
  {exchange:"HTX", values:{"1h":"60min","4h":"4hour","1d":"1day"}, url:v=>`https://api.hbdm.com/linear-swap-ex/market/history/kline?contract_code=BTC-USDT&period=${v}&size=3`},
  {exchange:"KuCoin", values:{"1h":"1hour","2h":"2hour","4h":"4hour","1d":"1day"}, url:v=>`https://api.kucoin.com/api/ua/v2/market/kline?symbol=XBTUSDTM&tradeType=FUTURES&klineType=TRADE&interval=${v}`},
  {exchange:"XT", values:{"1h":"1h","4h":"4h","1d":"1d"}, url:v=>`https://fapi.xt.com/future/market/v1/public/q/kline?symbol=btc_usdt&interval=${v}&limit=3`}
];
for(const spec of timeframeSpecs) for(const [tf,value] of Object.entries(spec.values)) deepProbes.push({exchange:spec.exchange,kind:`candles_${tf}`,timeframe:tf,url:spec.url(value),check:j=>candleShapeOk(spec.exchange,j)});
function buildSynthetic2h(exchange,j) {
  let rows=[];
  if(exchange==="MEXC"){
    const d=j?.data||{};
    rows=(d.time||[]).map((t,i)=>({t:Number(t)*1000,o:Number(d.open?.[i]),h:Number(d.high?.[i]),l:Number(d.low?.[i]),c:Number(d.close?.[i]),v:Number(d.vol?.[i]??d.volume?.[i]??0)}));
  } else if(exchange==="WEEX"){
    rows=(Array.isArray(j)?j:[]).map(x=>({t:Number(x?.[0]),o:Number(x?.[1]),h:Number(x?.[2]),l:Number(x?.[3]),c:Number(x?.[4]),v:Number(x?.[5]??0)}));
  } else if(exchange==="HTX"){
    rows=(j?.data||[]).map(x=>({t:Number(x?.id)*1000,o:Number(x?.open),h:Number(x?.high),l:Number(x?.low),c:Number(x?.close),v:Number(x?.vol??x?.amount??0)}));
  } else if(exchange==="XT"){
    rows=(j?.result||[]).map(x=>({t:Number(x?.t),o:Number(x?.o),h:Number(x?.h),l:Number(x?.l),c:Number(x?.c),v:Number(x?.a??x?.v??0)}));
  }
  rows=rows.filter(x=>Number.isFinite(x.t)&&[x.o,x.h,x.l,x.c].every(Number.isFinite)&&x.c>0);
  rows.sort((a,b)=>a.t-b.t);
  const now=Date.now();
  for(let i=rows.length-2;i>=0;i--){
    const a=rows[i],b=rows[i+1];
    if(a.t%7200000===0&&b.t-a.t===3600000&&a.t+7200000<=now-120000&&now-(a.t+7200000)<=4*60*60*1000+300000){
      const aggregate={startAt:new Date(a.t).toISOString(),endAt:new Date(a.t+7200000).toISOString(),open:a.o,high:Math.max(a.h,b.h),low:Math.min(a.l,b.l),close:b.c,volume:a.v+b.v,sourceBars:2};
      if(aggregate.high>=aggregate.low&&aggregate.open>0&&aggregate.close>0&&aggregate.volume>=0)return aggregate;
    }
  }
  return null;
}
const fundingProbes = [
  {exchange:"MEXC",kind:"funding_rate",url:"https://api.mexc.com/api/v1/contract/ticker",check:j=>j?.success===true&&Array.isArray(j?.data)&&j.data.some(x=>x?.symbol==="BTC_USDT"&&Number.isFinite(Number(x.fundingRate)))},
  {exchange:"BingX",kind:"funding_rate",url:"https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex?symbol=BTC-USDT",check:j=>(j?.code===0||j?.code==="0")&&j?.data&&Number.isFinite(Number(Array.isArray(j.data)?j.data[0]?.lastFundingRate:j.data.lastFundingRate))},
  {exchange:"OKX",kind:"funding_rate",url:"https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP",check:j=>String(j?.code)==="0"&&Array.isArray(j?.data)&&Number.isFinite(Number(j.data[0]?.fundingRate))},
  {exchange:"WEEX",kind:"funding_rate",url:"https://api-contract.weex.com/capi/v3/market/premiumIndex?symbol=BTCUSDT",check:j=>Array.isArray(j)&&j.some(x=>x?.symbol==="BTCUSDT"&&Number.isFinite(Number(x.lastFundingRate))&&Number.isFinite(Number(x.forecastFundingRate)))},
  {exchange:"HTX",kind:"funding_rate",url:"https://api.hbdm.com/linear-swap-api/v1/swap_funding_rate?contract_code=BTC-USDT",check:j=>j?.status==="ok"&&j?.data&&Number.isFinite(Number(Array.isArray(j.data)?(j.data[0]?.funding_rate??j.data[0]?.fundingRate): (j.data.funding_rate??j.data.fundingRate)))},
  {exchange:"KuCoin",kind:"funding_rate",url:"https://api-futures.kucoin.com/api/v1/funding-rate/XBTUSDTM/current",check:j=>String(j?.code)==="200000"&&j?.data&&Number.isFinite(Number(j.data.value))},
  {exchange:"XT",kind:"funding_rate",url:"https://fapi.xt.com/future/market/v1/public/q/funding-rate?symbol=btc_usdt",check:j=>j?.returnCode===0&&j?.result&&Number.isFinite(Number(j.result.fundingRate??j.result.items?.[0]?.fundingRate))}
];
function buildSynthetic1dUtc(exchange,j) {
  if(exchange!=="HTX") return null;
  const rows=(j?.data||[]).map(x=>({t:Number(x?.id)*1000,o:Number(x?.open),h:Number(x?.high),l:Number(x?.low),c:Number(x?.close),v:Number(x?.vol??x?.amount??0)}))
    .filter(x=>Number.isFinite(x.t)&&[x.o,x.h,x.l,x.c].every(Number.isFinite)&&x.c>0)
    .sort((a,b)=>a.t-b.t);
  const dayMs=86400000,now=Date.now(),days=new Map();
  for(const row of rows){const start=Math.floor(row.t/dayMs)*dayMs;if(!days.has(start))days.set(start,[]);days.get(start).push(row);}
  const starts=[...days.keys()].sort((a,b)=>b-a);
  for(const start of starts){
    const bars=days.get(start).sort((a,b)=>a.t-b.t);
    if(start+dayMs>now-120000||now-(start+dayMs)>2*dayMs||bars.length!==24)continue;
    let contiguous=true;for(let i=1;i<bars.length;i++)if(bars[i].t-bars[i-1].t!==3600000){contiguous=false;break;}
    if(!contiguous||bars[0].t!==start||bars[23].t!==start+23*3600000)continue;
    return {startAt:new Date(start).toISOString(),endAt:new Date(start+dayMs).toISOString(),open:bars[0].o,high:Math.max(...bars.map(x=>x.h)),low:Math.min(...bars.map(x=>x.l)),close:bars[23].c,volume:bars.reduce((n,x)=>n+x.v,0),sourceBars:24,timezone:"UTC"};
  }
  return null;
}
const xtSymbolDetailProbes=[{exchange:"XT",kind:"contract_detail_probe",symbol:"BTC_USDT",url:"https://fapi.xt.com/future/market/v1/public/symbol/detail?symbol=btc_usdt",check:j=>j?.returnCode===0&&j?.result&&typeof j.result==="object"}];
const synthetic2hProbes = [
  {exchange:"MEXC",kind:"synthetic_2h",check:j=>true,url:"https://api.mexc.com/api/v1/contract/kline/BTC_USDT?interval=Min60"},
  {exchange:"WEEX",kind:"synthetic_2h",check:j=>true,url:"https://api-contract.weex.com/capi/v3/market/klines?symbol=BTCUSDT&interval=1h&limit=10"},
  {exchange:"HTX",kind:"synthetic_2h",check:j=>true,url:"https://api.hbdm.com/linear-swap-ex/market/history/kline?contract_code=BTC-USDT&period=60min&size=10"},
  {exchange:"XT",kind:"synthetic_2h",check:j=>true,url:"https://fapi.xt.com/future/market/v1/public/q/kline?symbol=btc_usdt&interval=1h&limit=10"}
];
function getContractRows(exchange,j) {
  if(exchange==="MEXC") return j?.data;
  if(exchange==="BingX") return j?.data;
  if(exchange==="OKX") return j?.data;
  if(exchange==="WEEX") return Array.isArray(j)?j:j?.symbols;
  if(exchange==="HTX") return j?.data;
  if(exchange==="KuCoin") return j?.data;
  if(exchange==="XT") return j?.result;
  return null;
}
function normalizeContract(exchange,x) {
  if(exchange==="MEXC") return {symbol:x?.symbol,base:x?.baseCoin,quote:x?.quoteCoin,settle:x?.settleCoin,contractType:Number(x?.futureType)===1?"perpetual":Number(x?.futureType)===2?"delivery":"unknown",exchangeState:x?.state,active:Number(x?.state)===0,apiAllowed:typeof x?.apiAllowed==="boolean"?x.apiAllowed:null};
  if(exchange==="BingX") return {symbol:x?.symbol,base:x?.asset,quote:x?.currency||"USDT",settle:x?.currency||"USDT",contractType:"perpetual",exchangeState:x?.status??null,active:typeof x?.status==="number"?x.status===1:null,apiAllowed:x?.apiStateOpen==="true"};
  if(exchange==="OKX"){const parts=String(x?.instId||"").split("-");return {symbol:x?.instId,base:parts[0]||null,quote:parts[1]||null,settle:x?.settleCcy||null,contractType:x?.ctType||"swap",exchangeState:x?.state,active:x?.state==="live",apiAllowed:null};}
  if(exchange==="WEEX"){const base=x?.baseAsset||String(x?.symbol||"").replace(/USDT$/,"");return {symbol:x?.symbol,base,quote:x?.quoteAsset||"USDT",settle:x?.marginAsset||"USDT",contractType:x?.contractType||"perpetual-market-ticker",exchangeState:x?.status??null,active:typeof x?.status==="number"?x.status===1:typeof x?.status==="string"?(x.status.toUpperCase()==="TRADING"||x.status.toUpperCase()==="OPEN"):null,marketDataAvailable:Number(x?.lastPrice)>0,apiAllowed:null};}
  if(exchange==="HTX"){const parts=String(x?.contract_code||"").split("-");return {symbol:x?.contract_code,base:x?.symbol||parts[0],quote:x?.quote_asset||parts[1],settle:x?.settle_asset||parts[1],contractType:x?.contract_type||"swap",exchangeState:x?.contract_status,active:Number(x?.contract_status)===1,apiAllowed:null};}
  if(exchange==="KuCoin") return {symbol:x?.symbol,base:x?.baseCurrency==="XBT"?"BTC":x?.baseCurrency,quote:x?.quoteCurrency,settle:x?.settleCurrency,contractType:x?.type,exchangeState:x?.status??null,active:x?.status==="Open"||x?.status==="Trading"||x?.status===true,apiAllowed:null};
  if(exchange==="XT"){const symbol=String(x?.symbol||x?.s||"");const parts=symbol.split("_");const tradeSwitch=typeof x?.tradeSwitch==="boolean"?x.tradeSwitch:null;return {symbol,base:x?.baseCoin||parts[0]?.toUpperCase()||null,quote:x?.quoteCoin||parts[1]?.toUpperCase()||null,settle:x?.settleCoin||(String(x?.underlyingType||"").toUpperCase().includes("USDT")?"USDT":parts[1]?.toUpperCase()||null),contractType:x?.contractType||x?.productType||"unknown",exchangeState:tradeSwitch===null?null:(tradeSwitch?"trading_enabled":"trading_disabled"),active:tradeSwitch,marketDataAvailable:null,apiAllowed:null};}
  return null;
}
function summarizeContractCatalog(exchange,j) {
  const raw=getContractRows(exchange,j);
  const normalized=(Array.isArray(raw)?raw:[]).map(x=>normalizeContract(exchange,x)).filter(Boolean);
  const symbols=normalized.map(x=>String(x.symbol||"").trim().toUpperCase()).filter(Boolean);
  const unique=new Set(symbols);
  const active=normalized.filter(x=>x.active===true).length;
  const marketDataAvailableCount=normalized.filter(x=>x.marketDataAvailable===true).length;
  const usdtSettledCount=normalized.filter(x=>String(x.settle||x.quote||"").toUpperCase()==="USDT").length;
  const activeUsdtSettledCount=normalized.filter(x=>x.active===true&&String(x.settle||x.quote||"").toUpperCase()==="USDT").length;
  const inverseCount=normalized.filter(x=>x.contractType==="inverse"||(x.base&&x.settle&&String(x.base).toUpperCase()===String(x.settle).toUpperCase())).length;
  const inactive=normalized.filter(x=>x.active===false).length;
  const unknownStatus=normalized.filter(x=>x.active===null).length;
  const btcAliases=exchange==="MEXC"?["BTC_USDT"]:exchange==="BingX"?["BTC-USDT"]:exchange==="OKX"?["BTC-USDT-SWAP"]:exchange==="WEEX"?["BTCUSDT"]:exchange==="HTX"?["BTC-USDT"]:exchange==="KuCoin"?["XBTUSDTM","BTCUSDTM"]:["BTC_USDT"];
  return {rawCount:Array.isArray(raw)?raw.length:0,normalizedCount:normalized.length,uniqueSymbolCount:unique.size,duplicateSymbolCount:symbols.length-unique.size,missingSymbolCount:normalized.length-symbols.length,activeCount:active,inactiveCount:inactive,unknownStatusCount:unknownStatus,marketDataAvailableCount,usdtSettledCount,activeUsdtSettledCount,inverseCount,benchmarkFound:btcAliases.some(x=>unique.has(x)),sample:normalized.slice(0,5)};
}
const contractListProbes = [
  {exchange:"MEXC",kind:"contract_list",url:"https://api.mexc.com/api/v1/contract/detail",check:j=>j?.success===true&&j?.code===0&&Array.isArray(j?.data)&&j.data.some(x=>x?.symbol==="BTC_USDT"),count:j=>j.data.length},
  {exchange:"BingX",kind:"contract_list",url:"https://open-api.bingx.com/openApi/swap/v2/quote/contracts",check:j=>(j?.code===0||j?.code==="0")&&Array.isArray(j?.data)&&j.data.some(x=>x?.symbol==="BTC-USDT"),count:j=>j.data.length},
  {exchange:"OKX",kind:"contract_list",url:"https://www.okx.com/api/v5/public/instruments?instType=SWAP",check:j=>String(j?.code)==="0"&&Array.isArray(j?.data)&&j.data.some(x=>x?.instId==="BTC-USDT-SWAP"),count:j=>j.data.length},
  {exchange:"WEEX",kind:"contract_list",url:"https://api-contract.weex.com/capi/v3/market/exchangeInfo?contractType=PERPETUAL",check:j=>Array.isArray(j?.symbols)&&j.symbols.some(x=>x?.symbol==="BTCUSDT"&&x?.contractType==="PERPETUAL"),count:j=>j.symbols.length},
  {exchange:"WEEX",kind:"market_symbol_list",url:"https://api-contract.weex.com/capi/v3/market/ticker/24hr",check:j=>Array.isArray(j)&&j.some(x=>x?.symbol==="BTCUSDT"&&Number(x?.lastPrice)>0),count:j=>j.length},
  {exchange:"WEEX",kind:"api_trading_symbols",url:"https://api-contract.weex.com/capi/v3/market/apiTradingSymbols",check:j=>{const rows=Array.isArray(j)?j:Array.isArray(j?.symbols)?j.symbols:Array.isArray(j?.data)?j.data:[];return rows.some(x=>String(typeof x==="string"?x:x?.symbol||"").toUpperCase()==="BTCUSDT");}},
  {exchange:"HTX",kind:"contract_list",url:"https://api.hbdm.com/linear-swap-api/v1/swap_contract_info",check:j=>j?.status==="ok"&&Array.isArray(j?.data)&&j.data.some(x=>x?.contract_code==="BTC-USDT"),count:j=>j.data.length},
  {exchange:"KuCoin",kind:"contract_list",url:"https://api-futures.kucoin.com/api/v1/contracts/active",check:j=>String(j?.code)==="200000"&&Array.isArray(j?.data)&&j.data.some(x=>x?.symbol==="XBTUSDTM"),count:j=>j.data.length},
  {exchange:"XT",kind:"contract_list",url:"https://fapi.xt.com/future/market/v1/public/symbol/list",check:j=>j?.returnCode===0&&Array.isArray(j?.result)&&j.result.some(x=>String(x?.symbol||"").toLowerCase()==="btc_usdt"),count:j=>j.result.length}
];
const symbolCoverageProbes = [];
const symbolPairs = {
  MEXC:[["BTC","BTC_USDT"],["ETH","ETH_USDT"],["SOL","SOL_USDT"],["XRP","XRP_USDT"],["DOGE","DOGE_USDT"]],
  BingX:[["BTC","BTC-USDT"],["ETH","ETH-USDT"],["SOL","SOL-USDT"],["XRP","XRP-USDT"],["DOGE","DOGE-USDT"]],
  OKX:[["BTC","BTC-USDT-SWAP"],["ETH","ETH-USDT-SWAP"],["SOL","SOL-USDT-SWAP"],["XRP","XRP-USDT-SWAP"],["DOGE","DOGE-USDT-SWAP"]],
  WEEX:[["BTC","BTCUSDT"],["ETH","ETHUSDT"],["SOL","SOLUSDT"],["XRP","XRPUSDT"],["DOGE","DOGEUSDT"]],
  HTX:[["BTC","BTC-USDT"],["ETH","ETH-USDT"],["SOL","SOL-USDT"],["XRP","XRP-USDT"],["DOGE","DOGE-USDT"]],
  KuCoin:[["BTC","XBTUSDTM"],["ETH","ETHUSDTM"],["SOL","SOLUSDTM"],["XRP","XRPUSDTM"],["DOGE","DOGEUSDTM"]],
  XT:[["BTC","btc_usdt"],["ETH","eth_usdt"],["SOL","sol_usdt"],["XRP","xrp_usdt"],["DOGE","doge_usdt"]]
};
for(const [exchange,pairs] of Object.entries(symbolPairs)) for(const [base,symbol] of pairs){
  let url="";
  if(exchange==="MEXC") url=`https://api.mexc.com/api/v1/contract/kline/${symbol}?interval=Min15`;
  if(exchange==="BingX") url=`https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol=${symbol}&interval=15m&limit=3`;
  if(exchange==="OKX") url=`https://www.okx.com/api/v5/market/candles?instId=${symbol}&bar=15m&limit=3`;
  if(exchange==="WEEX") url=`https://api-contract.weex.com/capi/v3/market/klines?symbol=${symbol}&interval=15m&limit=3`;
  if(exchange==="HTX") url=`https://api.hbdm.com/linear-swap-ex/market/history/kline?contract_code=${symbol}&period=15min&size=3`;
  if(exchange==="KuCoin") url=`https://api.kucoin.com/api/ua/v2/market/kline?symbol=${symbol}&tradeType=FUTURES&klineType=TRADE&interval=15min`;
  if(exchange==="XT") url=`https://fapi.xt.com/future/market/v1/public/q/kline?symbol=${symbol}&interval=15m&limit=3`;
  symbolCoverageProbes.push({exchange,kind:"candles_15m_symbol_coverage",symbol:base,timeframe:"15m",url,check:j=>candleShapeOk(exchange,j)});
}

function oiHistoryTimestamp(exchange,j) {
  let raw=null;
  if(exchange==="OKX") raw=j?.data?.[0]?.[0];
  if(exchange==="HTX") raw=j?.data?.tick?.[0]?.ts;
  if(exchange==="KuCoin") raw=j?.data?.[0]?.ts;
  const n=Number(raw);
  if(!Number.isFinite(n))return null;
  return n<100000000000?n*1000:n;
}
const oiHistoryProbes=[
  {exchange:"OKX",kind:"open_interest_history_15m",timeframe:"15m",url:"https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history?instId=BTC-USDT-SWAP&period=15m&limit=3",check:j=>String(j?.code)==="0"&&Array.isArray(j?.data)&&j.data.length>0&&Number(j.data[0]?.[1])>0},
  {exchange:"HTX",kind:"open_interest_history_1h",timeframe:"1h",url:"https://api.hbdm.com/linear-swap-api/v1/swap_his_open_interest?contract_code=BTC-USDT&period=60min&amount_type=1&size=3",check:j=>j?.status==="ok"&&Array.isArray(j?.data?.tick)&&j.data.tick.length>0&&Number(j.data.tick[0]?.volume)>0},
  {exchange:"KuCoin",kind:"open_interest_history_15m",timeframe:"15m",url:"https://api.kucoin.com/api/ua/v2/market/open-interest?symbol=XBTUSDTM&interval=15min&pageSize=3",check:j=>String(j?.code)==="200000"&&Array.isArray(j?.data)&&j.data.length>0&&Number(j.data[0]?.openInterest)>0}
];
const allDeepProbes=[...deepProbes,...fundingProbes,...contractListProbes,...synthetic2hProbes,...xtSymbolDetailProbes,...symbolCoverageProbes,...oiHistoryProbes];
async function runDeepProbe(probe) {
  const started=Date.now();let response=null,raw="",json=null,retryCount=0;
  try {
    for(let attempt=0;attempt<2;attempt++){
      const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),TIMEOUT_MS);
      try{
        response=await fetch(probe.url,{headers:{accept:"application/json","user-agent":"exchange-api-connectivity-test/1.0"},signal:controller.signal});
        raw=await response.text();try{json=JSON.parse(raw)}catch{json=null}
      }finally{clearTimeout(timer)}
      const apiCode=Number(json?.code??json?.returnCode);
      const transient=(!response.ok&&[429,500,502,503,504].includes(response.status))||[100410,100429].includes(apiCode);
      if(attempt===0&&transient){retryCount=1;await new Promise(resolve=>setTimeout(resolve,1800));continue;}
      break;
    }
    let schemaOk=Boolean(json&&probe.check(json)),latestCandleAt=null,ageMs=null,fresh=null,synthetic2h=null,synthetic1dUtc=null,oiHistoryAt=null,oiHistoryAgeMinutes=null,oiHistoryFresh=null;
    if(probe.kind==="synthetic_1d_utc"&&json){synthetic1dUtc=buildSynthetic1dUtc(probe.exchange,json);schemaOk=Boolean(synthetic1dUtc);}
    if(probe.kind.startsWith("open_interest_history_")&&json&&schemaOk){oiHistoryAt=oiHistoryTimestamp(probe.exchange,json);const age=oiHistoryAt===null?null:Date.now()-oiHistoryAt;oiHistoryAgeMinutes=age===null?null:Math.round(age/60000);const maxAge=(timeframeMinutes[probe.timeframe]||60)*60000*2+300000;oiHistoryFresh=age!==null&&age>=-120000&&age<=maxAge;schemaOk=schemaOk&&oiHistoryFresh;}
    if(probe.kind==="synthetic_2h"&&json){synthetic2h=buildSynthetic2h(probe.exchange,json);schemaOk=Boolean(synthetic2h);}
    if(probe.kind.startsWith("candles_")&&schemaOk){
      latestCandleAt=candleTimestamp(probe.exchange,json);
      ageMs=latestCandleAt===null?null:Date.now()-latestCandleAt;
      const maxAge=timeframeMinutes[probe.timeframe]*60*1000*2+5*60*1000;
      fresh=ageMs!==null&&ageMs>=-120000&&ageMs<=maxAge;
      schemaOk=schemaOk&&fresh;
    }
    return {exchange:probe.exchange,kind:probe.kind,status:response.ok&&schemaOk?"PASS":"FAIL",httpStatus:response.status,latencyMs:Date.now()-started,retryCount,schemaOk,contractCount:["contract_list","market_symbol_list"].includes(probe.kind)?probe.count(json):null,contractStats:["contract_list","market_symbol_list"].includes(probe.kind)?summarizeContractCatalog(probe.exchange,json):null,synthetic2h,synthetic1dUtc,oiHistoryAt:oiHistoryAt?new Date(oiHistoryAt).toISOString():null,oiHistoryAgeMinutes,oiHistoryFresh,latestCandleAt:latestCandleAt?new Date(latestCandleAt).toISOString():null,candleAgeMinutes:ageMs===null?null:Math.round(ageMs/60000),fresh,endpoint:probe.url,preview:raw.slice(0,220),error:!response.ok?`HTTP ${response.status}`:!json?"Invalid JSON":!schemaOk?(fresh===false?"Candle data is stale or timestamp is outside allowed clock skew":"Response schema/value check failed"):null};
  }catch(error){return {exchange:probe.exchange,kind:probe.kind,status:"FAIL",httpStatus:null,latencyMs:Date.now()-started,schemaOk:false,endpoint:probe.url,preview:"",error:error.name==="AbortError"?`Timeout after ${TIMEOUT_MS} ms`:String(error.message||error)}}
  }
async function runWeexCatalogAudit(){
  const urls=[
    "https://api-contract.weex.com/capi/v3/market/exchangeInfo?contractType=PERPETUAL",
    "https://api-contract.weex.com/capi/v3/market/ticker/24hr"
  ];
  const responses=[];
  for(const url of urls){
    const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),TIMEOUT_MS);
    try{const r=await fetch(url,{headers:{accept:"application/json","user-agent":"exchange-api-connectivity-test/1.0"},signal:controller.signal});const raw=await r.text();let json;try{json=JSON.parse(raw)}catch{json=null}responses.push({ok:r.ok,json,httpStatus:r.status,error:null});}
    catch(e){responses.push({ok:false,json:null,httpStatus:null,error:e.name==="AbortError"?"timeout":String(e.message||e)});}
    finally{clearTimeout(timer);}
  }
  const contracts=responses[0]?.json?.symbols||[];
  const tickers=Array.isArray(responses[1]?.json)?responses[1].json:[];
  const contractSet=new Set(contracts.map(x=>String(x?.symbol||"").toUpperCase()).filter(Boolean));
  const tickerSet=new Set(tickers.map(x=>String(x?.symbol||"").toUpperCase()).filter(Boolean));
  const matched=[...tickerSet].filter(x=>contractSet.has(x));
  const matchedWithPrice=tickers.filter(x=>contractSet.has(String(x?.symbol||"").toUpperCase())&&Number(x?.lastPrice)>0).length;
  const tickerWithPrice=tickers.filter(x=>Number(x?.lastPrice)>0).length;
  return {exchange:"WEEX",contractListHttp:responses[0]?.httpStatus,tickerListHttp:responses[1]?.httpStatus,contractListCount:contracts.length,tickerListCount:tickers.length,tickerWithPrice,matchedSymbolCount:matched.length,matchedWithPriceCount:matchedWithPrice,tickerOnlyCount:[...tickerSet].filter(x=>!contractSet.has(x)).length,contractWithoutTickerCount:[...contractSet].filter(x=>!tickerSet.has(x)).length,tickerOnlySample:[...tickerSet].filter(x=>!contractSet.has(x)).slice(0,10),contractWithoutTickerSample:[...contractSet].filter(x=>!tickerSet.has(x)).slice(0,10),errors:responses.filter(x=>!x.ok).map(x=>x.error||`HTTP ${x.httpStatus}`)};
}
async function fetchJsonWithRetry(url){
  let response=null,raw="",json=null,retryCount=0;
  for(let attempt=0;attempt<2;attempt++){
    const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),TIMEOUT_MS);
    try{response=await fetch(url,{headers:{accept:"application/json","user-agent":"exchange-api-connectivity-test/1.0"},signal:controller.signal});raw=await response.text();try{json=JSON.parse(raw)}catch{json=null}}
    finally{clearTimeout(timer)}
    const apiCode=Number(json?.code??json?.returnCode);
    const transient=(!response.ok&&[429,500,502,503,504].includes(response.status))||[100410,100429].includes(apiCode);
    if(attempt===0&&transient){retryCount=1;await new Promise(resolve=>setTimeout(resolve,1800));continue;}
    break;
  }
  return {response,raw,json,retryCount};
}
async function runHypeSymbolProbe(exchange){
  const specs={
    MEXC:{catalog:"https://api.mexc.com/api/v1/contract/detail",symbol:"HYPE_USDT",find:j=>(j?.data||[]).find(x=>x?.symbol==="HYPE_USDT"),active:x=>Number(x?.state)===0,candle:"https://api.mexc.com/api/v1/contract/kline/HYPE_USDT?interval=Min15"},
    OKX:{catalog:"https://www.okx.com/api/v5/public/instruments?instType=SWAP",symbol:"HYPE-USDT-SWAP",find:j=>(j?.data||[]).find(x=>x?.instId==="HYPE-USDT-SWAP"),active:x=>x?.state==="live",candle:"https://www.okx.com/api/v5/market/candles?instId=HYPE-USDT-SWAP&bar=15m&limit=3"}
  };
  const spec=specs[exchange],started=Date.now();
  try{
    const catalog=await fetchJsonWithRetry(spec.catalog);
    if(!catalog.response?.ok||!catalog.json)return {exchange,kind:"hype_15m_crosscheck",symbol:"HYPE",status:"FAIL",httpStatus:catalog.response?.status??null,retryCount:catalog.retryCount,error:"Contract catalog request failed",catalogEndpoint:spec.catalog};
    const contract=spec.find(catalog.json);
    if(!contract)return {exchange,kind:"hype_15m_crosscheck",symbol:"HYPE",status:"NOT_LISTED",httpStatus:catalog.response.status,retryCount:catalog.retryCount,catalogCount:Array.isArray(catalog.json.data)?catalog.json.data.length:0,error:"HYPE contract not present in this exchange's public contract catalog",catalogEndpoint:spec.catalog};
    if(!spec.active(contract))return {exchange,kind:"hype_15m_crosscheck",symbol:"HYPE",status:"NOT_ACTIVE",httpStatus:catalog.response.status,retryCount:catalog.retryCount,exchangeState:contract.state??contract.status??null,error:"HYPE contract exists but is not marked active",catalogEndpoint:spec.catalog};
    const candles=await fetchJsonWithRetry(spec.candle);
    const valid=candles.response?.ok&&candles.json&&candleShapeOk(exchange,candles.json);
    const stamp=valid?candleTimestamp(exchange,candles.json):null;
    const age=stamp===null?null:Date.now()-stamp;
    const fresh=age!==null&&age>=-120000&&age<=35*60000;
    let oiAvailable=false,fundingAvailable=false,oiValue=null,fundingRate=null,marketDataError=null;
    if(exchange==="MEXC"){
      const ticker=await fetchJsonWithRetry("https://api.mexc.com/api/v1/contract/ticker");
      const row=(ticker.json?.data||[]).find(x=>x?.symbol==="HYPE_USDT");
      oiValue=Number(row?.holdVol);fundingRate=Number(row?.fundingRate);
      oiAvailable=ticker.response?.ok&&Number.isFinite(oiValue)&&oiValue>0;
      fundingAvailable=ticker.response?.ok&&Number.isFinite(fundingRate);
      if(!ticker.response?.ok)marketDataError="MEXC HYPE ticker request failed";
    }else{
      const [oi,funding]=await Promise.all([
        fetchJsonWithRetry("https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=HYPE-USDT-SWAP"),
        fetchJsonWithRetry("https://www.okx.com/api/v5/public/funding-rate?instId=HYPE-USDT-SWAP")
      ]);
      oiValue=Number(oi.json?.data?.[0]?.oi);fundingRate=Number(funding.json?.data?.[0]?.fundingRate);
      oiAvailable=oi.response?.ok&&String(oi.json?.code)==="0"&&Number.isFinite(oiValue)&&oiValue>0;
      fundingAvailable=funding.response?.ok&&String(funding.json?.code)==="0"&&Number.isFinite(fundingRate);
      if(!oiAvailable||!fundingAvailable)marketDataError="HYPE OI or funding endpoint did not return a valid value";
    }
    const allChecks=valid&&fresh&&oiAvailable&&fundingAvailable;
    return {exchange,kind:"hype_15m_crosscheck",symbol:"HYPE",status:allChecks?"PASS":"FAIL",httpStatus:candles.response?.status??null,retryCount:catalog.retryCount+candles.retryCount,catalogCount:Array.isArray(catalog.json.data)?catalog.json.data.length:0,latestCandleAt:stamp?new Date(stamp).toISOString():null,candleAgeMinutes:age===null?null:Math.round(age/60000),fresh,openInterestAvailable:oiAvailable,openInterestValue:oiValue,fundingAvailable,fundingRate,marketDataError,catalogEndpoint:spec.catalog,candleEndpoint:spec.candle,error:!candles.response?.ok?`HTTP ${candles.response.status}`:!valid?"15m candle response invalid":!fresh?"15m candle is stale":marketDataError,latencyMs:Date.now()-started};
  }catch(error){return {exchange,kind:"hype_15m_crosscheck",symbol:"HYPE",status:"FAIL",retryCount:0,latencyMs:Date.now()-started,error:error.name==="AbortError"?`Timeout after ${TIMEOUT_MS} ms`:String(error.message||error)}}
}
async function runDeepSweep(){
  const groups=new Map();
  for(const probe of allDeepProbes){if(!groups.has(probe.exchange))groups.set(probe.exchange,[]);groups.get(probe.exchange).push(probe)}
  const batches=await Promise.all([...groups.entries()].map(async([exchange,items])=>{
    const results=[];
    for(let i=0;i<items.length;i++){
      if(i>0) await new Promise(resolve=>setTimeout(resolve,1100));
      results.push(await runDeepProbe(items[i]));
    }
    return results;
  }));
  const results=[...batches.flat(),...(await Promise.all([runHypeSymbolProbe("MEXC"),runHypeSymbolProbe("OKX")]))];
  const catalogAudits=[await runWeexCatalogAudit()];
  return {finishedAt:new Date().toISOString(),passed:results.filter(x=>x.status==="PASS").length,notListed:results.filter(x=>x.status==="NOT_LISTED").length,failed:results.filter(x=>x.status==="FAIL"||x.status==="NOT_ACTIVE").length,total:results.length,results,catalogAudits};
}

async function runDataProbe(probe) {
  const started=Date.now(), controller=new AbortController(), timer=setTimeout(()=>controller.abort(),TIMEOUT_MS);
  try {
    const response=await fetch(probe.url,{headers:{accept:"application/json","user-agent":"exchange-api-connectivity-test/1.0"},signal:controller.signal});
    const raw=await response.text(); let json; try{json=JSON.parse(raw)}catch{json=null}
    let schemaOk=Boolean(json&&probe.check(json));
    let latestCandleAt=null,candleAgeMinutes=null,fresh=null;
    if(probe.kind.startsWith("candles_")&&schemaOk){
      const tf=probe.kind.slice("candles_".length);
      const stamp=candleTimestamp(probe.exchange,json);
      const age=stamp===null?null:Date.now()-stamp;
      const maxAge=(timeframeMinutes[tf]||15)*60000*2+300000;
      fresh=age!==null&&age>=-120000&&age<=maxAge;
      schemaOk=schemaOk&&fresh;
      latestCandleAt=stamp?new Date(stamp).toISOString():null;
      candleAgeMinutes=age===null?null:Math.round(age/60000);
    }
    return {exchange:probe.exchange,kind:probe.kind,status:response.ok&&schemaOk?"PASS":"FAIL",httpStatus:response.status,latencyMs:Date.now()-started,schemaOk,latestCandleAt,candleAgeMinutes,fresh,endpoint:probe.url,preview:raw.slice(0,220),error:!response.ok?`HTTP ${response.status}`:!json?"Invalid JSON":!schemaOk?(fresh===false?"Candle data is stale or timestamp is outside allowed clock skew":"Response schema/value check failed"):null};
  } catch(error) {
    return {exchange:probe.exchange,kind:probe.kind,status:"FAIL",httpStatus:null,latencyMs:Date.now()-started,schemaOk:false,endpoint:probe.url,preview:"",error:error.name==="AbortError"?`Timeout after ${TIMEOUT_MS} ms`:String(error.message||error)};
  } finally {clearTimeout(timer)}
}

async function runProbe(probe) {
  const started = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const response = await fetch(probe.url, {
      method: "GET",
      headers: { "accept": "application/json", "user-agent": "exchange-api-connectivity-test/1.0" },
      signal: controller.signal,
    });
    const raw = await response.text();
    let json;
    try { json = JSON.parse(raw); } catch { json = null; }
    const schemaOk = Boolean(json && probe.check(json));
    const apiCode = json?.code ?? json?.returnCode ?? json?.error?.code ?? null;
    const contentType = response.headers.get("content-type");
    return {
      exchange: probe.exchange,
      status: response.ok && schemaOk ? "PASS" : "FAIL",
      httpStatus: response.status,
      latencyMs: Date.now() - started,
      schemaOk,
      benchmarkSymbol: probe.benchmark || "BTCUSDT",
      marketCount: Array.isArray(json?.data) ? json.data.length : (Array.isArray(json?.result) ? json.result.length : (json?.tick ? 1 : 0)),
      apiCode,
      contentType,
      responseBytes: Buffer.byteLength(raw),
      preview: raw.slice(0, 360),
      error: !response.ok ? `HTTP ${response.status}` : (!json ? "پاسخ JSON معتبر نیست" : (!schemaOk ? "ساختار پاسخ با قالب رسمی مورد انتظار مطابقت ندارد" : null)),
      endpoint: probe.url,
    };
  } catch (error) {
    return {
      exchange: probe.exchange,
      status: "FAIL",
      httpStatus: null,
      latencyMs: Date.now() - started,
      schemaOk: false,
      benchmarkSymbol: probe.benchmark || "BTCUSDT",
      marketCount: 0,
      apiCode: null,
      contentType: null,
      responseBytes: 0,
      preview: "",
      error: error.name === "AbortError" ? `Timeout after ${TIMEOUT_MS} ms` : String(error.message || error),
      endpoint: probe.url,
    };
  } finally {
    clearTimeout(timer);
  }
}

const page = `<!doctype html>
<html lang="fa" dir="rtl">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>تست اتصال API هفت صرافی</title>
<style>
body{font-family:system-ui,sans-serif;background:#0b1220;color:#e5e7eb;margin:0;padding:24px}
main{max-width:1000px;margin:auto}h1{font-size:24px}p{color:#aab4c5}
button{background:#2563eb;color:white;border:0;border-radius:9px;padding:12px 20px;font-size:16px;cursor:pointer}
button:disabled{opacity:.5}table{width:100%;border-collapse:collapse;margin-top:20px;background:#111c2e;border-radius:12px;overflow:hidden}
th,td{text-align:right;padding:12px;border-bottom:1px solid #263449;font-size:14px}
.pass{color:#34d399;font-weight:700}.fail{color:#f87171;font-weight:700}
small{color:#94a3b8;word-break:break-all}.summary{margin-top:16px}
</style></head>
<body><main>
<h1>تست اتصال API هفت صرافی فیوچرز</h1>
<p>این ابزار فقط درخواست‌های عمومی GET می‌فرستد؛ کلید API یا اطلاعات حساب لازم نیست. هر هفت تست هم‌زمان اجرا می‌شوند.</p>
<button id="run">اجرای تست تیکر هر ۷ صرافی</button>\n<button id="dataRun" style="margin-right:8px;background:#0f766e">تست کندل و Open Interest</button>\n<button id="deepRun" style="margin-right:8px;background:#7c3aed">تست تایم‌فریم‌ها و Funding</button>\n<pre id="dataSummary" style="white-space:pre-wrap;overflow-wrap:anywhere;color:#cbd5e1;background:#111c2e;padding:12px;border-radius:10px;margin-top:14px">تست تکمیلی داده‌ها هنوز اجرا نشده است.</pre>
<div class="summary" id="summary">هنوز تستی اجرا نشده است.</div>
<table><thead><tr><th>صرافی</th><th>نتیجه</th><th>HTTP</th><th>زمان (ms)</th><th>جزئیات</th></tr></thead><tbody id="rows"></tbody></table>
<p><small>PASS یعنی پاسخ HTTP موفق و ساختار پایه پاسخ قابل تشخیص بوده است؛ این تست به‌تنهایی تضمین‌کننده پایداری یا پوشش کامل بازار نیست.</small></p>
</main>
<script>
const btn=document.getElementById('run'), rows=document.getElementById('rows'), summary=document.getElementById('summary');\nconst dataBtn=document.getElementById('dataRun'), dataSummary=document.getElementById('dataSummary');\nconst deepBtn=document.getElementById('deepRun');
deepBtn.addEventListener('click',async()=>{deepBtn.disabled=true;deepBtn.textContent='در حال تست عمیق...';dataSummary.textContent='در حال بررسی تایم‌فریم‌های ۱ساعت، ۲ساعت، ۴ساعت، روزانه و Funding...';try{const r=await fetch('/api/deep-test',{cache:'no-store'});if(!r.ok)throw new Error('HTTP '+r.status);const b=await r.json();const lines=b.results.map(x=>x.exchange+' | '+x.kind+(x.symbol?' | '+x.symbol:'')+' | '+x.status+' | '+x.latencyMs+'ms'+(x.candleAgeMinutes!==null&&x.candleAgeMinutes!==undefined?' | age '+x.candleAgeMinutes+'m':'')+(x.error?' | '+x.error:''));const catalogs=b.results.filter(x=>x.contractStats).map(x=>x.exchange+' | کل: '+x.contractStats.rawCount+' | یکتا: '+x.contractStats.uniqueSymbolCount+' | فعال: '+x.contractStats.activeCount+' | فعال USDT: '+x.contractStats.activeUsdtSettledCount+' | وضعیت نامشخص: '+x.contractStats.unknownStatusCount+' | USDT کل: '+x.contractStats.usdtSettledCount+' | قیمت زنده: '+x.contractStats.marketDataAvailableCount);const audits=(b.catalogAudits||[]).map(x=>x.exchange+' | قراردادها: '+x.contractListCount+' | تیکرها: '+x.tickerListCount+' | منطبق: '+x.matchedSymbolCount+' | منطبق با قیمت: '+x.matchedWithPriceCount+' | فقط تیکر: '+x.tickerOnlyCount);dataSummary.textContent='موفق: '+b.passed+' از '+b.total+' | نماد فهرست‌نشده: '+(b.notListed||0)+' | خطا/غیرفعال: '+(b.failed||0)+'\\n\\nفهرست قراردادها:\\n'+catalogs.join('\\n')+'\\n\\nمقایسه منابع بازار:\\n'+audits.join('\\n')+'\\n\\nجزئیات تست‌ها:\\n'+lines.join('\\n')+'\\n\\nجزئیات JSON: /api/deep-test';}catch(e){dataSummary.textContent='خطا: '+e.message}finally{deepBtn.disabled=false;deepBtn.textContent='اجرای دوباره تست تایم‌فریم‌ها و Funding'}});
dataBtn.addEventListener('click',async()=>{dataBtn.disabled=true;dataBtn.textContent='در حال تست داده‌ها...';dataSummary.textContent='در حال بررسی کندل ۱۵ دقیقه و Open Interest در صرافی‌ها...';try{const r=await fetch('/api/data-test',{cache:'no-store'});if(!r.ok)throw new Error('HTTP '+r.status);const b=await r.json();dataSummary.textContent='موفق: '+b.passed+' از '+b.total+'\\n'+b.results.map(x=>x.exchange+' | '+x.kind+' | '+x.status+' | '+x.latencyMs+'ms'+(x.error?' | '+x.error:'')).join('\\n')+'\\n\\nجزئیات JSON در /api/data-test';}catch(e){dataSummary.textContent='خطا: '+e.message}finally{dataBtn.disabled=false;dataBtn.textContent='اجرای دوباره تست داده‌ها'}});
btn.addEventListener('click',async()=>{
 btn.disabled=true;btn.textContent='در حال آزمایش...';summary.textContent='درخواست‌ها به‌صورت هم‌زمان ارسال شدند.';
 try{
  const response=await fetch('/api/test',{cache:'no-store'}); if(!response.ok) throw new Error('HTTP '+response.status); const body=await response.json();
  rows.innerHTML=body.results.map(r=>'<tr><td>'+r.exchange+'</td><td class="'+(r.status==='PASS'?'pass':'fail')+'">'+r.status+'</td><td>'+(r.httpStatus??'—')+'</td><td>'+r.latencyMs+'</td><td>'+(r.error||'پاسخ معتبر')+'<br><small>API: '+(r.apiCode??'—')+' | '+(r.contentType??'بدون Content-Type')+'<br>'+r.endpoint+'<br>'+r.preview.replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))+'</small></td></tr>').join('');
  summary.textContent='موفق: '+body.passed+' از ۷ | ناموفق: '+(7-body.passed)+' | زمان پایان: '+new Date(body.finishedAt).toLocaleString();
 }catch(e){summary.textContent='خطا در اجرای تست: '+e.message}
 finally{btn.disabled=false;btn.textContent='اجرای دوباره تست'}
});
</script></body></html>`;

async function runDataSweep() {
  const results=await Promise.all(dataProbes.map(runDataProbe));
  const passed=results.filter(x=>x.status==="PASS").length;
  console.log(JSON.stringify({event:"startup_market_data_probe",finishedAt:new Date().toISOString(),passed,total:results.length,results}));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  res.setHeader("access-control-allow-origin", "*");
  if (url.pathname === "/health") {
    res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    return res.end(JSON.stringify({ ok: true, service: "exchange-api-connectivity-test", openInterestHistory: { configured: Boolean(oiSnapshotPool), databaseReady: oiSnapshotDbReady, collectorIntervalMinutes: OI_SNAPSHOT_INTERVAL_MS / 60000, note: oiSnapshotPool ? (oiSnapshotDbReady ? "Persistent snapshots enabled" : "Database configured but not ready") : "DATABASE_URL is not configured; historical snapshots are disabled" } }));
  }

  if (url.pathname === "/api/drive-backup-status") {
    res.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
    return res.end(JSON.stringify({
      configured: Boolean(process.env.GOOGLE_DRIVE_BACKUP_URL && process.env.GOOGLE_DRIVE_BACKUP_SECRET),
      databaseReady: oiSnapshotDbReady,
      intervalHours: GOOGLE_DRIVE_BACKUP_INTERVAL_MS / 3600000,
      lastBackupAt: oiDriveBackupLastAt,
      lastResult: oiDriveBackupLastResult
    }));
  }

  if (url.pathname === "/api/open-interest-status") {
    const status = await getOiSnapshotStatus();
    res.writeHead(status.databaseReady ? 200 : 503, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
    return res.end(JSON.stringify(status));
  }
  if (url.pathname === "/api/open-interest-trend") {
    if (!oiSnapshotPool || !oiSnapshotDbReady) {
      res.writeHead(503, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
      return res.end(JSON.stringify({ error: "Persistent Open Interest history is not configured", status: await getOiSnapshotStatus() }));
    }
    const hours = Math.min(168, Math.max(1, Number(url.searchParams.get("hours") || 24)));
    const exchange = url.searchParams.get("exchange");
    const params = [new Date(Date.now() - hours * 3600000)];
    let filter = "collected_at >= $1 AND metric = 'open_interest_native' AND value > 0";
    if (exchange) { params.push(exchange); filter += " AND exchange = $2"; }
    const sql = `
      WITH samples AS (
        SELECT exchange, symbol, metric, value, collected_at, source_endpoint,
          FIRST_VALUE(value) OVER (
            PARTITION BY exchange, symbol, metric ORDER BY collected_at ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS first_value,
          FIRST_VALUE(collected_at) OVER (
            PARTITION BY exchange, symbol, metric ORDER BY collected_at ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS first_at,
          FIRST_VALUE(value) OVER (
            PARTITION BY exchange, symbol, metric ORDER BY collected_at DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS last_value,
          FIRST_VALUE(collected_at) OVER (
            PARTITION BY exchange, symbol, metric ORDER BY collected_at DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS last_at,
          COUNT(*) OVER (PARTITION BY exchange, symbol, metric)::int AS sample_count
        FROM oi_snapshots
        WHERE ${filter}
      )
      SELECT DISTINCT exchange, symbol, metric,
        first_value::text AS first_value, last_value::text AS last_value,
        first_at, last_at, sample_count
      FROM samples
      WHERE sample_count >= 2
      ORDER BY exchange, symbol
      LIMIT 500
    `;
    try {
      const result = await oiSnapshotPool.query(sql, params);
      const trends = result.rows.map(row => {
        const firstValue = Number(row.first_value);
        const lastValue = Number(row.last_value);
        const changePct = firstValue > 0 ? ((lastValue - firstValue) / firstValue) * 100 : null;
        return {
          exchange: row.exchange,
          symbol: row.symbol,
          metric: row.metric,
          firstValue: row.first_value,
          lastValue: row.last_value,
          changePct: changePct === null ? null : Number(changePct.toFixed(6)),
          firstAt: row.first_at,
          lastAt: row.last_at,
          sampleCount: row.sample_count,
          elapsedMinutes: Math.max(0, Math.round((new Date(row.last_at).getTime() - new Date(row.first_at).getTime()) / 60000)),
          units: "exchange-native; compare changes only within the same exchange and symbol",
          sourceEndpoint: row.source_endpoint || null
        };
      });
      res.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
      return res.end(JSON.stringify({
        hours,
        exchange: exchange || null,
        count: trends.length,
        minSamples: 2,
        results: trends,
        warning: "Trend is computed from stored native-unit snapshots, not a cross-exchange comparison. Verify each exchange's field semantics before using it as a trading signal."
      }));
    } catch (error) {
      res.writeHead(500, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
      return res.end(JSON.stringify({ error: String(error.message || error) }));
    }
  }
  if (url.pathname === "/api/open-interest-history") {
    if (!oiSnapshotPool || !oiSnapshotDbReady) {
      res.writeHead(503, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
      return res.end(JSON.stringify({ error: "Persistent Open Interest history is not configured", status: await getOiSnapshotStatus() }));
    }
    const hours = Math.min(168, Math.max(1, Number(url.searchParams.get("hours") || 24)));
    const exchange = url.searchParams.get("exchange");
    const params = [new Date(Date.now() - hours * 3600000)];
    let sql = "SELECT exchange, symbol, metric, value::text AS value, collected_at, source_endpoint FROM oi_snapshots WHERE collected_at >= $1";
    if (exchange) { params.push(exchange); sql += " AND exchange = $2"; }
    sql += " ORDER BY collected_at DESC LIMIT 5000";
    try {
      const result = await oiSnapshotPool.query(sql, params);
      res.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
      return res.end(JSON.stringify({ hours, count: result.rowCount, results: result.rows, unitsNote: "Open Interest values are in exchange-native units; do not compare raw values across exchanges." }));
    } catch (error) {
      res.writeHead(500, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
      return res.end(JSON.stringify({ error: String(error.message || error) }));
    }
  }
  if (url.pathname === "/api/history-depth-test") {
    // Paginated 4h history validation. Each exchange has its own documented cursor/limit behavior.
    const targetCandles = 1200, intervalMs = 4 * 60 * 60 * 1000, maxPages = 20;
    const specs = [
      { exchange:"MEXC", pageLimit:2000, url:()=> "https://api.mexc.com/api/v1/contract/kline/BTC_USDT?interval=Hour4", rows:j=>{const d=j?.data||{};return (d.time||[]).map((t,i)=>({t:Number(t)*1000,c:Number(d.close?.[i])}));} },
      { exchange:"BingX", pageLimit:1000, url:(cursor)=>"https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol=BTC-USDT&interval=4h&limit=1000"+(cursor?("&startTime="+(cursor-999*intervalMs)+"&endTime="+cursor):""), rows:j=>(j?.data||[]).map(x=>({t:Number(x?.time),c:Number(x?.close)})) },
      { exchange:"OKX", pageLimit:300, url:(cursor)=>"https://www.okx.com/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=4H&limit=300"+(cursor?("&after="+cursor):""), rows:j=>(j?.data||[]).map(x=>({t:Number(x?.[0]),c:Number(x?.[4])})) },
      { exchange:"WEEX", pageLimit:100, url:(cursor)=>"https://api-contract.weex.com/capi/v3/market/historyKlines?symbol=BTCUSDT&interval=4h&limit=100"+(cursor?("&startTime="+(cursor-99*intervalMs)+"&endTime="+cursor):""), rows:j=>(Array.isArray(j)?j:[]).map(x=>({t:Number(x?.[0]),c:Number(x?.[4])})) },
      { exchange:"HTX", pageLimit:2000, url:()=> "https://api.hbdm.com/linear-swap-ex/market/history/kline?contract_code=BTC-USDT&period=4hour&size=1200", rows:j=>(j?.data||[]).map(x=>({t:Number(x?.id)*1000,c:Number(x?.close)})) },
      { exchange:"KuCoin", pageLimit:200, url:(cursor)=>"https://api-futures.kucoin.com/api/v1/kline/query?symbol=XBTUSDTM&granularity=240"+(cursor?("&to="+cursor):""), rows:j=>(j?.data||[]).map(x=>({t:Number(x?.[0])>1e12?Number(x?.[0]):Number(x?.[0])*1000,c:Number(x?.[4])})) },
      { exchange:"XT", pageLimit:1200, url:()=> "https://fapi.xt.com/future/market/v1/public/q/kline?symbol=btc_usdt&interval=4h&limit=1200", rows:j=>(Array.isArray(j?.result)?j.result:[]).map(x=>({t:Number(x?.t),c:Number(x?.c)})) }
    ];
    const results = await Promise.all(specs.map(async spec => {
      const started=Date.now(), allRows=[], pageReports=[];
      let cursor=null, lastHttpStatus=null, lastRaw="", lastError=null, pages=0;
      try {
        for(let page=0; page<maxPages && allRows.length<targetCandles; page++){
          const controller=new AbortController(), timer=setTimeout(()=>controller.abort(),TIMEOUT_MS);
          let response;
          try {
            for(let attempt=0;attempt<3;attempt++){
              response=await fetch(spec.url(cursor),{headers:{accept:"application/json","user-agent":"exchange-api-connectivity-test/1.0"},signal:controller.signal});
              lastHttpStatus=response.status; lastRaw=await response.text();
              let retryJson; try{retryJson=JSON.parse(lastRaw)}catch{retryJson=null}
              if(attempt<2 && response.ok && spec.exchange==="BingX" && [100410,100429].includes(Number(retryJson?.code))){
                await new Promise(resolve=>setTimeout(resolve,2000*(attempt+1)));
                continue;
              }
              break;
            }
          } finally { clearTimeout(timer); }
          let json; try{json=JSON.parse(lastRaw)}catch{json=null}
          if(!response.ok){lastError="HTTP "+response.status;break}
          if(spec.exchange==="BingX" && [100410,100429].includes(Number(json?.code))){lastError="BingX temporary API error "+json.code+": "+String(json?.msg||"");break}
          const parsed=spec.rows(json).filter(x=>Number.isFinite(x.t)&&Number.isFinite(x.c)&&x.c>0);
          const before=allRows.length; allRows.push(...parsed); pages++;
          const uniqueNow=[...new Map(allRows.map(x=>[x.t,x])).values()];
          const oldest=parsed.length?Math.min(...parsed.map(x=>x.t)):null;
          pageReports.push({page:page+1,rows:parsed.length,uniqueTotal:uniqueNow.length,oldestAt:oldest?new Date(oldest).toISOString():null});
          if(uniqueNow.length>=targetCandles || parsed.length===0 || oldest===null) break;
          if(spec.exchange==="MEXC"||spec.exchange==="HTX"||spec.exchange==="XT") break;
          const nextCursor=oldest-1;
          if(cursor!==null && nextCursor>=cursor){lastError="Pagination cursor did not move backward";break}
          cursor=nextCursor;
          if(allRows.length===before && page>0){lastError="Page returned no new rows";break}
        }
        allRows.sort((a,b)=>a.t-b.t);
        const unique=[...new Map(allRows.map(x=>[x.t,x])).values()];
        const gaps=[]; for(let i=1;i<unique.length;i++){const delta=unique[i].t-unique[i-1].t;if(delta!==intervalMs)gaps.push({from:new Date(unique[i-1].t).toISOString(),to:new Date(unique[i].t).toISOString(),hours:Number((delta/3600000).toFixed(2))});}
        const spanHours=unique.length>1?(unique[unique.length-1].t-unique[0].t)/3600000:0;
        const latest=unique.length?unique[unique.length-1].t:null, ageMinutes=latest===null?null:Math.round((Date.now()-latest)/60000);
        const enough=unique.length>=targetCandles;
        return {exchange:spec.exchange,status:lastHttpStatus>=200&&lastHttpStatus<300&&unique.length?"RESPONSE_OK":"FAIL",httpStatus:lastHttpStatus,requestedCandles:targetCandles,returnedRows:allRows.length,uniqueCandles:unique.length,pages, pageReports,coverageDays:Number((spanHours/24).toFixed(2)),expectedCoverageDays:Number((1199*4/24).toFixed(2)),gapCount:gaps.length,has1200Candles:enough,historyReady:enough&&gaps.length===0&&ageMinutes!==null&&ageMinutes<=480,firstCandleAt:unique.length?new Date(unique[0].t).toISOString():null,lastCandleAt:latest?new Date(latest).toISOString():null,lastCandleAgeMinutes:ageMinutes,endpoint:spec.url(null),latencyMs:Date.now()-started,error:lastError||(!unique.length?"No parseable candles":null),exampleGaps:gaps.slice(0,3),preview:lastRaw.slice(0,120)};
      } catch(error) {
        return {exchange:spec.exchange,status:"FAIL",httpStatus:lastHttpStatus,requestedCandles:targetCandles,returnedRows:allRows.length,uniqueCandles:new Set(allRows.map(x=>x.t)).size,pages,pageReports,coverageDays:0,expectedCoverageDays:Number((1199*4/24).toFixed(2)),gapCount:null,has1200Candles:false,historyReady:false,endpoint:spec.url(null),latencyMs:Date.now()-started,error:error.name==="AbortError"?"Timeout after "+TIMEOUT_MS+" ms":String(error.message||error)};
      }
    }));
    const passed=results.filter(x=>x.status==="RESPONSE_OK").length;
    const ready=results.filter(x=>x.historyReady).length;
    res.writeHead(200,{"content-type":"application/json; charset=utf-8","cache-control":"no-store"});
    return res.end(JSON.stringify({finishedAt:new Date().toISOString(),purpose:"Paginated 4h history test for CCI(1200); historyReady requires 1,200 unique candles, zero 4h gaps, and a fresh latest candle.",passed,total:results.length,historyReadyCount:ready,results,warning:"This validates BTC-USDT history only; it does not by itself confirm every symbol or production integration."}));
  }
  if (url.pathname === "/api/data-test") {
    const results = await Promise.all(dataProbes.map(runDataProbe));
    const passed = results.filter((x) => x.status === "PASS").length;
    res.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
    return res.end(JSON.stringify({ finishedAt: new Date().toISOString(), passed, total: results.length, results }));
  }
  if (url.pathname === "/api/deep-test") {
    const body = await runDeepSweep();
    res.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
    return res.end(JSON.stringify(body));
  }
  if (url.pathname === "/api/test") {
    const results = await Promise.all(probes.map(runProbe));
    const passed = results.filter((x) => x.status === "PASS").length;
    res.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
    return res.end(JSON.stringify({ finishedAt: new Date().toISOString(), passed, total: results.length, results }));
  }
  if (url.pathname === "/" || url.pathname === "/index.html") {
    res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    return res.end(page);
  }
  res.writeHead(404, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify({ error: "Not found" }));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Exchange API test service listening on ${PORT}`);
  if (oiSnapshotPool) {
    initOiSnapshotDb().then(async ready => {
      console.log(JSON.stringify({ event: "open_interest_snapshot_db", ready, error: oiSnapshotDbError }));
      if (ready) {
        await collectOpenInterestSnapshots();
        setInterval(collectOpenInterestSnapshots, OI_SNAPSHOT_INTERVAL_MS).unref();
        await backupOpenInterestToGoogleDrive();
        setInterval(backupOpenInterestToGoogleDrive, GOOGLE_DRIVE_BACKUP_INTERVAL_MS).unref();
      }
    });
  } else {
    console.log(JSON.stringify({ event: "open_interest_snapshot_db", ready: false, reason: "DATABASE_URL not configured" }));
  }
  // Run one diagnostic sweep after each deploy/restart so results are available in Render logs.
  Promise.all(probes.map(runProbe)).then(async (results) => {
    const passed = results.filter((item) => item.status === "PASS").length;
    console.log(JSON.stringify({ event: "startup_exchange_probe", finishedAt: new Date().toISOString(), passed, total: results.length, results }));
    await runDataSweep();
    const deep = await runDeepSweep();
    console.log(JSON.stringify({ event: "startup_deep_market_probe", ...deep }));
    const historyResponse = await fetch(`http://127.0.0.1:${PORT}/api/history-depth-test`, { headers: { accept: "application/json" }, signal: AbortSignal.timeout(30000) });
    const historyBody = await historyResponse.json();
    console.log(JSON.stringify({ event: "startup_history_depth_probe", httpStatus: historyResponse.status, ...historyBody }));
  }).catch((error) => {
    console.error("Startup exchange probe failed:", error?.message || String(error));
  });
});
