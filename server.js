import http from "node:http";

const PORT = Number(process.env.PORT || 3000);
const TIMEOUT_MS = 15000;

const probes = [
  {
    exchange: "MEXC",
    url: "https://contract.mexc.com/api/v1/contract/ticker",
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
  { exchange:"MEXC", kind:"candles_15m", url:"https://contract.mexc.com/api/v1/contract/kline/BTC_USDT?interval=Min15", check:j=>j?.success===true && j?.code===0 && (Array.isArray(j?.data?.time) ? j.data.time.length>0 && Array.isArray(j.data.close) && Number(j.data.close[0])>0 : Array.isArray(j?.data) && j.data.length>0) },
  { exchange:"BingX", kind:"candles_15m", url:"https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol=BTC-USDT&interval=15m&limit=3", check:j=>(j?.code===0||j?.code==="0") && Array.isArray(j?.data) && j.data.length>0 && Number(j.data[0]?.close)>0 },
  { exchange:"BingX", kind:"open_interest", url:"https://open-api.bingx.com/openApi/swap/v2/quote/openInterest?symbol=BTC-USDT", check:j=>(j?.code===0||j?.code==="0") && j?.data && Number(j.data.openInterest)>0 },
  { exchange:"OKX", kind:"candles_15m", url:"https://www.okx.com/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=15m&limit=3", check:j=>String(j?.code)==="0" && Array.isArray(j?.data) && j.data.length>0 && Number(j.data[0]?.[4])>0 },
  { exchange:"OKX", kind:"open_interest", url:"https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=BTC-USDT-SWAP", check:j=>String(j?.code)==="0" && Array.isArray(j?.data) && j.data.length>0 && Number(j.data[0]?.oi)>0 },
  { exchange:"WEEX", kind:"candles_15m", url:"https://api-contract.weex.com/capi/v3/market/klines?symbol=BTCUSDT&interval=15m&limit=3", check:j=>Array.isArray(j) && j.length>0 && Number(j[0]?.[4])>0 },
  { exchange:"WEEX", kind:"open_interest", url:"https://api-contract.weex.com/capi/v3/market/openInterest?symbol=BTCUSDT", check:j=>j?.symbol==="BTCUSDT" && Number(j.openInterest)>0 },
  { exchange:"HTX", kind:"candles_15m", url:"https://api.hbdm.com/linear-swap-ex/market/history/kline?contract_code=BTC-USDT&period=15min&size=3", check:j=>j?.status==="ok" && Array.isArray(j.data) && j.data.length>0 && Number(j.data[0]?.close)>0 },
  { exchange:"HTX", kind:"open_interest", url:"https://api.hbdm.com/linear-swap-api/v1/swap_open_interest?contract_code=BTC-USDT", check:j=>j?.status==="ok" && j?.data && (Array.isArray(j.data) ? j.data.some(x=>Number(x?.volume??x?.amount??x?.open_interest)>0) : Number(j.data.volume??j.data.amount??j.data.open_interest)>0) },
  { exchange:"KuCoin", kind:"candles_15m", url:"https://api.kucoin.com/api/ua/v2/market/kline?symbol=XBTUSDTM&tradeType=FUTURES&klineType=TRADE&interval=15min", check:j=>String(j?.code)==="200000" && Array.isArray(j?.data?.list) && j.data.list.length>0 && Number(j.data.list[0]?.[4])>0 },
  { exchange:"KuCoin", kind:"open_interest_15m", url:"https://api.kucoin.com/api/ua/v2/market/open-interest?symbol=XBTUSDTM&interval=15min&pageSize=3", check:j=>String(j?.code)==="200000" && Array.isArray(j?.data) && j.data.length>0 && Number(j.data[0]?.openInterest)>0 },
  { exchange:"XT", kind:"candles_15m", url:"https://fapi.xt.com/future/market/v1/public/q/kline?symbol=btc_usdt&interval=15m&limit=3", check:j=>j?.returnCode===0 && (Array.isArray(j?.result) ? j.result.length>0 : Array.isArray(j?.result?.items) && j.result.items.length>0) },
  { exchange:"XT", kind:"open_interest", url:"https://fapi.xt.com/future/market/v1/public/contract/open-interest?symbol=btc_usdt", check:j=>j?.returnCode===0 && j?.result && Number(j.result.openInterest??j.result.openInterestUsd)>0 },
];

async function runDataProbe(probe) {
  const started=Date.now(), controller=new AbortController(), timer=setTimeout(()=>controller.abort(),TIMEOUT_MS);
  try {
    const response=await fetch(probe.url,{headers:{accept:"application/json","user-agent":"exchange-api-connectivity-test/1.0"},signal:controller.signal});
    const raw=await response.text(); let json; try{json=JSON.parse(raw)}catch{json=null}
    const schemaOk=Boolean(json&&probe.check(json));
    return {exchange:probe.exchange,kind:probe.kind,status:response.ok&&schemaOk?"PASS":"FAIL",httpStatus:response.status,latencyMs:Date.now()-started,schemaOk,endpoint:probe.url,preview:raw.slice(0,220),error:!response.ok?`HTTP ${response.status}`:!json?"Invalid JSON":!schemaOk?"Response schema/value check failed":null};
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
<button id="run">اجرای تست تیکر هر ۷ صرافی</button>\n<button id="dataRun" style="margin-right:8px;background:#0f766e">تست کندل و Open Interest</button>\n<pre id="dataSummary" style="white-space:pre-wrap;overflow-wrap:anywhere;color:#cbd5e1;background:#111c2e;padding:12px;border-radius:10px;margin-top:14px">تست تکمیلی داده‌ها هنوز اجرا نشده است.</pre>
<div class="summary" id="summary">هنوز تستی اجرا نشده است.</div>
<table><thead><tr><th>صرافی</th><th>نتیجه</th><th>HTTP</th><th>زمان (ms)</th><th>جزئیات</th></tr></thead><tbody id="rows"></tbody></table>
<p><small>PASS یعنی پاسخ HTTP موفق و ساختار پایه پاسخ قابل تشخیص بوده است؛ این تست به‌تنهایی تضمین‌کننده پایداری یا پوشش کامل بازار نیست.</small></p>
</main>
<script>
const btn=document.getElementById('run'), rows=document.getElementById('rows'), summary=document.getElementById('summary');\nconst dataBtn=document.getElementById('dataRun'), dataSummary=document.getElementById('dataSummary');\ndataBtn.addEventListener('click',async()=>{dataBtn.disabled=true;dataBtn.textContent='در حال تست داده‌ها...';dataSummary.textContent='در حال بررسی کندل ۱۵ دقیقه و Open Interest در صرافی‌ها...';try{const r=await fetch('/api/data-test',{cache:'no-store'});if(!r.ok)throw new Error('HTTP '+r.status);const b=await r.json();dataSummary.textContent='موفق: '+b.passed+' از '+b.total+'\\n'+b.results.map(x=>x.exchange+' | '+x.kind+' | '+x.status+' | '+x.latencyMs+'ms'+(x.error?' | '+x.error:'')).join('\\n')+'\\n\\nجزئیات JSON در /api/data-test';}catch(e){dataSummary.textContent='خطا: '+e.message}finally{dataBtn.disabled=false;dataBtn.textContent='اجرای دوباره تست داده‌ها'}});
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
    return res.end(JSON.stringify({ ok: true, service: "exchange-api-connectivity-test" }));
  }
  if (url.pathname === "/api/data-test") {
    const results = await Promise.all(dataProbes.map(runDataProbe));
    const passed = results.filter((x) => x.status === "PASS").length;
    res.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
    return res.end(JSON.stringify({ finishedAt: new Date().toISOString(), passed, total: results.length, results }));
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
  // Run one diagnostic sweep after each deploy/restart so results are available in Render logs.
  Promise.all(probes.map(runProbe)).then(async (results) => {
    const passed = results.filter((item) => item.status === "PASS").length;
    console.log(JSON.stringify({ event: "startup_exchange_probe", finishedAt: new Date().toISOString(), passed, total: results.length, results }));
    await runDataSweep();
  }).catch((error) => {
    console.error("Startup exchange probe failed:", error?.message || String(error));
  });
});
