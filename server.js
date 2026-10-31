import http from "node:http";

const PORT = Number(process.env.PORT || 3000);
const TIMEOUT_MS = 12000;

const probes = [
  {
    exchange: "MEXC",
    url: "https://contract.mexc.com/api/v1/contract/ticker",
    check: (j) => j && (j.success === true || j.code === 0) && (Array.isArray(j.data) || (j.data && typeof j.data === "object")),
  },
  {
    exchange: "BingX",
    url: "https://open-api.bingx.com/openApi/swap/v2/quote/ticker",
    check: (j) => j && (j.code === 0 || j.code === "0") && j.data !== undefined,
  },
  {
    exchange: "OKX",
    url: "https://www.okx.com/api/v5/market/tickers?instType=SWAP",
    check: (j) => j && String(j.code) === "0" && Array.isArray(j.data),
  },
  {
    exchange: "WEEX",
    url: "https://api-contract.weex.com/capi/v3/market/ticker/24hr?symbol=BTCUSDT",
    check: (j) => j && (j.code === 0 || j.code === "0" || Array.isArray(j) || typeof j.lastPrice === "string" || typeof j.last === "string"),
  },
  {
    exchange: "HTX",
    url: "https://api.hbdm.com/linear-swap-ex/market/detail/merged?contract_code=BTC-USDT",
    check: (j) => j && j.status === "ok" && j.tick !== undefined,
  },
  {
    exchange: "KuCoin",
    url: "https://api-futures.kucoin.com/api/v1/allTickers",
    check: (j) => j && String(j.code) === "200000" && j.data !== undefined,
  },
  {
    exchange: "XT",
    url: "https://fapi.xt.com/future/market/v1/public/ticker?symbol=btc_usdt",
    check: (j) => j && (j.rc === 0 || j.rc === "0" || j.code === 0 || j.code === "0" || j.result !== undefined || j.data !== undefined),
  },
];

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
    return {
      exchange: probe.exchange,
      status: response.ok && schemaOk ? "PASS" : "FAIL",
      httpStatus: response.status,
      latencyMs: Date.now() - started,
      schemaOk,
      preview: raw.slice(0, 260),
      error: !response.ok ? `HTTP ${response.status}` : (!json ? "Response was not valid JSON" : (!schemaOk ? "Unexpected response structure or API-level error" : null)),
      endpoint: probe.url,
    };
  } catch (error) {
    return {
      exchange: probe.exchange,
      status: "FAIL",
      httpStatus: null,
      latencyMs: Date.now() - started,
      schemaOk: false,
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
<button id="run">اجرای تست هر ۷ صرافی</button>
<div class="summary" id="summary">هنوز تستی اجرا نشده است.</div>
<table><thead><tr><th>صرافی</th><th>نتیجه</th><th>HTTP</th><th>زمان (ms)</th><th>جزئیات</th></tr></thead><tbody id="rows"></tbody></table>
<p><small>PASS یعنی پاسخ HTTP موفق و ساختار پایه پاسخ قابل تشخیص بوده است؛ این تست به‌تنهایی تضمین‌کننده پایداری یا پوشش کامل بازار نیست.</small></p>
</main>
<script>
const btn=document.getElementById('run'), rows=document.getElementById('rows'), summary=document.getElementById('summary');
btn.addEventListener('click',async()=>{
 btn.disabled=true;btn.textContent='در حال آزمایش...';summary.textContent='درخواست‌ها به‌صورت هم‌زمان ارسال شدند.';
 try{
  const response=await fetch('/api/test'); const body=await response.json();
  rows.innerHTML=body.results.map(r=>'<tr><td>'+r.exchange+'</td><td class="'+(r.status==='PASS'?'pass':'fail')+'">'+r.status+'</td><td>'+(r.httpStatus??'—')+'</td><td>'+r.latencyMs+'</td><td>'+(r.error||'پاسخ معتبر')+'<br><small>'+r.preview.replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))+'</small></td></tr>').join('');
  summary.textContent='موفق: '+body.passed+' از ۷ | ناموفق: '+(7-body.passed)+' | زمان پایان: '+new Date(body.finishedAt).toLocaleString();
 }catch(e){summary.textContent='خطا در اجرای تست: '+e.message}
 finally{btn.disabled=false;btn.textContent='اجرای دوباره تست'}
});
</script></body></html>`;

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  res.setHeader("access-control-allow-origin", "*");
  if (url.pathname === "/health") {
    res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    return res.end(JSON.stringify({ ok: true, service: "exchange-api-connectivity-test" }));
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
});
