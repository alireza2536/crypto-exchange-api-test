const STATUS_URL = "https://full-universe-ohlc-audit.onrender.com/api/full-universe-ohlc-status";
const EXCHANGES = ["MEXC", "BingX", "OKX", "WEEX", "HTX", "KuCoin", "XT"];
const STABLE = new Set(("USDT USDC USDE USDS USDD USDP DAI USD1 USDG PYUSD RLUSD FDUSD TUSD GUSD GHO USDF USDT0 BUSD UST USTC FRAX CRVUSD MIM LUSD SUSD DOLA ALUSD FEI EUSD USDK USDJ USN USDX USD0 USDB USDA USDH USDL USDQ USDZ USX USDC.E USDT.E USDCN USDTB USDR USDT+ USDC+ USDCV AUSD USR USDY USYC USDM USDV USDO USDN USDAI USDFI USDSC").split(" "));
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function json(url) {
  const r = await fetch(url, {headers:{accept:"application/json","user-agent":"initial-filter-live-count-audit/1.0"},signal:AbortSignal.timeout(20000)});
  const t = await r.text();
  if (!r.ok) throw new Error("HTTP "+r.status+" "+url);
  try { return JSON.parse(t); } catch { throw new Error("Invalid JSON "+url); }
}
function baseOf(ex,s) {
  let x=String(s||"").toUpperCase();
  if(ex==="OKX") x=x.replace(/-USDT-SWAP$/,"");
  else if(ex==="BingX"||ex==="HTX") x=x.replace(/-USDT$/,"");
  else if(ex==="MEXC"||ex==="XT") x=x.replace(/[_-]USDT$/,"");
  else if(ex==="KuCoin") x=x.replace(/USDTM$/,"");
  else if(ex==="WEEX") x=x.replace(/USDT$/,"");
  x=x.replace(/^(1000000|100000|10000|1000)/,"");
  return x;
}
function rows(j) {
  if(Array.isArray(j)) return j;
  if(Array.isArray(j?.data)) return j.data;
  if(Array.isArray(j?.data?.ticker)) return j.data.ticker;
  if(Array.isArray(j?.result)) return j.result;
  return [];
}
function priceOf(row) {
  for(const k of ["lastPrice","last","price","c","close"]) {
    const n=Number(row?.[k]);
    if(Number.isFinite(n)&&n>0) return n;
  }
  return null;
}
async function concurrent(items,n,fn) {
  const out=new Array(items.length); let next=0;
  await Promise.all(Array.from({length:Math.min(n,items.length)},async()=>{
    while(true){const i=next++;if(i>=items.length)return;try{out[i]=await fn(items[i],i)}catch(e){out[i]={_error:String(e.message||e)}}}
  }));
  return out;
}
async function bulkPrices(ex) {
  if(ex==="MEXC") return new Map(rows(await json("https://api.mexc.com/api/v1/contract/ticker")).map(x=>[String(x.symbol).toUpperCase(),priceOf(x)]));
  if(ex==="BingX") return new Map(rows(await json("https://open-api.bingx.com/openApi/swap/v2/quote/ticker")).map(x=>[String(x.symbol).toUpperCase(),priceOf(x)]));
  if(ex==="OKX") return new Map(rows(await json("https://www.okx.com/api/v5/market/tickers?instType=SWAP")).map(x=>[String(x.instId).toUpperCase(),priceOf({last:x.last})]));
  if(ex==="KuCoin") return new Map(rows(await json("https://api-futures.kucoin.com/api/v1/allTickers")).map(x=>[String(x.symbol).toUpperCase(),priceOf(x)]));
  if(ex==="XT") return new Map(rows(await json("https://fapi.xt.com/future/market/v1/public/q/tickers")).map(x=>[String(x.s).toUpperCase(),priceOf({c:x.c})]));
  return null;
}
async function individualPrice(ex,symbol) {
  if(ex==="WEEX") return priceOf(await json("https://api-contract.weex.com/capi/v3/market/ticker/24hr?symbol="+encodeURIComponent(symbol)));
  const j=await json("https://api.hbdm.com/linear-swap-ex/market/detail/merged?contract_code="+encodeURIComponent(symbol));
  return priceOf(j?.tick);
}
const status = await json(STATUS_URL);
if(status?.status!=="COMPLETED" || !status?.result?.results) throw new Error("Full-universe audit status is not COMPLETED; refusing to estimate.");
const report=[];
for(const ex of EXCHANGES) {
  const er=status.result.results.find(x=>x.exchange===ex);
  if(!er || !Array.isArray(er.symbolResults) || er.symbolResults.length!==er.discoveredCount) throw new Error(ex+" audit universe incomplete; refusing to estimate.");
  const symbols=[...new Set(er.symbolResults.map(x=>x.symbol).filter(Boolean))];
  let prices=null, bulkError=null;
  try { prices=await bulkPrices(ex); } catch(e) { bulkError=String(e.message||e); }
  const per=prices?[]:await concurrent(symbols,ex==="BingX"?1:3,async s=>({symbol:s,price:await individualPrice(ex,s)}));
  const perMap=new Map(per.filter(x=>x&&!x._error).map(x=>[String(x.symbol).toUpperCase(),x.price]));
  const counts={total:symbols.length,stablecoin:0,over30:0,eligible:0,unavailable:0};
  const details=[];
  for(const s of symbols) {
    const b=baseOf(ex,s);
    if(STABLE.has(b)){counts.stablecoin++;details.push({symbol:s,reason:"stablecoin"});continue;}
    const p=prices?.get(String(s).toUpperCase()) ?? perMap.get(String(s).toUpperCase()) ?? null;
    if(!(Number.isFinite(p)&&p>0)){counts.unavailable++;details.push({symbol:s,reason:"price_unavailable"});continue;}
    if(p>30){counts.over30++;details.push({symbol:s,reason:"price_over_30",price:p});continue;}
    counts.eligible++;
  }
  report.push({exchange:ex,...counts,priceSource:prices?"bulk_ticker": "per_symbol",bulkError});
}
const total=report.reduce((a,x)=>{for(const k of ["total","stablecoin","over30","eligible","unavailable"])a[k]+=x[k];return a;},{total:0,stablecoin:0,over30:0,eligible:0,unavailable:0});
const lines=["# Live initial-entry filter count audit","", "As of: "+new Date().toISOString(),"Source universe: completed full-universe audit; all symbols must match the audited discovered count.","Rule: exclude USD-pegged stablecoin bases; exclude last price > $30; exactly $30 is eligible; unavailable prices are fail-closed.","","| Exchange | Universe | Stablecoin | Price > $30 | Price unavailable | Eligible |","|---|---:|---:|---:|---:|---:|",...report.map(x=>`| ${x.exchange} | ${x.total} | ${x.stablecoin} | ${x.over30} | ${x.unavailable} | ${x.eligible} |`),"",`| **Total** | **${total.total}** | **${total.stablecoin}** | **${total.over30}** | **${total.unavailable}** | **${total.eligible}** |`,"","Counts are exchange-specific contract listings; the same coin listed on multiple exchanges is counted once per exchange."];
console.log(JSON.stringify({report,total,markdown:lines.join("\n")},null,2));
