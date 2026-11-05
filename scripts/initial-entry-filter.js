export const USD_STABLECOIN_BASES = new Set([
  "USDT", "USDC", "USDE", "USDS", "USDD", "USDP", "DAI", "USD1", "USDG",
  "PYUSD", "RLUSD", "FDUSD", "TUSD", "GUSD", "GHO", "USDF", "USDT0",
  "BUSD", "UST", "USTC", "FRAX", "CRVUSD", "MIM", "LUSD", "SUSD",
  "DOLA", "ALUSD", "FEI", "EUSD", "USDK", "USDJ", "USN", "USDX",
  "USD0", "USDB", "USDA", "USDH", "USDL", "USDQ", "USDZ", "USX",
  "USDC.E", "USDT.E", "USDCN", "USDTB", "USDR", "USDT+", "USDC+", "USDCV"
]);

function rawBaseSymbolFromContract(exchange, symbol) {
  const value = String(symbol ?? "").trim().toUpperCase();
  if (exchange === "MEXC" || exchange === "XT") return value.split("_")[0];
  if (exchange === "BingX" || exchange === "OKX" || exchange === "HTX") return value.split("-")[0];
  if (exchange === "WEEX") return value.replace(/USDT$/, "");
  if (exchange === "KuCoin") return value.replace(/USDTM$/, "");
  return value;
}

export function baseSymbolFromContract(exchange, symbol) {
  let base = rawBaseSymbolFromContract(exchange, symbol);
  for (const multiplier of ["1000000", "100000", "10000", "1000"]) {
    if (base.startsWith(multiplier) && base.length > multiplier.length) {
      base = base.slice(multiplier.length);
      break;
    }
  }
  return base;
}

function contractPriceDivisor(exchange, symbol) {
  const rawBase = rawBaseSymbolFromContract(exchange, symbol);
  for (const multiplier of ["1000000", "100000", "10000", "1000"]) {
    if (rawBase.startsWith(multiplier) && rawBase.length > multiplier.length) {
      return Number(multiplier);
    }
  }
  return 1;
}

export function initialEntryDecision(exchange, symbol, price) {
  const base = baseSymbolFromContract(exchange, symbol);
  if (USD_STABLECOIN_BASES.has(base)) {
    return { allowed: false, reason: "USD_STABLECOIN", price: null };
  }
  const contractPrice = Number(price);
  if (!Number.isFinite(contractPrice) || contractPrice <= 0) {
    return { allowed: false, reason: "PRICE_UNAVAILABLE", price: null };
  }
  const underlyingPrice = contractPrice / contractPriceDivisor(exchange, symbol);
  if (underlyingPrice > 30) {
    return { allowed: false, reason: "PRICE_ABOVE_30_USDT", price: underlyingPrice };
  }
  return { allowed: true, reason: "ALLOWED", price: underlyingPrice };
}

export function newInitialFilterCounts() {
  return { candidates: 0, allowed: 0, USD_STABLECOIN: 0, PRICE_ABOVE_30_USDT: 0, PRICE_UNAVAILABLE: 0 };
}

export function recordInitialFilterDecision(counts, decision) {
  counts.candidates += 1;
  if (decision.allowed) counts.allowed += 1;
  else counts[decision.reason] += 1;
}
