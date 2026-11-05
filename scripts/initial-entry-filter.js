export const USD_STABLECOIN_BASES = new Set([
  "USDT", "USDC", "USDE", "USDS", "USDD", "USDP", "DAI", "USD1", "USDG",
  "PYUSD", "RLUSD", "FDUSD", "TUSD", "GUSD", "GHO", "USDF", "USDT0",
  "BUSD", "UST", "USTC", "FRAX", "CRVUSD", "MIM", "LUSD", "SUSD",
  "DOLA", "ALUSD", "FEI", "EUSD", "USDK", "USDJ", "USN", "USDX",
  "USD0", "USDB", "USDA", "USDH", "USDL", "USDQ", "USDZ", "USX",
  "USDC.E", "USDT.E", "USDCN", "USDTB", "USDR", "USDT+", "USDC+", "USDCV"
]);

export function baseSymbolFromContract(exchange, symbol) {
  const value = String(symbol ?? "").trim().toUpperCase();
  let base = value;
  if (exchange === "MEXC" || exchange === "XT") base = value.split("_")[0];
  else if (exchange === "BingX" || exchange === "OKX" || exchange === "HTX") base = value.split("-")[0];
  else if (exchange === "WEEX") base = value.replace(/USDT$/, "");
  else if (exchange === "KuCoin") base = value.replace(/USDTM$/, "");
  for (const multiplier of ["1000000", "100000", "10000", "1000"]) {
    if (base.startsWith(multiplier) && base.length > multiplier.length) {
      base = base.slice(multiplier.length);
      break;
    }
  }
  return base;
}

export function initialEntryDecision(exchange, symbol, price) {
  const base = baseSymbolFromContract(exchange, symbol);
  if (USD_STABLECOIN_BASES.has(base)) {
    return { allowed: false, reason: "USD_STABLECOIN", price: null };
  }
  const last = Number(price);
  if (!Number.isFinite(last) || last <= 0) {
    return { allowed: false, reason: "PRICE_UNAVAILABLE", price: null };
  }
  if (last > 30) {
    return { allowed: false, reason: "PRICE_ABOVE_30_USDT", price: last };
  }
  return { allowed: true, reason: "ALLOWED", price: last };
}

export function newInitialFilterCounts() {
  return { candidates: 0, allowed: 0, USD_STABLECOIN: 0, PRICE_ABOVE_30_USDT: 0, PRICE_UNAVAILABLE: 0 };
}

export function recordInitialFilterDecision(counts, decision) {
  counts.candidates += 1;
  if (decision.allowed) counts.allowed += 1;
  else counts[decision.reason] += 1;
}
