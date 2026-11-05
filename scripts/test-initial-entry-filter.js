import test from "node:test";
import assert from "node:assert/strict";
import {
  baseSymbolFromContract, initialEntryDecision, newInitialFilterCounts,
  recordInitialFilterDecision
} from "./initial-entry-filter.js";

test("base asset extraction supports all seven exchange symbol formats", () => {
  assert.equal(baseSymbolFromContract("MEXC", "USDC_USDT"), "USDC");
  assert.equal(baseSymbolFromContract("BingX", "USDT-USDT"), "USDT");
  assert.equal(baseSymbolFromContract("OKX", "1000SHIB-USDT-SWAP"), "SHIB");
  assert.equal(baseSymbolFromContract("WEEX", "1000USDTUSDT"), "USDT");
  assert.equal(baseSymbolFromContract("HTX", "DAI-USDT"), "DAI");
  assert.equal(baseSymbolFromContract("KuCoin", "XBTUSDTM"), "XBT");
  assert.equal(baseSymbolFromContract("XT", "USD1_USDT"), "USD1");
});

test("known USD-pegged stablecoins are rejected before price checks", () => {
  for (const [exchange, symbol] of [
    ["MEXC", "USDT_USDT"], ["BingX", "USDC-USDT"], ["OKX", "DAI-USDT-SWAP"],
    ["WEEX", "USDEUSDT"], ["HTX", "USDD-USDT"], ["KuCoin", "USDPUSDTM"],
    ["XT", "1000USDT_USDT"]
  ]) {
    assert.equal(initialEntryDecision(exchange, symbol, 1).reason, "USD_STABLECOIN");
  }
});

test("price above 30 is rejected, while exactly 30 is allowed", () => {
  assert.equal(initialEntryDecision("OKX", "BTC-USDT-SWAP", 30.0001).reason, "PRICE_ABOVE_30_USDT");
  assert.equal(initialEntryDecision("OKX", "ALT-USDT-SWAP", 30).allowed, true);
  assert.equal(initialEntryDecision("OKX", "ALT-USDT-SWAP", 0.01).allowed, true);
});

test("multiplier-denominated contracts are compared by underlying coin price", () => {
  assert.equal(initialEntryDecision("OKX", "1000SHIB-USDT-SWAP", 30000).allowed, true);
  assert.equal(initialEntryDecision("OKX", "1000SHIB-USDT-SWAP", 30001).reason, "PRICE_ABOVE_30_USDT");
});

test("missing, zero, and invalid prices fail closed", () => {
  assert.equal(initialEntryDecision("MEXC", "ALT_USDT", undefined).reason, "PRICE_UNAVAILABLE");
  assert.equal(initialEntryDecision("MEXC", "ALT_USDT", 0).reason, "PRICE_UNAVAILABLE");
  assert.equal(initialEntryDecision("MEXC", "ALT_USDT", "NaN").reason, "PRICE_UNAVAILABLE");
});

test("filter counts track allowed and rejected symbols", () => {
  const counts = newInitialFilterCounts();
  recordInitialFilterDecision(counts, initialEntryDecision("OKX", "ALT-USDT-SWAP", 30));
  recordInitialFilterDecision(counts, initialEntryDecision("OKX", "USDC-USDT-SWAP", 1));
  recordInitialFilterDecision(counts, initialEntryDecision("OKX", "EXPENSIVE-USDT-SWAP", 31));
  assert.deepEqual(counts, {
    candidates: 3, allowed: 1, USD_STABLECOIN: 1, PRICE_ABOVE_30_USDT: 1, PRICE_UNAVAILABLE: 0
  });
});
