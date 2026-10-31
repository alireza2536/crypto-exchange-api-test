# Crypto Exchange API Test

Independent public-market API connectivity checker for MEXC, BingX, OKX, WEEX, HTX, KuCoin, and XT.

## Run locally

Requires Node.js 20 or newer.

```bash
npm start
```

Open `http://localhost:3000`.

## Endpoints

- `GET /` — Persian test dashboard
- `GET /health` — health check
- `GET /api/test` — runs all seven public GET probes concurrently and returns JSON
- On every service start/deploy, the same seven probes run automatically and their structured results are written to Render application logs

No API keys or account credentials are used. Each request has a 15-second timeout. Probes run concurrently.

## Deploy on Render

Create a new Web Service from this repository. Render can use the included `render.yaml`, or set:

- Runtime: Node
- Build command: `npm install`
- Start command: `npm start`
- Health check path: `/health`

## Interpreting results

PASS requires an HTTP-success response and a recognizable exchange response structure. FAIL can mean a changed endpoint/schema, rate limiting, network restriction, or exchange-side outage. A PASS confirms only this public endpoint probe at that moment; it does not certify all futures endpoints or trading operations.

XT uses the official public USDT-M futures all-tickers endpoint `/future/market/v1/public/q/tickers`; the response is validated using `returnCode` and the ticker fields `s` (symbol) and `c` (last price).
