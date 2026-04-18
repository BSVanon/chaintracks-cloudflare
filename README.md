# chaintracks-cloudflare

BSV block header tracking service on Cloudflare Workers. Rust compiled to WASM.

Reimplementation of the Node.js [`chaintracks-server`](https://github.com/bsv-blockchain/chaintracks-server) as a single Cloudflare Worker, replacing a 2× VPS + Docker deployment. Serves block header queries and merkle root validation for SPV consumers.

## What it does

- Polls WhatsOnChain every minute (Workers cron) for new block headers
- Stores headers in Cloudflare D1 (~945k rows), serves bulk headers from R2
- Exposes 12 HTTP endpoints matching the original TS server's public API
- Detects and handles reorgs up to 400 blocks deep
- Validates merkle roots for SPV consumers

## Differences from Node.js chaintracks-server

Cloudflare Workers are stateless and request-based, so several TS server features are intentionally dropped or reshaped:

- **No event subscriptions.** TS exposes `subscribeHeaders()` / `subscribeReorgs()` for live callbacks. Workers cannot hold persistent channels — consumers should poll `/getInfo` or `/currentHeight`.
- **No WebSocket ingestion.** TS uses `LiveIngestorWhatsOnChainWs` for push ingest. Workers cannot hold outbound WebSocket connections. Rust uses HTTP polling via 1-minute cron.
- **Manual bulk header export.** TS auto-exports CDN bulk header files every 30 min. Rust exposes `/admin/export-r2` as a manual endpoint — trigger it directly or set up an external scheduler to call it.
- **No in-process watchdog.** TS runs a self-check loop that restarts its Docker container on stall. Workers handle restarts at the infra layer; redundant.
- **Simpler multi-source fallback.** TS reads Babbage CDN + WhatsOnChain with watchdog failover. Rust falls back to a configurable upstream chaintracks URL during catch-up, then direct WhatsOnChain calls.

None of these are missing features — they're architectural trade-offs for serverless. All public HTTP endpoints are at parity with TS.

## HTTP API

| Endpoint | Description |
|---|---|
| `GET /` | Health check (plain text) |
| `GET /getChain` | Returns `"main"` or `"test"` |
| `GET /getInfo` | Service status (height, header count, sync state) |
| `GET /currentHeight` | Current chain tip height |
| `GET /findChainTipHashHex` | Chain tip block hash |
| `GET /findChainTipHeaderHex` | Chain tip header (JSON object) |
| `GET /findHeaderHexForHeight?height=N` | Header at height N |
| `GET /findHeaderHexForBlockHash?hash=H` | Header by block hash (active chain only) |
| `GET /getHeaders?height=N&count=M` | M headers starting at N (concatenated hex) |
| `GET /isValidRootForHeight?root=R&height=N` | Validate merkle root at height |
| `GET /admin/bulk-sync?file=IDX` | Admin: ingest bulk header file from CDN |
| `GET /admin/export-r2` | Admin: export D1 headers to R2 bulk files |

## Architecture

```
Request  → lib.rs → routes.rs → storage.rs → D1
Cron 1m  → lib.rs (scheduled) → sync.rs → WhatsOnChain → D1
Bulk     → R2 bucket (CDN replacement)
```

- **lib.rs** — Worker entry: `#[event(fetch)]` + `#[event(scheduled)]`
- **routes.rs** — HTTP routing, 12 endpoints
- **storage.rs** — D1 read/write operations
- **sync.rs** — Cron-triggered chain sync, reorg detection
- **d1.rs** — Parameterized D1 query builder
- **types.rs** — `BlockHeader`, `Chain`, `ChaintracksInfo`, 80-byte serialization

## Cloudflare Bindings

| Binding | Type | Purpose |
|---|---|---|
| `DB` | D1 | Block header storage |
| `BULK_HEADERS` | R2 | Bulk header binary files |
| `CHAIN` | Var | `"main"` or `"test"` |
| `WHATSONCHAIN_API_KEY` | Var/Secret | Optional WoC API key |

## Build and Deploy

```bash
npm install
npm run dev              # local dev (D1 emulated)
worker-build --release   # build WASM
npm run deploy           # deploy to Cloudflare Workers
```

### Initial Cloudflare setup

1. Create a Cloudflare account, note your `account_id`
2. `npx wrangler d1 create chaintracks-cloudflare` — record the returned `database_id`
3. `npx wrangler r2 bucket create chaintracks-cloudflare-headers`
4. Fill `wrangler.toml` with your `account_id` and `database_id`
5. Apply migrations: `npx wrangler d1 migrations apply chaintracks-cloudflare --remote`
6. Deploy: `npm run deploy`
7. (Optional) Set WhatsOnChain key: `echo "<key>" | npx wrangler secret put WHATSONCHAIN_API_KEY`

## Testing

Quality gates — run all five before shipping:

```bash
cargo fmt --all
cargo clippy --target wasm32-unknown-unknown -- -D warnings
cargo check --target wasm32-unknown-unknown
cargo test --lib
worker-build --release
```

- **Unit tests:** `cargo test --lib` (79 tests covering serialization, storage logic, reorg detection)
- **Comparison:** `tests/e2e/compare.sh` (13-test parity check against a reference chaintracks instance)

## Consumers

Any service implementing the `ChainTracker` trait from bsv-rs can point at this worker. Known consumers:

- `rust-wallet-infra` — merkle root validation
- `rust-overlay` — `WorkerChainTracker` for `/findHeaderHexForHeight`, `/currentHeight`

## License

MIT — see [LICENSE](LICENSE).
