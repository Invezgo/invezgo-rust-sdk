# Changelog

## 0.6.0

### Added
- `analysis().get_large_orders(..)` (`GET /analysis/large-orders/{code}`, realtime): the day's largest aggressor orders (up to 300) with side, lot, value, VWAP, trade count, broker and foreign flag.
- `analysis().get_footprint(..)` (`GET /analysis/footprint/{code}`, realtime): volume per minute per price, split by aggressor side.
- `analysis().get_candle_brokers(..)` (`GET /analysis/candle-brokers/{code}`): aggressive and passive brokers, net per broker and foreign lots for a time window of up to 1 hour.
- Types: `LargeOrdersParams` / `LargeOrdersResponse`, `FootprintParams` / `FootprintResponse`, `CandleBrokersParams` / `CandleBrokersResponse`.

### Changed
- `analysis().get_stock_running_trade(..)`: `limit` now allows at most 500 rows per page (was 1000).
- Batch order book: `time` accepts `HH:mm:ss` as well as `HH:mm`; the nearest snapshot on that date is returned.

## 0.5.0

Breaking: `realtime::Channel` gains the `Stock` and `Index` variants; exhaustive `match` on it needs new arms.

### Added
- Realtime: `subscribe_stock(codes)` (stock quote: OHLC, volume, value, freq, IEP/IEV; max 50 codes) and `subscribe_index(codes)` (index quotes, all indices when `codes` is empty; max 100 codes), delivered as `RealtimeEvent::Stock(StockQuote)` and `RealtimeEvent::Index(IndexQuote)`. `unsubscribe()` accepts `Channel::Stock` / `Channel::Index`; both are replayed after reconnect.

## 0.4.0

Breaking: `InvezgoError` gains the `Webhook` and `WebSocket` variants; exhaustive `match` on it needs a new arm.

### Added
- Optional cargo feature `webhook`: `webhook::verify_webhook`, `verify_webhook_signature` and `parse_webhook` (HMAC-SHA256 over `timestamp.rawBody`, constant-time compare, 300 s tolerance), `WebhookEvent` with `name()`/`data_as()`, and typed data models for `update.*`, `alert.*`, `test.ping` and `quota.*`.
- Optional cargo feature `websocket`: `client.realtime()` / `realtime::Realtime::builder(api_key)` real-time client for `wss://api.invezgo.com/ws` (running trade and order book) delivering `RealtimeEvent`s via `next().await` or `Stream`, with subscriptions queued until `connected`, auto-reconnect (1-30 s backoff) and subscription replay.
- `InvezgoError::Webhook { reason, message }`, `InvezgoError::WebSocket { close_code, message }` and `WebhookErrorReason`.

## 0.3.0

Regenerated from the latest OpenAPI spec (Invezgo API 1.2, 109 endpoints).

### Added
- `analysis().get_flow_of_funds(code, &FlowOfFundsParams)`: Flow of Funds (revenue → profit flow per period, with nodes, links and bridge) for `/analysis/flow-of-funds/{code}`.
- `analysis().get_free_float(code, &FreeFloatParams)`: free float composition over time for `/analysis/free-float/{code}`.
- `type` filter on `analysis().get_disclosure` (30 disclosure categories).
- `id` field on disclosure items; `foreign_*` breakdown fields on shareholder classification and classify-table responses.

### Changed (breaking)
- Response models gained new public fields, so code that builds them with struct literals must add the new fields.
- Screener and alert `formula` now accepts up to 2048 characters.
- Order book `time` accepts `HH:mm:ss` as well as `HH:mm`.
- Usage logs `limit` maximum raised to 520000.

### Fixed
- Crate `repository` URL now points to https://github.com/Invezgo/invezgo-rust-sdk.

## 0.2.0

Rewrite generated from the official Invezgo OpenAPI spec (`codegen/gen_rust.py`).

### Added
- All 107 endpoints, fully typed: request bodies (`models::*Request`), query params (`params::*Params` with
  `new(required..)`), request enums, and responses for every endpoint (no more `()` / `serde_json::Value`).
- Rustdoc on every method (summary, description, route, cache tier) and field; generated `docs/API.md`.
- L1 in-memory LRU response cache, on by default (`CacheConfig { ttl: 60s, realtime_ttl: 5s, max_entries: 1000,
  max_bytes: 32 MiB }`), canonical query keys, automatic purge of personal data after mutations,
  `InvezgoClient::clear_cache()`, `InvezgoClientBuilder::no_cache()`.
- Multiple API keys: `additional_api_keys([...])`; automatic rotation on HTTP 429 (per-endpoint block for the
  per-minute limit, whole-key block for the monthly quota, honouring `Retry-After` / `X-RateLimit-Reset`).
- Errors: `InvezgoError::Api(Box<ApiError>)` with `ErrorKind`, `InvezgoError::RateLimited(Box<RateLimitError>)`,
  `InvezgoError::Config`, helpers `status()` and `is_timeout()`.
- Tolerant decoding: missing/unexpected `null` fields default, numeric strings parse to numbers.
- `InvezgoClient::new(api_key)` shortcut; `Debug` never prints API keys.

### Changed (breaking)
- Models moved to `invezgo_sdk::models`, params to `invezgo_sdk::params`; DTOs renamed `*Dto` → `*Request`.
- Methods follow the spec, e.g. `usage().get_api_usage()`, `analysis().get_stock_running_trade(..)`,
  `batch().get_order_book(..)`.
- Query parameters are passed as one params struct instead of positional `Option`s.
- MSRV 1.82; the unused `url` dependency and `reqwest`'s `json` feature were dropped.

### Removed
- `membership` and `recommendation` resources (not part of the official spec).

### Migrating from 0.1

| 0.1.x | 0.2.0 |
|---|---|
| `models::analysis::*`, `models::shared::*`, `models::batch::*` | All models in `invezgo_sdk::models`, query params in `invezgo_sdk::params` |
| Query parameters as positional `Option<..>` arguments | One `XxxParams` struct per endpoint (`::new(required..)` plus `Option` fields) |
| `CreateWatchlistDto`, `ScreenDto`, ... | `AddWatchlistRequest`, `ScreenRequest`, ... (`*Request`) |
| `usage().usage_api()`, `analysis().stock_running_trade(..)`, `batch().get_order_book_batch(..)` | `usage().get_api_usage()`, `analysis().get_stock_running_trade(..)`, `batch().get_order_book(..)` |
| `InvezgoError::ApiError { .. }`, `InvezgoError::Other` | `InvezgoError::Api(Box<ApiError>)` (with `kind`), `InvezgoError::RateLimited`, `InvezgoError::Config` |
| Some endpoints returned `()` / `serde_json::Value` | Every endpoint is fully typed |
| No cache | Cache is on by default; use `.no_cache()` for the previous behaviour |

Other method names follow the `get_*` / `list` / `add` / `update` / `delete` pattern; see `docs/API.md`.

## 0.1.1

- Initial release.
