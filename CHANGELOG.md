# Changelog

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
