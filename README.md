# Invezgo Rust SDK

[![Crates.io](https://img.shields.io/crates/v/invezgo-sdk.svg)](https://crates.io/crates/invezgo-sdk)
[![Docs.rs](https://docs.rs/invezgo-sdk/badge.svg)](https://docs.rs/invezgo-sdk)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

SDK resmi untuk [Invezgo API](https://invezgo.com), penyedia data pasar modal Indonesia (BEI/IDX). SDK ini menyediakan akses bertipe ke seluruh endpoint Invezgo API, dilengkapi cache in-memory dan rotasi API key otomatis.

[Dokumentasi API](https://api.invezgo.com/documentation) · [Referensi SDK](docs/API.md) · [Changelog](CHANGELOG.md)

## Daftar Isi

- [Persyaratan](#persyaratan)
- [Instalasi](#instalasi)
- [Penggunaan](#penggunaan)
- [Konfigurasi](#konfigurasi)
- [Memanggil Endpoint](#memanggil-endpoint)
- [Tipe Data](#tipe-data)
- [Caching](#caching)
- [Rotasi API Key](#rotasi-api-key)
- [Penanganan Error](#penanganan-error)
- [Pagination](#pagination)
- [Praktik yang Disarankan](#praktik-yang-disarankan)
- [Pengembangan](#pengembangan)
- [Dukungan](#dukungan)
- [Lisensi](#lisensi)

## Persyaratan

- Rust 1.82 atau lebih baru
- Runtime Tokio (dibutuhkan oleh `reqwest`)
- API key Invezgo, dapat dibuat di [halaman pengaturan API](https://invezgo.com/id/setting/api)

SDK ini menggunakan `reqwest` dengan `rustls`, sehingga tidak membutuhkan OpenSSL.

## Instalasi

```toml
[dependencies]
invezgo-sdk = "0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Penggunaan

```rust,no_run
use invezgo_sdk::{params::TopChangeParams, InvezgoClient};

#[tokio::main]
async fn main() -> invezgo_sdk::Result<()> {
    let client = InvezgoClient::new(std::env::var("INVEZGO_API_KEY").unwrap_or_default())?;

    let stocks = client.analysis().get_stock_list().await?;
    let info = client.analysis().get_information("BBCA").await?;
    let top = client.analysis().get_top_change(&TopChangeParams::new("2024-12-30")).await?;

    println!("{} saham, {:?}", stocks.len(), info);
    println!("{} {}", top.gain[0].code, top.gain[0].change);
    Ok(())
}
```

> **Penting:** simpan API key di environment variable atau secret manager, bukan di source code.

## Konfigurasi

```rust,no_run
use std::time::Duration;
use invezgo_sdk::{CacheConfig, InvezgoClient};

# fn main() -> invezgo_sdk::Result<()> {
let client = InvezgoClient::builder()
    .api_key(std::env::var("INVEZGO_API_KEY").unwrap_or_default())
    .additional_api_keys([std::env::var("INVEZGO_API_KEY_2").unwrap_or_default()])
    .timeout(Duration::from_secs(30))
    .cache(CacheConfig {
        ttl: Duration::from_secs(60),
        realtime_ttl: Duration::from_secs(5),
        ..Default::default()
    })
    .build()?;
# Ok(()) }
```

| Method builder | Tipe | Default | Keterangan |
|---|---|---|---|
| `api_key` | `impl Into<String>` | wajib | API key utama. Nilai kosong menghasilkan `InvezgoError::Config`. |
| `additional_api_keys` | `impl IntoIterator<Item = impl Into<String>>` | kosong | API key cadangan, digunakan saat terjadi `429`. Lihat [Rotasi API Key](#rotasi-api-key). |
| `base_url` | `impl Into<String>` | `https://api.invezgo.com` | Base URL API. |
| `timeout` | `Duration` | 30 detik | Timeout per request. |
| `cache` | `CacheConfig` | aktif | Konfigurasi cache. |
| `no_cache` | — | — | Menonaktifkan cache. |

| Field `CacheConfig` | Tipe | Default | Keterangan |
|---|---|---|---|
| `ttl` | `Duration` | 60 detik | TTL endpoint reguler. `Duration::ZERO` menonaktifkan tier ini. |
| `realtime_ttl` | `Duration` | 5 detik | TTL endpoint realtime. `Duration::ZERO` menonaktifkan tier ini. |
| `max_entries` | `usize` | `1000` | Jumlah maksimum entri cache. |
| `max_bytes` | `usize` | `33554432` | Ukuran maksimum cache (32 MiB). |

Buat satu instance client dan gunakan kembali di seluruh aplikasi. `InvezgoClient` dapat di-`clone()` dengan biaya rendah; seluruh clone berbagi connection pool, cache, dan status rotasi API key.

## Memanggil Endpoint

Endpoint dikelompokkan berdasarkan resource:

| Resource | Cakupan |
|---|---|
| `analysis()` | Daftar saham, broker, dan indeks; harga, chart, intraday, order book, running trade; broker summary; pemegang saham; laporan keuangan; kalender aksi korporasi; berita dan keterbukaan informasi |
| `batch()` | Order book dan data intraday untuk beberapa saham dalam satu request |
| `search()` | Pencarian saham dan pengguna |
| `screener()` | Eksekusi screener dan pengelolaan preset |
| `usage()` | Pemakaian dan sisa kuota API |
| `watchlists()` | Watchlist dan grup watchlist |
| `journals()` | Jurnal transaksi |
| `portfolios()` | Portofolio |
| `alerts()` | Alert berbasis formula |
| `posts()` | Post, komentar, like, dan vote komunitas |
| `trades()` | Realized trade |

Setiap method mengikuti pola berikut:

```text
client.<resource>().<method>(path_params..., body?, params?).await
```

| Argumen | Keterangan |
|---|---|
| `path_params` | Parameter path sebagai `&str` (atau enum jika nilainya terbatas), sesuai urutan di URL. |
| `body` | Request body untuk `POST`, `PUT`, `PATCH`, dan `DELETE`, bertipe `&models::*Request`. |
| `params` | Query parameter dalam satu struct `&params::<Nama>Params`. Bertipe `Option<&<Nama>Params>` jika seluruh query parameter bersifat opsional. |

Query parameter wajib menjadi argumen `<Nama>Params::new(...)`; parameter opsional bertipe `Option<T>`.

```rust,no_run
use invezgo_sdk::models::ScreenRequest;
use invezgo_sdk::params::{
    BrokerStalkerInvestor, BrokerStalkerMarket, BrokerStalkerParams, StockRunningTradeParams,
    StockRunningTradeType,
};

# async fn run(client: invezgo_sdk::InvezgoClient) -> invezgo_sdk::Result<()> {
// Path parameter dan query parameter wajib
let params = BrokerStalkerParams::new("2024-12-01", "2024-12-31", BrokerStalkerInvestor::All, BrokerStalkerMarket::Rg);
client.analysis().get_broker_stalker("YP", "BBCA", &params).await?;

// Query parameter opsional
let params = StockRunningTradeParams {
    r#type: Some(StockRunningTradeType::Buy),
    limit: Some(100),
    ..StockRunningTradeParams::new("2024-12-30")
};
client.analysis().get_stock_running_trade("BBCA", &params).await?;

// Request body
let body = ScreenRequest {
    formula: "close > prev AND volume > 1000000".into(),
    category: Some(vec!["SYARIAH".into()]),
};
client.screener().screen(&body).await?;

// Beberapa saham sekaligus, dihitung sebagai satu request
client.batch().get_order_book("BBCA|BBRI|BMRI", None).await?;
# Ok(()) }
```

Nama field mengikuti nama parameter API dalam snake_case. Keyword Rust di-escape, misalnya `type` menjadi `r#type`.

Daftar lengkap method, parameter, nilai default, dan tipe kembalian tersedia di [docs/API.md](docs/API.md) atau melalui `cargo doc --open`.

## Tipe Data

Model request dan response tersedia di modul `invezgo_sdk::models`, sedangkan query parameter dan enum-nya tersedia di `invezgo_sdk::params`.

```rust,no_run
use invezgo_sdk::models::TopChangeResponse;
use invezgo_sdk::params::{TopChangeFilterColumn, TopChangeFilterOperator, TopChangeParams};

# async fn run(client: invezgo_sdk::InvezgoClient) -> invezgo_sdk::Result<()> {
let params = TopChangeParams {
    filter_column: Some(TopChangeFilterColumn::Change),
    filter_operator: Some(TopChangeFilterOperator::Gt),
    filter_value: Some(5.0),
    ..TopChangeParams::new("2024-12-30")
};

let top: TopChangeResponse = client.analysis().get_top_change(&params).await?;
# let _ = top;
# Ok(()) }
```

Ketentuan tipe:

| Kasus | Representasi |
|---|---|
| Parameter enum | `enum` Rust, misalnya `StockRunningTradeType::Buy`; tersedia `as_str()` dan `Display` |
| Field nullable atau opsional | `Option<T>` |
| Response array | `Vec<T>` |
| Response yang dapat bernilai `null` | `Option<T>`, misalnya `get_shareholder_classify_table` |
| Angka yang dapat dikirim sebagai string | Di-parse otomatis ke `f64` atau `i64` |
| Nilai enum pada response | `String` |
| Objek dengan key dinamis | `HashMap<String, T>` |

Field yang tidak ada atau bernilai `null` secara tidak terduga diisi dengan nilai default, sehingga perubahan kecil pada response tidak menggagalkan proses decoding. Seluruh model mengimplementasikan `Serialize`, `Deserialize`, `Clone`, `Debug`, `Default`, dan `PartialEq`.

Response berbentuk union dapat dibedakan melalui field `type`:

```rust,no_run
use invezgo_sdk::params::ShareholderRelationParams;

# async fn run(client: invezgo_sdk::InvezgoClient) -> invezgo_sdk::Result<()> {
let params = ShareholderRelationParams { code: Some("BBCA".into()), ..Default::default() };
let graph = client.analysis().get_shareholder_relation(Some(&params)).await?;

for node in &graph.nodes {
    if node.r#type == "stock" {
        println!("{:?} {:?}", node.code, node.company_name);
    } else {
        println!("{}", node.label);
    }
}
# Ok(()) }
```

## Caching

Cache in-memory aktif secara default untuk mengurangi pemakaian kuota dan latensi.

- Hanya request `GET` yang disimpan. Cache key terdiri dari path dan query string yang telah dinormalisasi.
- Cache menggunakan algoritma LRU dengan batas jumlah entri (`max_entries`) dan total ukuran (`max_bytes`). Entri yang paling lama tidak digunakan akan dihapus saat salah satu batas terlampaui.
- Endpoint realtime (order book, running trade, intraday, chart, batch, dan sejenisnya) menggunakan `realtime_ttl`. Endpoint lainnya menggunakan `ttl`.
- Endpoint `usage` dan request selain `GET` tidak disimpan di cache.
- Setiap request `POST`, `PUT`, `PATCH`, atau `DELETE` yang berhasil akan menghapus cache data personal (`watchlists`, `journals`, `portfolios`, `trades`, `alerts`, `screener`).

```rust,no_run
use std::time::Duration;
use invezgo_sdk::{CacheConfig, InvezgoClient};

# fn main() -> invezgo_sdk::Result<()> {
// Nonaktifkan cache
let client = InvezgoClient::builder().api_key("API_KEY").no_cache().build()?;

// Nonaktifkan cache untuk endpoint realtime, TTL endpoint reguler 5 menit
let client = InvezgoClient::builder()
    .api_key("API_KEY")
    .cache(CacheConfig {
        ttl: Duration::from_secs(300),
        realtime_ttl: Duration::ZERO,
        ..Default::default()
    })
    .build()?;

// Hapus seluruh cache
client.clear_cache();
# Ok(()) }
```

## Rotasi API Key

Setiap API key memiliki batas request per menit untuk setiap endpoint dan kuota bulanan. Jika Anda memiliki lebih dari satu API key, tambahkan sebagai cadangan:

```rust,no_run
# fn main() -> invezgo_sdk::Result<()> {
let client = invezgo_sdk::InvezgoClient::builder()
    .api_key(std::env::var("INVEZGO_API_KEY").unwrap_or_default())
    .additional_api_keys([
        std::env::var("INVEZGO_API_KEY_2").unwrap_or_default(),
        std::env::var("INVEZGO_API_KEY_3").unwrap_or_default(),
    ])
    .build()?;
# Ok(()) }
```

Ketika API mengembalikan `429 Too Many Requests`:

1. Lama penundaan diambil dari header `Retry-After`. Jika tidak tersedia, digunakan `X-RateLimit-Reset`, kemudian 60 detik.
2. Untuk batas per menit, API key ditangguhkan hanya pada endpoint tersebut. Untuk kuota bulanan, API key ditangguhkan pada seluruh endpoint.
3. Request dikirim ulang menggunakan API key berikutnya yang tersedia.
4. API key utama digunakan kembali setelah masa penangguhan berakhir.
5. Jika seluruh API key sedang ditangguhkan, SDK mengembalikan `InvezgoError::RateLimited` tanpa mengirim request.

Rotasi hanya dilakukan untuk status `429`. Status penangguhan aman digunakan bersama oleh banyak task. Untuk endpoint data personal seperti watchlist dan portofolio, gunakan API key dari akun yang sama.

Sisa kuota dapat diperiksa tanpa mengurangi kuota:

```rust,no_run
# async fn run(client: invezgo_sdk::InvezgoClient) -> invezgo_sdk::Result<()> {
let usage = client.usage().get_api_usage().await?;
println!("{} {} {} {}", usage.usage, usage.limit, usage.remaining, usage.expire);
# Ok(()) }
```

## Penanganan Error

Seluruh method mengembalikan `invezgo_sdk::Result<T>` dengan tipe error `InvezgoError`.

| Variant | Penyebab |
|---|---|
| `Api(ApiError)` | Status HTTP non-2xx selain `429`. Kategori tersedia di `ApiError::kind`. |
| `RateLimited(RateLimitError)` | Batas request atau kuota tercapai pada seluruh API key |
| `Http` | Kegagalan jaringan, TLS, atau timeout (`is_timeout()`) |
| `Json` | Response tidak sesuai dengan tipe yang diharapkan |
| `Config` | Konfigurasi client tidak valid |

| `ErrorKind` | Status HTTP | Penyebab |
|---|---|---|
| `Validation` | 400, 422 | Parameter atau body tidak valid |
| `Authentication` | 401 | API key tidak valid atau kedaluwarsa |
| `PaymentRequired` | 402 | Endpoint tidak termasuk dalam paket langganan |
| `NotFound` | 404 | Data tidak ditemukan |
| `Server` | 5xx | Kesalahan pada server |
| `Other` | lainnya | Status lain |

`ApiError` menyediakan `status`, `kind`, `message`, `error`, `request_id`, `path`, dan `body`. `RateLimitError` menyediakan `retry_after` (`Duration`), `limit`, `remaining`, `reset`, `quota_exceeded`, dan `message`. Method `InvezgoError::status()` mengembalikan kode HTTP jika tersedia.

```rust,no_run
use invezgo_sdk::params::{SummaryStockInvestor, SummaryStockMarket, SummaryStockParams};
use invezgo_sdk::{ErrorKind, InvezgoError};

# async fn run(client: invezgo_sdk::InvezgoClient) {
let params = SummaryStockParams::new("2024-12-01", "2024-12-31", SummaryStockInvestor::All, SummaryStockMarket::Rg);

match client.analysis().get_summary_stock("BBCA", &params).await {
    Ok(summary) => println!("{} baris", summary.len()),
    Err(InvezgoError::RateLimited(e)) => {
        eprintln!("Rate limit tercapai, coba lagi dalam {:?}", e.retry_after);
    }
    Err(InvezgoError::Api(e)) if e.kind == ErrorKind::Validation => {
        eprintln!("Parameter tidak valid: {}", e.message);
    }
    Err(InvezgoError::Api(e)) => {
        eprintln!("{} {} {:?}", e.status, e.message, e.request_id);
    }
    Err(e) => eprintln!("{e}"),
}
# }
```

## Pagination

Endpoint dengan pagination mengembalikan `page`, `total_page`, dan `next_page`. Nilai `next_page` adalah `None` pada halaman terakhir.

```rust,no_run
use invezgo_sdk::params::NewsParams;

# async fn run(client: invezgo_sdk::InvezgoClient) -> invezgo_sdk::Result<()> {
let mut page = Some(1);

while let Some(current) = page {
    let params = NewsParams { page: Some(current), limit: Some(50), ..Default::default() };
    let res = client.analysis().get_news(Some(&params)).await?;
    for item in &res.data {
        println!("{}", item.title);
    }
    page = res.next_page;
}
# Ok(()) }
```

## Praktik yang Disarankan

- Gunakan satu instance client untuk seluruh aplikasi, lalu `clone()` ke task lain.
- Simpan API key di environment variable atau secret manager.
- Gunakan format tanggal `YYYY-MM-DD`. Data end-of-day tersedia setelah pukul 18.00 WIB.
- Field waktu bersufiks `Z` menggunakan waktu WIB (UTC+7); jangan dikonversi ke zona waktu lain.
- Gunakan resource `batch()` untuk mengambil data beberapa saham sekaligus.
- Saat menerima `InvezgoError::RateLimited`, tunggu sesuai `retry_after` sebelum mencoba kembali.
- Untuk polling data realtime, sesuaikan `realtime_ttl` dengan interval polling.

Contoh lengkap tersedia di direktori [examples](examples/) dan dapat dijalankan dengan `cargo run --example demo`.

## Pengembangan

File `src/models.rs`, `src/params.rs`, `src/resources.rs`, dan `docs/API.md` dihasilkan dari spesifikasi OpenAPI dan tidak boleh diubah secara manual.

```bash
python ../codegen/generate.py rust              # generate ulang kode dari OpenAPI
cargo clippy --all-targets -- -D warnings      # linting
cargo test                                     # unit test, integration test, dan doctest
```

## Dukungan

Sertakan nilai `request_id` dari error saat menghubungi tim dukungan Invezgo.

- Website: [invezgo.com](https://invezgo.com)
- Dokumentasi API: [api.invezgo.com/documentation](https://api.invezgo.com/documentation)

## Lisensi

Dirilis di bawah [MIT License](LICENSE).
