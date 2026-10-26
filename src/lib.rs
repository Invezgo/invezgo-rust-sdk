//! # Invezgo SDK for Rust
//!
//! Fully typed async client for the [Invezgo API](https://invezgo.com) (Indonesia Stock Exchange data),
//! generated from the official OpenAPI spec.
//!
//! * Every endpoint grouped per resource: `client.analysis()`, `client.watchlists()`, ...
//! * Typed request bodies ([`models`]), query parameters ([`params`]) and responses ([`models`]).
//! * Built-in L1 LRU response cache (on by default, bounded by entries and bytes).
//! * Multiple API keys with automatic rotation on HTTP 429 (per-minute limit or monthly quota).
//!
//! ```no_run
//! use invezgo_sdk::{params::TopChangeParams, InvezgoClient, InvezgoError};
//!
//! # async fn run() -> Result<(), InvezgoError> {
//! let client = InvezgoClient::builder()
//!     .api_key("PRIMARY_KEY")
//!     .additional_api_keys(["BACKUP_KEY"])
//!     .build()?;
//!
//! let top = client.analysis().get_top_change(&TopChangeParams::new("2024-12-30")).await?;
//! for s in &top.gain {
//!     println!("{} {:+.2}%", s.code, s.change);
//! }
//! # Ok(()) }
//! ```
//!
//! See `docs/API.md` for every method.

mod cache;
mod client;
mod de;
pub mod error;
pub mod models;
pub mod params;
pub mod resources;
mod transport;

pub use client::{CacheConfig, InvezgoClient, InvezgoClientBuilder, DEFAULT_BASE_URL};
pub use error::{ApiError, ErrorKind, InvezgoError, RateLimitError, Result};

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;
