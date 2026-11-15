//! Webhook signature verification and payload parsing (feature `webhook`).
//!
//! Standalone helpers: no client or API key is needed, only the webhook `secret`.
//!
//! ```no_run
//! use invezgo_sdk::webhook::{verify_webhook, VerifyOptions, WebhookUpdateData};
//! # fn handle(body: &[u8], headers: &reqwest::header::HeaderMap) -> invezgo_sdk::Result<()> {
//! let event = verify_webhook(body, headers, "WEBHOOK_SECRET", VerifyOptions::default())?;
//! if event.domain == "update" {
//!     let data: WebhookUpdateData = event.data_as()?;
//!     println!("{} {:?}", event.name(), data.path);
//! }
//! # Ok(()) }
//! ```

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use reqwest::header::HeaderMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::error::{InvezgoError, WebhookErrorReason};

/// Options for [`verify_webhook`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifyOptions {
    /// Maximum allowed difference between `X-Invezgo-Timestamp` and now (default 300 s).
    pub tolerance: Duration,
    /// Current Unix time in seconds; `None` uses the system clock.
    pub now: Option<u64>,
}

impl Default for VerifyOptions {
    fn default() -> Self {
        Self { tolerance: Duration::from_secs(300), now: None }
    }
}

/// A webhook delivery. `domain` and `type` stay plain strings so new events never break decoding.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct WebhookEvent {
    /// Delivery ID (same as `X-Invezgo-Delivery`, unchanged on retries; use it to deduplicate).
    pub id: String,
    /// Event group: `test`, `quota`, `update` or `alert`.
    pub domain: String,
    /// Event type within the domain, e.g. `summary`.
    #[serde(rename = "type")]
    pub r#type: String,
    /// Unix time (seconds) when the event was created.
    pub created_at: i64,
    /// Event-specific payload; decode it with [`WebhookEvent::data_as`].
    pub data: serde_json::Value,
}

impl WebhookEvent {
    /// `"{domain}.{type}"`, e.g. `update.summary` (same as `X-Invezgo-Event`).
    pub fn name(&self) -> String {
        format!("{}.{}", self.domain, self.r#type)
    }

    /// Decode `data` into one of the typed models, e.g. [`WebhookUpdateData`] for `update.*`.
    pub fn data_as<T: DeserializeOwned>(&self) -> Result<T, InvezgoError> {
        Ok(T::deserialize(&self.data)?)
    }
}

/// `data` of every `update.*` event (summary, news, disclosure, financial, ...).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct WebhookUpdateData {
    /// `YYYY-MM-DD` (`YYYY-MM-DD HH:mm:ss` WIB for `news`).
    pub date: String,
    /// API endpoints whose data changed.
    pub path: Vec<String>,
    /// Stock code, for per-issuer events.
    pub code: Option<String>,
    /// Title, for `news` and `disclosure`.
    pub title: Option<String>,
}

/// A stock entering or leaving a Live Alert formula.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct WebhookAlertStock {
    /// Stock code.
    pub code: String,
    /// Last price when the change was detected.
    pub price: f64,
}

/// `data` of `alert.triggered`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct WebhookAlertTriggeredData {
    /// Live Alert ID.
    pub alert_id: String,
    /// Live Alert name.
    pub alert_name: String,
    /// Stocks that entered the formula.
    pub stocks_in: Vec<WebhookAlertStock>,
    /// Stocks that left the formula.
    pub stocks_out: Vec<WebhookAlertStock>,
}

/// `data` of `alert.disabled`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct WebhookAlertDisabledData {
    /// Live Alert ID.
    pub alert_id: String,
    /// Live Alert name.
    pub alert_name: String,
    /// Why the alert was disabled.
    pub reason: String,
}

/// `data` of `test.ping`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct WebhookTestPingData {
    /// Test message.
    pub message: String,
}

/// `data` of `quota.low` and `quota.exhausted`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct WebhookQuotaData {
    /// Plan name, e.g. `PRIME`.
    pub plan: String,
    /// Monthly request quota.
    pub limit: i64,
    /// Requests used this period.
    pub used: i64,
    /// Requests remaining this period.
    pub remaining: i64,
    /// Unix time (seconds) when the quota resets.
    pub reset_at: Option<i64>,
}

fn fail(reason: WebhookErrorReason, message: impl Into<String>) -> InvezgoError {
    InvezgoError::Webhook { reason, message: message.into() }
}

/// Verify a delivery using its headers (looked up case-insensitively) and parse the event.
///
/// `raw_body` must be the exact bytes received, before any JSON parsing.
pub fn verify_webhook(
    raw_body: &[u8],
    headers: &HeaderMap,
    secret: &str,
    options: VerifyOptions,
) -> Result<WebhookEvent, InvezgoError> {
    let get = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("");
    verify_webhook_signature(raw_body, get("x-invezgo-signature"), get("x-invezgo-timestamp"), secret, options)
}

/// Verify a delivery from the `X-Invezgo-Signature` and `X-Invezgo-Timestamp` values and parse the event.
pub fn verify_webhook_signature(
    raw_body: &[u8],
    signature: &str,
    timestamp: &str,
    secret: &str,
    options: VerifyOptions,
) -> Result<WebhookEvent, InvezgoError> {
    if timestamp.is_empty() || !timestamp.bytes().all(|b| b.is_ascii_digit()) {
        return Err(fail(WebhookErrorReason::Timestamp, "X-Invezgo-Timestamp must be Unix seconds"));
    }
    let now = options
        .now
        .unwrap_or_else(|| SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0));
    match timestamp.parse::<u64>() {
        Ok(ts) if ts.abs_diff(now) <= options.tolerance.as_secs() => {}
        _ => return Err(fail(WebhookErrorReason::Timestamp, "X-Invezgo-Timestamp is outside the tolerance")),
    }

    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(raw_body);
    let mut expected = String::from("sha256=");
    for b in mac.finalize().into_bytes() {
        expected.push_str(&format!("{b:02x}"));
    }
    // constant-time comparison (the length is not secret)
    let same = expected.len() == signature.len()
        && expected.bytes().zip(signature.bytes()).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0;
    if !same {
        return Err(fail(WebhookErrorReason::Signature, "X-Invezgo-Signature does not match"));
    }
    parse_webhook(raw_body)
}

/// Parse a delivery body without verifying it (tests or already verified bodies).
pub fn parse_webhook(raw_body: &[u8]) -> Result<WebhookEvent, InvezgoError> {
    serde_json::from_slice(raw_body).map_err(|e| fail(WebhookErrorReason::Payload, e.to_string()))
}
