//! Error types returned by every SDK call.

use std::time::Duration;

/// Shorthand for `Result<T, InvezgoError>`.
pub type Result<T, E = InvezgoError> = std::result::Result<T, E>;

/// Every failure an SDK call can produce.
#[derive(Debug, thiserror::Error)]
pub enum InvezgoError {
    /// Network failure, timeout or TLS error (see [`InvezgoError::is_timeout`]).
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    /// The response (or request body) could not be (de)serialized.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// The API answered with a non-2xx status other than 429.
    #[error(transparent)]
    Api(Box<ApiError>),
    /// HTTP 429 and no other API key is available (per-minute limit or monthly quota).
    #[error(transparent)]
    RateLimited(Box<RateLimitError>),
    /// Invalid client configuration (e.g. empty API key).
    #[error("configuration error: {0}")]
    Config(String),
}

impl InvezgoError {
    /// HTTP status code, when the error came from an HTTP response.
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Api(e) => Some(e.status),
            Self::RateLimited(_) => Some(429),
            Self::Http(e) => e.status().map(|s| s.as_u16()),
            _ => None,
        }
    }

    /// `true` when the request timed out.
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Http(e) if e.is_timeout())
    }
}

/// Category of an [`ApiError`], derived from the HTTP status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// 400 / 422: invalid parameters or body.
    Validation,
    /// 401: missing, invalid or expired API key.
    Authentication,
    /// 402: your plan does not include this endpoint.
    PaymentRequired,
    /// 404: resource not found.
    NotFound,
    /// 5xx: server-side failure, usually safe to retry later.
    Server,
    /// Any other status.
    Other,
}

impl ErrorKind {
    pub(crate) fn from_status(status: u16) -> Self {
        match status {
            400 | 422 => Self::Validation,
            401 => Self::Authentication,
            402 => Self::PaymentRequired,
            404 => Self::NotFound,
            s if s >= 500 => Self::Server,
            _ => Self::Other,
        }
    }
}

/// A non-2xx response from the Invezgo API.
#[derive(Debug, Clone, thiserror::Error)]
#[error("Invezgo API error {status} ({kind:?}): {message}")]
pub struct ApiError {
    /// HTTP status code.
    pub status: u16,
    /// Category derived from `status`.
    pub kind: ErrorKind,
    /// Human-readable message (array messages are joined with `"; "`).
    pub message: String,
    /// HTTP status text from the body, e.g. `Unauthorized`.
    pub error: Option<String>,
    /// Request ID to quote to Invezgo support.
    pub request_id: Option<String>,
    /// Request path reported by the API.
    pub path: Option<String>,
    /// Decoded JSON body, or the raw text as a JSON string when it is not JSON.
    pub body: serde_json::Value,
}

/// HTTP 429 after every configured API key was rate limited.
#[derive(Debug, Clone, thiserror::Error)]
#[error("Invezgo rate limit (retry after {}s): {message}", retry_after.as_secs())]
pub struct RateLimitError {
    /// How long until the earliest key becomes usable again.
    pub retry_after: Duration,
    /// `X-RateLimit-Limit` of the last 429 response (requests per minute).
    pub limit: Option<u64>,
    /// `X-RateLimit-Remaining` of the last 429 response.
    pub remaining: Option<u64>,
    /// `X-RateLimit-Reset` of the last 429 response (Unix seconds).
    pub reset: Option<u64>,
    /// `true` when the monthly quota (not the per-minute limit) is exhausted.
    pub quota_exceeded: bool,
    /// Message of the last 429 response.
    pub message: String,
    /// Request ID of the last 429 response.
    pub request_id: Option<String>,
}
