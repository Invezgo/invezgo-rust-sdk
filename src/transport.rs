//! Request pipeline: canonical URL, L1 cache, API key rotation on 429, error mapping.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::header::{HeaderMap, ACCEPT, CONTENT_TYPE};
use reqwest::Method;
use serde::de::DeserializeOwned;

use crate::cache::LruCache;
use crate::client::CacheConfig;
use crate::error::{ApiError, ErrorKind, InvezgoError, RateLimitError};
use crate::resources::PERSONAL_SEGMENTS;

/// Cache tier of an operation (from the spec's `[REALTIME]` tag).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheTier {
    None,
    Default,
    Realtime,
}

/// Percent-encodes like JavaScript's `encodeURIComponent`.
pub(crate) fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[derive(Default)]
struct Blocks {
    /// key index → blocked until (monthly quota exhausted).
    key: HashMap<usize, Instant>,
    /// (key index, route template) → blocked until (per-minute limit of that endpoint).
    route: HashMap<(usize, &'static str), Instant>,
    last_quota: bool,
}

impl Blocks {
    fn until(&self, i: usize, route: &'static str) -> Option<Instant> {
        let a = self.key.get(&i).copied();
        let b = self.route.get(&(i, route)).copied();
        a.max(b)
    }

    fn pick(&mut self, route: &'static str, now: Instant, tried: &[bool]) -> Option<usize> {
        self.key.retain(|_, u| *u > now);
        self.route.retain(|_, u| *u > now);
        (0..tried.len()).find(|&i| !tried[i] && self.until(i, route).is_none_or(|u| u <= now))
    }

    fn min_wait(&self, route: &'static str, now: Instant, n: usize) -> Duration {
        (0..n)
            .filter_map(|i| self.until(i, route))
            .map(|u| u.saturating_duration_since(now))
            .min()
            .unwrap_or_default()
    }
}

pub(crate) struct Transport {
    pub(crate) http: reqwest::Client,
    pub(crate) base_url: String,
    pub(crate) keys: Vec<String>,
    pub(crate) cache: Option<(CacheConfig, Mutex<LruCache>)>,
    blocks: Mutex<Blocks>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic while holding the lock cannot leave cache/blocks inconsistent enough to matter.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn header_u64(h: &HeaderMap, name: &str) -> Option<u64> {
    h.get(name)?.to_str().ok()?.trim().parse().ok()
}

fn first_segment(key: &str) -> &str {
    key.split(['/', '?']).nth(1).unwrap_or("")
}

fn ceil_secs(d: Duration) -> Duration {
    Duration::from_secs(d.as_secs() + u64::from(d.subsec_nanos() > 0))
}

impl Transport {
    pub(crate) fn new(http: reqwest::Client, base_url: String, keys: Vec<String>, cache: Option<CacheConfig>) -> Self {
        Self {
            http,
            base_url,
            keys,
            cache: cache.map(|c| (c, Mutex::new(LruCache::new(c.max_entries, c.max_bytes)))),
            blocks: Mutex::new(Blocks::default()),
        }
    }

    pub(crate) fn clear_cache(&self) {
        if let Some((_, c)) = &self.cache {
            lock(c).clear();
        }
    }

    pub(crate) async fn execute<R: DeserializeOwned + Default>(
        &self,
        method: Method,
        route: &'static str,
        path: String,
        mut query: Vec<(&'static str, String)>,
        body: Option<Vec<u8>>,
        tier: CacheTier,
    ) -> Result<R, InvezgoError> {
        query.sort_by(|a, b| a.0.cmp(b.0));
        let qs: Vec<String> = query.iter().map(|(k, v)| format!("{}={}", encode(k), encode(v))).collect();
        let target = if qs.is_empty() { path } else { format!("{path}?{}", qs.join("&")) };

        let ttl = match (&self.cache, tier) {
            (Some((c, _)), CacheTier::Default) if method == Method::GET => Some(c.ttl),
            (Some((c, _)), CacheTier::Realtime) if method == Method::GET => Some(c.realtime_ttl),
            _ => None,
        }
        .filter(|t| !t.is_zero());

        if let (Some(_), Some((_, cache))) = (ttl, &self.cache) {
            let hit = lock(cache).get(&target, Instant::now());
            if let Some(text) = hit {
                return decode(&text);
            }
        }

        let mut tried = vec![false; self.keys.len()];
        let mut last: Option<RateLimitError> = None;
        loop {
            let picked = lock(&self.blocks).pick(route, Instant::now(), &tried);
            let Some(i) = picked else {
                return Err(self.exhausted(route, last));
            };
            tried[i] = true;

            let mut req = self
                .http
                .request(method.clone(), format!("{}{}", self.base_url, target))
                .bearer_auth(&self.keys[i])
                .header(ACCEPT, "application/json");
            if let Some(b) = &body {
                req = req.header(CONTENT_TYPE, "application/json").body(b.clone());
            }
            let res = req.send().await?;
            let status = res.status();
            let headers = res.headers().clone();
            let text = res.text().await?;

            if status.as_u16() == 429 {
                let api = api_error(429, &text);
                let wait = header_u64(&headers, "retry-after")
                    .filter(|s| *s > 0)
                    .or_else(|| {
                        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
                        header_u64(&headers, "x-ratelimit-reset").filter(|r| *r > now).map(|r| r - now)
                    })
                    .unwrap_or(60);
                let msg = api.message.to_lowercase();
                let quota = ["quota", "kuota", "monthly", "bulanan"].iter().any(|w| msg.contains(w));
                {
                    let mut b = lock(&self.blocks);
                    let until = Instant::now() + Duration::from_secs(wait);
                    if quota {
                        b.key.insert(i, until);
                    } else {
                        b.route.insert((i, route), until);
                    }
                    b.last_quota = quota;
                }
                last = Some(RateLimitError {
                    retry_after: Duration::from_secs(wait),
                    limit: header_u64(&headers, "x-ratelimit-limit"),
                    remaining: header_u64(&headers, "x-ratelimit-remaining"),
                    reset: header_u64(&headers, "x-ratelimit-reset"),
                    quota_exceeded: quota,
                    message: api.message,
                    request_id: api.request_id,
                });
                continue;
            }

            if !status.is_success() {
                return Err(InvezgoError::Api(Box::new(api_error(status.as_u16(), &text))));
            }

            let value = decode(&text)?;
            if let Some((_, cache)) = &self.cache {
                let mut cache = lock(cache);
                if let Some(ttl) = ttl {
                    cache.set(target, Arc::from(text), ttl, Instant::now());
                } else if method != Method::GET {
                    cache.purge(|k| PERSONAL_SEGMENTS.contains(&first_segment(k)));
                }
            }
            return Ok(value);
        }
    }

    fn exhausted(&self, route: &'static str, last: Option<RateLimitError>) -> InvezgoError {
        let b = lock(&self.blocks);
        let retry_after = ceil_secs(b.min_wait(route, Instant::now(), self.keys.len()));
        let mut err = last.unwrap_or_else(|| RateLimitError {
            retry_after,
            limit: None,
            remaining: None,
            reset: None,
            quota_exceeded: b.last_quota,
            message: format!(
                "all {} API key(s) are rate limited for {route}; retry after {}s",
                self.keys.len(),
                retry_after.as_secs()
            ),
            request_id: None,
        });
        err.retry_after = retry_after;
        InvezgoError::RateLimited(Box::new(err))
    }
}

fn decode<R: DeserializeOwned + Default>(text: &str) -> Result<R, InvezgoError> {
    let t = text.trim();
    if t.is_empty() || t == "null" {
        return Ok(R::default());
    }
    Ok(serde_json::from_str(t)?)
}

fn api_error(status: u16, text: &str) -> ApiError {
    let body: serde_json::Value =
        serde_json::from_str(text).unwrap_or_else(|_| serde_json::Value::String(text.to_string()));
    let s = |k: &str| body.get(k).and_then(|v| v.as_str()).map(str::to_string);
    let message = match body.get("message") {
        Some(serde_json::Value::String(m)) => m.clone(),
        Some(serde_json::Value::Array(a)) => {
            a.iter().map(|v| v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string())).collect::<Vec<_>>().join("; ")
        }
        _ if !text.trim().is_empty() && !body.is_object() => text.trim().to_string(),
        _ => reqwest::StatusCode::from_u16(status)
            .ok()
            .and_then(|s| s.canonical_reason())
            .unwrap_or("Unknown error")
            .to_string(),
    };
    ApiError {
        status,
        kind: ErrorKind::from_status(status),
        message,
        error: s("error"),
        request_id: s("requestId"),
        path: s("path"),
        body,
    }
}
