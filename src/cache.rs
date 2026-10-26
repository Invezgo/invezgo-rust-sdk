//! In-memory (L1) LRU cache of raw response bodies, bounded by entry count and total bytes.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Entry {
    body: Arc<str>,
    expires: Instant,
    tick: u64,
}

/// LRU keyed by request target (`path?query`). `order` maps a monotonically increasing
/// access tick to its key, so the least recently used entry is always the first one.
pub(crate) struct LruCache {
    map: HashMap<String, Entry>,
    order: BTreeMap<u64, String>,
    bytes: usize,
    tick: u64,
    max_entries: usize,
    max_bytes: usize,
}

impl LruCache {
    pub(crate) fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self { map: HashMap::new(), order: BTreeMap::new(), bytes: 0, tick: 0, max_entries, max_bytes }
    }

    pub(crate) fn get(&mut self, key: &str, now: Instant) -> Option<Arc<str>> {
        let entry = self.map.get_mut(key)?;
        if entry.expires <= now {
            self.remove(key);
            return None;
        }
        self.order.remove(&entry.tick);
        self.tick += 1;
        entry.tick = self.tick;
        self.order.insert(self.tick, key.to_string());
        Some(entry.body.clone())
    }

    pub(crate) fn set(&mut self, key: String, body: Arc<str>, ttl: Duration, now: Instant) {
        self.remove(&key);
        if body.len() > self.max_bytes || self.max_entries == 0 {
            return;
        }
        self.tick += 1;
        self.bytes += body.len();
        self.order.insert(self.tick, key.clone());
        self.map.insert(key, Entry { body, expires: now + ttl, tick: self.tick });
        while self.map.len() > self.max_entries || self.bytes > self.max_bytes {
            let Some((_, oldest)) = self.order.pop_first() else { break };
            if let Some(e) = self.map.remove(&oldest) {
                self.bytes -= e.body.len();
            }
        }
    }

    pub(crate) fn remove(&mut self, key: &str) {
        if let Some(e) = self.map.remove(key) {
            self.order.remove(&e.tick);
            self.bytes -= e.body.len();
        }
    }

    /// Removes every entry whose key matches `pred`.
    pub(crate) fn purge(&mut self, pred: impl Fn(&str) -> bool) {
        let doomed: Vec<String> = self.map.keys().filter(|k| pred(k)).cloned().collect();
        for k in doomed {
            self.remove(&k);
        }
    }

    pub(crate) fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
        self.bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(60);

    fn body(s: &str) -> Arc<str> {
        Arc::from(s)
    }

    #[test]
    fn evicts_least_recently_used_by_entries() {
        let now = Instant::now();
        let mut c = LruCache::new(2, 1 << 20);
        c.set("a".into(), body("1"), TTL, now);
        c.set("b".into(), body("2"), TTL, now);
        assert!(c.get("a", now).is_some()); // a becomes MRU
        c.set("c".into(), body("3"), TTL, now);
        assert!(c.get("b", now).is_none());
        assert!(c.get("a", now).is_some());
        assert!(c.get("c", now).is_some());
        assert_eq!(c.map.len(), c.order.len());
    }

    #[test]
    fn evicts_by_bytes_and_skips_oversized() {
        let now = Instant::now();
        let mut c = LruCache::new(100, 10);
        c.set("a".into(), body("12345"), TTL, now);
        c.set("b".into(), body("12345"), TTL, now);
        assert_eq!(c.bytes, 10);
        c.set("c".into(), body("1"), TTL, now);
        assert!(c.get("a", now).is_none());
        assert_eq!(c.bytes, 6);
        c.set("big".into(), body("12345678901"), TTL, now);
        assert!(c.get("big", now).is_none());
        assert_eq!(c.bytes, 6);
    }

    #[test]
    fn expires_and_replaces() {
        let now = Instant::now();
        let mut c = LruCache::new(10, 100);
        c.set("a".into(), body("old"), Duration::from_millis(10), now);
        c.set("a".into(), body("newer"), Duration::from_millis(10), now);
        assert_eq!(c.bytes, 5);
        assert_eq!(&*c.get("a", now).unwrap(), "newer");
        assert!(c.get("a", now + Duration::from_millis(11)).is_none());
        assert_eq!((c.bytes, c.map.len(), c.order.len()), (0, 0, 0));
    }

    #[test]
    fn purge_and_clear() {
        let now = Instant::now();
        let mut c = LruCache::new(10, 100);
        c.set("/watchlists".into(), body("1"), TTL, now);
        c.set("/analysis/list/stock".into(), body("2"), TTL, now);
        c.purge(|k| k.starts_with("/watchlists"));
        assert!(c.get("/watchlists", now).is_none());
        assert!(c.get("/analysis/list/stock", now).is_some());
        c.clear();
        assert_eq!((c.bytes, c.map.len()), (0, 0));
    }
}
