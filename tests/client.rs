//! Behaviour tests against a local mock HTTP server (no real API calls).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use invezgo_sdk::models::{AddWatchlistRequest, ScreenRequest};
use invezgo_sdk::params::{IndicatorChartIndicator, IndicatorChartParams, TopChangeFilterColumn, TopChangeFilterOperator, TopChangeParams};
use invezgo_sdk::{CacheConfig, ErrorKind, InvezgoClient, InvezgoError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
struct Req {
    method: String,
    target: String,
    key: String,
    body: String,
}

struct Reply {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: String,
}

fn ok(body: &str) -> Reply {
    Reply { status: 200, headers: vec![], body: body.into() }
}

fn status(status: u16, body: &str) -> Reply {
    Reply { status, headers: vec![], body: body.into() }
}

fn too_many(retry_after: u64, message: &str) -> Reply {
    Reply {
        status: 429,
        headers: vec![
            ("Retry-After", retry_after.to_string()),
            ("X-RateLimit-Limit", "250".into()),
            ("X-RateLimit-Remaining", "0".into()),
        ],
        body: format!(r#"{{"statusCode":429,"message":"{message}","error":"Too Many Requests"}}"#),
    }
}

type Handler = Arc<dyn Fn(&Req) -> Reply + Send + Sync>;

struct Mock {
    url: String,
    log: Arc<Mutex<Vec<Req>>>,
}

impl Mock {
    async fn start(handler: impl Fn(&Req) -> Reply + Send + Sync + 'static) -> Self {
        let handler: Handler = Arc::new(handler);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let log = Arc::new(Mutex::new(Vec::new()));
        let log2 = log.clone();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let handler = handler.clone();
                let log = log2.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let head_end = loop {
                        let n = sock.read(&mut chunk).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break p + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let mut lines = head.lines();
                    let mut first = lines.next().unwrap().split(' ');
                    let (method, target) = (first.next().unwrap().to_string(), first.next().unwrap().to_string());
                    let mut len = 0;
                    let mut key = String::new();
                    for l in lines {
                        let (k, v) = l.split_once(':').unwrap_or((l, ""));
                        match k.to_ascii_lowercase().as_str() {
                            "content-length" => len = v.trim().parse().unwrap(),
                            "authorization" => key = v.trim().trim_start_matches("Bearer ").to_string(),
                            _ => {}
                        }
                    }
                    while buf.len() < head_end + len {
                        let n = sock.read(&mut chunk).await.unwrap();
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    let req = Req { method, target, key, body: String::from_utf8_lossy(&buf[head_end..]).to_string() };
                    let reply = handler(&req);
                    log.lock().unwrap().push(req);
                    let mut out = format!(
                        "HTTP/1.1 {} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                        reply.status,
                        reply.body.len()
                    );
                    for (k, v) in reply.headers {
                        out.push_str(&format!("{k}: {v}\r\n"));
                    }
                    out.push_str("\r\n");
                    out.push_str(&reply.body);
                    sock.write_all(out.as_bytes()).await.unwrap();
                    let _ = sock.shutdown().await;
                });
            }
        });
        Mock { url, log }
    }

    fn requests(&self) -> Vec<Req> {
        self.log.lock().unwrap().clone()
    }

    fn count(&self) -> usize {
        self.log.lock().unwrap().len()
    }

    fn client(&self) -> invezgo_sdk::InvezgoClientBuilder {
        InvezgoClient::builder().api_key("k1").base_url(format!("{}/", self.url))
    }
}

const STOCKS: &str = r#"[{"code":"BBCA","name":"Bank Central Asia","sector":null,"logo":null}]"#;

#[tokio::test]
async fn builder_rejects_empty_key() {
    assert!(matches!(InvezgoClient::builder().build(), Err(InvezgoError::Config(_))));
    assert!(matches!(InvezgoClient::new("  "), Err(InvezgoError::Config(_))));
}

#[tokio::test]
async fn caches_get_and_decodes_typed() {
    let m = Mock::start(|_| ok(STOCKS)).await;
    let c = m.client().build().unwrap();
    let a = c.analysis().get_stock_list().await.unwrap();
    let b = c.analysis().get_stock_list().await.unwrap();
    assert_eq!(a, b);
    assert_eq!(a[0].code, "BBCA");
    assert_eq!(a[0].sector, None);
    assert_eq!(m.count(), 1);
    let r = &m.requests()[0];
    assert_eq!((r.method.as_str(), r.target.as_str(), r.key.as_str()), ("GET", "/analysis/list/stock", "k1"));

    c.clear_cache();
    c.analysis().get_stock_list().await.unwrap();
    assert_eq!(m.count(), 2);
}

#[tokio::test]
async fn query_is_canonical_and_path_encoded() {
    let m = Mock::start(|_| ok("{}")).await;
    let c = m.client().build().unwrap();
    let p = TopChangeParams {
        filter_value: Some(5.0),
        filter_operator: Some(TopChangeFilterOperator::Gt),
        filter_column: Some(TopChangeFilterColumn::Change),
        ..TopChangeParams::new("2024-12-30")
    };
    c.analysis().get_top_change(&p).await.unwrap();
    c.analysis().get_top_change(&p).await.unwrap();
    c.analysis().get_information("BB CA/").await.unwrap();
    let reqs = m.requests();
    assert_eq!(reqs.len(), 2);
    assert_eq!(reqs[0].target, "/analysis/top/change?date=2024-12-30&filter_column=change&filter_operator=%3E&filter_value=5");
    assert_eq!(reqs[1].target, "/analysis/information/BB%20CA%2F");
}

#[tokio::test]
async fn ttl_expiry_realtime_tier_and_no_cache() {
    let m = Mock::start(|_| ok("[]")).await;
    let cfg = CacheConfig { ttl: Duration::from_millis(80), realtime_ttl: Duration::ZERO, ..Default::default() };
    let c = m.client().cache(cfg).build().unwrap();
    c.analysis().get_stock_list().await.unwrap();
    c.analysis().get_stock_list().await.unwrap();
    assert_eq!(m.count(), 1);
    tokio::time::sleep(Duration::from_millis(120)).await;
    c.analysis().get_stock_list().await.unwrap();
    assert_eq!(m.count(), 2);

    // realtime tier disabled → every call hits the API
    c.batch().get_intraday_index("COMPOSITE").await.unwrap();
    c.batch().get_intraday_index("COMPOSITE").await.unwrap();
    assert_eq!(m.count(), 4);

    let nc = m.client().no_cache().build().unwrap();
    nc.analysis().get_broker_list().await.unwrap();
    nc.analysis().get_broker_list().await.unwrap();
    assert_eq!(m.count(), 6);
}

#[tokio::test]
async fn lru_evicts_by_entries_and_bytes() {
    let m = Mock::start(|_| ok("[]")).await;
    let c = m.client().cache(CacheConfig { max_entries: 2, ..Default::default() }).build().unwrap();
    c.analysis().get_stock_list().await.unwrap();
    c.analysis().get_broker_list().await.unwrap();
    c.analysis().get_index_list().await.unwrap(); // evicts stock list
    c.analysis().get_index_list().await.unwrap();
    assert_eq!(m.count(), 3);
    c.analysis().get_stock_list().await.unwrap();
    assert_eq!(m.count(), 4);

    let m = Mock::start(|_| ok(STOCKS)).await;
    let c = m.client().cache(CacheConfig { max_bytes: STOCKS.len() - 1, ..Default::default() }).build().unwrap();
    c.analysis().get_stock_list().await.unwrap();
    c.analysis().get_stock_list().await.unwrap();
    assert_eq!(m.count(), 2, "bodies larger than max_bytes are never cached");
}

#[tokio::test]
async fn mutation_purges_personal_segments_only() {
    let m = Mock::start(|r| if r.method == "POST" { ok(r#"{"message":"ok"}"#) } else { ok("[]") }).await;
    let c = m.client().build().unwrap();
    c.watchlists().list(None).await.unwrap();
    c.analysis().get_stock_list().await.unwrap();
    let body = AddWatchlistRequest::new("cm2x8k1q40000abcd1234efgh", "BBCA", 9900.0, vec!["COMPOSITE".into()]);
    c.watchlists().add(&body).await.unwrap();
    assert_eq!(m.count(), 3);
    let post = &m.requests()[2];
    assert!(post.body.contains(r#""code":"BBCA""#) && !post.body.contains("note"), "{}", post.body);
    c.watchlists().list(None).await.unwrap();
    c.analysis().get_stock_list().await.unwrap();
    assert_eq!(m.count(), 4, "watchlists refetched, stock list still cached");
}

#[tokio::test]
async fn per_minute_429_blocks_only_that_route() {
    let m = Mock::start(|r| {
        if r.key == "k1" && r.target == "/analysis/list/stock" {
            too_many(30, "Too many requests: 250 requests per minute on this endpoint")
        } else {
            ok("[]")
        }
    })
    .await;
    let c = m.client().additional_api_keys(["k2", "k1", ""]).no_cache().build().unwrap();
    c.analysis().get_stock_list().await.unwrap();
    c.analysis().get_broker_list().await.unwrap();
    c.analysis().get_stock_list().await.unwrap();
    let keys: Vec<_> = m.requests().iter().map(|r| r.key.clone()).collect();
    assert_eq!(keys, ["k1", "k2", "k1", "k2"]);
}

#[tokio::test]
async fn quota_429_blocks_whole_key() {
    let m = Mock::start(|r| if r.key == "k1" { too_many(3600, "Monthly quota exceeded") } else { ok("[]") }).await;
    let c = m.client().additional_api_keys(["k2"]).no_cache().build().unwrap();
    c.analysis().get_stock_list().await.unwrap();
    c.analysis().get_broker_list().await.unwrap();
    let keys: Vec<_> = m.requests().iter().map(|r| r.key.clone()).collect();
    assert_eq!(keys, ["k1", "k2", "k2"]);
}

#[tokio::test]
async fn all_keys_blocked_fails_fast_then_recovers_after_retry_after() {
    let hits = Arc::new(Mutex::new(0));
    let h = hits.clone();
    let m = Mock::start(move |_| {
        let mut n = h.lock().unwrap();
        *n += 1;
        if *n == 1 {
            too_many(1, "per minute")
        } else {
            ok("[]")
        }
    })
    .await;
    let c = m.client().no_cache().build().unwrap();
    match c.analysis().get_stock_list().await {
        Err(InvezgoError::RateLimited(rl)) => {
            assert_eq!(rl.retry_after, Duration::from_secs(1));
            assert_eq!(rl.limit, Some(250));
            assert!(!rl.quota_exceeded);
        }
        other => panic!("{other:?}"),
    }
    let second = c.analysis().get_stock_list().await;
    assert!(matches!(second, Err(InvezgoError::RateLimited(_))));
    assert_eq!(m.count(), 1, "blocked key must not be sent");
    assert_eq!(second.unwrap_err().status(), Some(429));

    tokio::time::sleep(Duration::from_millis(1100)).await;
    c.analysis().get_stock_list().await.unwrap();
    assert_eq!(m.count(), 2);
}

#[tokio::test]
async fn maps_errors() {
    let m = Mock::start(|r| match r.target.as_str() {
        "/analysis/list/stock" => status(401, r#"{"statusCode":401,"message":"Authentication required","error":"Unauthorized","requestId":"rid-1","path":"/analysis/list/stock"}"#),
        "/analysis/list/broker" => status(422, r#"{"statusCode":422,"message":["a is bad","b is bad"],"error":"Unprocessable Entity"}"#),
        "/analysis/list/index" => status(402, r#"{"statusCode":402,"message":"Starter role user only","error":"Payment Required"}"#),
        "/analysis/notation" => status(404, "not json"),
        _ => status(503, ""),
    })
    .await;
    let c = m.client().build().unwrap();
    let api = |e: InvezgoError| match e {
        InvezgoError::Api(a) => *a,
        other => panic!("{other:?}"),
    };
    let e = api(c.analysis().get_stock_list().await.unwrap_err());
    assert_eq!((e.status, e.kind, e.message.as_str()), (401, ErrorKind::Authentication, "Authentication required"));
    assert_eq!((e.request_id.as_deref(), e.error.as_deref()), (Some("rid-1"), Some("Unauthorized")));
    let e = api(c.analysis().get_broker_list().await.unwrap_err());
    assert_eq!((e.kind, e.message.as_str()), (ErrorKind::Validation, "a is bad; b is bad"));
    assert_eq!(api(c.analysis().get_index_list().await.unwrap_err()).kind, ErrorKind::PaymentRequired);
    let e = api(c.analysis().get_notation().await.unwrap_err());
    assert_eq!((e.kind, e.message.as_str()), (ErrorKind::NotFound, "not json"));
    let e = api(c.analysis().get_uma(None).await.unwrap_err());
    assert_eq!((e.kind, e.message.as_str()), (ErrorKind::Server, "Service Unavailable"));
    assert_eq!(m.count(), 5, "errors are never cached");
}

#[tokio::test]
async fn decodes_null_flexnum_and_flatten() {
    let m = Mock::start(|r| match r.target.split('?').next().unwrap() {
        "/analysis/shareholder/classify-table/BBCA" => ok("null"),
        "/analysis/intraday-index/COMPOSITE" => ok(r#"{"code":"COMPOSITE","open":"7123.5","high":7200,"low":null,"close":"","volume":null}"#),
        "/analysis/chart/stock/bdm/BBCA" => ok(r#"[{"date":"2024-12-02T00:00:00.000Z","value":"-12.5"},{"date":null,"value":3}]"#),
        "/screener/screen" => ok(r#"[{"code":"BBCA","matched":true,"close":9875,"rsi(14)":null}]"#),
        _ => ok(""),
    })
    .await;
    let c = m.client().build().unwrap();
    assert_eq!(c.analysis().get_shareholder_classify_table("BBCA").await.unwrap(), None);

    let idx = c.analysis().get_intraday_index("COMPOSITE").await.unwrap();
    assert_eq!((idx.open, idx.high, idx.low, idx.close), (Some(7123.5), Some(7200.0), None, None));

    let chart = c
        .analysis()
        .get_indicator_chart(IndicatorChartIndicator::Bdm, "BBCA", &IndicatorChartParams::new("2024-12-01", "2024-12-31"))
        .await
        .unwrap();
    assert_eq!(chart[0].value, Some(-12.5));
    assert_eq!((chart[1].date.as_str(), chart[1].value), ("", Some(3.0)));

    let rows = c.screener().screen(&ScreenRequest::new("close > prev")).await.unwrap();
    assert_eq!((rows[0].code.as_str(), rows[0].matched), ("BBCA", true));
    assert_eq!(rows[0].values.get("close"), Some(&Some(9875.0)));
    assert_eq!(rows[0].values.get("rsi(14)"), Some(&None));
    assert_eq!(m.requests().last().unwrap().body, r#"{"formula":"close > prev"}"#);

    // empty 2xx body on an object endpoint → Default
    let info = c.analysis().get_information("XXXX").await.unwrap();
    assert_eq!(info, Default::default());
}
