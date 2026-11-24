//! Real-time WebSocket client for running trade, order book, stock quote and index (feature `websocket`, Elite plan).
//!
//! The connection runs in a background Tokio task: it authenticates with
//! `Authorization: Bearer <api_key>`, sends subscriptions once the server says `connected`,
//! reconnects with exponential backoff (1, 2, 4, 8, 16, 30 s) and replays every subscription
//! after each reconnect.
//!
//! ```no_run
//! use invezgo_sdk::realtime::RealtimeEvent;
//!
//! # async fn run(client: invezgo_sdk::InvezgoClient) -> invezgo_sdk::Result<()> {
//! let mut rt = client.realtime().connect()?;
//! rt.subscribe_trades(&["BBCA", "TLKM"], Some(500), None)?;
//! rt.subscribe_orderbook(&["BBCA"])?;
//! rt.subscribe_stock(&["BBCA"])?;
//! rt.subscribe_index(&["COMPOSITE", "LQ45"])?;
//!
//! while let Some(event) = rt.next().await {
//!     match event? {
//!         RealtimeEvent::Trades { data, .. } => {
//!             for t in data {
//!                 println!("{} {} {} {}", t.code, t.price, t.lot, t.side);
//!             }
//!         }
//!         RealtimeEvent::Orderbook { code, side, levels, .. } => println!("{code} {side} {levels:?}"),
//!         RealtimeEvent::Stock(q) => println!("{} {} {:?}", q.code, q.last, q.change_pct),
//!         RealtimeEvent::Index(q) => println!("{} {} {:?}", q.code, q.last, q.change_pct),
//!         RealtimeEvent::Reconnecting { .. } => { /* backfill via REST after the next `Connected` */ }
//!         _ => {}
//!     }
//! }
//! # Ok(()) }
//! ```

use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Deserializer};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{HeaderValue, AUTHORIZATION, USER_AGENT};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use crate::client::USER_AGENT as SDK_USER_AGENT;
use crate::error::{ErrorKind, InvezgoError, RateLimitError};

/// Default WebSocket URL.
pub const DEFAULT_WS_URL: &str = "wss://api.invezgo.com/ws";

const MAX_TRADE_CODES: usize = 200;
const MAX_ORDERBOOK_CODES: usize = 20;
const MAX_STOCK_CODES: usize = 50;
const MAX_INDEX_CODES: usize = 100;

/// Subscription channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    /// Running trade.
    Trades,
    /// Order book.
    Orderbook,
    /// Stock quote.
    Stock,
    /// Index quote.
    Index,
}

impl Channel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::Orderbook => "orderbook",
            Self::Stock => "stock",
            Self::Index => "index",
        }
    }
}

/// One running trade.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct Trade {
    /// Exchange trade ID (a string: it exceeds the JavaScript safe integer range).
    pub id: String,
    /// Trade time `HHMMSS` (WIB).
    pub time: String,
    /// Stock code.
    pub code: String,
    /// Price.
    pub price: f64,
    /// Volume in lots.
    pub lot: i64,
    /// Value in Rupiah (`price * lot * 100`).
    pub value: i64,
    /// Change versus the previous close.
    pub change: f64,
    /// `buy` (HAKA) or `sell` (HAKI).
    pub side: String,
}

/// Full stock quote snapshot (`stock` channel). Fields not yet provided by the exchange are `None`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct StockQuote {
    /// Stock code.
    pub code: String,
    /// Last price.
    pub last: f64,
    /// Previous close.
    pub prev: Option<f64>,
    /// Open.
    pub open: Option<f64>,
    /// Day high.
    pub high: Option<f64>,
    /// Day low.
    pub low: Option<f64>,
    /// `last - prev`.
    pub change: Option<f64>,
    /// Change in percent.
    pub change_pct: Option<f64>,
    /// Volume today in shares (not lots).
    pub volume: Option<i64>,
    /// Value today in Rupiah.
    pub value: Option<i64>,
    /// Number of trades today.
    pub freq: Option<i64>,
    /// Volume-weighted average price today.
    pub avg: Option<f64>,
    /// Pre-opening Indicative Equilibrium Price.
    pub iep: Option<f64>,
    /// Pre-opening Indicative Equilibrium Volume, in shares.
    pub iev: Option<i64>,
    /// Data time `HHMMSS` (WIB).
    pub time: String,
    /// Exchange sequence number.
    pub seq: Option<i64>,
}

/// Index snapshot (`index` channel). `code` keeps the exchange's casing (e.g. `Investor33`), so
/// compare it case-insensitively.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct IndexQuote {
    /// Index code, e.g. `COMPOSITE`.
    pub code: String,
    /// Last value.
    pub last: f64,
    /// Previous close.
    pub prev: Option<f64>,
    /// Open.
    pub open: Option<f64>,
    /// Day high.
    pub high: Option<f64>,
    /// Day low.
    pub low: Option<f64>,
    /// `last - prev`.
    pub change: Option<f64>,
    /// Change in percent (3 decimals).
    pub change_pct: Option<f64>,
    /// Volume today in shares.
    pub volume: Option<i64>,
    /// Value today in Rupiah.
    pub value: Option<i64>,
    /// Number of trades today.
    pub freq: Option<i64>,
    /// Data time `HHMMSS` (WIB).
    pub time: String,
}

/// Something delivered by [`Realtime::next`]. Unknown server message types arrive as [`RealtimeEvent::Unknown`].
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
#[non_exhaustive]
pub enum RealtimeEvent {
    /// The server accepted the connection; queued subscriptions are (re)sent right after.
    Connected,
    /// Current subscription of a channel, after every subscribe/unsubscribe.
    Subscribed {
        /// `trades`, `orderbook`, `stock` or `index`.
        #[serde(default)]
        channel: String,
        /// Subscribed codes; `None` means all stocks or indices (`"*"`).
        #[serde(default, deserialize_with = "codes_or_all")]
        codes: Option<Vec<String>>,
        /// Minimum lot filter (`trades` only).
        #[serde(default)]
        min_lot: Option<i64>,
        /// Minimum value filter (`trades` only).
        #[serde(default)]
        min_value: Option<i64>,
    },
    /// Reply to [`Realtime::ping`].
    Pong,
    /// The server rejected a client message; it then closes the connection with code `4400`.
    Error {
        /// Reason.
        #[serde(default)]
        message: String,
    },
    /// The client read too slowly and `missed` messages were dropped.
    Lagged {
        /// Number of skipped messages.
        #[serde(default)]
        missed: i64,
    },
    /// A batch of running trades.
    Trades {
        /// Server send time, Unix milliseconds.
        #[serde(default)]
        ts: i64,
        /// Trades matching the subscription and filters.
        #[serde(default)]
        data: Vec<Trade>,
    },
    /// Full snapshot of one side of an order book.
    Orderbook {
        /// Stock code.
        #[serde(default)]
        code: String,
        /// `bid` or `offer`.
        #[serde(default)]
        side: String,
        /// Price levels (shape not documented, kept as raw JSON).
        #[serde(default)]
        levels: Vec<Value>,
        /// Exchange sequence number.
        #[serde(default)]
        seq: Option<i64>,
    },
    /// Stock quote update (full snapshot).
    Stock(StockQuote),
    /// Index update (full snapshot).
    Index(IndexQuote),
    /// The connection dropped; the client waits `delay` and reconnects. Backfill missed data via
    /// REST (running trade / order book) after the next [`RealtimeEvent::Connected`].
    #[serde(skip)]
    Reconnecting {
        /// Reconnect attempt number, starting at 1.
        attempt: u32,
        /// Wait before this attempt.
        delay: Duration,
        /// Why the previous connection ended or failed.
        error: InvezgoError,
    },
    /// A server message with an unknown `type` (or an unexpected shape), as raw JSON text.
    #[serde(skip)]
    Unknown(String),
}

fn codes_or_all<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<String>>, D::Error> {
    Ok(match Value::deserialize(d)? {
        Value::Array(a) => Some(a.into_iter().filter_map(|v| v.as_str().map(str::to_string)).collect()),
        _ => None,
    })
}

/// Desired subscriptions, replayed after every (re)connect.
#[derive(Debug, Default)]
struct Subs {
    /// `None` = not subscribed, `Some(None)` = all stocks, `Some(Some(codes))` = these codes.
    trades: Option<Option<BTreeSet<String>>>,
    min_lot: Option<u64>,
    min_value: Option<u64>,
    orderbook: BTreeSet<String>,
    stock: BTreeSet<String>,
    /// Same shape as `trades`.
    index: Option<Option<BTreeSet<String>>>,
    /// Bumped on every change, so commands already covered by a replay are not sent twice.
    version: u64,
}

impl Subs {
    fn replay(&self) -> Vec<Value> {
        let mut out = Vec::new();
        if let Some(codes) = &self.trades {
            if codes.as_ref().is_none_or(|c| !c.is_empty()) {
                out.push(trades_msg(codes.as_ref().map(|c| c.iter().cloned().collect()), self.min_lot, self.min_value));
            }
        }
        if !self.orderbook.is_empty() {
            out.push(json!({"op": "subscribe", "channel": "orderbook", "codes": self.orderbook}));
        }
        if !self.stock.is_empty() {
            out.push(json!({"op": "subscribe", "channel": "stock", "codes": self.stock}));
        }
        match &self.index {
            Some(None) => out.push(json!({"op": "subscribe", "channel": "index"})),
            Some(Some(c)) if !c.is_empty() => out.push(json!({"op": "subscribe", "channel": "index", "codes": c})),
            _ => {}
        }
        out
    }
}

fn trades_msg(codes: Option<Vec<String>>, min_lot: Option<u64>, min_value: Option<u64>) -> Value {
    let mut m = json!({"op": "subscribe", "channel": "trades"});
    if let Some(c) = codes {
        m["codes"] = json!(c);
    }
    if let Some(v) = min_lot {
        m["min_lot"] = json!(v);
    }
    if let Some(v) = min_value {
        m["min_value"] = json!(v);
    }
    m
}

#[derive(Debug)]
enum Cmd {
    /// Message plus the subscription version it belongs to (`None` for ping).
    Send(Value, Option<u64>),
    Close,
}

/// Builder for [`Realtime`].
#[derive(Clone)]
pub struct RealtimeBuilder {
    api_key: String,
    url: String,
    reconnect: bool,
}

impl std::fmt::Debug for RealtimeBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // never print the API key
        f.debug_struct("RealtimeBuilder").field("url", &self.url).field("reconnect", &self.reconnect).finish()
    }
}

impl RealtimeBuilder {
    /// WebSocket URL (default `wss://api.invezgo.com/ws`).
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    /// Reconnect automatically after a drop (default `true`).
    pub fn reconnect(mut self, enabled: bool) -> Self {
        self.reconnect = enabled;
        self
    }

    /// Start connecting in a background task. Must be called inside a Tokio runtime.
    /// Connection errors are delivered by [`Realtime::next`].
    pub fn connect(self) -> Result<Realtime, InvezgoError> {
        if self.api_key.trim().is_empty() {
            return Err(InvezgoError::Config("api_key is required".into()));
        }
        let subs = Arc::new(Mutex::new(Subs::default()));
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        tokio::spawn(run(self, subs.clone(), cmd_rx, ev_tx));
        Ok(Realtime { subs, cmd: cmd_tx, events: ev_rx })
    }
}

/// Real-time connection handle. Read events with [`Realtime::next`] or as a [`Stream`].
///
/// The stream yields `Err` once for a fatal error (handshake 401/403, close code `4400`, or any drop
/// when reconnect is disabled) and then ends. It ends without an error after [`Realtime::close`].
#[derive(Debug)]
pub struct Realtime {
    subs: Arc<Mutex<Subs>>,
    cmd: mpsc::UnboundedSender<Cmd>,
    events: mpsc::UnboundedReceiver<Result<RealtimeEvent, InvezgoError>>,
}

fn upper(codes: &[&str]) -> Vec<String> {
    codes.iter().map(|c| c.trim().to_uppercase()).collect()
}

impl Realtime {
    /// Standalone client with an API key (use [`RealtimeBuilder::url`] to change the endpoint).
    pub fn builder(api_key: impl Into<String>) -> RealtimeBuilder {
        RealtimeBuilder { api_key: api_key.into(), url: DEFAULT_WS_URL.to_string(), reconnect: true }
    }

    /// Next event, or `None` once the connection is closed for good.
    pub async fn next(&mut self) -> Option<Result<RealtimeEvent, InvezgoError>> {
        self.events.recv().await
    }

    /// Subscribe to running trades of `codes` (empty = all stocks), optionally filtered by size.
    /// Absent filters keep the current value; `Some(0)` clears one.
    pub fn subscribe_trades(&self, codes: &[&str], min_lot: Option<u64>, min_value: Option<u64>) -> Result<(), InvezgoError> {
        let codes = upper(codes);
        self.update(|s| {
            let next = match (&s.trades, codes.is_empty()) {
                (_, true) | (Some(None), _) => None,
                (Some(Some(cur)), false) => Some(cur.iter().cloned().chain(codes.iter().cloned()).collect()),
                (None, false) => Some(codes.iter().cloned().collect::<BTreeSet<_>>()),
            };
            if next.as_ref().is_some_and(|c: &BTreeSet<String>| c.len() > MAX_TRADE_CODES) {
                return Err(format!("at most {MAX_TRADE_CODES} trade codes per connection"));
            }
            s.trades = Some(next);
            s.min_lot = min_lot.or(s.min_lot);
            s.min_value = min_value.or(s.min_value);
            Ok(trades_msg((!codes.is_empty()).then(|| codes.clone()), min_lot, min_value))
        })
    }

    /// Subscribe to the order book of `codes` (required, at most 20 per connection).
    pub fn subscribe_orderbook(&self, codes: &[&str]) -> Result<(), InvezgoError> {
        let codes = upper(codes);
        self.update(|s| {
            if codes.is_empty() {
                return Err("orderbook codes must not be empty".into());
            }
            let next: BTreeSet<String> = s.orderbook.iter().cloned().chain(codes.iter().cloned()).collect();
            if next.len() > MAX_ORDERBOOK_CODES {
                return Err(format!("at most {MAX_ORDERBOOK_CODES} orderbook codes per connection"));
            }
            s.orderbook = next;
            Ok(json!({"op": "subscribe", "channel": "orderbook", "codes": codes}))
        })
    }

    /// Subscribe to the stock quote (OHLC, volume, value, freq, IEP/IEV) of `codes` (required, at most 50 per connection).
    pub fn subscribe_stock(&self, codes: &[&str]) -> Result<(), InvezgoError> {
        let codes = upper(codes);
        self.update(|s| {
            if codes.is_empty() {
                return Err("stock codes must not be empty".into());
            }
            let next: BTreeSet<String> = s.stock.iter().cloned().chain(codes.iter().cloned()).collect();
            if next.len() > MAX_STOCK_CODES {
                return Err(format!("at most {MAX_STOCK_CODES} stock quote codes per connection"));
            }
            s.stock = next;
            Ok(json!({"op": "subscribe", "channel": "stock", "codes": codes}))
        })
    }

    /// Subscribe to index quotes of `codes` (e.g. `COMPOSITE`, `LQ45`; empty = every index, at most 100 codes).
    pub fn subscribe_index(&self, codes: &[&str]) -> Result<(), InvezgoError> {
        let codes = upper(codes);
        self.update(|s| {
            let next = match (&s.index, codes.is_empty()) {
                (_, true) | (Some(None), _) => None,
                (Some(Some(cur)), false) => Some(cur.iter().cloned().chain(codes.iter().cloned()).collect()),
                (None, false) => Some(codes.iter().cloned().collect::<BTreeSet<_>>()),
            };
            if next.as_ref().is_some_and(|c: &BTreeSet<String>| c.len() > MAX_INDEX_CODES) {
                return Err(format!("at most {MAX_INDEX_CODES} index codes per connection"));
            }
            s.index = Some(next);
            let mut m = json!({"op": "subscribe", "channel": "index"});
            if !codes.is_empty() {
                m["codes"] = json!(codes);
            }
            Ok(m)
        })
    }

    /// Unsubscribe `codes` from a channel; empty `codes` ends the whole channel (and clears trade filters).
    pub fn unsubscribe(&self, channel: Channel, codes: &[&str]) -> Result<(), InvezgoError> {
        let codes = upper(codes);
        self.update(|s| {
            match (channel, codes.is_empty()) {
                (Channel::Trades, true) => (s.trades, s.min_lot, s.min_value) = (None, None, None),
                (Channel::Orderbook, true) => s.orderbook.clear(),
                (Channel::Stock, true) => s.stock.clear(),
                (Channel::Index, true) => s.index = None,
                (Channel::Index, false) => {
                    if let Some(Some(cur)) = &mut s.index {
                        cur.retain(|c| !codes.contains(c));
                    }
                }
                (Channel::Stock, false) => s.stock.retain(|c| !codes.contains(c)),
                // shortcut: "all stocks minus some codes" is not tracked, so a replay re-subscribes all stocks.
                (Channel::Trades, false) => {
                    if let Some(Some(cur)) = &mut s.trades {
                        cur.retain(|c| !codes.contains(c));
                    }
                }
                (Channel::Orderbook, false) => s.orderbook.retain(|c| !codes.contains(c)),
            }
            let mut m = json!({"op": "unsubscribe", "channel": channel.as_str()});
            if !codes.is_empty() {
                m["codes"] = json!(codes);
            }
            Ok(m)
        })
    }

    /// Send `{"op":"ping"}`; the server answers [`RealtimeEvent::Pong`]. Dropped while not connected.
    pub fn ping(&self) -> Result<(), InvezgoError> {
        self.send(Cmd::Send(json!({"op": "ping"}), None))
    }

    /// Close the connection for good; [`Realtime::next`] then returns `None`.
    pub fn close(&self) {
        let _ = self.cmd.send(Cmd::Close);
    }

    fn update(&self, f: impl FnOnce(&mut Subs) -> Result<Value, String>) -> Result<(), InvezgoError> {
        let mut s = self.subs.lock().unwrap_or_else(|e| e.into_inner());
        let msg = f(&mut s).map_err(InvezgoError::Config)?;
        s.version += 1;
        self.send(Cmd::Send(msg, Some(s.version)))
    }

    fn send(&self, cmd: Cmd) -> Result<(), InvezgoError> {
        self.cmd.send(cmd).map_err(|_| closed(None, "connection is closed"))
    }
}

impl Stream for Realtime {
    type Item = Result<RealtimeEvent, InvezgoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.events.poll_recv(cx)
    }
}

fn closed(close_code: Option<u16>, message: impl Into<String>) -> InvezgoError {
    InvezgoError::WebSocket { close_code, message: message.into() }
}

/// Map a handshake/connection error to the SDK error types.
fn map_ws_error(e: WsError) -> InvezgoError {
    let WsError::Http(res) = e else { return closed(None, e.to_string()) };
    let status = res.status().as_u16();
    let text = res.body().as_deref().map(String::from_utf8_lossy).unwrap_or_default();
    let mut api = crate::transport::api_error(status, &text);
    match status {
        403 => api.kind = ErrorKind::PaymentRequired, // no active Elite plan
        429 => {
            let retry = res.headers().get("retry-after").and_then(|v| v.to_str().ok()?.trim().parse().ok());
            return InvezgoError::RateLimited(Box::new(RateLimitError {
                retry_after: Duration::from_secs(retry.unwrap_or(0)),
                limit: None,
                remaining: None,
                reset: None,
                quota_exceeded: false,
                message: api.message,
                request_id: api.request_id,
            }));
        }
        _ => {}
    }
    InvezgoError::Api(Box::new(api))
}

/// What ended a connection.
enum End {
    /// User closed or dropped the handle: stop silently.
    Closed,
    /// Do not reconnect (401/403, 4400).
    Fatal(InvezgoError),
    /// Reconnect if enabled.
    Drop(InvezgoError),
}

async fn run(
    cfg: RealtimeBuilder,
    subs: Arc<Mutex<Subs>>,
    mut cmds: mpsc::UnboundedReceiver<Cmd>,
    events: mpsc::UnboundedSender<Result<RealtimeEvent, InvezgoError>>,
) {
    let mut attempt: u32 = 0;
    loop {
        let err = match session(&cfg, &subs, &mut cmds, &events, &mut attempt).await {
            End::Closed => return,
            End::Fatal(e) => {
                let _ = events.send(Err(e));
                return;
            }
            End::Drop(e) if !cfg.reconnect => {
                let _ = events.send(Err(e));
                return;
            }
            End::Drop(e) => e,
        };
        let delay = Duration::from_secs((1u64 << attempt.min(5)).min(30));
        attempt += 1;
        if events.send(Ok(RealtimeEvent::Reconnecting { attempt, delay, error: err })).is_err() {
            return;
        }
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => break,
                cmd = cmds.recv() => match cmd {
                    None | Some(Cmd::Close) => return,
                    Some(Cmd::Send(..)) => {} // covered by the replay after `connected`
                },
            }
        }
    }
}

async fn session(
    cfg: &RealtimeBuilder,
    subs: &Mutex<Subs>,
    cmds: &mut mpsc::UnboundedReceiver<Cmd>,
    events: &mpsc::UnboundedSender<Result<RealtimeEvent, InvezgoError>>,
    attempt: &mut u32,
) -> End {
    let mut req = match cfg.url.as_str().into_client_request() {
        Ok(r) => r,
        Err(e) => return End::Fatal(closed(None, e.to_string())),
    };
    let auth = match HeaderValue::from_str(&format!("Bearer {}", cfg.api_key)) {
        Ok(v) => v,
        Err(_) => return End::Fatal(InvezgoError::Config("api_key contains invalid characters".into())),
    };
    req.headers_mut().insert(AUTHORIZATION, auth);
    req.headers_mut().insert(USER_AGENT, HeaderValue::from_static(SDK_USER_AGENT));

    let connect = tokio_tungstenite::connect_async(req);
    tokio::pin!(connect);
    let mut ws = loop {
        tokio::select! {
            res = &mut connect => match res {
                Ok((ws, _)) => break ws,
                Err(e) => {
                    let e = map_ws_error(e);
                    return match e.status() {
                        Some(401 | 403) => End::Fatal(e),
                        _ => End::Drop(e),
                    };
                }
            },
            cmd = cmds.recv() => match cmd {
                None | Some(Cmd::Close) => return End::Closed,
                Some(Cmd::Send(..)) => {} // covered by the replay after `connected`
            },
        }
    };

    // version of the last replay: queued commands at or below it were already sent
    let mut replayed: Option<u64> = None;
    loop {
        tokio::select! {
            cmd = cmds.recv() => match cmd {
                None | Some(Cmd::Close) => {
                    let _ = ws.close(None).await;
                    return End::Closed;
                }
                Some(Cmd::Send(msg, version)) => {
                    let Some(done) = replayed else { continue };
                    if version.is_some_and(|v| v <= done) {
                        continue;
                    }
                    if let Err(e) = ws.send(Message::text(msg.to_string())).await {
                        return End::Drop(closed(None, e.to_string()));
                    }
                }
            },
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let event = serde_json::from_str::<RealtimeEvent>(&text)
                        .unwrap_or_else(|_| RealtimeEvent::Unknown(text.to_string()));
                    let connected = matches!(event, RealtimeEvent::Connected);
                    if events.send(Ok(event)).is_err() {
                        let _ = ws.close(None).await;
                        return End::Closed;
                    }
                    if connected {
                        *attempt = 0;
                        let (msgs, version) = {
                            let s = subs.lock().unwrap_or_else(|e| e.into_inner());
                            (s.replay(), s.version)
                        };
                        for m in msgs {
                            if let Err(e) = ws.send(Message::text(m.to_string())).await {
                                return End::Drop(closed(None, e.to_string()));
                            }
                        }
                        replayed = Some(version);
                    }
                }
                Some(Ok(Message::Close(frame))) => {
                    let (code, reason) = frame.map(|f| (u16::from(f.code), f.reason.to_string())).unwrap_or((1005, String::new()));
                    let e = closed(Some(code), format!("closed by server ({code}) {reason}").trim_end().to_string());
                    return if code == 4400 { End::Fatal(e) } else { End::Drop(e) };
                }
                Some(Ok(_)) => {} // ping/pong frames are answered by tungstenite
                Some(Err(e)) => return End::Drop(closed(None, e.to_string())),
                None => return End::Drop(closed(None, "connection closed")),
            },
        }
    }
}
