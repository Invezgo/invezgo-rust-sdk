//! WebSocket client against a local in-process server (feature `websocket`, no real API calls).
#![cfg(feature = "websocket")]

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use invezgo_sdk::realtime::{Realtime, RealtimeEvent};
use invezgo_sdk::{ErrorKind, InvezgoClient, InvezgoError};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

type Ws = WebSocketStream<TcpStream>;

async fn listener() -> (TcpListener, u16) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    (l, port)
}

/// Accept one connection, asserting the upgrade request (path, bearer key, User-Agent).
async fn accept(l: &TcpListener) -> Ws {
    let (tcp, _) = l.accept().await.unwrap();
    tokio_tungstenite::accept_hdr_async(tcp, |req: &Request, res: Response| {
        assert_eq!(req.uri().path(), "/ws");
        assert_eq!(req.uri().query(), None, "API key must not be in the URL");
        assert_eq!(req.headers()["authorization"], "Bearer KEY");
        assert!(req.headers()["user-agent"].to_str().unwrap().starts_with("invezgo-rust-sdk/"));
        Ok(res)
    })
    .await
    .unwrap()
}

async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::text(v.to_string())).await.unwrap();
}

async fn recv(ws: &mut Ws) -> Value {
    loop {
        match timeout(Duration::from_secs(5), ws.next()).await.unwrap().unwrap().unwrap() {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            _ => continue,
        }
    }
}

async fn next(rt: &mut Realtime) -> Result<RealtimeEvent, InvezgoError> {
    timeout(Duration::from_secs(10), rt.next()).await.expect("event timeout").expect("stream ended")
}

#[tokio::test]
async fn queued_subscriptions_sent_after_connected_and_messages_decoded() {
    let (l, port) = listener().await;
    let client = InvezgoClient::builder().api_key("KEY").base_url(format!("http://127.0.0.1:{port}/")).build().unwrap();
    let mut rt = client.realtime().connect().unwrap();
    // queued before the server says `connected`
    rt.subscribe_trades(&["bbca", "tlkm"], Some(500), None).unwrap();
    rt.subscribe_orderbook(&["bbca"]).unwrap();
    rt.subscribe_stock(&["bbca"]).unwrap();
    rt.subscribe_index(&["composite", "LQ45"]).unwrap();
    rt.unsubscribe(invezgo_sdk::realtime::Channel::Index, &["lq45"]).unwrap();

    let mut ws = accept(&l).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    send(&mut ws, json!({"type": "connected"})).await;
    assert_eq!(recv(&mut ws).await, json!({"op":"subscribe","channel":"trades","codes":["BBCA","TLKM"],"min_lot":500}));
    assert_eq!(recv(&mut ws).await, json!({"op":"subscribe","channel":"orderbook","codes":["BBCA"]}));
    assert_eq!(recv(&mut ws).await, json!({"op":"subscribe","channel":"stock","codes":["BBCA"]}));
    assert_eq!(recv(&mut ws).await, json!({"op":"subscribe","channel":"index","codes":["COMPOSITE"]}));

    send(&mut ws, json!({"type":"subscribed","channel":"trades","codes":"*","min_lot":0,"min_value":0})).await;
    send(&mut ws, json!({"type":"trades","ts":1793240653184_i64,"data":[{"id":"202610290000255958","time":"092413","code":"ISAT","price":1465.0,"lot":9,"value":1318500,"change":-105.0,"side":"buy"}]})).await;
    send(&mut ws, json!({"type":"orderbook","code":"BBCA","side":"bid","levels":[[9875,120]],"seq":null})).await;
    send(&mut ws, json!({"type":"stock","code":"BBCA","last":5650.0,"prev":5550.0,"open":null,"change_pct":1.8,"volume":114428700,"iev":7927800,"time":"110436","seq":4408664})).await;
    send(&mut ws, json!({"type":"index","code":"Investor33","last":5221.553,"change_pct":1.462,"volume":20865566335_i64,"time":"110512"})).await;
    send(&mut ws, json!({"type":"lagged","missed":120})).await;
    send(&mut ws, json!({"type":"brand_new","x":1})).await;

    assert!(matches!(next(&mut rt).await.unwrap(), RealtimeEvent::Connected));
    match next(&mut rt).await.unwrap() {
        RealtimeEvent::Subscribed { channel, codes, min_lot, .. } => {
            assert_eq!((channel.as_str(), codes, min_lot), ("trades", None, Some(0)));
        }
        e => panic!("{e:?}"),
    }
    match next(&mut rt).await.unwrap() {
        RealtimeEvent::Trades { ts, data } => {
            assert_eq!(ts, 1793240653184);
            let t = &data[0];
            assert_eq!((t.id.as_str(), t.time.as_str(), t.code.as_str(), t.side.as_str()), ("202610290000255958", "092413", "ISAT", "buy"));
            assert_eq!((t.price, t.lot, t.value, t.change), (1465.0, 9, 1318500, -105.0));
        }
        e => panic!("{e:?}"),
    }
    match next(&mut rt).await.unwrap() {
        RealtimeEvent::Orderbook { code, side, levels, seq } => {
            assert_eq!((code.as_str(), side.as_str(), levels, seq), ("BBCA", "bid", vec![json!([9875, 120])], None));
        }
        e => panic!("{e:?}"),
    }
    match next(&mut rt).await.unwrap() {
        RealtimeEvent::Stock(q) => {
            assert_eq!((q.code.as_str(), q.last, q.prev, q.open, q.volume, q.iev, q.seq), ("BBCA", 5650.0, Some(5550.0), None, Some(114428700), Some(7927800), Some(4408664)));
            assert_eq!(q.time, "110436");
        }
        e => panic!("{e:?}"),
    }
    match next(&mut rt).await.unwrap() {
        RealtimeEvent::Index(q) => {
            assert_eq!((q.code.as_str(), q.last, q.change_pct, q.prev, q.volume), ("Investor33", 5221.553, Some(1.462), None, Some(20865566335)));
        }
        e => panic!("{e:?}"),
    }
    assert!(matches!(next(&mut rt).await.unwrap(), RealtimeEvent::Lagged { missed: 120 }));
    match next(&mut rt).await.unwrap() {
        RealtimeEvent::Unknown(raw) => assert!(raw.contains("brand_new")),
        e => panic!("{e:?}"),
    }

    // after connected, commands go out directly
    rt.ping().unwrap();
    assert_eq!(recv(&mut ws).await, json!({"op":"ping"}));
    rt.unsubscribe(invezgo_sdk::realtime::Channel::Trades, &["tlkm"]).unwrap();
    assert_eq!(recv(&mut ws).await, json!({"op":"unsubscribe","channel":"trades","codes":["TLKM"]}));

    rt.close();
    assert!(timeout(Duration::from_secs(5), rt.next()).await.unwrap().is_none());
}

#[tokio::test]
async fn reconnect_replays_subscriptions() {
    let (l, port) = listener().await;
    let mut rt = Realtime::builder("KEY").url(format!("ws://127.0.0.1:{port}/ws")).connect().unwrap();

    let mut ws = accept(&l).await;
    send(&mut ws, json!({"type": "connected"})).await;
    assert!(matches!(next(&mut rt).await.unwrap(), RealtimeEvent::Connected));
    rt.subscribe_trades(&[], None, Some(100_000_000)).unwrap();
    assert_eq!(recv(&mut ws).await, json!({"op":"subscribe","channel":"trades","min_value":100000000}));
    rt.subscribe_orderbook(&["BBCA", "TLKM"]).unwrap();
    assert_eq!(recv(&mut ws).await, json!({"op":"subscribe","channel":"orderbook","codes":["BBCA","TLKM"]}));
    rt.unsubscribe(invezgo_sdk::realtime::Channel::Orderbook, &["TLKM"]).unwrap();
    assert_eq!(recv(&mut ws).await, json!({"op":"unsubscribe","channel":"orderbook","codes":["TLKM"]}));
    drop(ws); // network drop

    match next(&mut rt).await.unwrap() {
        RealtimeEvent::Reconnecting { attempt, delay, .. } => assert_eq!((attempt, delay), (1, Duration::from_secs(1))),
        e => panic!("{e:?}"),
    }
    let mut ws = accept(&l).await;
    send(&mut ws, json!({"type": "connected"})).await;
    assert_eq!(recv(&mut ws).await, json!({"op":"subscribe","channel":"trades","min_value":100000000}));
    assert_eq!(recv(&mut ws).await, json!({"op":"subscribe","channel":"orderbook","codes":["BBCA"]}));
    assert!(matches!(next(&mut rt).await.unwrap(), RealtimeEvent::Connected));
    rt.close();
}

#[tokio::test]
async fn close_4400_stops_reconnect() {
    let (l, port) = listener().await;
    let mut rt = Realtime::builder("KEY").url(format!("ws://127.0.0.1:{port}/ws")).connect().unwrap();
    let mut ws = accept(&l).await;
    send(&mut ws, json!({"type": "connected"})).await;
    send(&mut ws, json!({"type": "error", "message": "unknown op"})).await;
    ws.send(Message::Close(Some(CloseFrame { code: CloseCode::from(4400), reason: "invalid message".into() }))).await.unwrap();

    assert!(matches!(next(&mut rt).await.unwrap(), RealtimeEvent::Connected));
    assert!(matches!(next(&mut rt).await.unwrap(), RealtimeEvent::Error { message } if message == "unknown op"));
    match next(&mut rt).await {
        Err(InvezgoError::WebSocket { close_code, .. }) => assert_eq!(close_code, Some(4400)),
        e => panic!("{e:?}"),
    }
    assert!(timeout(Duration::from_secs(5), rt.next()).await.unwrap().is_none());
}

#[tokio::test]
async fn handshake_401_is_fatal_authentication_error() {
    let (l, port) = listener().await;
    let mut rt = Realtime::builder("KEY").url(format!("ws://127.0.0.1:{port}/ws")).connect().unwrap();
    let (tcp, _) = l.accept().await.unwrap();
    let _ = tokio_tungstenite::accept_hdr_async(tcp, |_: &Request, _: Response| {
        let mut res = ErrorResponse::new(Some(r#"{"statusCode":401,"message":"Invalid API key"}"#.into()));
        *res.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
        Err(res)
    })
    .await;
    match next(&mut rt).await {
        Err(InvezgoError::Api(e)) => {
            assert_eq!((e.status, e.kind, e.message.as_str()), (401, ErrorKind::Authentication, "Invalid API key"));
        }
        e => panic!("{e:?}"),
    }
    assert!(timeout(Duration::from_secs(5), rt.next()).await.unwrap().is_none());
}

#[tokio::test]
async fn client_side_guards() {
    let rt = Realtime::builder("KEY").url("ws://127.0.0.1:9/ws").reconnect(false).connect().unwrap();
    assert!(matches!(rt.subscribe_orderbook(&[]), Err(InvezgoError::Config(_))));
    let many: Vec<String> = (0..21).map(|i| format!("C{i}")).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    assert!(matches!(rt.subscribe_orderbook(&many), Err(InvezgoError::Config(_))));
    let many: Vec<String> = (0..201).map(|i| format!("C{i}")).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    assert!(matches!(rt.subscribe_trades(&many, None, None), Err(InvezgoError::Config(_))));
    assert!(!matches!(rt.subscribe_trades(&many[..200], None, None), Err(InvezgoError::Config(_))));
    assert!(matches!(rt.subscribe_stock(&[]), Err(InvezgoError::Config(_))));
    assert!(matches!(rt.subscribe_stock(&many[..51]), Err(InvezgoError::Config(_))));
    assert!(matches!(rt.subscribe_index(&many[..101]), Err(InvezgoError::Config(_))));
    assert!(matches!(Realtime::builder(" ").connect(), Err(InvezgoError::Config(_))));
}
