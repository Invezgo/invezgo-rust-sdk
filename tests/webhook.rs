//! Webhook signature verification (feature `webhook`).
#![cfg(feature = "webhook")]

use std::time::Duration;

use hmac::{Hmac, Mac};
use invezgo_sdk::webhook::{
    parse_webhook, verify_webhook, verify_webhook_signature, VerifyOptions, WebhookAlertTriggeredData, WebhookQuotaData,
    WebhookUpdateData,
};
use invezgo_sdk::{InvezgoError, WebhookErrorReason};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::Sha256;

const SECRET: &str = "whsec_test";
const NOW: u64 = 1_762_425_300;
const BODY: &str = r#"{"id":"6f1c","domain":"update","type":"summary","created_at":1762425300,"data":{"date":"2026-11-06","path":["/analysis/summary/stock/BBCA"]}}"#;

fn sign(ts: &str, body: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(format!("{ts}.{body}").as_bytes());
    let hex: String = mac.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect();
    format!("sha256={hex}")
}

fn opts() -> VerifyOptions {
    VerifyOptions { now: Some(NOW), ..Default::default() }
}

fn reason(r: Result<impl std::fmt::Debug, InvezgoError>) -> WebhookErrorReason {
    match r {
        Err(InvezgoError::Webhook { reason, .. }) => reason,
        other => panic!("expected webhook error, got {other:?}"),
    }
}

#[test]
fn valid_signature_parses_event() {
    let ts = NOW.to_string();
    let ev = verify_webhook_signature(BODY.as_bytes(), &sign(&ts, BODY), &ts, SECRET, opts()).unwrap();
    assert_eq!(ev.id, "6f1c");
    assert_eq!(ev.name(), "update.summary");
    assert_eq!(ev.created_at, 1_762_425_300);
    let data: WebhookUpdateData = ev.data_as().unwrap();
    assert_eq!(data.date, "2026-11-06");
    assert_eq!(data.path, ["/analysis/summary/stock/BBCA"]);
    assert_eq!(data.code, None);
}

#[test]
fn bad_signature() {
    let ts = NOW.to_string();
    let wrong = sign(&ts, "{}");
    assert_eq!(reason(verify_webhook_signature(BODY.as_bytes(), &wrong, &ts, SECRET, opts())), WebhookErrorReason::Signature);
    assert_eq!(reason(verify_webhook_signature(BODY.as_bytes(), "", &ts, SECRET, opts())), WebhookErrorReason::Signature);
    let other_secret = VerifyOptions { now: Some(NOW), ..Default::default() };
    assert_eq!(
        reason(verify_webhook_signature(BODY.as_bytes(), &sign(&ts, BODY), &ts, "other", other_secret)),
        WebhookErrorReason::Signature
    );
}

#[test]
fn stale_and_future_timestamps() {
    for ts in [NOW - 301, NOW + 301] {
        let ts = ts.to_string();
        let r = verify_webhook_signature(BODY.as_bytes(), &sign(&ts, BODY), &ts, SECRET, opts());
        assert_eq!(reason(r), WebhookErrorReason::Timestamp);
    }
    // inside the tolerance, and a custom tolerance
    let ts = (NOW - 300).to_string();
    assert!(verify_webhook_signature(BODY.as_bytes(), &sign(&ts, BODY), &ts, SECRET, opts()).is_ok());
    let tight = VerifyOptions { tolerance: Duration::from_secs(10), now: Some(NOW) };
    assert_eq!(
        reason(verify_webhook_signature(BODY.as_bytes(), &sign(&ts, BODY), &ts, SECRET, tight)),
        WebhookErrorReason::Timestamp
    );
}

#[test]
fn non_digit_timestamp() {
    for ts in ["", "-1762425300", "1762425300.5", " 1762425300", "abc", "99999999999999999999999"] {
        let r = verify_webhook_signature(BODY.as_bytes(), &sign(ts, BODY), ts, SECRET, opts());
        assert_eq!(reason(r), WebhookErrorReason::Timestamp, "{ts:?}");
    }
}

#[test]
fn bad_json_is_payload_error() {
    let ts = NOW.to_string();
    let body = "not json";
    assert_eq!(reason(verify_webhook_signature(body.as_bytes(), &sign(&ts, body), &ts, SECRET, opts())), WebhookErrorReason::Payload);
    assert_eq!(reason(parse_webhook(b"[1,2]")), WebhookErrorReason::Payload);
}

#[test]
fn headers_are_case_insensitive() {
    let ts = NOW.to_string();
    let mut headers = HeaderMap::new();
    headers.insert(HeaderName::from_bytes(b"X-INVEZGO-TIMESTAMP").unwrap(), HeaderValue::from_str(&ts).unwrap());
    headers.insert(HeaderName::from_bytes(b"x-Invezgo-Signature").unwrap(), HeaderValue::from_str(&sign(&ts, BODY)).unwrap());
    assert_eq!(verify_webhook(BODY.as_bytes(), &headers, SECRET, opts()).unwrap().name(), "update.summary");

    let missing = HeaderMap::new();
    assert_eq!(reason(verify_webhook(BODY.as_bytes(), &missing, SECRET, opts())), WebhookErrorReason::Timestamp);
}

#[test]
fn typed_data_and_unknown_events() {
    let alert = parse_webhook(
        br#"{"id":"evt_1","domain":"alert","type":"triggered","created_at":1,"data":{"alert_id":"a1","alert_name":"Breakout","stocks_in":[{"code":"BBCA","price":9875}],"stocks_out":[]}}"#,
    )
    .unwrap();
    let data: WebhookAlertTriggeredData = alert.data_as().unwrap();
    assert_eq!((data.stocks_in[0].code.as_str(), data.stocks_in[0].price), ("BBCA", 9875.0));
    assert!(data.stocks_out.is_empty());

    let quota = parse_webhook(
        br#"{"id":"q","domain":"quota","type":"low","created_at":1,"data":{"plan":"PRIME","limit":90000,"used":85500,"remaining":4500,"reset_at":null}}"#,
    )
    .unwrap();
    let data: WebhookQuotaData = quota.data_as().unwrap();
    assert_eq!((data.limit, data.used, data.remaining, data.reset_at), (90000, 85500, 4500, None));

    let future = parse_webhook(br#"{"id":"x","domain":"new","type":"thing","created_at":1,"data":{"k":1}}"#).unwrap();
    assert_eq!(future.name(), "new.thing");
}
