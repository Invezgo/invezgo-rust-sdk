//! Run: `INVEZGO_API_KEY=xxx INVEZGO_API_KEYS_BACKUP=yyy,zzz cargo run --example demo`

use std::time::Duration;

use invezgo_sdk::params::{BatchOrderBookMarket, BatchOrderBookParams, StockRunningTradeParams, StockRunningTradeType, TopChangeParams};
use invezgo_sdk::{CacheConfig, ErrorKind, InvezgoClient, InvezgoError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = std::env::var("INVEZGO_API_KEY").unwrap_or_else(|_| "YOUR_API_KEY".into());
    let backups: Vec<String> = std::env::var("INVEZGO_API_KEYS_BACKUP")
        .map(|v| v.split(',').map(str::to_string).collect())
        .unwrap_or_default();

    let client = InvezgoClient::builder()
        .api_key(api_key)
        .additional_api_keys(backups) // rotated automatically on HTTP 429
        .timeout(Duration::from_secs(15))
        .cache(CacheConfig { ttl: Duration::from_secs(120), ..Default::default() })
        .build()?;

    // 1. Simple list (cached for 120 s; the second call does not hit the network)
    match client.analysis().get_stock_list().await {
        Ok(stocks) => {
            println!("{} stocks listed", stocks.len());
            for s in stocks.iter().take(3) {
                println!("- {} {} ({})", s.code, s.name, s.sector.as_deref().unwrap_or("-"));
            }
        }
        Err(e) => report(&e),
    }
    let _again = client.analysis().get_stock_list().await; // served from cache

    // 2. Required + optional query params
    let top = client.analysis().get_top_change(&TopChangeParams::new("2024-12-30")).await;
    match top {
        Ok(top) => {
            for s in top.gain.iter().take(5) {
                println!("{:<6} {:+.2}%", s.code, s.change);
            }
        }
        Err(e) => report(&e),
    }

    // 3. Enums for constrained values
    let params = StockRunningTradeParams {
        r#type: Some(StockRunningTradeType::Buy),
        limit: Some(20),
        ..StockRunningTradeParams::new("2024-12-30")
    };
    if let Err(e) = client.analysis().get_stock_running_trade("BBCA", &params).await {
        report(&e);
    }

    // 4. Batch endpoint with an optional params struct
    let batch = BatchOrderBookParams { market: Some(BatchOrderBookMarket::Rg), ..Default::default() };
    match client.batch().get_order_book("BBCA|BBRI|BMRI", Some(&batch)).await {
        Ok(books) => println!("{} order books", books.len()),
        Err(e) => report(&e),
    }

    // 5. Quota check (never cached, does not consume quota)
    match client.usage().get_api_usage().await {
        Ok(u) => println!("plan {}: {}/{} used", u.role, u.usage, u.limit),
        Err(e) => report(&e),
    }
    Ok(())
}

fn report(e: &InvezgoError) {
    match e {
        InvezgoError::RateLimited(rl) => {
            let what = if rl.quota_exceeded { "monthly quota" } else { "per-minute limit" };
            eprintln!("every API key hit the {what}; retry in {:?}", rl.retry_after);
        }
        InvezgoError::Api(api) => match api.kind {
            ErrorKind::Authentication => eprintln!("invalid API key: {}", api.message),
            ErrorKind::PaymentRequired => eprintln!("upgrade your plan: {}", api.message),
            ErrorKind::Validation => eprintln!("bad parameters: {}", api.message),
            _ => eprintln!("API error {} (request id {:?}): {}", api.status, api.request_id, api.message),
        },
        other if other.is_timeout() => eprintln!("timed out"),
        other => eprintln!("error: {other}"),
    }
}
