use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use rustls::crypto::ring::default_provider;
use serde_json::{json, Value};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WS_RECV_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Parser, Debug)]
#[command(about = "Race Binance SPOT bookTicker endpoints to see which delivers each update first.")]
struct Args {
    #[arg(long, default_value = "BTCUSDT")]
    symbol: String,

    /// How long to run the race for, in seconds.
    #[arg(long, default_value_t = 30)]
    seconds: u64,

    /// How many parallel websocket connections to open per endpoint.
    #[arg(long, default_value_t = 3)]
    conns: u64,
}

/// One candidate spot endpoint under test.
struct Endpoint {
    name: &'static str,
    uri: String,
    /// `true` for the combined `/stream` endpoint that needs a SUBSCRIBE frame.
    use_subscribe: bool,
    stream_name: String,
    stats: Stats,
}

#[derive(Default)]
struct Stats {
    connects: AtomicU64,
    messages: AtomicU64,
    wins: AtomicU64,
    /// Deliveries where another endpoint already had this `u` (i.e. we were late).
    losses: AtomicU64,
    /// Sum of lateness (microseconds) over losing deliveries.
    lateness_sum_us: AtomicU64,
    /// Worst single lateness observed (microseconds).
    lateness_max_us: AtomicU64,
}

/// Global record of the first arrival time per update id `u`.
type FirstSeen = Arc<Mutex<HashMap<u64, Instant>>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = default_provider().install_default();
    let args = Args::parse();
    let symbol = args.symbol.trim().to_lowercase();
    let stream_name = format!("{symbol}@bookTicker");

    let endpoints: Vec<Arc<Endpoint>> = vec![
        Arc::new(Endpoint {
            name: "raw-9443",
            uri: format!("wss://stream.binance.com:9443/ws/{stream_name}"),
            use_subscribe: false,
            stream_name: stream_name.clone(),
            stats: Stats::default(),
        }),
        Arc::new(Endpoint {
            name: "raw-443",
            uri: format!("wss://stream.binance.com:443/ws/{stream_name}"),
            use_subscribe: false,
            stream_name: stream_name.clone(),
            stats: Stats::default(),
        }),
        Arc::new(Endpoint {
            name: "data-vision",
            uri: format!("wss://data-stream.binance.vision/ws/{stream_name}"),
            use_subscribe: false,
            stream_name: stream_name.clone(),
            stats: Stats::default(),
        }),
        Arc::new(Endpoint {
            name: "combined-9443",
            uri: "wss://stream.binance.com:9443/stream".to_string(),
            use_subscribe: true,
            stream_name: stream_name.clone(),
            stats: Stats::default(),
        }),
    ];

    let first_seen: FirstSeen = Arc::new(Mutex::new(HashMap::new()));
    let deadline = Instant::now() + Duration::from_secs(args.seconds);

    println!(
        "Racing {} spot endpoints x{} connections each ({} sockets) for {} ({}s)...",
        endpoints.len(),
        args.conns,
        endpoints.len() as u64 * args.conns,
        stream_name,
        args.seconds
    );

    let mut handles = Vec::new();
    for endpoint in &endpoints {
        for conn_id in 1..=args.conns {
            let endpoint = Arc::clone(endpoint);
            let first_seen = Arc::clone(&first_seen);
            handles.push(tokio::spawn(async move {
                run_endpoint(endpoint, conn_id, first_seen, deadline).await;
            }));
        }
    }

    for handle in handles {
        let _ = handle.await;
    }

    print_summary(&endpoints, &first_seen, args.seconds);
    Ok(())
}

async fn run_endpoint(
    endpoint: Arc<Endpoint>,
    conn_id: u64,
    first_seen: FirstSeen,
    deadline: Instant,
) {
    let mut message_id = 1_u64;

    while Instant::now() < deadline {
        match connect_async(&endpoint.uri).await {
            Ok((mut ws, _)) => {
                endpoint.stats.connects.fetch_add(1, Ordering::Relaxed);

                if endpoint.use_subscribe {
                    let sub = json!({
                        "method": "SUBSCRIBE",
                        "params": [endpoint.stream_name],
                        "id": message_id,
                    })
                    .to_string();
                    message_id += 1;
                    if ws.send(Message::Text(sub.into())).await.is_err() {
                        sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                }

                loop {
                    if Instant::now() >= deadline {
                        return;
                    }
                    match timeout(WS_RECV_TIMEOUT, ws.next()).await {
                        Ok(Some(Ok(message))) => {
                            let now = Instant::now();
                            if let Some(u) = extract_update_id(message, endpoint.use_subscribe) {
                                record(&endpoint, &first_seen, u, now);
                            }
                        }
                        Ok(Some(Err(_))) | Ok(None) => {
                            sleep(Duration::from_millis(200)).await;
                            break;
                        }
                        Err(_) => {}
                    }
                }
            }
            Err(err) => {
                eprintln!("[{}#{}] connect error: {}", endpoint.name, conn_id, err);
                sleep(Duration::from_millis(300)).await;
            }
        }
    }
}

fn record(endpoint: &Endpoint, first_seen: &FirstSeen, u: u64, now: Instant) {
    endpoint.stats.messages.fetch_add(1, Ordering::Relaxed);

    let winner_at = {
        let mut guard = first_seen.lock().unwrap();
        match guard.get(&u) {
            Some(&t0) => Some(t0),
            None => {
                guard.insert(u, now);
                None
            }
        }
    };

    match winner_at {
        None => {
            endpoint.stats.wins.fetch_add(1, Ordering::Relaxed);
        }
        Some(t0) => {
            let lateness_us = now.saturating_duration_since(t0).as_micros() as u64;
            endpoint.stats.losses.fetch_add(1, Ordering::Relaxed);
            endpoint
                .stats
                .lateness_sum_us
                .fetch_add(lateness_us, Ordering::Relaxed);
            endpoint
                .stats
                .lateness_max_us
                .fetch_max(lateness_us, Ordering::Relaxed);
        }
    }
}

fn extract_update_id(message: Message, combined: bool) -> Option<u64> {
    let raw = match message {
        Message::Text(text) => text.to_string().into_bytes(),
        Message::Binary(binary) => binary.to_vec(),
        _ => return None,
    };
    let payload: Value = serde_json::from_slice(&raw).ok()?;
    let book = if combined {
        payload.get("data")?
    } else {
        &payload
    };
    book.get("u")
        .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
}

fn print_summary(endpoints: &[Arc<Endpoint>], first_seen: &FirstSeen, seconds: u64) {
    let total_updates = first_seen.lock().unwrap().len() as u64;
    let secs = seconds.max(1) as f64;

    println!("\n================ SPOT ENDPOINT RACE RESULTS ================");
    println!("Distinct order-book updates seen: {total_updates}\n");
    println!(
        "{:<15} {:>7} {:>9} {:>8} {:>8} {:>7} {:>12} {:>12}",
        "endpoint", "conn", "msgs", "msg/s", "wins", "win%", "avgLate(us)", "maxLate(us)"
    );

    for endpoint in endpoints {
        let s = &endpoint.stats;
        let connects = s.connects.load(Ordering::Relaxed);
        let messages = s.messages.load(Ordering::Relaxed);
        let wins = s.wins.load(Ordering::Relaxed);
        let losses = s.losses.load(Ordering::Relaxed);
        let lateness_sum = s.lateness_sum_us.load(Ordering::Relaxed);
        let lateness_max = s.lateness_max_us.load(Ordering::Relaxed);

        let win_pct = if total_updates > 0 {
            wins as f64 * 100.0 / total_updates as f64
        } else {
            0.0
        };
        let avg_late = if losses > 0 {
            lateness_sum as f64 / losses as f64
        } else {
            0.0
        };

        println!(
            "{:<15} {:>7} {:>9} {:>8.1} {:>8} {:>6.1}% {:>12.0} {:>12}",
            endpoint.name,
            connects,
            messages,
            messages as f64 / secs,
            wins,
            win_pct,
            avg_late,
            lateness_max,
        );
    }

    println!("\nHigher win% = delivers updates first more often.");
    println!("avgLate(us) = mean delay vs the fastest endpoint on updates it lost.");
    println!("===========================================================");
}
