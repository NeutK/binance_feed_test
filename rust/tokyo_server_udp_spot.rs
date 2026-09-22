use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::Local;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use rustls::crypto::ring::default_provider;
use serde_json::{json, Value};
use tokio::net::UdpSocket;
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const DEFAULT_AMSTERDAM_IP: &str = "127.0.0.1";
const DEFAULT_PORT_CONFIG: &str = "symbol_ports_spot.json";

const SYMBOL_SIZE: usize = 16;
const PACKET_SIZE: usize = SYMBOL_SIZE + 8 + 8 + 8 * 5;
/// Reconnect if no text/binary WebSocket frame for this long (silent stall / half-open TCP).
const WS_IDLE_RECONNECT: Duration = Duration::from_secs(5);
const WS_RECV_TIMEOUT: Duration = Duration::from_secs(1);
/// Emit a periodic "still waiting for payload" log while socket stays idle.
const WS_IDLE_PROGRESS_LOG: Duration = Duration::from_secs(1);
const SLOW_CONNECTION_CHECK: Duration = Duration::from_secs(60 * 60);
/// Minimum number of deduplicated updates decided in a window before judging connections.
const WIN_CHECK_MIN_UPDATES: u64 = 1_000;
/// Reconnect a connection whose win share is below (fair share / this factor).
const WIN_STARVATION_FACTOR: f64 = 4.0;

/// Spot WebSocket endpoints. Unlike futures (`fstream.binance.com`), spot has no
/// `/public/ws` variant; instead we fan out across the two stream ports, the
/// market-data-only mirror, and the combined `/stream` endpoint. The spot
/// `bookTicker` payload has no `E`/`T` event time, so latency is measured by
/// "who delivered each update id first" (win-based ranking) rather than exchange time.
///
/// Tuple layout: (name, base_uri, combined, connection_count).
const ENDPOINT_LAYOUT: [(&str, &str, bool, usize); 4] = [
    ("raw-9443", "wss://stream.binance.com:9443/ws", false, 1),
    ("raw-443", "wss://stream.binance.com:443/ws", false, 1),
    (
        "data-vision",
        "wss://data-stream.binance.vision/ws",
        false,
        1,
    ),
    ("combined", "wss://stream.binance.com:9443/stream", true, 1),
];

#[derive(Parser, Debug)]
#[command(about = "Tokyo UDP relay for a single Binance SPOT symbol.")]
struct Args {
    #[arg(long)]
    symbol: String,

    #[arg(long, default_value = DEFAULT_AMSTERDAM_IP)]
    amsterdam_ip: String,

    #[arg(long, default_value = DEFAULT_PORT_CONFIG)]
    port_config: String,
}

#[derive(Clone)]
struct ConnectionRelay {
    symbol: String,
    endpoint_name: &'static str,
    base_uri: &'static str,
    combined: bool,
    connection_id: usize,
}

impl ConnectionRelay {
    fn name(&self) -> String {
        format!(
            "{}-{}-{}",
            self.endpoint_name, self.connection_id, self.symbol
        )
    }
}

/// Latest raw WebSocket payload plus the Tokyo receive time (unix ms) captured
/// the instant it arrived off the socket.
#[derive(Default)]
struct LatestBuffer {
    latest: Option<(Vec<u8>, u64)>,
}

struct BufferState {
    latest: Mutex<LatestBuffer>,
    notify: Notify,
}

impl BufferState {
    fn new() -> Self {
        Self {
            latest: Mutex::new(LatestBuffer::default()),
            notify: Notify::new(),
        }
    }
}

struct ConnectionHealth {
    name: String,
    /// Number of updates this connection delivered first within the current window.
    wins: AtomicU64,
    reconnect_tx: watch::Sender<u64>,
}

struct WinSnapshot {
    health: Arc<ConnectionHealth>,
    name: String,
    wins: u64,
}

impl ConnectionHealth {
    fn new(name: String) -> (Arc<Self>, watch::Receiver<u64>) {
        let (reconnect_tx, reconnect_rx) = watch::channel(0_u64);
        let health = Arc::new(Self {
            name,
            wins: AtomicU64::new(0),
            reconnect_tx,
        });

        (health, reconnect_rx)
    }

    fn record_win(&self) {
        self.wins.fetch_add(1, Ordering::Relaxed);
    }

    fn request_reconnect(&self) {
        let current = *self.reconnect_tx.borrow();
        let _ = self.reconnect_tx.send(current.wrapping_add(1));
    }

    fn drain_win_snapshot(health: &Arc<Self>) -> WinSnapshot {
        let wins = health.wins.swap(0, Ordering::Relaxed);
        WinSnapshot {
            health: Arc::clone(health),
            name: health.name.clone(),
            wins,
        }
    }
}

#[derive(Clone)]
struct TokyoSymbolRelay {
    inner: Arc<TokyoSymbolRelayInner>,
    shutdown_tx: watch::Sender<bool>,
}

struct TokyoSymbolRelayInner {
    symbol: String,
    amsterdam_ip: String,
    amsterdam_port: u16,
    udp_socket: Arc<UdpSocket>,
    udp_addr: SocketAddr,
    messages_received: AtomicU64,
    messages_sent: AtomicU64,
    last_u: AtomicU64,
    connection_health: Mutex<Vec<Arc<ConnectionHealth>>>,
}

impl TokyoSymbolRelay {
    async fn new(
        symbol: String,
        amsterdam_ip: String,
        port_config: String,
    ) -> std::io::Result<Self> {
        let symbol = normalize_symbol(&symbol);
        if symbol.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "symbol must not be empty",
            ));
        }

        let amsterdam_port = load_udp_port(&port_config, &symbol)?;
        let udp_socket = UdpSocket::bind("0.0.0.0:0").await?;
        let udp_addr: SocketAddr = format!("{amsterdam_ip}:{amsterdam_port}")
            .parse()
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
        let (shutdown_tx, _) = watch::channel(false);

        Ok(Self {
            inner: Arc::new(TokyoSymbolRelayInner {
                symbol,
                amsterdam_ip,
                amsterdam_port,
                udp_socket: Arc::new(udp_socket),
                udp_addr,
                messages_received: AtomicU64::new(0),
                messages_sent: AtomicU64::new(0),
                last_u: AtomicU64::new(0),
                connection_health: Mutex::new(Vec::new()),
            }),
            shutdown_tx,
        })
    }

    async fn run(&self) -> std::io::Result<()> {
        info("Starting Tokyo single-symbol SPOT UDP relay");
        info(&format!(
            "Symbol={} packet_size={}B destination={}:{}",
            self.inner.symbol, PACKET_SIZE, self.inner.amsterdam_ip, self.inner.amsterdam_port
        ));

        let mut join_handles = self.spawn_connection_tasks();
        let monitor_handle = {
            let relay = self.clone();
            tokio::spawn(async move {
                relay.monitor_stats(relay.shutdown_tx.subscribe()).await;
            })
        };
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    info("Shutdown requested");
                }
            }
            ctrl_c = tokio::signal::ctrl_c() => {
                match ctrl_c {
                    Ok(()) => info("Shutting down due to Ctrl-C"),
                    Err(err) => error(&format!("Ctrl-C handler error: {}", err)),
                }
            }
        }

        self.request_shutdown();

        for handle in join_handles.drain(..) {
            let _ = handle.await;
        }
        let _ = monitor_handle.await;

        info("Relay shutdown complete");
        Ok(())
    }

    fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    fn spawn_connection_tasks(&self) -> Vec<JoinHandle<()>> {
        let mut join_handles = Vec::new();

        for (endpoint_name, base_uri, combined, connection_count) in ENDPOINT_LAYOUT {
            for connection_id in 1..=connection_count {
                let connection = ConnectionRelay {
                    symbol: self.inner.symbol.clone(),
                    endpoint_name,
                    base_uri,
                    combined,
                    connection_id,
                };
                let buffer = Arc::new(BufferState::new());
                let (health, reconnect_rx) = ConnectionHealth::new(connection.name());
                self.inner
                    .connection_health
                    .lock()
                    .unwrap()
                    .push(Arc::clone(&health));

                join_handles.push(tokio::spawn(connection_reader(
                    self.clone(),
                    connection.clone(),
                    Arc::clone(&buffer),
                    reconnect_rx,
                    self.shutdown_tx.subscribe(),
                )));
                join_handles.push(tokio::spawn(connection_processor(
                    self.clone(),
                    connection,
                    buffer,
                    health,
                    self.shutdown_tx.subscribe(),
                )));
            }
        }

        info(&format!(
            "Started {} websocket connections for {}",
            join_handles.len() / 2,
            self.inner.symbol
        ));
        join_handles
    }

    async fn monitor_stats(&self, mut shutdown_rx: watch::Receiver<bool>) {
        let mut last_monitor = Instant::now();
        let mut last_slow_check = Instant::now();

        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = sleep(Duration::from_secs(60)) => {
                    let elapsed = last_monitor.elapsed().as_secs_f64().max(1e-9);
                    let received = self.inner.messages_received.swap(0, Ordering::Relaxed);
                    let sent = self.inner.messages_sent.swap(0, Ordering::Relaxed);

                    info(&format!(
                        "Stats {} - Received: {} ({:.2}/s), Sent: {} ({:.2}/s)",
                        self.inner.symbol,
                        received,
                        received as f64 / elapsed,
                        sent,
                        sent as f64 / elapsed,
                    ));

                    last_monitor = Instant::now();

                    if last_slow_check.elapsed() >= SLOW_CONNECTION_CHECK {
                        self.replace_starved_connection_if_needed();
                        last_slow_check = Instant::now();
                    }
                }
            }
        }
    }

    /// Spot `bookTicker` carries no exchange timestamp, so connection quality is
    /// judged by how often each connection delivered an update first ("wins").
    /// A connection whose win share falls far below its fair share is treated as
    /// chronically slow and reconnected.
    fn replace_starved_connection_if_needed(&self) {
        let snapshots = {
            let health = self.inner.connection_health.lock().unwrap();
            health
                .iter()
                .map(ConnectionHealth::drain_win_snapshot)
                .collect::<Vec<_>>()
        };

        if snapshots.len() < 2 {
            return;
        }

        let total_wins: u64 = snapshots.iter().map(|snapshot| snapshot.wins).sum();
        if total_wins < WIN_CHECK_MIN_UPDATES {
            info(&format!(
                "Win check skipped: only {} decided updates this window (need {})",
                total_wins, WIN_CHECK_MIN_UPDATES
            ));
            return;
        }

        let fair_share = 1.0 / snapshots.len() as f64;
        let threshold_share = fair_share / WIN_STARVATION_FACTOR;

        let worst = snapshots
            .iter()
            .min_by_key(|snapshot| snapshot.wins)
            .unwrap();
        let worst_share = worst.wins as f64 / total_wins as f64;

        if worst_share < threshold_share {
            info(&format!(
                "[{}] win share {:.1}% (={} of {}) below {:.1}% threshold; reconnecting starved connection",
                worst.name,
                worst_share * 100.0,
                worst.wins,
                total_wins,
                threshold_share * 100.0,
            ));
            worst.health.request_reconnect();
        } else {
            info(&format!(
                "Win check ok: worst={} share={:.1}%, fair share={:.1}%, threshold={:.1}%",
                worst.name,
                worst_share * 100.0,
                fair_share * 100.0,
                threshold_share * 100.0,
            ));
        }
    }

    async fn forward_udp(&self, payload: &Value, u_value: u64, recv_unix_ms: u64) -> Result<(), String> {
        let payload_symbol = payload
            .get("s")
            .and_then(Value::as_str)
            .map(normalize_symbol)
            .ok_or_else(|| "missing symbol".to_string())?;

        if payload_symbol != self.inner.symbol {
            return Err(format!(
                "unexpected symbol {} on {} relay",
                payload_symbol, self.inner.symbol
            ));
        }

        let bid_price = payload
            .get("b")
            .and_then(json_to_f64)
            .ok_or_else(|| "missing bid price".to_string())?;
        let bid_size = payload
            .get("B")
            .and_then(json_to_f64)
            .ok_or_else(|| "missing bid size".to_string())?;
        let ask_price = payload
            .get("a")
            .and_then(json_to_f64)
            .ok_or_else(|| "missing ask price".to_string())?;
        let ask_size = payload
            .get("A")
            .and_then(json_to_f64)
            .ok_or_else(|| "missing ask size".to_string())?;
        let server_clock_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| err.to_string())?
            .as_nanos() as u64;

        let mut packet = [0_u8; PACKET_SIZE];
        let symbol_bytes = self.inner.symbol.as_bytes();
        let copy_len = symbol_bytes.len().min(SYMBOL_SIZE);
        packet[..copy_len].copy_from_slice(&symbol_bytes[..copy_len]);
        // Spot has no exchange event time; carry the Tokyo receive time (ms) instead.
        packet[16..24].copy_from_slice(&recv_unix_ms.to_le_bytes());
        packet[24..32].copy_from_slice(&server_clock_ns.to_le_bytes());
        packet[32..40].copy_from_slice(&bid_price.to_le_bytes());
        packet[40..48].copy_from_slice(&bid_size.to_le_bytes());
        packet[48..56].copy_from_slice(&ask_price.to_le_bytes());
        packet[56..64].copy_from_slice(&ask_size.to_le_bytes());
        packet[64..72].copy_from_slice(&u_value.to_le_bytes());

        self.inner
            .udp_socket
            .send_to(&packet, self.inner.udp_addr)
            .await
            .map_err(|err| err.to_string())?;
        self.inner.messages_sent.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }
}

async fn connection_reader(
    relay: TokyoSymbolRelay,
    connection: ConnectionRelay,
    buffer: Arc<BufferState>,
    mut reconnect_rx: watch::Receiver<u64>,
    mut stop_rx: watch::Receiver<bool>,
) {
    let stream_name = format!("{}@bookTicker", connection.symbol.to_lowercase());
    let uri = build_uri(connection.base_uri, connection.combined, &stream_name);
    let mut message_id = 1_u64;

    while !*stop_rx.borrow() {
        info(&format!("[{}] connecting to {}", connection.name(), uri));

        match connect_async(&uri).await {
            Ok((mut ws, _)) => {
                if connection.combined {
                    let message = json!({
                        "method": "SUBSCRIBE",
                        "params": [stream_name],
                        "id": message_id,
                    })
                    .to_string();
                    message_id += 1;

                    if let Err(err) = ws.send(Message::Text(message.into())).await {
                        if !*stop_rx.borrow() {
                            error(&format!("[{}] reader error: {}", connection.name(), err));
                            sleep(Duration::from_millis(200)).await;
                        }
                        continue;
                    }
                }

                info(&format!("[{}] connected", connection.name()));

                let mut last_payload = Instant::now();
                let mut last_idle_progress_log = Instant::now();

                loop {
                    tokio::select! {
                        changed = stop_rx.changed() => {
                            if changed.is_err() || *stop_rx.borrow() {
                                return;
                            }
                        }
                        changed = reconnect_rx.changed() => {
                            if changed.is_ok() && !*stop_rx.borrow() {
                                info(&format!("[{}] reconnect requested by slow connection monitor", connection.name()));
                                break;
                            }
                        }
                        recv_result = timeout(WS_RECV_TIMEOUT, ws.next()) => {
                            match recv_result {
                                Ok(Some(Ok(message))) => {
                                    if let Some(raw) = message_to_raw_bytes(message) {
                                        let recv_unix_ms = current_unix_millis();
                                        last_payload = Instant::now();
                                        relay.inner.messages_received.fetch_add(1, Ordering::Relaxed);
                                        {
                                            let mut guard = buffer.latest.lock().unwrap();
                                            guard.latest = Some((raw, recv_unix_ms));
                                        }
                                        buffer.notify.notify_one();
                                    }
                                }
                                Ok(Some(Err(err))) => {
                                    if !*stop_rx.borrow() {
                                        error(&format!("[{}] connection closed: {}", connection.name(), err));
                                        sleep(Duration::from_millis(200)).await;
                                    }
                                    break;
                                }
                                Ok(None) => {
                                    if !*stop_rx.borrow() {
                                        error(&format!("[{}] connection closed: websocket closed", connection.name()));
                                        sleep(Duration::from_millis(200)).await;
                                    }
                                    break;
                                }
                                Err(_) => {
                                    if last_payload.elapsed() >= WS_IDLE_RECONNECT {
                                        info(&format!(
                                            "[{}] idle {:?} without payload; reconnecting",
                                            connection.name(),
                                            WS_IDLE_RECONNECT
                                        ));
                                        break;
                                    }
                                    if last_idle_progress_log.elapsed() >= WS_IDLE_PROGRESS_LOG {
                                        info(&format!(
                                            "[{}] no payload for {:.1}s (waiting before reconnect threshold {:.1}s)",
                                            connection.name(),
                                            last_payload.elapsed().as_secs_f64(),
                                            WS_IDLE_RECONNECT.as_secs_f64(),
                                        ));
                                        last_idle_progress_log = Instant::now();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(err) => {
                if !*stop_rx.borrow() {
                    error(&format!("[{}] reader error: {}", connection.name(), err));
                    sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}

async fn connection_processor(
    relay: TokyoSymbolRelay,
    connection: ConnectionRelay,
    buffer: Arc<BufferState>,
    health: Arc<ConnectionHealth>,
    mut stop_rx: watch::Receiver<bool>,
) {
    let mut decode_fail_count: u64 = 0;
    let mut missing_u_count: u64 = 0;

    loop {
        let latest = {
            let mut guard = buffer.latest.lock().unwrap();
            guard.latest.take()
        };

        if let Some((raw, recv_unix_ms)) = latest {
            if let Some(payload) = decode_payload(&raw, connection.combined) {
                let u_value = payload.get("u").and_then(json_to_u64);
                if let Some(u_value) = u_value {
                    if register_new_u(&relay.inner.last_u, u_value) {
                        health.record_win();
                        if let Err(err) = relay.forward_udp(&payload, u_value, recv_unix_ms).await {
                            if !*stop_rx.borrow() {
                                error(&format!("[{}] processor error: {}", connection.name(), err));
                                sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }
                } else {
                    missing_u_count += 1;
                    if missing_u_count % 100 == 0 {
                        info(&format!(
                            "[{}] payloads missing 'u': {} samples seen",
                            connection.name(),
                            missing_u_count
                        ));
                    }
                }
            } else {
                decode_fail_count += 1;
                if decode_fail_count % 100 == 0 {
                    info(&format!(
                        "[{}] decode/drop count={} (combined={})",
                        connection.name(),
                        decode_fail_count,
                        connection.combined
                    ));
                }
            }

            continue;
        }

        if *stop_rx.borrow() {
            return;
        }

        tokio::select! {
            changed = stop_rx.changed() => {
                if changed.is_err() || *stop_rx.borrow() {
                    return;
                }
            }
            _ = buffer.notify.notified() => {}
        }
    }
}

fn load_udp_port(config_path: &str, symbol: &str) -> std::io::Result<u16> {
    let raw = std::fs::read(config_path)?;
    let configured_ports: HashMap<String, u16> = serde_json::from_slice(&raw).map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("failed to parse {}: {}", config_path, err),
        )
    })?;

    configured_ports
        .into_iter()
        .find_map(|(configured_symbol, port)| {
            (normalize_symbol(&configured_symbol) == symbol).then_some(port)
        })
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("symbol {} is missing from {}", symbol, config_path),
            )
        })
}

fn build_uri(base_uri: &str, combined: bool, stream_name: &str) -> String {
    if combined {
        base_uri.to_string()
    } else {
        format!("{}/{}", base_uri, stream_name)
    }
}

fn decode_payload(raw: &[u8], combined: bool) -> Option<Value> {
    let payload: Value = serde_json::from_slice(raw).ok()?;
    if combined {
        payload.get("data").cloned()
    } else if payload.is_object() {
        Some(payload)
    } else {
        None
    }
}

fn json_to_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()))
}

fn json_to_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_u64().map(|value| value as f64))
        .or_else(|| value.as_i64().map(|value| value as f64))
        .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()))
}

fn register_new_u(last_u: &AtomicU64, u_value: u64) -> bool {
    let mut current = last_u.load(Ordering::Relaxed);

    loop {
        if u_value <= current {
            return false;
        }

        match last_u.compare_exchange_weak(current, u_value, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

fn message_to_raw_bytes(message: Message) -> Option<Vec<u8>> {
    match message {
        Message::Text(text) => Some(text.to_string().into_bytes()),
        Message::Binary(binary) => Some(binary.to_vec()),
        _ => None,
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol.trim().to_uppercase()
}

fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn info(message: &str) {
    log_line("INFO", message, false);
}

fn error(message: &str) {
    log_line("ERROR", message, true);
}

fn log_line(level: &str, message: &str, to_stderr: bool) {
    let now = Local::now();
    let ts = now.format("%Y-%m-%d %H:%M:%S%.6f");
    let line = format!("{ts} - __main__ - {level} - {message}");
    if to_stderr {
        eprintln!("{line}");
    } else {
        println!("{line}");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = default_provider().install_default();

    let args = Args::parse();
    let relay = TokyoSymbolRelay::new(args.symbol, args.amsterdam_ip, args.port_config).await?;
    relay.run().await?;
    Ok(())
}
