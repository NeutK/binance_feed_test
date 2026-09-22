# Agent instructions — binance_feed_test

You are operating a **latency test kit**, not a trading system. Its only job is
to measure how fast a given AWS instance receives Binance market data, so the
best instance type / availability zone can be chosen for the predict.fun bots.
Read `README.md` for the human view; this file is the operating procedure.

## What runs here

```
Binance WS (fstream bookTicker x5) ─► rust/target/release/tokyo_server_udp      ─UDP 127.0.0.1:9125─┐
Binance WS (spot bookTicker x4)    ─► rust/target/release/tokyo_server_udp_spot ─UDP 127.0.0.1:9225─┤
                                                                                                   ▼
                                                                    feed_latency_sink.py ─► data/*.csv
```

- The Rust relays open several WebSocket connections per symbol, dedup on the
  Binance update id `u`, and fire one 72-byte UDP packet per update at loopback.
  Packet: `<16s symbol, Q exchange_ts_ms, Q server_clock_ns, d bid, d bid_sz, d ask, d ask_sz, Q u>`.
- The Python sink appends one CSV row per packet. Nothing leaves the instance.
- Ranking metric: `lat_rust_us` in the **fut** CSV = Binance event time `E` →
  relay ready-to-send, measured inside Rust. Compare instances on the median
  over ≥ 1 hour. `E` is 1 ms resolution, so single rows are meaningless.
- **Spot has no venue timestamp.** Spot `lat_rust_us` is decode time only.
  Spot is ranked only by racing two instances on `u` (see "Compare").

## Tasks you may be asked to do

### 1. Bring the kit up on a fresh instance
```bash
cd ~/binance_feed_test && ./bootstrap.sh        # apt, rustup, cargo build, chrony check, run.sh
```
If launched with `user-data.sh` this already happened; check
`/var/log/latency-bootstrap.log`. Then verify (section "Verify").

### 2. Start / stop
```bash
LABEL=<type>_<az> ./run.sh      # e.g. LABEL=c7gn_medium_1c ; default label = hostname
./stop.sh                        # relays first, then sink (final flush)
SYMBOLS_FUT="BTCUSDT ETHUSDT" SYMBOLS_SPOT="" ./run.sh   # choose symbols, '' = none
```
Both scripts match processes by the kit's **absolute path**; they never touch
other relay copies on the same box. Always set `LABEL` to something that
identifies instance type + AZ; it becomes the CSV prefix.

### 3. Verify it is measuring (do this after every start)
```bash
pgrep -af "binance_feed_test/"            # expect: sink + one relay per symbol
tail -n 6 logs/sink.log                  # a summary line per stream every 60 s
grep -E "connected|ERROR" logs/fut_BTCUSDT.log | tail   # 5 futures conns connected
chronyc tracking | grep -E "Reference|System time"      # AWS clock, offset ≪ 1 ms
chronyc sources -v | grep PHC                          # PTP up -> cross-box race is valid
cat data/*_meta.json                      # instance_type, availability_zone, az_id filled
```
Healthy numbers on a Tokyo box: fut ≈ 100–250 msg/s with `lat_us p50` around
1.0–1.5 ms; spot ≈ 30–60 msg/s; `hop_us p50` ≈ 100–200. `n=0 (no packets)` for
a stream means its relay is not running or is bound to the wrong port.

### 4. Summarise
```bash
python3 summarize.py data/                      # this instance
python3 summarize.py data/ --by hour            # per hour, to see busy vs quiet
python3 summarize.py boxA/data boxB/data        # several instances (after copying dirs)
```
Report as a table: label, instance type, AZ id, hours of data, fut msg/s,
lat p50 / p90 / p99. Say how long the window was and whether it included a
busy period (msg rate scales ~14× with volatility; tails move with it).

### 5. Compare instances  -- use race.py, not summarize.py
Collect each instance's `data/` directory to one place (scp from the laptop):

```bash
python3 race.py boxA/data boxB/data boxC/data --market fut  --symbol BTCUSDT
python3 race.py boxA/data boxB/data           --market spot --symbol BTCUSDT
```

`race.py` matches updates across boxes on the Binance update id `u` and
compares each box's own `server_clock_ns`: the smallest had it first. It never
reads `E`, so the 1 ms quantization baked into `lat_rust_us` does not apply,
and it works for **spot** exactly as for futures. Report win share plus
`lag_p50/p90/p99`.

Resolution is the clock offset *between* boxes, not anything about the venue.
On Nitro, AWS Time Sync over the local link already gives single-digit µs
(measured 0.5-6 µs offset, 3-30 µs RMS in ap-northeast-1), so a race is valid
without `/dev/ptp0`; `bootstrap.sh` enables the PTP hardware clock only when
the kernel's ena driver exposes one. Read `chronyc tracking` RMS offset on
every box and ignore differences below ~2x the worst RMS. An RMS above ~100 µs
means that box is still settling -- wait, or drop it.

`summarize.py` is still right for absolute latency and for checking one box
over time (`--by hour`) -- just not for ranking.

## Guardrails

- **Never** run `pkill -f tokyo_server_udp`, `pkill -f python`, or anything
  that matches by name only. The existing Tokyo production box runs the same
  relay binaries from `~/tokyo_proxy_server/` and a live trading bot; use
  `./stop.sh` or explicit PIDs.
- Do not edit the Rust sources for a test. They are the production relay with
  one change (default destination → 127.0.0.1); changing them makes results
  incomparable to the live feed.
- Do not stream data anywhere, add a database, or call any exchange endpoint
  that needs credentials. This kit needs no API key. If someone asks for the
  Binance SBE stream: it was tested, it needs a per-instance Ed25519 API key,
  and it delivered updates no earlier than JSON, so it was dropped.
- Never commit `data/`, `logs/`, `rust/target/`, or any key material. They are
  gitignored; keep it that way.
- Do not set up Binance API keys, AWS credentials, or SSH keys on a test
  instance. Instances are launched from the operator's laptop; that is where
  cloud credentials live.
- If a port is already in use, do **not** kill the owner. Restrict the sink
  with `--fut-symbols/--spot-symbols` (what `run.sh` does) or change the port
  in `rust/symbol_ports*.json` and restart the matching relay.

## Known gotchas

- `net.core.rmem_max` on a stock instance is ~212 kB; `bootstrap.sh` raises it
  so the sink's 8 MB receive buffer is honoured. Without it bursts can drop.
- The futures relay reconnects the slowest of its 5 connections once an hour
  if it is > 2× the fleet median; expect a `reconnecting slowest connection`
  log line, it is normal.
- Relays log every 60 s (`Stats ... Received/Sent`). Received counts all
  connections, Sent is unique updates after dedup; Sent ≈ Received / #conns.
- CSV files roll per UTC day and per stream; `summarize.py` reads all of them.
- Off AWS (e.g. a dev box in Europe) the kit still works; latency will be
  ~120 ms and `meta.json` fields from the metadata service will be null.
