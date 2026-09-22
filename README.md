# binance_feed_test — rank AWS instances by Binance feed latency

Self-contained test box: the Tokyo Rust relay (`tokyo_server_udp`,
`tokyo_server_udp_spot`) races several Binance WebSocket endpoints, dedups on
update id `u`, and fires one 72-byte UDP packet per update at **127.0.0.1**.
`feed_latency_sink.py` on the same box receives them and appends one CSV row per
message. Nothing leaves the instance.

```
fstream bookTicker (x5) ──► tokyo_server_udp      ──UDP 127.0.0.1:9125──► feed_latency_sink.py ──► data/*.csv
spot bookTicker    (x4) ──► tokyo_server_udp_spot ──UDP 127.0.0.1:9225──►        "
```

Operating an agent on this kit? Read `CLAUDE.md` (same content as `AGENTS.md`).

## Launch (from the laptop)

1. EC2 → Launch, Ubuntu 24.04 (x86_64 or arm64), region `ap-northeast-1`,
   spread candidates over AZs `1a / 1c / 1d`.
2. Security group: inbound 22 from your laptop only. Outbound open (443 + 9443).
3. Paste `user-data.sh` as user data after setting `REPO=` to this repo's URL.
4. Wait ~3 min. `tail -f /var/log/latency-bootstrap.log` shows rustup, the
   build (~1 min on 4 vCPU) and `run.sh` output.

Manual alternative on a running box:

```bash
git clone <repo> ~/binance_feed_test && cd ~/binance_feed_test && ./bootstrap.sh
```

## Operate

| Command | What |
|---|---|
| `./run.sh` | start sink, then relays (default BTCUSDT futures + spot); idempotent |
| `SYMBOLS_FUT="BTCUSDT ETHUSDT" SYMBOLS_SPOT="" ./run.sh` | choose symbols ('' = none) |
| `LABEL=c7gn_1c ./run.sh` | file prefix (default hostname) |
| `./stop.sh` | relays first, then the sink (final flush) |
| `tail -f logs/sink.log` | 60 s summary: n, msg/s, lat p50/p95/p99, hop p50/p99 |
| `tail -f logs/fut_BTCUSDT.log` | relay's own log: connections, stats, hourly slow-connection check |
| `python3 summarize.py data/ --by hour` | percentiles from the CSVs |
| `python3 summarize.py boxA/data boxB/data` | several instances side by side |
| `python3 race.py boxA/data boxB/data --market fut` | **rank instances**: match on `u`, compare clocks |

## Files

| File | Notes |
|---|---|
| `data/{label}_{fut,spot}_{SYMBOL}_{YYYYMMDD}.csv` | one row per deduplicated update |
| `data/{label}_meta.json` | instance type, AZ, AZ-id, kernel, start time |
| `race.py` | cross-box ranking: joins boxes on `u`, compares `server_clock_ns` |
| `rust/` | unmodified Tokyo relay sources except default destination `10.0.2.14 → 127.0.0.1` |

CSV columns: `recv_ns, market, symbol, u, exchange_ts_ms, server_clock_ns,
lat_rust_us, hop_us, bid, bid_sz, ask, ask_sz`.

## Reading the numbers

- **To rank instances, use `race.py`.** It matches updates across boxes on the
  Binance update id `u` and compares each box's own `server_clock_ns`, so it
  never touches Binance's timestamp: no 1 ms quantization, and it works for
  spot as well as futures. Its resolution is the clock offset *between* boxes,
  which is why `bootstrap.sh` enables the PTP hardware clock (~1-10 µs) rather
  than relying on NTP (~250 µs). Verify PTP on every box before comparing:
  `chronyc sources -v | grep PHC`.
- **`lat_rust_us` (futures)** = Binance event time `E` → relay ready-to-send,
  measured inside Rust. This is the ranking metric. `E` has 1 ms resolution and
  is Binance's clock, so compare instances on medians over ≥1 h, not on single
  rows. Instance clock must be synced (chrony → 169.254.169.123; bootstrap
  checks it).
- **`hop_us`** = relay send → Python receive over loopback. Sanity only; it
  measures the box's scheduler, not Binance.
- **Spot** bookTicker has **no venue timestamp**. The spot relay puts its own
  receive time in `exchange_ts_ms` (floored to ms), so spot `lat_rust_us` is
  decode time plus up to 1 ms of rounding -- do not read it as a latency.
  Rank spot with `race.py --market spot`, which needs no venue timestamp.
  Binance's SBE spot stream has a µs `eventTime` but needs a per-instance API
  key and delivered no earlier than JSON; dropped.
- Run each candidate for at least a few hours across a busy period; msg rate
  scales ~14× with volatility and tail latency moves with it.

## Housekeeping

Rates: BTC futures ≈ 200 unique msgs/s ≈ 25 MB/day of CSV; spot ≈ 50/s.
`logs/` grows ~10 MB/day per relay. Nothing rotates; the test is days, not months.
