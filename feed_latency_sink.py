#!/usr/bin/env python3
"""
Loopback UDP sink for the Tokyo Binance relay -> per-message latency CSV.

Runs on the SAME instance as tokyo_server_udp / tokyo_server_udp_spot, which
are started with --amsterdam-ip 127.0.0.1 (the default in this checkout).
Each relay fires one 72-byte packet per deduplicated bookTicker update:

    <16s  symbol
    Q     exchange_ts_ms   fut:  Binance event time "E" (ms, Binance clock)
                           spot: relay receive time (ms, local clock) -- spot
                                 bookTicker carries no venue timestamp
    Q     server_clock_ns  relay send time, CLOCK_REALTIME ns (local clock)
    d d   bid px, bid sz
    d d   ask px, ask sz
    Q     u                Binance update id

Headline metric (futures only):
    lat_rust_us = server_clock_ns/1e3 - exchange_ts_ms*1e3
i.e. Binance event time -> relay ready-to-send, measured inside the Rust
process with no Python in the path. The Python receive time is recorded as a
sanity column only (hop_us = loopback + asyncio wakeup).

E has 1 ms resolution and includes Binance's own internal delay; both are the
same for every candidate instance, so the number ranks instances correctly
but is not a wire latency. The instance clock must be NTP/PTP synced
(Ubuntu AWS AMIs run chrony against 169.254.169.123 by default).

Rows are queued from the asyncio receive path and written by a background
thread in batches, so the hot path never touches the filesystem.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import datetime as dt
import json
import os
import queue
import signal
import socket
import statistics
import struct
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

PACKET_FORMAT = "<16sQQddddQ"
PACKET_SIZE = struct.calcsize(PACKET_FORMAT)  # 72

CSV_HEADER = [
    "recv_ns",          # python datagram_received, CLOCK_REALTIME ns
    "market",           # fut | spot
    "symbol",
    "u",
    "exchange_ts_ms",   # fut: Binance E ; spot: relay recv ms
    "server_clock_ns",  # relay send time
    "lat_rust_us",      # fut: E -> relay send ; spot: relay recv -> send (decode only)
    "hop_us",           # relay send -> python recv
    "bid",
    "bid_sz",
    "ask",
    "ask_sz",
]


@dataclass(frozen=True)
class Stream:
    market: str
    symbol: str
    port: int


class Sink(asyncio.DatagramProtocol):
    def __init__(self, stream: Stream, rows: queue.SimpleQueue, stats: dict):
        self.stream = stream
        self.rows = rows
        self.stats = stats
        self.bad = 0

    def datagram_received(self, data: bytes, _addr) -> None:
        recv_ns = time.time_ns()
        if len(data) != PACKET_SIZE:
            self.bad += 1
            return
        (_sym, ex_ms, srv_ns, bid, bid_sz, ask, ask_sz, u) = struct.unpack(PACKET_FORMAT, data)
        lat_us = srv_ns / 1_000.0 - ex_ms * 1_000.0
        hop_us = (recv_ns - srv_ns) / 1_000.0
        self.rows.put((
            recv_ns, self.stream.market, self.stream.symbol, u, ex_ms, srv_ns,
            f"{lat_us:.1f}", f"{hop_us:.1f}", bid, bid_sz, ask, ask_sz,
        ))
        s = self.stats[self.stream]
        s["n"] += 1
        s["lat"].append(lat_us)
        s["hop"].append(hop_us)

    def error_received(self, exc: Exception) -> None:
        print(f"[{self.stream.market}:{self.stream.symbol}] socket error: {exc}", flush=True)


class CsvWriter(threading.Thread):
    """Drains the row queue every flush_s and appends to one CSV per stream per UTC day."""

    def __init__(self, rows: queue.SimpleQueue, out_dir: Path, label: str, flush_s: float):
        super().__init__(daemon=True, name="csv-writer")
        self.rows = rows
        self.out_dir = out_dir
        self.label = label
        self.flush_s = flush_s
        self.stop_event = threading.Event()
        self.files: dict[tuple[str, str, str], tuple[object, csv.writer]] = {}
        self.written = 0

    def _writer_for(self, market: str, symbol: str, day: str) -> csv.writer:
        key = (market, symbol, day)
        if key not in self.files:
            path = self.out_dir / f"{self.label}_{market}_{symbol}_{day}.csv"
            new = not path.exists() or path.stat().st_size == 0
            fh = open(path, "a", newline="", buffering=1 << 20)
            w = csv.writer(fh)
            if new:
                w.writerow(CSV_HEADER)
            self.files[key] = (fh, w)
            # close yesterday's handles for this stream
            for k in [k for k in self.files if k[:2] == (market, symbol) and k[2] != day]:
                self.files.pop(k)[0].close()
        return self.files[key][1]

    def drain(self) -> None:
        n = 0
        while True:
            try:
                row = self.rows.get_nowait()
            except queue.Empty:
                break
            day = dt.datetime.fromtimestamp(row[0] / 1e9, dt.timezone.utc).strftime("%Y%m%d")
            self._writer_for(row[1], row[2], day).writerow(row)
            n += 1
        if n:
            for fh, _ in self.files.values():
                fh.flush()
        self.written += n

    def run(self) -> None:
        while not self.stop_event.wait(self.flush_s):
            self.drain()
        self.drain()
        for fh, _ in self.files.values():
            fh.close()


def load_streams(configs: dict[str, str | None], wanted: dict[str, str | None]) -> list[Stream]:
    """configs: market -> port json; wanted: market -> space-separated symbols ('' = none, None = all)."""
    streams: list[Stream] = []
    for market, path in configs.items():
        if not path:
            continue
        want = wanted.get(market)
        want_set = None if want is None else {w.strip().upper() for w in want.split() if w.strip()}
        with open(path, encoding="utf-8") as f:
            for symbol, port in json.load(f).items():
                symbol = symbol.strip().upper()
                if want_set is not None and symbol not in want_set:
                    continue
                streams.append(Stream(market, symbol, int(port)))
    if not streams:
        raise SystemExit("no streams configured")
    return streams


def imds(path: str) -> str | None:
    """IMDSv2 lookup; returns None off-AWS."""
    try:
        req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token", method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
        )
        token = urllib.request.urlopen(req, timeout=1).read().decode()
        req = urllib.request.Request(
            f"http://169.254.169.254/latest/meta-data/{path}",
            headers={"X-aws-ec2-metadata-token": token},
        )
        return urllib.request.urlopen(req, timeout=1).read().decode()
    except Exception:
        return None


def write_meta(out_dir: Path, label: str, streams: list[Stream]) -> None:
    meta = {
        "label": label,
        "hostname": socket.gethostname(),
        "instance_type": imds("instance-type"),
        "availability_zone": imds("placement/availability-zone"),
        "az_id": imds("placement/availability-zone-id"),
        "instance_id": imds("instance-id"),
        "kernel": os.uname().release,
        "python": sys.version.split()[0],
        "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "streams": [s.__dict__ for s in streams],
    }
    with open(out_dir / f"{label}_meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    print(json.dumps(meta), flush=True)


def pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    qs = statistics.quantiles(xs, n=100, method="inclusive") if len(xs) > 1 else [xs[0]] * 99
    return qs[min(max(int(p) - 1, 0), 98)]


async def summary_loop(stats: dict, writer: CsvWriter, every: float) -> None:
    while True:
        await asyncio.sleep(every)
        now = dt.datetime.now(dt.timezone.utc).strftime("%H:%M:%S")
        for stream, s in stats.items():
            lat, hop, n = s["lat"], s["hop"], s["n"]
            s["lat"], s["hop"], s["n"] = [], [], 0
            if not n:
                print(f"{now} {stream.market}:{stream.symbol} n=0 (no packets)", flush=True)
                continue
            print(
                f"{now} {stream.market}:{stream.symbol} n={n} ({n / every:.1f}/s) "
                f"lat_us p50={pct(lat, 50):.0f} p95={pct(lat, 95):.0f} p99={pct(lat, 99):.0f} "
                f"max={max(lat):.0f} | hop_us p50={pct(hop, 50):.0f} p99={pct(hop, 99):.0f} "
                f"| written={writer.written}",
                flush=True,
            )


async def main_async(args: argparse.Namespace) -> None:
    streams = load_streams(
        {"fut": args.fut_config, "spot": args.spot_config},
        {"fut": args.fut_symbols, "spot": args.spot_symbols},
    )
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    write_meta(out_dir, args.label, streams)

    rows: queue.SimpleQueue = queue.SimpleQueue()
    stats = {s: {"n": 0, "lat": [], "hop": []} for s in streams}
    writer = CsvWriter(rows, out_dir, args.label, args.flush_interval)
    writer.start()

    loop = asyncio.get_running_loop()
    transports = []
    for s in streams:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, args.rcvbuf)
        sock.setblocking(False)
        try:
            sock.bind((args.bind_host, s.port))
        except OSError as exc:
            raise SystemExit(f"cannot bind {args.bind_host}:{s.port} for {s.market}:{s.symbol}: {exc} "
                             f"(another process owns the port; restrict --{s.market}-symbols or change the port json)")
        transport, _ = await loop.create_datagram_endpoint(lambda s=s: Sink(s, rows, stats), sock=sock)
        transports.append(transport)
        print(f"listening udp {args.bind_host}:{s.port} -> {s.market}:{s.symbol}", flush=True)

    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    summary = asyncio.create_task(summary_loop(stats, writer, args.summary_interval))
    await stop.wait()
    print("stopping", flush=True)
    summary.cancel()
    for t in transports:
        t.close()
    writer.stop_event.set()
    writer.join(timeout=10)
    print(f"done, rows written={writer.written}", flush=True)


def main() -> None:
    here = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--fut-config", default=str(here / "rust" / "symbol_ports.json"),
                   help="futures symbol->port json ('' to disable)")
    p.add_argument("--spot-config", default=str(here / "rust" / "symbol_ports_spot.json"),
                   help="spot symbol->port json ('' to disable)")
    for m in ("fut", "spot"):
        p.add_argument(f"--{m}-symbols", default=None,
                       help=f"space-separated {m} symbols to bind (default: all in the json; '' = none)")
    p.add_argument("--out-dir", default=str(here / "data"))
    p.add_argument("--label", default=socket.gethostname(), help="prefix for csv/meta files")
    p.add_argument("--bind-host", default="127.0.0.1")
    p.add_argument("--rcvbuf", type=int, default=8 * 1024 * 1024)
    p.add_argument("--flush-interval", type=float, default=1.0, help="seconds between csv flushes")
    p.add_argument("--summary-interval", type=float, default=60.0)
    args = p.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
