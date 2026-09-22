#!/usr/bin/env python3
"""
Summarise feed-latency CSVs produced by feed_latency_sink.py.

    python3 summarize.py data/                      # this instance
    python3 summarize.py inst_a/data inst_b/data    # several instances side by side
    python3 summarize.py data/ --market fut --symbol BTCUSDT --by hour

Prints, per (label, market, symbol[, hour]): n, msgs/s, and lat_rust_us
p50/p90/p99/max plus hop_us p50. Stdlib only.

For futures lat_rust_us = Binance event time -> relay ready-to-send.
For spot the venue gives no timestamp, so lat_rust_us there is only relay
receive -> send (decode time) and is NOT comparable to the futures column;
spot instances must be compared with a cross-box race on `u` instead.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import os
import statistics
from collections import defaultdict


def pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    if len(xs) == 1:
        return xs[0]
    return statistics.quantiles(xs, n=100, method="inclusive")[min(max(int(p) - 1, 0), 98)]


def load(paths: list[str], market: str | None, symbol: str | None, by: str):
    groups: dict[tuple, dict] = defaultdict(lambda: {"lat": [], "hop": [], "t0": None, "t1": None})
    for d in paths:
        files = sorted(f for m in ("fut", "spot") for f in glob.glob(os.path.join(d, f"*_{m}_*.csv")))
        for f in files:
            base = os.path.basename(f)[:-4]
            # {label}_{market}_{symbol}_{yyyymmdd}
            label, mkt, sym, day = base.rsplit("_", 3)
            if market and mkt != market:
                continue
            if symbol and sym != symbol:
                continue
            with open(f, newline="") as fh:
                for row in csv.DictReader(fh):
                    ts = int(row["recv_ns"]) / 1e9
                    key = [label, mkt, sym]
                    if by == "hour":
                        key.append(dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%m-%d %Hh"))
                    elif by == "day":
                        key.append(day)
                    g = groups[tuple(key)]
                    g["lat"].append(float(row["lat_rust_us"]))
                    g["hop"].append(float(row["hop_us"]))
                    g["t0"] = ts if g["t0"] is None else min(g["t0"], ts)
                    g["t1"] = ts if g["t1"] is None else max(g["t1"], ts)
    return groups


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("dirs", nargs="+", help="one or more data directories")
    p.add_argument("--market", choices=["fut", "spot"])
    p.add_argument("--symbol")
    p.add_argument("--by", choices=["all", "hour", "day"], default="all")
    args = p.parse_args()

    groups = load(args.dirs, args.market, args.symbol, args.by)
    if not groups:
        raise SystemExit("no rows found")

    hdr = f"{'label':<22} {'mkt':<4} {'symbol':<8} {'bucket':<12} {'n':>8} {'msg/s':>7} " \
          f"{'lat_p50':>8} {'lat_p90':>8} {'lat_p99':>8} {'lat_max':>8} {'hop_p50':>8}"
    print(hdr)
    print("-" * len(hdr))
    for key in sorted(groups):
        g = groups[key]
        label, mkt, sym = key[:3]
        bucket = key[3] if len(key) > 3 else "all"
        span = max((g["t1"] or 0) - (g["t0"] or 0), 1e-9)
        lat = g["lat"]
        print(
            f"{label:<22} {mkt:<4} {sym:<8} {bucket:<12} {len(lat):>8} {len(lat) / span:>7.1f} "
            f"{pct(lat, 50):>8.0f} {pct(lat, 90):>8.0f} {pct(lat, 99):>8.0f} {max(lat):>8.0f} "
            f"{pct(g['hop'], 50):>8.0f}"
        )


if __name__ == "__main__":
    main()
