#!/usr/bin/env python3
"""
Race two or more instances on the same Binance updates.

    python3 race.py boxA/data boxB/data boxC/data --market fut --symbol BTCUSDT

For every update id `u` seen by ALL boxes, compare each box's own
`server_clock_ns` (relay receive->ready-to-send time, its own clock). The box
with the smallest value had the update first. Reports, per box, the share of
updates it won and how far behind the winner it typically was.

Why this and not summarize.py:
  summarize.py ranks on lat_rust_us = Binance event time `E` -> relay send.
  `E` is 1 ms resolution, so it adds a uniform 0-1000 us quantization to every
  row. That bias is identical on every box and cancels in a median, but it
  bounds how fine a difference you can see, and spot bookTicker has no `E` at
  all.
  This script never reads `E`. It compares boxes directly, so resolution is
  limited only by clock sync between them -- which is why bootstrap.sh
  configures the PTP hardware clock (~1-10 us) instead of plain NTP (~250 us).
  It works for spot exactly as it does for futures.

Requires: boxes ran over the same wall-clock window, clocks PTP-synced.
Memory: deltas are kept as int32 microseconds (~4 bytes/update/box).
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import os
import statistics
import sys
from array import array
from collections import defaultdict


def box_files(d: str, market: str, symbol: str) -> dict[str, list[str]]:
    """label -> sorted day files for this market/symbol."""
    out: dict[str, list[str]] = defaultdict(list)
    for f in sorted(glob.glob(os.path.join(d, f"*_{market}_{symbol}_*.csv"))):
        label, mkt, sym, _day = os.path.basename(f)[:-4].rsplit("_", 3)
        if mkt == market and sym == symbol:
            out[label].append(f)
    return out


def stream(files: list[str]):
    """Yield (u, server_clock_ns, recv_ns) in strictly increasing u."""
    last = -1
    for f in files:
        with open(f, newline="") as fh:
            for row in csv.DictReader(fh):
                try:
                    u = int(row["u"])
                    ns = int(row["server_clock_ns"])
                    rc = int(row["recv_ns"])
                except (KeyError, ValueError):
                    continue
                if u <= last:      # reconnect / out-of-order: skip
                    continue
                last = u
                yield u, ns, rc


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("dirs", nargs="+", help="two or more data directories")
    p.add_argument("--market", default="fut", choices=["fut", "spot"])
    p.add_argument("--symbol", default="BTCUSDT")
    args = p.parse_args()

    # collect boxes (label -> files); a dir may hold more than one label
    boxes: dict[str, list[str]] = {}
    for d in args.dirs:
        for label, files in box_files(d, args.market, args.symbol).items():
            boxes.setdefault(label, []).extend(files)
    if len(boxes) < 2:
        raise SystemExit(f"need >=2 boxes with {args.market}:{args.symbol} data, found {len(boxes)}: "
                         f"{sorted(boxes) or 'none'}")

    labels = sorted(boxes)
    its = {l: stream(boxes[l]) for l in labels}
    heads: dict[str, tuple] = {}
    for l in labels:
        try:
            heads[l] = next(its[l])
        except StopIteration:
            raise SystemExit(f"no usable rows for box {l}")

    wins = dict.fromkeys(labels, 0)
    lag = {l: array("i") for l in labels}                      # us behind winner
    pair = {(a, b): array("i") for a in labels for b in labels if a < b}
    matched = 0
    t_lo = t_hi = None

    while True:
        us = [heads[l][0] for l in labels]
        hi = max(us)
        if all(u == hi for u in us):
            ts = {l: heads[l][1] for l in labels}
            best = min(ts.values())
            wins[min(ts, key=ts.get)] += 1
            for l in labels:
                lag[l].append(int((ts[l] - best) / 1000))
            for (a, b) in pair:
                pair[(a, b)].append(int((ts[a] - ts[b]) / 1000))
            rc = heads[labels[0]][2] / 1e9
            t_lo = rc if t_lo is None else min(t_lo, rc)
            t_hi = rc if t_hi is None else max(t_hi, rc)
            matched += 1
            advance = labels
        else:
            advance = [l for l in labels if heads[l][0] < hi]
        try:
            for l in advance:
                heads[l] = next(its[l])
        except StopIteration:
            break

    if not matched:
        raise SystemExit("no update ids seen by all boxes -- did they run over the same window?")

    span_h = ((t_hi or 0) - (t_lo or 0)) / 3600.0
    lo = dt.datetime.fromtimestamp(t_lo, dt.timezone.utc).strftime("%m-%d %H:%M")
    hi_s = dt.datetime.fromtimestamp(t_hi, dt.timezone.utc).strftime("%m-%d %H:%M")
    print(f"{args.market}:{args.symbol}  matched={matched:,} updates seen by all "
          f"{len(labels)} boxes   window {lo} -> {hi_s} UTC ({span_h:.1f}h)\n")

    w = max(len(l) for l in labels) + 2
    print(f"{'box':<{w}} {'win%':>7} {'lag_p50':>9} {'lag_p90':>9} {'lag_p99':>9}  (us behind winner)")
    print("-" * (w + 40))
    for l in sorted(labels, key=lambda x: -wins[x]):
        xs = sorted(lag[l])
        q = lambda pp: xs[min(int(len(xs) * pp / 100), len(xs) - 1)]
        print(f"{l:<{w}} {100*wins[l]/matched:>6.1f}% {q(50):>9} {q(90):>9} {q(99):>9}")

    print(f"\npairwise median (row minus col, us; negative = row is faster):")
    print(f"{'':<{w}}" + "".join(f"{b[:12]:>14}" for b in labels))
    for a in labels:
        cells = ""
        for b in labels:
            if a == b:
                cells += f"{'-':>14}"
            elif a < b:
                cells += f"{statistics.median(pair[(a, b)]):>14.0f}"
            else:
                cells += f"{-statistics.median(pair[(b, a)]):>14.0f}"
        print(f"{a:<{w}}" + cells)

    print(f"\nnote: resolution is bounded by clock sync between boxes. With PTP "
          f"(/dev/ptp0) that is ~1-10 us;\n      with plain NTP it is ~250 us, so "
          f"ignore differences smaller than that. Check `chronyc tracking` on each box.")


if __name__ == "__main__":
    main()
