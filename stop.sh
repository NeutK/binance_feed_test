#!/usr/bin/env bash
# Stop THIS kit's relays first, then its sink (final flush). Matches by absolute
# path so other relay copies on the same box (e.g. the production Tokyo relays)
# are never touched.
cd "$(dirname "$0")"
KIT="$(pwd)"
BIN="$KIT/rust/target/release"
pkill -TERM -f "^$BIN/tokyo_server_udp" || true
sleep 1
pkill -TERM -f "$KIT/feed_latency_sink.py --label" || true
sleep 2
pgrep -af "^$BIN/|$KIT/feed_latency_sink.py" && echo "still running" || echo "stopped"
