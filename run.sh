#!/usr/bin/env bash
# Start the sink first (so no packets are lost), then one relay per symbol.
# Idempotent: skips anything already running. Everything stays on this box.
#   SYMBOLS_FUT="BTCUSDT ETHUSDT" SYMBOLS_SPOT="BTCUSDT" ./run.sh
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p logs data
SYMBOLS_FUT="${SYMBOLS_FUT:-BTCUSDT}"
SYMBOLS_SPOT="${SYMBOLS_SPOT:-BTCUSDT}"
LABEL="${LABEL:-$(hostname)}"
KIT="$(pwd)"                      # absolute paths so pgrep/pkill never touch other copies
BIN="$KIT/rust/target/release"

if ! pgrep -f "[f]eed_latency_sink.py --label" >/dev/null; then
  setsid nohup python3 -u "$KIT/feed_latency_sink.py" --label "$LABEL" \
    --fut-symbols "$SYMBOLS_FUT" --spot-symbols "$SYMBOLS_SPOT" \
    >> logs/sink.log 2>&1 < /dev/null &
  echo "started sink"
  sleep 1
fi

start_relay() {  # $1=binary $2=symbol $3=logname
  if pgrep -f "^$BIN/$1 --symbol $2\$" >/dev/null; then echo "$1 $2 already running"; return; fi
  ( cd rust && setsid nohup "$BIN/$1" --symbol "$2" >> "../logs/$3.log" 2>&1 < /dev/null & )
  echo "started $1 $2"
}
for s in $SYMBOLS_FUT;  do start_relay tokyo_server_udp      "$s" "fut_$s";  done
for s in $SYMBOLS_SPOT; do start_relay tokyo_server_udp_spot "$s" "spot_$s"; done
sleep 2
pgrep -af "^$BIN/|[f]eed_latency_sink.py --label" || true
