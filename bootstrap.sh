#!/usr/bin/env bash
# Fresh Ubuntu 24.04 (x86_64 or arm64) -> built relay + running sink. Idempotent.
set -euo pipefail
cd "$(dirname "$0")"
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config git chrony python3 >/dev/null

if ! command -v cargo >/dev/null && [ ! -x "$HOME/.cargo/bin/cargo" ]; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
fi
export PATH="$HOME/.cargo/bin:$PATH"

# clock. Two levels:
#   NTP  (169.254.169.123)      ~100-250 us  -- enough to read exchange_ts
#   PTP  (/dev/ptp0, Nitro)     ~1-10 us     -- needed to race two boxes on `u`
# The cross-box race compares each box's own server_clock_ns for the same
# update id, so the clock offset between boxes IS the measurement error.
sudo systemctl enable --now chrony >/dev/null 2>&1 || true

if [ -e /dev/ptp0 ]; then
  if ! grep -q "refclock PHC /dev/ptp0" /etc/chrony/chrony.conf 2>/dev/null; then
    echo "refclock PHC /dev/ptp0 poll 0 delay 0.000010 prefer" \
      | sudo tee -a /etc/chrony/chrony.conf >/dev/null
    sudo systemctl restart chrony
    sleep 3
  fi
  echo "clock: PTP hardware clock enabled (/dev/ptp0)"
else
  echo "clock: no /dev/ptp0 -- this kernel's ena driver exposes no PTP hardware clock"
  echo "       (check: modinfo ena | grep -i phc). Falling back to AWS Time Sync over"
  echo "       the local link, which on Nitro is hardware-backed and in practice holds"
  echo "       single-digit us -- adequate for cross-box races. Confirm with the RMS"
  echo "       offset printed below; if it stays above ~100 us, do not trust a race."
fi

chronyc tracking | grep -E "Reference ID|System time|RMS offset" || true
chronyc sources -v 2>/dev/null | grep -E "PHC|\^\*" || true

( cd rust && cargo build --release 2>&1 | tail -3 )
ls -l rust/target/release/tokyo_server_udp rust/target/release/tokyo_server_udp_spot

# tune udp rcvbuf ceiling so the sink's 8 MB request is honoured
sudo sysctl -q -w net.core.rmem_max=16777216

./run.sh
