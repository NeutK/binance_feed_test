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

# clock: AWS time sync is what makes exchange_ts vs local clock comparable
sudo systemctl enable --now chrony >/dev/null 2>&1 || true
chronyc tracking | grep -E "Reference ID|System time" || true

( cd rust && cargo build --release 2>&1 | tail -3 )
ls -l rust/target/release/tokyo_server_udp rust/target/release/tokyo_server_udp_spot

# tune udp rcvbuf ceiling so the sink's 8 MB request is honoured
sudo sysctl -q -w net.core.rmem_max=16777216

./run.sh
