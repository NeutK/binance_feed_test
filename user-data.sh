#!/bin/bash
# EC2 user-data: paste at launch (Ubuntu 24.04 AMI). Clones the repo as the
# ubuntu user and runs bootstrap.sh; progress in /var/log/latency-bootstrap.log
REPO="${REPO:-https://github.com/NeutK/binance_feed_test.git}"
exec > /var/log/latency-bootstrap.log 2>&1
set -x
sudo -u ubuntu -H bash -c "
  cd ~ && ( [ -d binance_feed_test ] || git clone '$REPO' binance_feed_test ) &&
  cd binance_feed_test && chmod +x *.sh && ./bootstrap.sh
"
