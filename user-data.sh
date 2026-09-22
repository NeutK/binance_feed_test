#!/bin/bash
# EC2 user-data: paste at launch (Ubuntu 24.04 AMI). Clones the repo as the
# ubuntu user and runs bootstrap.sh; progress in /var/log/latency-bootstrap.log
REPO="${REPO:-https://github.com/NeutK/binance_feed_test.git}"
exec > /var/log/latency-bootstrap.log 2>&1
set -x

# Self-label from instance metadata: <instance_type>_<az-id>, e.g.
# c7gn_large_apne1-az4. race.py identifies boxes by this label and refuses to
# compare fewer than two, so every box in a fleet must get a distinct one.
# AZ *letters* are shuffled per account (1a here is apne1-az4), so the az-id is
# the only stable name -- never label with the letter.
TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null)
md() { curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  "http://169.254.169.254/latest/meta-data/$1" 2>/dev/null; }
ITYPE=$(md instance-type | tr '.' '_')
AZID=$(md placement/availability-zone-id)
LABEL="${LABEL:-${ITYPE:-unknown}_${AZID:-unknown}}"

sudo -u ubuntu -H bash -c "
  export LABEL='$LABEL'
  cd ~ && ( [ -d binance_feed_test ] || git clone '$REPO' binance_feed_test ) &&
  cd binance_feed_test && chmod +x *.sh && ./bootstrap.sh
"
