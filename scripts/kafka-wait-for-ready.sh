#!/usr/bin/env bash
set -euo pipefail
trap 'echo "Script failed at line $LINENO: $BASH_COMMAND"' ERR

is_service_started() {
  kafka-broker-api-versions.sh --bootstrap-server localhost:29092
}

wait_for_service_ready() {
  local max_retries=30
  local wait_time=0.5
  local count=0
  echo "\nWaiting for kafka to become ready..."
  until is_service_started || false; do
    sleep "$wait_time"
    ((++count))
    if (( count >= max_retries )); then
      echo "Timeout waiting for kafka to become ready."
      return 1
    fi
  done
}

wait_for_service_ready
