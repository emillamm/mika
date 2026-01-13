#!/usr/bin/env bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
KAFKA_DATA_DIR="$PROJECT_ROOT/.devbox/kafka-data"
KAFKA_CONFIG="$KAFKA_DATA_DIR/server.properties"

exec kafka-server-start.sh "$KAFKA_CONFIG"
