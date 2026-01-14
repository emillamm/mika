#!/usr/bin/env bash
set -e

KAFKA_DATA_DIR="$DEVBOX_PROJECT_ROOT/.devbox/kafka-data"
KAFKA_CONFIG="$KAFKA_DATA_DIR/server.properties"

exec kafka-server-start.sh "$KAFKA_CONFIG"
