#!/usr/bin/env bash
set -ex

KAFKA_DATA_DIR="$DEVBOX_PROJECT_ROOT/.devbox/kafka-data"
KAFKA_CONFIG="$KAFKA_DATA_DIR/server.properties"
CLUSTER_ID_FILE="$KAFKA_DATA_DIR/cluster_id"

mkdir -p "$KAFKA_DATA_DIR"

# Generate runtime config with actual paths
sed "s|__KAFKA_DATA_DIR__|$KAFKA_DATA_DIR|g" "$DEVBOX_PROJECT_ROOT/kafka.properties" > "$KAFKA_CONFIG"

# Generate cluster ID if it doesn't exist
if [ ! -f "$CLUSTER_ID_FILE" ]; then
  kafka-storage.sh random-uuid > "$CLUSTER_ID_FILE"
fi

CLUSTER_ID=$(cat "$CLUSTER_ID_FILE")

# Format storage if not already formatted
if [ ! -f "$KAFKA_DATA_DIR/__cluster_metadata-0/partition.metadata" ]; then
  kafka-storage.sh format -t "$CLUSTER_ID" -c "$KAFKA_CONFIG" --ignore-formatted
fi

echo "Kafka initialized with cluster ID: $CLUSTER_ID"
