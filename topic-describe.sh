#!/bin/bash

# Configuration
BOOTSTRAP_SERVER="localhost:9092"
OUTPUT_FILE="kafka_topic_configs.csv"

# Write CSV header
echo "topic,retention_ms,cleanup_policy" > "$OUTPUT_FILE"

# Fetch topic descriptions and process only topic-level lines
kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --describe | awk '/^Topic:/ && /Configs:/' | while read -r line; do
  # Extract topic name
  topic=$(echo "$line" | grep -oP 'Topic: \K[^\s]+')

  # Extract retention.ms and cleanup.policy
  retention_ms=$(echo "$line" | grep -oP 'retention.ms=\K[^,]+' || echo "default")
  cleanup_policy=$(echo "$line" | grep -oP 'cleanup.policy=\K[^,]+' || echo "default")

  # Ensure empty values are marked as default
  [ -z "$retention_ms" ] && retention_ms="default"
  [ -z "$cleanup_policy" ] && cleanup_policy="default"

  # Append to CSV
  echo "$topic,$retention_ms,$cleanup_policy" >> "$OUTPUT_FILE"
done

echo "Output saved to $OUTPUT_FILE"
