#!/bin/bash

BOOTSTRAP_SERVER="localhost:9092"
OUTPUT_FILE="kafka_topic_configs.csv"

echo "topic,retention_ms,cleanup_policy" > "$OUTPUT_FILE"

# Fetch all topic details
kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --describe | while read -r line; do
  # Skip empty lines or non-topic lines
  if [[ $line == Topic:* ]]; then
    topic=$(echo "$line" | grep -oP 'Topic: \K[^\s]+')
    retention_ms=$(echo "$line" | grep -oP 'retention.ms=\K[^,]+' || echo "default")
    cleanup_policy=$(echo "$line" | grep -oP 'cleanup.policy=\K[^,]+' || echo "default")

    [ -z "$retention_ms" ] && retention_ms="default"
    [ -z "$cleanup_policy" ] && cleanup_policy="default"

    echo "$topic,$retention_ms,$cleanup_policy" >> "$OUTPUT_FILE"
  fi
done

echo "Output saved to $OUTPUT_FILE"
