#!/bin/bash

# Configuration
BOOTSTRAP_SERVER="localhost:9092"
OUTPUT_FILE="kafka_topic_configs.csv"

# Write CSV header
echo "topic,retention_ms,cleanup_policy" > "$OUTPUT_FILE"

# Get topic list to avoid duplicates
topics=$(kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list)

# Loop through each topic
for topic in $topics; do
  # Fetch topic configurations
  configs=$(kafka-configs.sh --bootstrap-server "$BOOTSTRAP_SERVER" --entity-type topics --entity-name "$topic" --describe)

  # Extract retention.ms (specifically, not delete.retention.ms)
  retention_ms=$(echo "$configs" | grep -oP '\bretention.ms=\K[^,]+' || echo "default")
  cleanup_policy=$(echo "$configs" | grep -oP 'cleanup.policy=\K[^,]+' || echo "default")

  # Ensure empty values are marked as default
  [ -z "$retention_ms" ] && retention_ms="default"
  [ -z "$cleanup_policy" ] && cleanup_policy="default"

  # Append to CSV (only once per topic)
  echo "$topic,$retention_ms,$cleanup_policy" >> "$OUTPUT_FILE"
done

echo "Output saved to $OUTPUT_FILE"
