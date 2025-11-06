#!/bin/bash

BOOTSTRAP_SERVERS="localhost:9092"

echo "topic name|size|retention.ms|cleanup.policy"
kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVERS --list | while read topic; do
    # Get size - sum across all partitions using efficient parsing
    size_bytes=$(kafka-log-dirs.sh --bootstrap-server $BOOTSTRAP_SERVERS --describe --topic-list "$topic" 2>/dev/null | \
        grep -o '"size":[0-9]*' | \
        awk -F: '{sum += $2} END {print sum}')
    
    # Handle empty result
    size_bytes=${size_bytes:-0}
    
    # Get configuration
    config=$(kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVERS --entity-type topics --entity-name "$topic" --describe 2>/dev/null)
    retention_ms=$(echo "$config" | tr ',' '\n' | grep "retention.ms" | head -1 | awk -F'=' '{print $2}' | sed 's/\s*sensitive//g')
    cleanup_policy=$(echo "$config" | tr ',' '\n' | grep "cleanup.policy" | head -1 | awk -F'=' '{print $2}' | sed 's/\s*sensitive//g')
    
    echo "$topic|$size_bytes|${retention_ms:-default}|${cleanup_policy:-delete}"
done > topics_retention_report.csv

echo "Report generated: topics_retention_report.csv"
