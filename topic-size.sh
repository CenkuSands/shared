#!/bin/bash

BOOTSTRAP_SERVERS="localhost:9092"

{
    echo "topic name|size|retention.ms|cleanup.policy|replication.factor"
    kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVERS --list | while read topic; do
        # Get topic description to extract replication factor
        topic_desc=$(kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVERS --topic "$topic" --describe 2>/dev/null)
        replication_factor=$(echo "$topic_desc" | awk -F'ReplicationFactor:' '{print $2}' | awk '{print $1}' | head -1)
        replication_factor=${replication_factor:-1}
        
        # Get size and divide by replication factor
        total_size=$(kafka-log-dirs.sh --bootstrap-server $BOOTSTRAP_SERVERS --describe --topic-list "$topic" 2>/dev/null | \
            grep -o '"size":[0-9]*' | \
            awk -F: '{sum += $2} END {print sum}')
        
        # Calculate size per replica
        if [ -n "$total_size" ] && [ "$total_size" -gt 0 ] && [ "$replication_factor" -gt 0 ]; then
            size_bytes=$((total_size / replication_factor))
        else
            size_bytes=0
        fi
        
        # Get configuration - carefully extract only retention.ms
        config=$(kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVERS --entity-type topics --entity-name "$topic" --describe 2>/dev/null)
        
        # Extract retention.ms (not delete.retention.ms) using precise matching
        retention_ms=$(echo "$config" | sed 's/,/\n/g' | awk -F= '
            /^[[:space:]]*retention\.ms[[:space:]]*=/ {
                gsub(/^[[:space:]]*retention\.ms[[:space:]]*=|[[:space:]]*$/, "", $0)
                gsub(/[[:space:]]*sensitive[[:space:]]*$/, "", $2)
                print $2
                exit
            }
        ')
        
        cleanup_policy=$(echo "$config" | sed 's/,/\n/g' | awk -F= '
            /^[[:space:]]*cleanup\.policy[[:space:]]*=/ {
                gsub(/^[[:space:]]*cleanup\.policy[[:space:]]*=|[[:space:]]*$/, "", $0)
                gsub(/[[:space:]]*sensitive[[:space:]]*$/, "", $2)
                print $2
                exit
            }
        ')
        
        echo "$topic|$size_bytes|${retention_ms:-default}|${cleanup_policy:-delete}|$replication_factor"
    done
} > topics_retention_report.csv

echo "Report generated: topics_retention_report.csv"
