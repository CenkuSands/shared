#!/bin/bash

BOOTSTRAP_SERVERS="localhost:9092"
TEMP_FILE=$(mktemp)

{
    echo "topic name|size|retention.ms|cleanup.policy"
    
    topics=$(kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVERS --list)
    
    if [ -z "$topics" ]; then
        echo "No topics found or unable to connect to Kafka cluster"
        exit 1
    fi
    
    echo "Found $(echo "$topics" | wc -l) topics to process"
    
    # Get all sizes
    echo "Fetching topic sizes..."
    kafka-log-dirs.sh --bootstrap-server $BOOTSTRAP_SERVERS --describe --topic-list "$(echo "$topics" | tr '\n' ',' | sed 's/,$//')" > "$TEMP_FILE" 2>/dev/null
    
    echo "$topics" | while read topic; do
        # More robust size extraction
        size_bytes=$(grep -o "\"topic\":\"$topic\".*\"size\":[0-9]*" "$TEMP_FILE" | \
            sed "s/.*\"size\":\([0-9]*\).*/\1/g" | \
            paste -sd+ | bc 2>/dev/null || echo 0)
        
        # Alternative method if above fails
        if [ "$size_bytes" = "0" ]; then
            size_bytes=$(awk -v topic="$topic" '
                /"topic":"[^"]*/ { 
                    if (match($0, "\"topic\":\"[^\"]*\"")) {
                        current_topic = substr($0, RSTART+9, RLENGTH-10)
                    }
                }
                /"size":[0-9]*/ {
                    if (current_topic == topic && match($0, /"size":[0-9]*/)) {
                        size = substr($0, RSTART+7, RLENGTH-7)
                        sum += size
                    }
                }
                END { print sum+0 }
            ' "$TEMP_FILE")
        fi
        
        # Get configuration
        config=$(kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVERS --entity-type topics --entity-name "$topic" --describe 2>/dev/null)
        retention_ms=$(echo "$config" | tr ',' '\n' | grep "retention.ms" | head -1 | awk -F'=' '{print $2}' | sed 's/\s*sensitive//g')
        cleanup_policy=$(echo "$config" | tr ',' '\n' | grep "cleanup.policy" | head -1 | awk -F'=' '{print $2}' | sed 's/\s*sensitive//g')
        
        echo "$topic|${size_bytes}|${retention_ms:-default}|${cleanup_policy:-delete}"
    done
    
    rm -f "$TEMP_FILE"
} > topics_retention_report.csv

echo "Report generated: topics_retention_report.csv"
