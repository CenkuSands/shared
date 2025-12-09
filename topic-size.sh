#!/bin/bash

# Configuration
BOOTSTRAP_SERVER="localhost:9092"
OUTPUT_FILE="topic_metrics.csv"

# Check dependencies
if ! command -v jq &> /dev/null; then
    echo "Error: 'jq' is not installed."
    exit 1
fi

echo "Gathering data from cluster: $BOOTSTRAP_SERVER..."

# --- STEP 1: CALCULATE SIZES ---
echo "Fetching log directories..."
kafka-log-dirs.sh --bootstrap-server "$BOOTSTRAP_SERVER" --describe > raw_output.json

# Clean the output (Remove "Querying..." text, keep JSON)
sed -n '/^{/,$p' raw_output.json > clean_output.json

echo "Parsing sizes..."
# FIX APPLIED HERE: We extract the topic name from the "partition" string
cat clean_output.json | jq -r '
  [ .brokers[].logDirs[].partitions[]? ] 
  | map(. + {topic: (.partition | sub("-[0-9]+$"; ""))}) 
  | group_by(.topic) 
  | map({topic: .[0].topic, size: (map(.size) | add)}) 
  | .[] 
  | "\(.topic),\(.size)"
' > temp_sizes.csv

# Cleanup temp JSON files
rm raw_output.json clean_output.json

# --- STEP 2: GET TOPIC LIST ---
TOPICS=$(kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list)

# Header
echo "Topic,ReplicationFactor,Retention(ms),CleanupPolicy,LogicalSize(MB),TotalDiskUsage(MB)" > "$OUTPUT_FILE"

echo "Extracting configs and merging data..."

# --- STEP 3: LOOP TOPICS ---
for TOPIC in $TOPICS; do
    
    # Get Replication Factor
    REP_FACTOR=$(kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --describe --topic "$TOPIC" | head -n 1 | awk -F'ReplicationFactor:' '{print $2}' | awk '{print $1}')

    # Get Configs (Exact Match)
    CONFIGS_RAW=$(kafka-configs.sh --bootstrap-server "$BOOTSTRAP_SERVER" --entity-type topics --entity-name "$TOPIC" --describe --all)
    
    RETENTION=$(echo "$CONFIGS_RAW" | tr ',' '\n' | tr ' ' '\n' | grep "^retention.ms=" | cut -d'=' -f2)
    CLEANUP=$(echo "$CONFIGS_RAW" | tr ',' '\n' | tr ' ' '\n' | grep "^cleanup.policy=" | cut -d'=' -f2)

    # Get Size (Sum of all replicas)
    TOTAL_BYTES=$(grep "^$TOPIC," temp_sizes.csv | cut -d',' -f2)
    
    if [ -z "$TOTAL_BYTES" ]; then 
        TOTAL_BYTES="0"
    fi

    # Calculate Sizes in MB
    TOTAL_MB=$(awk "BEGIN {printf \"%.2f\", $TOTAL_BYTES/1024/1024}")
    
    if [ "$REP_FACTOR" -gt 0 ]; then
        LOGICAL_MB=$(awk "BEGIN {printf \"%.2f\", $TOTAL_MB/$REP_FACTOR}")
    else
        LOGICAL_MB="0"
    fi

    echo "$TOPIC,$REP_FACTOR,$RETENTION,$CLEANUP,$LOGICAL_MB,$TOTAL_MB"
    echo "$TOPIC,$REP_FACTOR,$RETENTION,$CLEANUP,$LOGICAL_MB,$TOTAL_MB" >> "$OUTPUT_FILE"

done

rm temp_sizes.csv
echo "------------------------------------------------"
echo "Done! Report saved to $OUTPUT_FILE"
