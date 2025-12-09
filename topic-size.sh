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

# 1. Get Topic Sizes (Sum of ALL replicas on ALL brokers)
# This results in the TOTAL PHYSICAL DISK USAGE
echo "Calculating topic sizes (fetching log dirs)..."
kafka-log-dirs.sh --bootstrap-server "$BOOTSTRAP_SERVER" --describe | \
grep "^{" | \
jq -r '[.brokers[].logDirs[].partitions[]] | group_by(.topic) | map({topic: .[0].topic, size: (map(.size) | add)}) | .[] | "\(.topic),\(.size)"' > temp_sizes.csv

# 2. Get List of Topics
TOPICS=$(kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list)

# Header
echo "Topic,ReplicationFactor,Retention(ms),CleanupPolicy,LogicalSize(MB),TotalDiskUsage(MB)" > "$OUTPUT_FILE"

echo "Extracting configs..."

# 3. Loop
for TOPIC in $TOPICS; do
    
    # --- Get Replication Factor ---
    REP_FACTOR=$(kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --describe --topic "$TOPIC" | head -n 1 | awk -F'ReplicationFactor:' '{print $2}' | awk '{print $1}')

    # --- Get Configs (Exact Match) ---
    CONFIGS_RAW=$(kafka-configs.sh --bootstrap-server "$BOOTSTRAP_SERVER" --entity-type topics --entity-name "$TOPIC" --describe --all)
    
    RETENTION=$(echo "$CONFIGS_RAW" | tr ',' '\n' | tr ' ' '\n' | grep "^retention.ms=" | cut -d'=' -f2)
    CLEANUP=$(echo "$CONFIGS_RAW" | tr ',' '\n' | tr ' ' '\n' | grep "^cleanup.policy=" | cut -d'=' -f2)

    # --- Get Size ---
    # TOTAL_BYTES is the sum of all replicas (Leader + Followers)
    TOTAL_BYTES=$(grep "^$TOPIC," temp_sizes.csv | cut -d',' -f2)
    
    if [ -z "$TOTAL_BYTES" ]; then 
        TOTAL_BYTES="0"
    fi

    # Calculate Sizes in MB (Using awk for floating point math)
    # Total Disk Usage (Physical footprint)
    TOTAL_MB=$(awk "BEGIN {printf \"%.2f\", $TOTAL_BYTES/1024/1024}")
    
    # Logical Size (Approximate size of unique data sent by producers)
    # We avoid division by zero if rep factor is missing/0
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
echo "Note: 'TotalDiskUsage' is the physical space consumed across all brokers (Replication included)."
