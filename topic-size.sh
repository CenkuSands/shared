#!/bin/bash

BOOTSTRAP_SERVERS="localhost:9092"
OUTPUT_FILE="topics_retention_report.csv"

# Function to cleanup on exit
cleanup() {
    rm -f "$TEMP_FILE"
}
trap cleanup EXIT

TEMP_FILE=$(mktemp)

echo "Starting Kafka topic analysis..."
echo "topic name|size|retention.ms|cleanup.policy" > "$OUTPUT_FILE"

# Get all topics
topics_array=($(kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVERS --list))
if [ ${#topics_array[@]} -eq 0 ]; then
    echo "Error: No topics found or cannot connect to Kafka at $BOOTSTRAP_SERVERS"
    exit 1
fi

echo "Processing ${#topics_array[@]} topics..."

# Process in larger batches for better efficiency
batch_size=50
for ((i=0; i<${#topics_array[@]}; i+=batch_size)); do
    batch=("${topics_array[@]:i:batch_size}")
    echo "Processing batch $((i/batch_size + 1)) of $(( (${#topics_array[@]} + batch_size - 1) / batch_size ))..."
    
    # Get sizes for current batch
    topic_list=$(IFS=,; echo "${batch[*]}")
    kafka-log-dirs.sh --bootstrap-server $BOOTSTRAP_SERVERS --describe --topic-list "$topic_list" > "$TEMP_FILE" 2>/dev/null
    
    # Process each topic in the batch
    for topic in "${batch[@]}"; do
        # Get size from pre-fetched data
        size_bytes=$(grep "\"topic\":\"$topic\"" "$TEMP_FILE" 2>/dev/null | \
            awk -F'"size":' '{for(i=2;i<=NF;i++) {split($i,a,","); sum += a[1]}} END {print sum+0}')
        
        # Get topic configuration
        config=$(kafka-configs.sh --bootstrap-server $BOOTSTRAP_SERVERS \
            --entity-type topics --entity-name "$topic" --describe 2>/dev/null)
        
        retention_ms=$(echo "$config" | grep -o "retention.ms=[^,]*" | head -1 | cut -d= -f2)
        cleanup_policy=$(echo "$config" | grep -o "cleanup.policy=[^,]*" | head -1 | cut -d= -f2)
        
        echo "${topic}|${size_bytes:-0}|${retention_ms:-default}|${cleanup_policy:-delete}" >> "$OUTPUT_FILE"
    done
done

echo "Analysis complete: $OUTPUT_FILE"
echo "Sample output:"
head -n 5 "$OUTPUT_FILE"
