#!/bin/bash
# simple_extract_timestamp_keys.sh

BASE_DIR="/var/lib/kafka-streams"
OUTPUT_FILE="/tmp/timestamp_keys_simple_$(date +%Y%m%d_%H%M%S).txt"

echo "=== Simple extraction from KeyValueWithTimestamp ==="

find "$BASE_DIR" -type f -name "CURRENT" | while read current_file; do
    rocksdb_dir=$(dirname "$current_file")
    echo "Processing: $rocksdb_dir"
    
    # Directly use KeyValueWithTimestamp column family
    podman run --rm --security-opt label=disable \
      -v "${rocksdb_dir}:/data:ro" \
      docker.io/library/rocksdb-tool ldb --db=/data --try_load_options --column_family="KeyValueWithTimestamp" scan --key_hex 2>/dev/null | \
    while read -r key_hex; do
        if [[ ! -z "$key_hex" ]]; then
            key_hex_clean="${key_hex#0x}"
            key_text=$(echo "$key_hex_clean" | xxd -r -p 2>/dev/null)
            
            if echo "$key_text" | grep -qE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"; then
                timestamps=$(echo "$key_text" | grep -oE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z" | tr '\n' ',')
                echo "$rocksdb_dir|KeyValueWithTimestamp|$key_hex_clean|$key_text|${timestamps%,}" >> "$OUTPUT_FILE"
                echo "  ✓ Found key with timestamp"
            fi
        fi
    done
done

echo "=== Complete ==="
echo "Output: $OUTPUT_FILE"
