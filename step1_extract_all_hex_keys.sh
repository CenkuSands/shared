#!/bin/bash
# step1_extract_all_hex_keys.sh
# Step 1: Extract ALL keys in hex format only, no decoding

BASE_DIR="/var/lib/kafka-streams"
OUTPUT_FILE="/tmp/step1_all_hex_keys_$(date +%Y%m%d_%H%M%S).txt"

echo "=== STEP 1: Extract ALL keys in HEX format only ==="
echo ""

find "$BASE_DIR" -name "CURRENT" -type f | while read current_file; do
    db_dir=$(dirname "$current_file")
    echo "Processing: $db_dir"
    
    # Extract ALL keys in hex format only
    key_count=$(podman run --rm --security-opt label=disable \
      -v "${db_dir}:/data:ro" \
      docker.io/library/rocksdb-tool ldb --db=/data --try_load_options --column_family="KeyValueWithTimestamp" scan --key_hex 2>/dev/null | \
    while read -r key_hex; do
        if [[ ! -z "$key_hex" ]]; then
            # Remove 0x prefix and save
            key_hex_clean="${key_hex#0x}"
            echo "$db_dir|$key_hex_clean" >> "$OUTPUT_FILE"
            echo "  Found: $key_hex_clean"
        fi
    done | wc -l)
    
    echo "  Total keys: $key_count"
    echo ""
done

echo "=== STEP 1 COMPLETE ==="
echo "Output: $OUTPUT_FILE"
echo "Total keys found: $(wc -l < "$OUTPUT_FILE" 2>/dev/null || echo 0)"
