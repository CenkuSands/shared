#!/bin/bash
# quick_hex_extract.sh
# Quick extraction keeping 0x prefix

BASE_DIR="/var/lib/kafka-streams"
OUTPUT_FILE="/tmp/quick_hex_keys_$(date +%Y%m%d_%H%M%S).txt"

echo "=== Quick Hex Key Extraction (with 0x) ==="

find "$BASE_DIR" -name "CURRENT" -type f | while read current_file; do
    db_dir=$(dirname "$current_file")
    echo "=== $db_dir ==="
    
    podman run --rm --security-opt label=disable \
      -v "${db_dir}:/data:ro" \
      docker.io/library/rocksdb-tool ldb --db=/data --try_load_options --column_family="KeyValueWithTimestamp" scan --key_hex 2>/dev/null | \
    while read -r key_hex; do
        if [[ ! -z "$key_hex" ]]; then
            echo "$db_dir|$key_hex" >> "$OUTPUT_FILE"
            echo "  $key_hex"
        fi
    done
    echo ""
done

echo "Output: $OUTPUT_FILE"
