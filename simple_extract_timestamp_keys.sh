#!/bin/bash
# minimal_one_line.sh
# Minimal script with strict line control

BASE_DIR="/var/lib/kafka-streams"
OUTPUT_FILE="/tmp/minimal_timestamp_keys_$(date +%Y%m%d_%H%M%S).txt"

echo "=== Minimal One-Line Extraction ==="

find "$BASE_DIR" -name "CURRENT" -type f | while read current_file; do
    db_dir=$(dirname "$current_file" | tr -d '\n\r')
    
    podman run --rm --security-opt label=disable \
      -v "${db_dir}:/data:ro" \
      docker.io/library/rocksdb-tool ldb --db=/data --try_load_options --column_family="keyValueWithTimestamp" scan --key_hex 2>/dev/null | \
    while read -r key_hex; do
        if [[ ! -z "$key_hex" ]]; then
            key_hex_clean=$(echo "$key_hex" | sed 's/^0x//' | tr -d '\n\r')
            key_text=$(echo "$key_hex_clean" | xxd -r -p 2>/dev/null | tr -d '\n\r')
            
            if echo "$key_text" | grep -qE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"; then
                timestamps=$(echo "$key_text" | grep -oE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z" | tr '\n' ',' | sed 's/,$//')
                printf "%s|%s|%s|%s\n" "$db_dir" "$key_hex_clean" "$key_text" "$timestamps" >> "$OUTPUT_FILE"
            fi
        fi
    done
done

echo "Output: $OUTPUT_FILE"
