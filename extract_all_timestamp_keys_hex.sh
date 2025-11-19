#!/bin/bash
# extract_all_timestamp_keys_final.sh
# Uses correct column family: keyValueWithTimestamp
# Extracts keys in hex, decodes them, and filters for timestamps

set -e

BASE_DIR="/var/lib/kafka-streams"
OUTPUT_FILE="/tmp/all_timestamp_keys_final_$(date +%Y%m%d_%H%M%S).txt"
COLUMN_FAMILY="keyValueWithTimestamp"  # Correct: lowercase 'k'

echo "=== Comprehensive Timestamp Key Extraction ==="
echo "Column Family: $COLUMN_FAMILY"
echo ""

# Counter for tracking
total_dbs=0
processed_dbs=0
total_keys_found=0

# Find all RocksDB directories
find "$BASE_DIR" -type f -name "CURRENT" | while read current_file; do
    rocksdb_dir=$(dirname "$current_file")
    
    ((total_dbs++))
    echo "=== Database: $rocksdb_dir ==="
    
    # Check if column family exists
    if podman run --rm --security-opt label=disable \
       -v "${rocksdb_dir}:/data:ro" \
       docker.io/library/rocksdb-tool ldb --db=/data list_column_families 2>/dev/null | grep -q "$COLUMN_FAMILY"; then
        
        echo "  ✓ Column family exists"
        ((processed_dbs++))
        
        # Extract keys in HEX format
        key_count=0
        timestamp_key_count=0
        
        podman run --rm --security-opt label=disable \
          -v "${rocksdb_dir}:/data:ro" \
          docker.io/library/rocksdb-tool ldb --db=/data --try_load_options --column_family="$COLUMN_FAMILY" scan --key_hex 2>/dev/null | \
        while read -r key_hex; do
            if [[ ! -z "$key_hex" ]]; then
                ((key_count++))
                ((total_keys_found++))
                
                # Remove any prefix (0x, etc.) and clean the hex
                key_hex_clean=$(echo "$key_hex" | sed 's/^0x//')
                
                # Decode hex to text
                key_text=$(echo "$key_hex_clean" | xxd -r -p 2>/dev/null || echo "DECODE_ERROR")
                
                # Check if key contains timestamp (ISO format)
                if echo "$key_text" | grep -qE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"; then
                    ((timestamp_key_count++))
                    # Extract all timestamps found
                    timestamps=$(echo "$key_text" | grep -oE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z" | tr '\n' ',')
                    
                    # Save to output file
                    echo "$rocksdb_dir|$key_hex_clean|$key_text|${timestamps%,}" >> "$OUTPUT_FILE"
                    echo "    ✓ Key $key_count: Has timestamp"
                else
                    echo "    ✗ Key $key_count: No timestamp"
                fi
            fi
        done
        
        echo "  Summary: $key_count total keys, $timestamp_key_count with timestamps"
        
    else
        echo "  ✗ Column family '$COLUMN_FAMILY' not found"
        # Show available column families for debugging
        echo "  Available column families:"
        podman run --rm --security-opt label=disable \
          -v "${rocksdb_dir}:/data:ro" \
          docker.io/library/rocksdb-tool ldb --db=/data list_column_families 2>/dev/null | sed 's/^/    /' || echo "    (cannot list)"
    fi
    echo ""
done

echo ""
echo "=== PROCESSING COMPLETE ==="
echo "Total RocksDB databases scanned: $total_dbs"
echo "Databases with $COLUMN_FAMILY: $processed_dbs"
echo "Total keys found: $total_keys_found"

if [ -f "$OUTPUT_FILE" ] && [ -s "$OUTPUT_FILE" ]; then
    timestamp_keys_count=$(wc -l < "$OUTPUT_FILE")
    echo "Keys with timestamps: $timestamp_keys_count"
    echo "Output file: $OUTPUT_FILE"
    
    echo ""
    echo "=== SAMPLE OUTPUT ==="
    head -5 "$OUTPUT_FILE" | while IFS='|' read db_path hex_key text_key timestamps; do
        echo "Database: $(basename "$(dirname "$db_path")")"
        echo "Hex: $hex_key"
        echo "Text: $text_key"
        echo "Timestamps: $timestamps"
        echo "---"
    done
    
    echo ""
    echo "=== DATABASE DISTRIBUTION ==="
    cut -d'|' -f1 "$OUTPUT_FILE" | sort | uniq -c | sort -nr
else
    echo "No keys with timestamps found in any database."
    touch "$OUTPUT_FILE"
fi
