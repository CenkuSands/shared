#!/bin/bash
# extract_all_timestamp_keys_hex.sh
# Extracts timestamp-based keys in HEX format from ALL RocksDB databases under /var/lib/kafka-streams/

set -e

BASE_DIR="/var/lib/kafka-streams"
OUTPUT_FILE="/tmp/all_timestamp_keys_hex_$(date +%Y%m%d_%H%M%S).txt"
COLUMN_FAMILY="KeyValueWithTimestamp"

echo "=== Scanning for ALL RocksDB databases in: $BASE_DIR ==="
echo "Output: HEX keys with timestamps"
echo ""

# Counter for tracking
total_dbs=0
processed_dbs=0

# Find all RocksDB directories (looking for typical RocksDB file patterns)
find "$BASE_DIR" -type f -name "CURRENT" | while read current_file; do
    rocksdb_dir=$(dirname "$current_file")
    relative_path="${rocksdb_dir#$BASE_DIR/}"
    
    ((total_dbs++))
    echo "Found RocksDB: $relative_path"
    
    # Check if this is a KSQL-style database with KeyValueWithTimestamp column family
    if podman run --rm --security-opt label=disable \
       -v "${rocksdb_dir}:/data:ro" \
       docker.io/library/rocksdb-tool ldb --db=/data list_column_families 2>/dev/null | grep -q "$COLUMN_FAMILY"; then
        
        echo "  ✓ Contains $COLUMN_FAMILY, extracting HEX keys..."
        ((processed_dbs++))
        
        # Extract keys in HEX format only
        podman run --rm --security-opt label=disable \
          -v "${rocksdb_dir}:/data:ro" \
          docker.io/library/rocksdb-tool ldb --db=/data --try_load_options --column_family="$COLUMN_FAMILY" scan --key_hex 2>/dev/null | \
        while read -r key_hex; do
            if [[ ! -z "$key_hex" ]]; then
                # Remove 0x prefix if present and ensure it's clean hex
                key_hex_clean="${key_hex#0x}"
                
                # Decode hex to text to check for timestamps
                key_text=$(echo "$key_hex_clean" | xxd -r -p 2>/dev/null || echo "DECODE_ERROR")
                
                # Extract timestamp if present in key (looking for ISO date patterns)
                if echo "$key_text" | grep -qE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"; then
                    timestamps=$(echo "$key_text" | grep -oE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z" | tr '\n' ',')
                    echo "$relative_path|$key_hex_clean|$key_text|${timestamps%,}" >> "$OUTPUT_FILE"
                    echo "    ✓ Found key with timestamp: $key_hex_clean"
                fi
            fi
        done
    else
        echo "  ✗ No $COLUMN_FAMILY column family found"
    fi
done

echo ""
echo "=== Processing complete ==="
echo "Total RocksDB databases found: $total_dbs"
echo "Databases with $COLUMN_FAMILY: $processed_dbs"

if [ -f "$OUTPUT_FILE" ]; then
    echo "Results saved to: $OUTPUT_FILE"
    echo ""
    echo "=== Summary ==="
    echo "Total HEX keys with timestamps: $(wc -l < "$OUTPUT_FILE" 2>/dev/null || echo 0)"
    echo ""
    echo "Database distribution:"
    cut -d'|' -f1 "$OUTPUT_FILE" 2>/dev/null | sort | uniq -c | sort -nr || echo "No data found"
    echo ""
    echo "Sample of extracted HEX keys:"
    head -5 "$OUTPUT_FILE" 2>/dev/null | while IFS='|' read db_path hex_key text_key timestamp; do
        echo "  $db_path: $hex_key"
    done || echo "No sample data available"
else
    echo "No keys with timestamps found in any database."
fi
