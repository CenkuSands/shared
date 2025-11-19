#!/bin/bash
# extract_all_timestamp_keys_hex_proper.sh
# Better handling of column family detection

set -e

BASE_DIR="/var/lib/kafka-streams"
OUTPUT_FILE="/tmp/all_timestamp_keys_hex_$(date +%Y%m%d_%H%M%S).txt"

echo "=== Scanning for ALL RocksDB databases in: $BASE_DIR ==="
echo ""

# Counter for tracking
total_dbs=0
processed_dbs=0

# Find all RocksDB directories
find "$BASE_DIR" -type f -name "CURRENT" | while read current_file; do
    rocksdb_dir=$(dirname "$current_file")
    
    ((total_dbs++))
    echo "=== RocksDB: $rocksdb_dir ==="
    
    # Get ALL column families and check each one
    column_families=$(podman run --rm --security-opt label=disable \
      -v "${rocksdb_dir}:/data:ro" \
      docker.io/library/rocksdb-tool ldb --db=/data list_column_families 2>/dev/null)
    
    if [ $? -eq 0 ] && [ ! -z "$column_families" ]; then
        echo "Available column families:"
        echo "$column_families" | sed 's/^/  /'
        
        # Check each column family for KeyValueWithTimestamp pattern
        echo "$column_families" | while read cf; do
            # Clean the column family name (remove quotes, brackets, etc.)
            clean_cf=$(echo "$cf" | sed 's/["\[\]]//g' | tr -d '[:space:]')
            
            if [[ "$clean_cf" == *"KeyValueWithTimestamp"* ]]; then
                echo "  ✓ Found matching column family: $clean_cf"
                ((processed_dbs++))
                
                # Extract keys in HEX format from this column family
                echo "  Extracting keys from: $clean_cf"
                podman run --rm --security-opt label=disable \
                  -v "${rocksdb_dir}:/data:ro" \
                  docker.io/library/rocksdb-tool ldb --db=/data --try_load_options --column_family="$cf" scan --key_hex 2>/dev/null | \
                while read -r key_hex; do
                    if [[ ! -z "$key_hex" ]]; then
                        # Remove 0x prefix if present
                        key_hex_clean="${key_hex#0x}"
                        
                        # Decode hex to text to check for timestamps
                        key_text=$(echo "$key_hex_clean" | xxd -r -p 2>/dev/null || echo "DECODE_ERROR")
                        
                        # Check if key contains timestamp
                        if echo "$key_text" | grep -qE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"; then
                            timestamps=$(echo "$key_text" | grep -oE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z" | tr '\n' ',')
                            echo "$rocksdb_dir|$clean_cf|$key_hex_clean|$key_text|${timestamps%,}" >> "$OUTPUT_FILE"
                            echo "    ✓ Found key with timestamp"
                        fi
                    fi
                done
            fi
        done
    else
        echo "  ✗ Cannot access or no column families found"
    fi
    echo ""
done

echo ""
echo "=== Processing complete ==="
echo "Total RocksDB databases found: $total_dbs"
echo "Databases with KeyValueWithTimestamp: $processed_dbs"

if [ -f "$OUTPUT_FILE" ] && [ -s "$OUTPUT_FILE" ]; then
    echo "Results saved to: $OUTPUT_FILE"
    echo "Total HEX keys with timestamps: $(wc -l < "$OUTPUT_FILE")"
    
    echo ""
    echo "=== Sample output ==="
    head -3 "$OUTPUT_FILE" | while IFS='|' read path cf hex text timestamp; do
        echo "DB: $(basename $(dirname $path))"
        echo "CF: $cf"
        echo "Hex: $hex"
        echo "Text: $text"
        echo "Timestamp: $timestamp"
        echo "---"
    done
else
    echo "No keys with timestamps found in any database."
    touch "$OUTPUT_FILE"
fi
