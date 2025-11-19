#!/bin/bash
# extract_all_timestamp_keys_hex_fixed.sh
# Uses absolute paths for proper RocksDB access

set -e

BASE_DIR="/var/lib/kafka-streams"
OUTPUT_FILE="/tmp/all_timestamp_keys_hex_$(date +%Y%m%d_%H%M%S).txt"
COLUMN_FAMILY="KeyValueWithTimestamp"

echo "=== Scanning for ALL RocksDB databases in: $BASE_DIR ==="
echo "Using absolute paths..."
echo ""

# Counter for tracking
total_dbs=0
processed_dbs=0
has_column_family=0

# Find all RocksDB directories
find "$BASE_DIR" -type f -name "CURRENT" | while read current_file; do
    rocksdb_dir=$(dirname "$current_file")
    absolute_path="$rocksdb_dir"  # This is already absolute
    
    ((total_dbs++))
    echo "Found RocksDB: $absolute_path"
    
    # Check if database is accessible and has the column family
    if podman run --rm --security-opt label=disable \
       -v "${absolute_path}:/data:ro" \
       docker.io/library/rocksdb-tool ldb --db=/data list_column_families 2>/dev/null; then
       
        echo "  ✓ Database accessible"
        
        # Check specifically for KeyValueWithTimestamp
        if podman run --rm --security-opt label=disable \
           -v "${absolute_path}:/data:ro" \
           docker.io/library/rocksdb-tool ldb --db=/data list_column_families 2>/dev/null | grep -q "$COLUMN_FAMILY"; then
            
            ((has_column_family++))
            echo "  ✓ Contains $COLUMN_FAMILY, extracting HEX keys..."
            ((processed_dbs++))
            
            # Extract keys in HEX format only
            podman run --rm --security-opt label=disable \
              -v "${absolute_path}:/data:ro" \
              docker.io/library/rocksdb-tool ldb --db=/data --try_load_options --column_family="$COLUMN_FAMILY" scan --key_hex 2>/dev/null | \
            while read -r key_hex; do
                if [[ ! -z "$key_hex" ]]; then
                    # Remove 0x prefix if present
                    key_hex_clean="${key_hex#0x}"
                    
                    # Decode hex to text to check for timestamps
                    key_text=$(echo "$key_hex_clean" | xxd -r -p 2>/dev/null || echo "DECODE_ERROR")
                    
                    # Check if key contains timestamp
                    if echo "$key_text" | grep -qE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"; then
                        timestamps=$(echo "$key_text" | grep -oE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z" | tr '\n' ',')
                        echo "$absolute_path|$key_hex_clean|$key_text|${timestamps%,}" >> "$OUTPUT_FILE"
                        echo "    ✓ Found key with timestamp"
                    fi
                fi
            done
        else
            echo "  ✗ No $COLUMN_FAMILY column family"
            # List available column families for debugging
            echo "  Available column families:"
            podman run --rm --security-opt label=disable \
              -v "${absolute_path}:/data:ro" \
              docker.io/library/rocksdb-tool ldb --db=/data list_column_families 2>/dev/null | sed 's/^/    /' || echo "    (cannot list)"
        fi
    else
        echo "  ✗ Database not accessible or invalid"
    fi
    echo ""  # Add spacing between databases
done

echo ""
echo "=== Processing complete ==="
echo "Total RocksDB databases found: $total_dbs"
echo "Accessible databases: $processed_dbs"
echo "Databases with $COLUMN_FAMILY: $has_column_family"

if [ -f "$OUTPUT_FILE" ] && [ -s "$OUTPUT_FILE" ]; then
    echo "Results saved to: $OUTPUT_FILE"
    echo "Total HEX keys with timestamps: $(wc -l < "$OUTPUT_FILE")"
else
    echo "No keys with timestamps found in any database."
    # Create empty file to avoid errors
    touch "$OUTPUT_FILE"
fi
