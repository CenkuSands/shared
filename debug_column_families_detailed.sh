#!/bin/bash
# debug_column_families_detailed.sh

BASE_DIR="/var/lib/kafka-streams"

echo "=== Detailed Column Family Analysis ==="
echo ""

find "$BASE_DIR" -type f -name "CURRENT" | while read current_file; do
    rocksdb_dir=$(dirname "$current_file")
    echo "=== Database: $rocksdb_dir ==="
    
    # Get raw column family output
    echo "Raw column families output:"
    podman run --rm --security-opt label=disable \
      -v "${rocksdb_dir}:/data:ro" \
      docker.io/library/rocksdb-tool ldb --db=/data list_column_families 2>&1
    
    # Try different ways to detect KeyValueWithTimestamp
    echo ""
    echo "Detection methods:"
    
    # Method 1: Direct grep
    echo "1. Direct grep:"
    podman run --rm --security-opt label=disable \
      -v "${rocksdb_dir}:/data:ro" \
      docker.io/library/rocksdb-tool ldb --db=/data list_column_families 2>/dev/null | grep "KeyValueWithTimestamp" && echo "    ✓ Found" || echo "    ✗ Not found"
    
    # Method 2: Case insensitive
    echo "2. Case insensitive grep:"
    podman run --rm --security-opt label=disable \
      -v "${rocksdb_dir}:/data:ro" \
      docker.io/library/rocksdb-tool ldb --db=/data list_column_families 2>/dev/null | grep -i "keyvaluewithtimestamp" && echo "    ✓ Found" || echo "    ✗ Not found"
    
    # Method 3: Show all and let us see
    echo "3. All column families (raw):"
    podman run --rm --security-opt label=disable \
      -v "${rocksdb_dir}:/data:ro" \
      docker.io/library/rocksdb-tool ldb --db=/data list_column_families 2>/dev/null | od -c | head -5
    
    echo ""
done
