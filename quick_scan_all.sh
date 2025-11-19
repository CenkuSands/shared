#!/bin/bash
# quick_scan_all.sh

find /var/lib/kafka-streams -name "CURRENT" -type f | \
while read current_file; do
    db_dir=$(dirname "$current_file")
    rel_path="${db_dir#/var/lib/kafka-streams/}"
    echo "=== Scanning: $rel_path ==="
    
    podman run --rm --security-opt label=disable \
      -v "${db_dir}:/data:ro" \
      docker.io/library/rocksdb-tool ldb --db=/data --try_load_options --column_family=KeyValueWithTimestamp scan --key_only 2>/dev/null | \
    while read key_hex; do
        if [[ ! -z "$key_hex" ]]; then
            key_text=$(echo "${key_hex#0x}" | xxd -r -p 2>/dev/null)
            if echo "$key_text" | grep -qE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"; then
                echo "$rel_path: $key_text"
            fi
        fi
    done
done
