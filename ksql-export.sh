#!/bin/bash
KSQL_SERVER="http://localhost:8088"
OUTPUT_FILE="ksql_definitions.sql"
TEMP_FILE="temp_output.txt"

# Initialize the output file
> "$OUTPUT_FILE"

# Create a temporary SQL script
> temp_script.sql

# Get streams
streams=$(ksql $KSQL_SERVER --execute "SHOW STREAMS;" | grep -vE "Stream Name|===" | awk '{print $1}' | grep -v '^$')

# Add DESCRIBE for each stream
for stream in $streams; do
    echo "DESCRIBE $stream;" >> temp_script.sql
done

# Get queries
query_ids=$(ksql $KSQL_SERVER --execute "SHOW QUERIES;" | grep -vE "Query ID|===" | awk '{print $1}' | grep -v '^$')

# Add EXPLAIN for each query
for query_id in $query_ids; do
    echo "EXPLAIN $query_id;" >> temp_script.sql
done

# Run the script and capture output
ksql $KSQL_SERVER < temp_script.sql > "$TEMP_FILE"

# Extract only the Statement and Query String lines, removing decorative borders
grep -E "Statement\s*:|Query String\s*:" "$TEMP_FILE" | sed 's/.*Statement\s*:\s*//;s/.*Query String\s*:\s*//' | sed '/^$/d' | sed 's/$/;/' >> "$OUTPUT_FILE"

# Clean up
rm temp_script.sql "$TEMP_FILE"

echo "Stream and query definitions exported to $OUTPUT_FILE"
