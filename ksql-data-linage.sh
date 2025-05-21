#!/bin/bash
KSQL SERVER="http://localhost:8088"
METADATA_FILE="ksql_metadata.txt"
DEFINITIONS_FILE="ksql_definitions.sql"
LINEAGE_FILE="ksql_lineage.txt"
DOT_FILE="ksql_lineage.dot"

# Initialize output files
> "$METADATA_FILE"
> "$DEFINITIONS_FILE"
> "$LINEAGE_FILE"
> "$DOT_FILE"

# Create a temporary SQL script
> temp_script.sql

# Get streams
echo "SHOW STREAMS;" >> temp_script.sql
streams=$(ksql $KSQL_SERVER --execute "SHOW STREAMS;" | grep -vE "Stream Name|===|~~~|^$" | awk '{print $1}')
for stream in $streams; do
    echo "DESCRIBE $stream;" >> temp_script.sql
done

# Get tables
echo "SHOW TABLES;" >> temp_script.sql
tables=$(ksql $KSQL_SERVER --execute "SHOW TABLES;" | grep -vE "Table Name|===|~~~|^$" | awk '{print $1}')
for table in $tables; do
    echo "DESCRIBE $table;" >> temp_script.sql
done

# Get queries
echo "SHOW QUERIES;" >> temp_script.sql
query_ids=$(ksql $KSQL_SERVER --execute "SHOW QUERIES;" | grep -vE "Query ID|===|~~~|^$" | awk '{print $1}')
for query_id in $query_ids; do
    echo "EXPLAIN $query_id;" >> temp_script.sql
done

# Run the script and capture output
ksql $KSQL_SERVER < temp_script.sql > "$METADATA_FILE"

# Extract CREATE statements for ksql_definitions.sql
grep -E "Statement\s*:|Query String\s*:" "$METADATA_FILE" | sed 's/.*Statement\s*:\s*//;s/.*Query String\s*:\s*//' | sed '/^$/d' | sed 's/$/;/' >> "$DEFINITIONS_FILE"

# Generate lineage report
echo "ksqlDB Data Lineage Report" >> "$LINEAGE_FILE"
echo "=========================" >> "$LINEAGE_FILE"
echo "" >> "$LINEAGE_FILE"

# Streams
echo "Streams:" >> "$LINEAGE_FILE"
for stream in $streams; do
    topic=$(grep -A 10 "Name.*$stream" "$METADATA_FILE" | grep "Kafka Topic" | sed 's/.*Kafka Topic\s*:\s*//' | head -1)
    echo "- $stream (Kafka Topic: $topic)" >> "$LINEAGE_FILE"
done
echo "" >> "$LINEAGE_FILE"

# Tables
echo "Tables:" >> "$LINEAGE_FILE"
for table in $tables; do
    topic=$(grep -A 10 "Name.*$table" "$METADATA_FILE" | grep "Kafka Topic" | sed 's/.*Kafka Topic\s*:\s*//' | head -1)
    echo "- $table (Kafka Topic: $topic)" >> "$LINEAGE_FILE"
done
echo "" >> "$LINEAGE_FILE"

# Queries
echo "Queries:" >> "$LINEAGE_FILE"
for query_id in $query_ids; do
    query_string=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Query String" | sed 's/.*Query String\s*:\s*//' | head -1)
    sources=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Sources" | sed 's/.*Sources\s*:\s*//' | head -1)
    sinks=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Sinks" | sed 's/.*Sinks\s*:\s*//' | head -1)
    sink_topic=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Sink Kafka Topic" | sed 's/.*Sink Kafka Topic\s*:\s*//' | head -1)
    echo "- $query_id" >> "$LINEAGE_FILE"
    echo "  Query: $query_string" >> "$LINEAGE_FILE"
    echo "  Sources: ${sources:-None}" >> "$LINEAGE_FILE"
    echo "  Sinks: ${sinks:-None} (Kafka Topic: ${sink_topic:-None})" >> "$LINEAGE_FILE"
    echo "" >> "$LINEAGE_FILE"
done

# Generate Graphviz DOT file
echo "digraph ksql_lineage {" >> "$DOT_FILE"
echo "  rankdir=LR;" >> "$DOT_FILE"
# Add streams and tables as nodes
for stream in $streams; do
    topic=$(grep -A 10 "Name.*$stream" "$METADATA_FILE" | grep "Kafka Topic" | sed 's/.*Kafka Topic\s*:\s*//' | head -1)
    echo "  \"$topic\" -> \"$stream\" [label=\"Stream\"];" >> "$DOT_FILE"
done
for table in $tables; do
    topic=$(grep -A 10 "Name.*$table" "$METADATA_FILE" | grep "Kafka Topic" | sed 's/.*Kafka Topic\s*:\s*//' | head -1)
    echo "  \"$topic\" -> \"$table\" [label=\"Table\"];" >> "$DOT_FILE"
done
# Add query dependencies
for query_id in $query_ids; do
    sources=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Sources" | sed 's/.*Sources\s*:\s*//' | head -1)
    sinks=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Sinks" | sed 's/.*Sinks\s*:\s*//' | head -1)
    sink_topic=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Sink Kafka Topic" | sed 's/.*Sink Kafka Topic\s*:\s*//' | head -1)
    for source in $sources; do
        echo "  \"$source\" -> \"$query_id\" [label=\"Query Input\"];" >> "$DOT_FILE"
    done
    for sink in $sinks; do
        echo "  \"$query_id\" -> \"$sink\" [label=\"Query Output\"];" >> "$DOT_FILE"
        echo "  \"$sink\" -> \"$sink_topic\" [label=\"Sink Topic\"];" >> "$DOT_FILE"
    done
done
echo "}" >> "$DOT_FILE"

# Clean up
rm "$METADATA_FILE"

echo "Definitions exported to $DEFINITIONS_FILE"
echo "Lineage report generated in $LINEAGE_FILE"
echo "Graphviz DOT file generated in $DOT_FILE"
