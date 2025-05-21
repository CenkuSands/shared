#!/bin/bash
METADATA_FILE="ksql_metadata.json"
LINEAGE_FILE="ksql_lineage.txt"
DOT_FILE="ksql_lineage.dot"

# Initialize the lineage file
> "$LINEAGE_FILE"
echo "ksqlDB Data Lineage Report" >> "$LINEAGE_FILE"
echo "=========================" >> "$LINEAGE_FILE"
echo "" >> "$LINEAGE_FILE"

# Check if JSON output is available
if grep -q '"type":"streams"' "$METADATA_FILE"; then
    # JSON parsing with jq
    # Extract streams
    streams=$(jq -r '.[] | select(.type=="streams") | .streams[].name' "$METADATA_FILE")
    echo "Streams:" >> "$LINEAGE_FILE"
    for stream in $streams; do
        topic=$(jq -r ".[] | select(.type==\"source\" and .sourceDescription.name==\"$stream\") | .sourceDescription.topic" "$METADATA_FILE")
        echo "- $stream (Kafka Topic: $topic)" >> "$LINEAGE_FILE"
    done
    echo "" >> "$LINEAGE_FILE"

    # Extract tables
    tables=$(jq -r '.[] | select(.type=="tables") | .tables[].name' "$METADATA_FILE")
    echo "Tables:" >> "$LINEAGE_FILE"
    for table in $tables; do
        topic=$(jq -r ".[] | select(.type==\"source\" and .sourceDescription.name==\"$table\") | .sourceDescription.topic" "$METADATA_FILE")
        echo "- $table (Kafka Topic: $topic)" >> "$LINEAGE_FILE"
    done
    echo "" >> "$LINEAGE_FILE"

    # Extract queries and their lineage
    echo "Queries:" >> "$LINEAGE_FILE"
    query_ids=$(jq -r '.[] | select(.type=="queries") | .queries[].id' "$METADATA_FILE")
    for query_id in $query_ids; do
        query_string=$(jq -r ".[] | select(.type==\"query\" and .queryDescription.id==\"$query_id\") | .queryDescription.queryString" "$METADATA_FILE")
        sources=$(jq -r ".[] | select(.type==\"query\" and .queryDescription.id==\"$query_id\") | .queryDescription.sources[]" "$METADATA_FILE")
        sinks=$(jq -r ".[] | select(.type==\"query\" and .queryDescription.id==\"$query_id\") | .queryDescription.sinks[]" "$METADATA_FILE")
        sink_topic=$(jq -r ".[] | select(.type==\"query\" and .queryDescription.id==\"$query_id\") | .queryDescription.sinkTopic" "$METADATA_FILE")
        echo "- $query_id" >> "$LINEAGE_FILE"
        echo "  Query: $query_string" >> "$LINEAGE_FILE"
        echo "  Sources: $sources" >> "$LINEAGE_FILE"
        echo "  Sinks: $sinks (Kafka Topic: $sink_topic)" >> "$LINEAGE_FILE"
        echo "" >> "$LINEAGE_FILE"
    done
else
    # Fallback to TABLE parsing
    echo "Streams:" >> "$LINEAGE_FILE"
    streams=$(grep -A 1000 "Stream Name" "$METADATA_FILE" | grep -B 1000 -m 1 "^$" | grep -vE "Stream Name|===" | awk '{print $1}' | grep -v '^$')
    for stream in $streams; do
        topic=$(grep -A 10 "Name.*$stream" "$METADATA_FILE" | grep "Kafka Topic" | sed 's/.*Kafka Topic\s*:\s*//')
        echo "- $stream (Kafka Topic: $topic)" >> "$LINEAGE_FILE"
    done
    echo "" >> "$LINEAGE_FILE"

    echo "Tables:" >> "$LINEAGE_FILE"
    tables=$(grep -A 1000 "Table Name" "$METADATA_FILE" | grep -B 1000 -m 1 "^$" | grep -vE "Table Name|===" | awk '{print $1}' | grep -v '^$')
    for table in $tables; do
        topic=$(grep -A 10 "Name.*$table" "$METADATA_FILE" | grep "Kafka Topic" | sed 's/.*Kafka Topic\s*:\s*//')
        echo "- $table (Kafka Topic: $topic)" >> "$LINEAGE_FILE"
    done
    echo "" >> "$LINEAGE_FILE"

    echo "Queries:" >> "$LINEAGE_FILE"
    query_ids=$(grep -A 1000 "Query ID" "$METADATA_FILE" | grep -B 1000 -m 1 "^$" | grep -vE "Query ID|===" | awk '{print $1}' | grep -v '^$')
    for query_id in $query_ids; do
        query_string=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Query String" | sed 's/.*Query String\s*:\s*//')
        sources=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Sources" | sed 's/.*Sources\s*:\s*//')
        sinks=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Sinks" | sed 's/.*Sinks\s*:\s*//')
        sink_topic=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Sink Kafka Topic" | sed 's/.*Sink Kafka Topic\s*:\s*//')
        echo "- $query_id" >> "$LINEAGE_FILE"
        echo "  Query: $query_string" >> "$LINEAGE_FILE"
        echo "  Sources: $sources" >> "$LINEAGE_FILE"
        echo "  Sinks: $sinks (Kafka Topic: $sink_topic)" >> "$LINEAGE_FILE"
        echo "" >> "$LINEAGE_FILE"
    done
fi

# Generate a Graphviz DOT file for visualization
echo "digraph ksql_lineage {" > "$DOT_FILE"
echo "  rankdir=LR;" >> "$DOT_FILE"
# Add streams and tables as nodes
for stream in $streams; do
    topic=$(grep -A 10 "Name.*$stream" "$METADATA_FILE" | grep "Kafka Topic" | sed 's/.*Kafka Topic\s*:\s*//' || echo "unknown")
    echo "  \"$topic\" -> \"$stream\" [label=\"Stream\"];" >> "$DOT_FILE"
done
for table in $tables; do
    topic=$(grep -A 10 "Name.*$table" "$METADATA_FILE" | grep "Kafka Topic" | sed 's/.*Kafka Topic\s*:\s*//' || echo "unknown")
    echo "  \"$topic\" -> \"$table\" [label=\"Table\"];" >> "$DOT_FILE"
done
# Add query dependencies
for query_id in $query_ids; do
    sources=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Sources" | sed 's/.*Sources\s*:\s*//')
    sinks=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Sinks" | sed 's/.*Sinks\s*:\s*//')
    sink_topic=$(grep -A 20 "Query ID.*$query_id" "$METADATA_FILE" | grep "Sink Kafka Topic" | sed 's/.*Sink Kafka Topic\s*:\s*//' || echo "unknown")
    for source in $sources; do
        echo "  \"$source\" -> \"$query_id\" [label=\"Query Input\"];" >> "$DOT_FILE"
    done
    for sink in $sinks; do
        echo "  \"$query_id\" -> \"$sink\" [label=\"Query Output\"];" >> "$DOT_FILE"
        echo "  \"$sink\" -> \"$sink_topic\" [label=\"Sink Topic\"];" >> "$DOT_FILE"
    done
done
echo "}" >> "$DOT_FILE"

echo "Lineage report generated in $LINEAGE_FILE"
echo "Graphviz DOT file generated in $DOT_FILE"
