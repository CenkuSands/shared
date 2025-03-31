#!/bin/bash

# Configuration
CONNECT_URL="http://localhost:8083"  # Adjust this to your Kafka Connect URL
OUTPUT_DIR="./connectors"           # Directory to save the JSON files

# Ensure jq is installed (used for JSON parsing)
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required. Please install it (e.g., 'sudo apt install jq' on Ubuntu)."
    exit 1
fi

# Create output directory if it doesn’t exist
mkdir -p "$OUTPUT_DIR"

# Fetch the list of connectors
echo "Fetching list of connectors from $CONNECT_URL..."
connectors=$(curl -s "$CONNECT_URL/connectors")

if [ -z "$connectors" ] || [ "$connectors" = "[]" ]; then
    echo "No connectors found or failed to connect to $CONNECT_URL."
    exit 1
fi

# Parse the connectors array and loop through each one
echo "$connectors" | jq -r '.[]' | while read -r name; do
    echo "Fetching config for connector: $name..."
    # Fetch the config for this connector
    config=$(curl -s "$CONNECT_URL/connectors/$name/config")
    
    if [ -z "$config" ]; then
        echo "Warning: Failed to fetch config for $name. Skipping."
        continue
    fi

    # Save the config to a file named connector-<name>.json
    output_file="$OUTPUT_DIR/connector-$name.json"
    echo "$config" | jq . > "$output_file"
    
    if [ $? -eq 0 ]; then
        echo "Saved config to $output_file"
    else
        echo "Error: Failed to save config for $name."
    fi
done

echo "Done fetching and saving all connector configurations."
