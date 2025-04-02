#!/bin/bash

# Configuration
CONNECT_URL="http://localhost:8083"  # Kafka Connect REST API endpoint
CONFIG_DIR="./connectors"            # Directory with connector config JSON files

# Ensure jq is installed (for JSON validation)
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required. Install it (e.g., 'sudo apt install jq')."
    exit 1
fi

# Check if the config directory exists
if [ ! -d "$CONFIG_DIR" ]; then
    echo "Error: Directory $CONFIG_DIR not found."
    exit 1
fi

# Loop through all connector-*.json files in the directory
for config_file in "$CONFIG_DIR"/connector-*.json; do
    # Check if any files match the pattern
    if [ ! -e "$config_file" ]; then
        echo "No connector config files found in $CONFIG_DIR."
        exit 1
    fi

    # Extract connector name from filename (remove 'connector-' prefix and '.json' suffix)
    filename=$(basename "$config_file")
    connector_name="${filename#connector-}"
    connector_name="${connector_name%.json}"

    # Validate JSON file
    if ! jq . "$config_file" > /dev/null 2>&1; then
        echo "Error: Invalid JSON in $config_file. Skipping."
        continue
    fi

    echo "Updating connector: $connector_name..."

    # Send PUT request to update the connector config
    response=$(curl -s -X PUT -H "Content-Type: application/json" \
                --data "@$config_file" \
                "$CONNECT_URL/connectors/$connector_name/config" \
                -w "\nHTTP_STATUS:%{http_code}")

    # Extract HTTP status code from response
    http_status=$(echo "$response" | grep "HTTP_STATUS" | cut -d':' -f2)

    # Check if update was successful
    if [ "$http_status" -eq 200 ]; then
        echo "Successfully updated $connector_name."
    else
        echo "Failed to update $connector_name. HTTP Status: $http_status"
        echo "Response: $(echo "$response" | sed '/HTTP_STATUS/d')"
    fi
done

echo "Finished updating all connectors."
