#!/bin/bash

# Hardcoded Cluster IDs (replace with your actual IDs)
KAFKA_ID="KQey0SYmQ_uT6Vcq-0y9gA"       # Your Kafka cluster ID
SR_ID="schema-registry"                   # Your Schema Registry cluster ID
CONNECT_ID="connect-cluster"              # Your Connect cluster ID
KSQL_ID="default_"                        # Your ksqlDB cluster ID

# File containing the list of users
USER_FILE="user-list.txt"
OUTPUT_FILE="role-assignments.csv"

# Check if user file exists
if [ ! -f "$USER_FILE" ]; then
    echo "Error: User file '$USER_FILE' not found."
    exit 1
fi

# Perform Confluent login with error handling
echo "Logging in to Confluent MDS..."
confluent login --url https://xxx.xx.xxx.xxx:8090 --ca-cert-path /var/ssl/private/root.pem
if [ $? -ne 0 ]; then
    echo "Error: Confluent login failed. Check URL, certificate, or credentials."
    exit 1
fi

# Initialize CSV file with headers
echo "Cluster Type,Principal,Role Name,Resource Type,Resource Name,Pattern Type" > "$OUTPUT_FILE"

# Function to process output and append to CSV
process_output() {
    local cluster_type="$1"
    local output="$2"
    # Skip header lines and separators, then format into CSV
    echo "$output" | awk -v cluster="$cluster_type" '
        NR > 2 && $0 !~ /^[=-]+$/ {
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", $1);  # Trim Principal
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", $3);  # Trim Role Name
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", $5);  # Trim Resource Type
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", $7);  # Trim Resource Name
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", $9);  # Trim Pattern Type
            if ($1 != "") {
                print cluster "," $1 "," $3 "," $5 "," $7 "," $9
            }
        }
    ' >> "$OUTPUT_FILE"
}

# Read users from the file and process each
while IFS= read -r USER; do
    # Skip empty lines or comments
    [[ -z "$USER" || "$USER" =~ ^# ]] && continue

    # Assume USER is just the username; prepend "User:" if not already present
    if [[ "$USER" != User:* && "$USER" != ServiceAccount:* ]]; then
        PRINCIPAL="User:$USER"
    else
        PRINCIPAL="$USER"
    fi

    echo "Processing role assignments for $PRINCIPAL..."

    # List role bindings and capture output
    kafka_output=$(confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --principal "$PRINCIPAL")
    sr_output=$(confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --schema-registry-cluster-id "$SR_ID" --principal "$PRINCIPAL")
    connect_output=$(confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --connect-cluster-id "$CONNECT_ID" --principal "$PRINCIPAL")
    ksql_output=$(confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --ksql-cluster-id "$KSQL_ID" --principal "$PRINCIPAL")

    # Process each output into CSV
    process_output "Kafka" "$kafka_output"
    process_output "Schema Registry" "$sr_output"
    process_output "Connect" "$connect_output"
    process_output "ksqlDB" "$ksql_output"

done < "$USER_FILE"

echo "Done. Role assignments saved to $OUTPUT_FILE."
