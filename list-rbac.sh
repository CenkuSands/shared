#!/bin/bash

# Hardcoded Cluster IDs (replace with your actual IDs)
KAFKA_ID="aBcDeFgHiJkLmNoPqRsTuVwXyZ"       # Replace with your Kafka cluster ID
SR_ID="lsrc-abc12"                            # Replace with your Schema Registry cluster ID
CONNECT_ID="connect-cluster"                  # Replace with your Connect cluster ID
KSQL_ID="default_"                            # Replace with your ksqlDB cluster ID

# Prompt for user input
read -p "Enter the principal (e.g., User:alice): " USER
if [ -z "$USER" ]; then
    echo "Error: Principal cannot be empty."
    exit 1
fi

# Optional: Prompt for MDS login if RBAC is enabled
read -p "Is RBAC enabled? (y/n, default: n): " RBAC_ENABLED
RBAC_ENABLED=${RBAC_ENABLED:-n}
if [ "$RBAC_ENABLED" = "y" ]; then
    read -p "Enter MDS URL (default: http://localhost:8090): " MDS_URL
    MDS_URL=${MDS_URL:-http://localhost:8090}
    echo "Logging in to MDS..."
    confluent login --url "$MDS_URL"
    if [ $? -ne 0 ]; then
        echo "Error: MDS login failed. Check credentials or URL."
        exit 1
    fi
fi

# List role bindings
echo "=== Kafka Cluster ($KAFKA_ID) ==="
confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --principal "$USER"

echo "=== Schema Registry ($SR_ID) ==="
confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --schema-registry-cluster-id "$SR_ID" --principal "$USER"

echo "=== Connect ($CONNECT_ID) ==="
confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --connect-cluster-id "$CONNECT_ID" --principal "$USER"

echo "=== ksqlDB ($KSQL_ID) ==="
confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --ksql-cluster-id "$KSQL_ID" --principal "$USER"

echo "Done."
