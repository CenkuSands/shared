#!/bin/bash

# Hardcoded Cluster IDs (replace with your actual IDs)
KAFKA_ID="KQey0SYmQ_uT6Vcq-0y9gA"       # Your Kafka cluster ID
SR_ID="schema-registry"                   # Your Schema Registry cluster ID
CONNECT_ID="connect-cluster"              # Your Connect cluster ID
KSQL_ID="default_"                        # Your ksqlDB cluster ID

# File containing the list of users
USER_FILE="user-list.txt"

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
    echo "===================================================="

    # List role bindings
    echo "=== Kafka Cluster ($KAFKA_ID) ==="
    confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --principal "$PRINCIPAL"

    echo "=== Schema Registry ($SR_ID) ==="
    confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --schema-registry-cluster-id "$SR_ID" --principal "$PRINCIPAL"

    echo "=== Connect ($CONNECT_ID) ==="
    confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --connect-cluster-id "$CONNECT_ID" --principal "$PRINCIPAL"

    echo "=== ksqlDB ($KSQL_ID) ==="
    confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --ksql-cluster-id "$KSQL_ID" --principal "$PRINCIPAL"

    echo "===================================================="
done < "$USER_FILE"

echo "Done processing all users."
