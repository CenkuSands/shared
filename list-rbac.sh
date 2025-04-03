#!/bin/bash

# Prompt for confluent login

confluent login --url https://xxx.xx.xxx.xxx:8090 --ca-cert-path /var/ssl/private/root.pem

# Hardcoded Cluster IDs (replace with your actual IDs)
KAFKA_ID="KQey0SYmQ_uT6Vcq-0y9gA"       # Replace with your Kafka cluster ID
SR_ID="schema-registry"                            # Replace with your Schema Registry cluster ID
CONNECT_ID="connect-cluster"                  # Replace with your Connect cluster ID
KSQL_ID="default_"                            # Replace with your ksqlDB cluster ID
user_list=$(cat user-list.txt)

for USER in $user_list; do

# List role bindings
echo "=== Kafka Cluster ($KAFKA_ID) ==="
confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --principal "User:$USER"

echo "=== Schema Registry ($SR_ID) ==="
confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --schema-registry-cluster-id "$SR_ID" --principal "User:$USER"

echo "=== Connect ($CONNECT_ID) ==="
confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --connect-cluster-id "$CONNECT_ID" --principal "User:$USER"

echo "=== ksqlDB ($KSQL_ID) ==="
confluent iam rbac role-binding list --kafka-cluster-id "$KAFKA_ID" --ksql-cluster-id "$KSQL_ID" --principal "User:$USER"

echo "===================================================="

done < "$user_list"
echo "Done."
