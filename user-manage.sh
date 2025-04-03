#!/bin/bash

# Hardcoded Cluster IDs (replace with your actual IDs)
KAFKA_ID="KQey0SYmQ_uT6Vcq-0y9gA"       # Your Kafka cluster ID
SR_ID="schema-registry"                   # Your Schema Registry cluster ID
CONNECT_ID="connect-cluster"              # Your Connect cluster ID
KSQL_ID="default_"                        # Your ksqlDB cluster ID

# Perform Confluent login with error handling
echo "Logging in to Confluent MDS..."
confluent login --url https://xxx.xx.xxx.xxx:8090 --ca-cert-path /var/ssl/private/root.pem
if [ $? -ne 0 ]; then
    echo "Error: Confluent login failed. Check URL, certificate, or credentials."
    exit 1
fi

# Function to display menu and get selection
select_from_menu() {
    local prompt="$1"
    local options=("${@:2}")
    local PS3="$prompt: "
    select choice in "${options[@]}"; do
        if [ -n "$choice" ]; then
            echo "$choice"
            break
        else
            echo "Invalid selection. Please try again."
        fi
    done
}

# Step 1: Select Cluster Type
echo "Select the cluster type to assign RBAC roles for:"
CLUSTER_TYPES=("Kafka" "Schema Registry" "Connect" "ksqlDB" "Quit")
CLUSTER_TYPE=$(select_from_menu "Choose a cluster" "${CLUSTER_TYPES[@]}")
if [ "$CLUSTER_TYPE" = "Quit" ]; then
    echo "Exiting."
    exit 0
fi

# Map cluster type to cluster ID
case "$CLUSTER_TYPE" in
    "Kafka") CLUSTER_ID="$KAFKA_ID"; CLUSTER_FLAG="--kafka-cluster-id" ;;
    "Schema Registry") CLUSTER_ID="$SR_ID"; CLUSTER_FLAG="--schema-registry-cluster-id"; EXTRA_FLAG="--kafka-cluster-id $KAFKA_ID" ;;
    "Connect") CLUSTER_ID="$CONNECT_ID"; CLUSTER_FLAG="--connect-cluster-id"; EXTRA_FLAG="--kafka-cluster-id $KAFKA_ID" ;;
    "ksqlDB") CLUSTER_ID="$KSQL_ID"; CLUSTER_FLAG="--ksql-cluster-id"; EXTRA_FLAG="--kafka-cluster-id $KAFKA_ID" ;;
esac

# Step 2: Select Resource Type
echo "Select the resource type for $CLUSTER_TYPE:"
case "$CLUSTER_TYPE" in
    "Kafka") RESOURCE_TYPES=("Cluster" "Topic" "Group" "TransactionalId") ;;
    "Schema Registry") RESOURCE_TYPES=("Cluster" "Subject") ;;
    "Connect") RESOURCE_TYPES=("Cluster" "Connector") ;;
    "ksqlDB") RESOURCE_TYPES=("Cluster" "KsqlCluster") ;;
esac
RESOURCE_TYPE=$(select_from_menu "Choose a resource type" "${RESOURCE_TYPES[@]}")

# Step 3: Input Resource Name
read -p "Enter the resource name (e.g., my-topic, my-group, or 'kafka-cluster' for Cluster): " RESOURCE_NAME
if [ -z "$RESOURCE_NAME" ]; then
    echo "Error: Resource name cannot be empty."
    exit 1
fi

# Step 4: Select Role
echo "Select the role to assign:"
ROLES=("SystemAdmin" "SecurityAdmin" "ClusterAdmin" "Operator" "ResourceOwner" "DeveloperRead" "DeveloperWrite" "DeveloperManage")
ROLE=$(select_from_menu "Choose a role" "${ROLES[@]}")

# Step 5: Select Pattern Type (only if not Cluster)
if [ "$RESOURCE_TYPE" != "Cluster" ]; then
    echo "Select the pattern type for the resource name:"
    PATTERN_TYPES=("LITERAL" "PREFIXED")
    PATTERN_TYPE=$(select_from_menu "Choose a pattern type (LITERAL for exact match, PREFIXED for prefix match)" "${PATTERN_TYPES[@]}")
else
    PATTERN_TYPE="LITERAL"
    echo "Pattern type set to LITERAL (required for Cluster resources)."
fi

# Step 6: Select Principal Type
echo "Select the principal type:"
PRINCIPAL_TYPES=("User" "Group")
PRINCIPAL_TYPE=$(select_from_menu "Choose a principal type" "${PRINCIPAL_TYPES[@]}")

# Step 7: Input Principal Name
read -p "Enter the $PRINCIPAL_TYPE name (e.g., alice for User, developers for Group): " PRINCIPAL_NAME
if [ -z "$PRINCIPAL_NAME" ]; then
    echo "Error: Principal name cannot be empty."
    exit 1
fi
PRINCIPAL="$PRINCIPAL_TYPE:$PRINCIPAL_NAME"

# Step 8: Confirm and Execute
echo "About to assign the following RBAC role:"
echo "Cluster: $CLUSTER_TYPE ($CLUSTER_ID)"
echo "Resource Type: $RESOURCE_TYPE"
echo "Resource Name: $RESOURCE_NAME"
echo "Pattern Type: $PATTERN_TYPE"
echo "Role: $ROLE"
echo "Principal: $PRINCIPAL"
read -p "Confirm assignment? (y/n): " CONFIRM
if [ "$CONFIRM" != "y" ]; then
    echo "Assignment cancelled."
    exit 0
fi

# Build and execute the command
CMD="confluent iam rbac role-binding create \
    --principal $PRINCIPAL \
    --role $ROLE \
    --resource $RESOURCE_TYPE:$RESOURCE_NAME \
    --pattern-type $PATTERN_TYPE \
    $CLUSTER_FLAG $CLUSTER_ID"
if [ -n "$EXTRA_FLAG" ]; then
    CMD="$CMD $EXTRA_FLAG"
fi

echo "Executing: $CMD"
eval "$CMD"
if [ $? -eq 0 ]; then
    echo "Role assignment successful!"
else
    echo "Error: Role assignment failed."
    exit 1
fi
