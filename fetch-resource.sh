#!/bin/bash

# Check if namespace is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <namespace>"
  exit 1
fi

NAMESPACE=$1

# Check if oc is installed
if ! command -v oc &> /dev/null; then
  echo "Error: 'oc' command not found. Please install the OpenShift CLI."
  exit 1
fi

# Check if user is logged into OpenShift
if ! oc whoami &> /dev/null; then
  echo "Error: Not logged into OpenShift. Please run 'oc login' first."
  exit 1
fi

# Fetch all deployments in the specified namespace
DEPLOYMENTS=$(oc get deployments -n "$NAMESPACE" --no-headers -o custom-columns=":metadata.name")

if [ -z "$DEPLOYMENTS" ]; then
  echo "No deployments found in namespace '$NAMESPACE'."
  exit 0
fi

echo "Deployment Resource Configurations in Namespace: $NAMESPACE"
echo "-------------------------------------------------------------"
printf "%-30s %-20s %-20s %-20s %-20s\n" "DEPLOYMENT" "CPU REQUEST" "CPU LIMIT" "MEMORY REQUEST" "MEMORY LIMIT"
echo "-------------------------------------------------------------"

# Loop through each deployment
while IFS= read -r DEPLOYMENT; do
  # Fetch CPU and Memory requests/limits for the first container in the deployment
  CPU_REQUEST=$(oc get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].resources.requests.cpu}' 2>/dev/null || echo "Not set")
  CPU_LIMIT=$(oc get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].resources.limits.cpu}' 2>/dev/null || echo "Not set")
  MEMORY_REQUEST=$(oc get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].resources.requests.memory}' 2>/dev/null || echo "Not set")
  MEMORY_LIMIT=$(oc get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].resources.limits.memory}' 2>/dev/null || echo "Not set")

  # Print the results in a formatted table
  printf "%-30s %-20s %-20s %-20s %-20s\n" "$DEPLOYMENT" "$CPU_REQUEST" "$CPU_LIMIT" "$MEMORY_REQUEST" "$MEMORY_LIMIT"
done <<< "$DEPLOYMENTS"

echo "-------------------------------------------------------------"
