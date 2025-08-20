#!/bin/bash

# Check if namespaces are provided
if [ $# -eq 0 ]; then
  echo "Usage: $0 <namespace1> [<namespace2> ...]"
  exit 1
fi

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

echo "Deployment Resource Configurations"
printf "%-20s %-30s %-20s %-20s %-20s %-20s\n" "NAMESPACE" "DEPLOYMENT" "CPU_REQUEST" "CPU_LIMIT" "MEMORY_REQUEST" "MEMORY_LIMIT"

# Process each namespace provided as an argument
for NAMESPACE in "$@"; do
  # Fetch all deployments in the current namespace
  DEPLOYMENTS=$(oc get deployments -n "$NAMESPACE" --no-headers -o custom-columns=":metadata.name" 2>/dev/null)

  if [ -z "$DEPLOYMENTS" ]; then
    echo "No deployments found in namespace '$NAMESPACE'."
    continue
  fi

  # Loop through each deployment in the namespace
  while IFS= read -r DEPLOYMENT; do
    # Fetch CPU and Memory requests/limits for the first container in the deployment
    CPU_REQUEST=$(oc get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].resources.requests.cpu}' 2>/dev/null || echo "Not set")
    CPU_LIMIT=$(oc get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].resources.limits.cpu}' 2>/dev/null || echo "Not set")
    MEMORY_REQUEST=$(oc get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].resources.requests.memory}' 2>/dev/null || echo "Not set")
    MEMORY_LIMIT=$(oc get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].resources.limits.memory}' 2>/dev/null || echo "Not set")

    # Print the results in a formatted table, including the namespace
    printf "%-20s %-30s %-20s %-20s %-20s %-20s\n" "$NAMESPACE" "$DEPLOYMENT" "$CPU_REQUEST" "$CPU_LIMIT" "$MEMORY_REQUEST" "$MEMORY_LIMIT"
  done <<< "$DEPLOYMENTS"
done

