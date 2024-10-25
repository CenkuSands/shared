#!/bin/bash

# Configuration
ORG_URL="https://dev.azure.com/{organization}"  # Azure DevOps organization URL
PAT="your_personal_access_token"                # Personal Access Token with permissions to update policies
API_VERSION="6.0"

# Prompt the user for the project name
read -p "Enter the Azure DevOps project name: " PROJECT

# Validate project input
if [ -z "$PROJECT" ]; then
    echo "No project provided, exiting..."
    exit 1
fi

# Function to update the creatorVoteCounts setting for Minimum Number of Reviewers policy
update_creator_vote_counts() {
    local repo_id=$1
    local policy_id=$2

    # Update policy to set creatorVoteCounts to false
    curl -s -u ":$PAT" \
      -X PATCH \
      -H "Content-Type: application/json" \
      -d @- "$ORG_URL/$PROJECT/_apis/policy/configurations/$policy_id?api-version=$API_VERSION" <<EOF
{
  "settings": {
    "creatorVoteCounts": false
  }
}
EOF
}

# Get list of repositories in the project
REPOS=$(curl -s -u ":$PAT" "$ORG_URL/$PROJECT/_apis/git/repositories?api-version=$API_VERSION" | jq -r '.value[] | .id')

# Check if any repositories were found
if [ -z "$REPOS" ]; then
    echo "No repositories found for project: $PROJECT"
    exit 1
fi

# Loop through each repository
for REPO_ID in $REPOS; do
    echo "Checking repository ID: $REPO_ID"

    # Get policies for the repository
    POLICIES=$(curl -s -u ":$PAT" \
      "$ORG_URL/$PROJECT/_apis/policy/configurations?repositoryId=$REPO_ID&api-version=$API_VERSION" | jq -c '.value[]')

    # Loop through each policy and check for Minimum Number of Reviewers
    echo "$POLICIES" | while read -r policy; do
        POLICY_ID=$(echo "$policy" | jq -r '.id')
        POLICY_TYPE=$(echo "$policy" | jq -r '.type.displayName')

        if [[ "$POLICY_TYPE" == "Minimum Number of Reviewers" ]]; then
            echo "Found Minimum Number of Reviewers policy with ID: $POLICY_ID for repository ID: $REPO_ID"

            # Check if creatorVoteCounts is currently true
            CREATOR_VOTE_COUNTS=$(echo "$policy" | jq -r '.settings.creatorVoteCounts')
            if [ "$CREATOR_VOTE_COUNTS" == "true" ]; then
                echo "Updating creatorVoteCounts to false for policy ID: $POLICY_ID"
                update_creator_vote_counts "$REPO_ID" "$POLICY_ID"
                echo "Updated creatorVoteCounts to false for policy ID: $POLICY_ID"
            else
                echo "creatorVoteCounts is already set to false for policy ID: $POLICY_ID"
            fi
        fi
    done
done

echo "Mass update completed for project: $PROJECT."
