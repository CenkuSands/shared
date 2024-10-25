#!/bin/bash

# Configuration
USER="cen.ku"                             # The current system username or a custom one you pass
ORG_URL="https://devops.venetianqa.local"    # Azure DevOps Server URL
COLLECTION="DevOpsCollection"           # Collection name (organization equivalent)
PAT="xxxxx"         # Personal Access Token (PAT) for authentication
API_VERSION="6.0"
DEVELOP_BRANCH="refs/heads/develop"      # The branch you are merging from (develop)
MAIN_BRANCH="refs/heads/main"            # The branch you are merging to (main)
REVIEWER_PAT="xxxxxxx"         # Personal Access Token (PAT) for the reviewer account (to approve the PR)
REVIEWER_ACCOUNT="126xxxx259fd7" # The username or email of the reviewer account

# Prompt the user for the project name
read -p "Enter the project name to merge PRs for: " PROJECT

# Validate project input
if [ -z "$PROJECT" ]; then
    echo "No project provided, exiting..."
    exit 1
fi

# Log the current user performing the operation
echo "Starting the merge operation as user: $USER on project: $PROJECT"

# Get the list of repositories for the specified project
REPOS=$(curl -s -u "$USER:$PAT" \
    "$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories?api-version=$API_VERSION" \
    | jq -r '.value[] | .name, .webUrl')

# Check if repositories were found for the project
if [ -z "$REPOS" ]; then
    echo "No repositories found for project: $PROJECT, exiting..."
    exit 1
fi



    # Extract Pull Request ID from the response

    PR_ID=$(curl -s -u "$USER:$PAT" $ORG_URL/$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories/$REPO_NAME/pullRequests?api-version=$API_VERSION" | jq -r .value[].pullrequestid)



    if [ "$PR_ID" == "null" ]; then
        echo "Failed to create a pull request for repository: $REPO_NAME"
        continue
    else
        echo "Pull Request $PR_ID created for repository: $REPO_NAME"
    fi

    # Retrieve the lastMergeSourceCommit ID from the Pull Request details
    echo "Retrieving the lastMergeSourceCommit for PR $PR_ID in repository: $REPO_NAME..."
    PR_DETAILS=$(curl -s -u "$USER:$PAT" \
      -X GET \
      "$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories/$REPO_NAME/pullRequests/$PR_ID?api-version=$API_VERSION")

    LAST_MERGE_SOURCE_COMMIT=$(echo "$PR_DETAILS" | jq -r '.lastMergeSourceCommit.commitId')

    if [ "$LAST_MERGE_SOURCE_COMMIT" == "null" ]; then
        echo "Failed to retrieve the lastMergeSourceCommit for PR $PR_ID in repository: $REPO_NAME"
        continue
    else
        echo "LastMergeSourceCommit: $LAST_MERGE_SOURCE_COMMIT retrieved for PR $PR_ID"
    fi

    # Complete (Merge) the Pull Request automatically
    echo "Merging the Pull Request $PR_ID for repository: $REPO_NAME..."
    MERGE_RESPONSE=$(curl -s -u "$USER:$PAT" \
      -X PATCH \
      -H "Content-Type: application/json" \
      -d @- "$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories/$REPO_NAME/pullRequests/$PR_ID?api-version=$API_VERSION" <<EOF
{
  "status": "completed",
  "lastMergeSourceCommit": {
    "commitId": "$LAST_MERGE_SOURCE_COMMIT"
  },
  "completionOptions": {
    "deleteSourceBranch": false,   # Set to true if you want to delete the develop branch after merging
    "mergeStrategy": "rebaseMerge"      # Options: "noFastForward", "rebase", "rebaseMerge", "squash"
  }
}
EOF
)

    # Check if the merge was successful
    if [ "$(echo "$MERGE_RESPONSE" | jq -r '.status')" == "completed" ]; then
        echo "Pull Request $PR_ID successfully merged for repository: $REPO_NAME"
    else
        echo "Failed to merge Pull Request $PR_ID for repository: $REPO_NAME"
    fi

done <<< "$REPOS"

echo "Pull Requests and merges completed for project: $PROJECT."
