#!/bin/bash

# Configuration
USER=${USER}                             # The current system username or a custom one you pass
ORG_URL="http://your_server:8080/tfs"    # Azure DevOps Server URL
COLLECTION="DefaultCollection"           # Collection name (organization equivalent)
PAT="your_personal_access_token"         # Personal Access Token (PAT) for authentication
API_VERSION="6.0"
DEVELOP_BRANCH="refs/heads/develop"      # The branch you are merging from (develop)
MAIN_BRANCH="refs/heads/main"            # The branch you are merging to (main)

# Prompt the user for the project name
read -p "Enter the project name to create and merge PRs for: " PROJECT

# Validate project input
if [ -z "$PROJECT" ]; then
    echo "No project provided, exiting..."
    exit 1
fi

# Log the current user performing the operation
echo "Starting the PR and merge operation as user: $USER on project: $PROJECT"

# Get the list of repositories for the specified project
REPOS=$(curl -s -u "$USER:$PAT" \
    "$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories?api-version=$API_VERSION" \
    | jq -r '.value[] | .name, .webUrl')

# Check if repositories were found for the project
if [ -z "$REPOS" ]; then
    echo "No repositories found for project: $PROJECT, exiting..."
    exit 1
fi

# Loop through each repository in the current project
while IFS= read -r REPO_NAME && IFS= read -r REPO_URL; do
    echo "Processing repository: $REPO_NAME in project: $PROJECT as user: $USER..."

    # Create a Pull Request from develop to main
    echo "Creating Pull Request from $DEVELOP_BRANCH to $MAIN_BRANCH for repository: $REPO_NAME..."
    PR_RESPONSE=$(curl -s -u "$USER:$PAT" \
      -X POST \
      -H "Content-Type: application/json" \
      -d @- "$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories/$REPO_NAME/pullRequests?api-version=$API_VERSION" <<EOF
{
  "sourceRefName": "$DEVELOP_BRANCH",
  "targetRefName": "$MAIN_BRANCH",
  "title": "Auto Merge from Develop to Main by $USER",
  "description": "This PR merges changes from the develop branch to the main branch",
  "reviewers": []
}
EOF
)

    # Extract Pull Request ID from the response
    PR_ID=$(echo "$PR_RESPONSE" | jq -r '.pullRequestId')

    if [ "$PR_ID" == "null" ]; then
        echo "Failed to create a pull request for repository: $REPO_NAME"
        continue
    else
        echo "Pull Request $PR_ID created for repository: $REPO_NAME"
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
    "commitId": "$(echo "$PR_RESPONSE" | jq -r '.lastMergeSourceCommit.commitId')"
  },
  "completionOptions": {
    "deleteSourceBranch": false,   # Set to true if you want to delete the develop branch after merging
    "mergeStrategy": "squash"      # Options: "noFastForward", "rebase", "rebaseMerge", "squash"
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
