#!/bin/bash

# Configuration
USER=${USER}                             # The current system username or a custom one you pass
ORG_URL="http://your_server:8080/tfs"    # Azure DevOps Server URL
COLLECTION="DefaultCollection"           # Collection name (organization equivalent)
PAT="your_personal_access_token"         # Personal Access Token (PAT) for authentication
API_VERSION="6.0"
YAML_FILE_PATH="path/to/your_file.yaml"  # Path to the YAML file inside each repo
COMMIT_MESSAGE="Updated APM_ELASTIC_STACK_TRACE in YAML by $USER"
BRANCH="refs/heads/master"               # Branch name where changes should be pushed
TMP_FILE="./your_file.yaml"              # Temporary local copy of the YAML file
TMP_REFS="./refs.json"                   # Temporary file to store refs information

# Prompt the user for the project name
read -p "Enter the project name you want to update: " PROJECT

# Validate project input
if [ -z "$PROJECT" ]; then
    echo "No project provided, exiting..."
    exit 1
fi

# Log the current user performing the operation
echo "Starting the operation as user: $USER on project: $PROJECT"

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

    # Fetch all refs for the repository
    echo "Fetching branch information for repository: $REPO_NAME..."
    curl -s -u "$USER:$PAT" \
      "$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories/$REPO_NAME/refs?api-version=$API_VERSION" \
      -o "$TMP_REFS"

    # Use jq to extract the objectId for the specific branch (e.g., master)
    OLD_OBJECT_ID=$(cat "$TMP_REFS" | jq -r --arg BRANCH "$BRANCH" '.value[] | select(.name == $BRANCH) | .objectId')

    # Check if the branch was found
    if [ -z "$OLD_OBJECT_ID" ]; then
        echo "Branch $BRANCH not found in repository: $REPO_NAME"
        continue
    fi

    # Download the YAML file from the repository
    FILE_DOWNLOAD_URL="$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories/$REPO_NAME/items?path=$YAML_FILE_PATH&api-version=$API_VERSION"
    echo "Downloading the YAML file from $REPO_NAME as user: $USER..."
    curl -u "$USER:$PAT" -s \
        "$FILE_DOWNLOAD_URL" \
        -o "$TMP_FILE"

    # Check if the file was downloaded successfully
    if [ ! -f "$TMP_FILE" ]; then
        echo "Failed to download the YAML file from repository: $REPO_NAME!"
        continue
    fi

    # Modify the YAML file (e.g., using sed to add the new key-value pair)
    echo "Modifying the YAML file..."
    sed -i '$a\  APM_ELASTIC_STACK_TRACE: "-1ms"' "$TMP_FILE"

    # Encode the file content in Base64 to push it back
    BASE64_CONTENT=$(base64 "$TMP_FILE")

    # Create a JSON payload for pushing the updated file
    cat <<EOF > payload.json
{
  "refUpdates": [
    {
      "name": "$BRANCH",
      "oldObjectId": "$OLD_OBJECT_ID"
    }
  ],
  "commits": [
    {
      "comment": "$COMMIT_MESSAGE",
      "changes": [
        {
          "changeType": "edit",
          "item": {
            "path": "$YAML_FILE_PATH"
          },
          "newContent": {
            "content": "$BASE64_CONTENT",
            "contentType": "base64encoded"
          }
        }
      ]
    }
  ]
}
EOF

    # Push the changes back to the repository
    echo "Pushing the modified file back to the repository: $REPO_NAME as user: $USER..."
    curl -u "$USER:$PAT" \
        -H "Content-Type: application/json" \
        -X POST \
        --data-binary @payload.json \
        "$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories/$REPO_NAME/pushes?api-version=$API_VERSION"

    # Clean up temporary files
    rm -f "$TMP_FILE" "$TMP_REFS" payload.json

done <<< "$REPOS"

echo "Operation completed by user: $USER on project: $PROJECT."
