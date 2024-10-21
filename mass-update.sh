#!/bin/bash

# Configuration
ORG_URL="http://your_server:8080/tfs"    # Azure DevOps Server URL
COLLECTION="DefaultCollection"           # Collection name (organization equivalent)
PAT="your_personal_access_token"         # Personal Access Token
API_VERSION="6.0"
YAML_FILE_PATH="path/to/your_file.yaml"  # Path to the YAML file inside each repo
COMMIT_MESSAGE="Updated APM_ELASTIC_STACK_TRACE in YAML"
BRANCH="refs/heads/master"               # Branch name where changes should be pushed
TMP_FILE="./your_file.yaml"              # Temporary local copy of the YAML file

# Get the list of projects in the collection
echo "Fetching list of projects in collection..."
PROJECTS=$(curl -s -u ":$PAT" \
    "$ORG_URL/$COLLECTION/_apis/projects?api-version=$API_VERSION" \
    | jq -r '.value[] | .name')

# Loop through each project
for PROJECT in $PROJECTS; do
    echo "Processing project: $PROJECT"

    # Get the list of repositories for the current project
    REPOS=$(curl -s -u ":$PAT" \
        "$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories?api-version=$API_VERSION" \
        | jq -r '.value[] | .name, .webUrl')

    # Loop through each repository in the current project
    while IFS= read -r REPO_NAME && IFS= read -r REPO_URL; do
        echo "Processing repository: $REPO_NAME in project: $PROJECT"

        # Download the YAML file from the repository
        FILE_DOWNLOAD_URL="$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories/$REPO_NAME/items?path=$YAML_FILE_PATH&api-version=$API_VERSION"
        echo "Downloading the YAML file from $REPO_NAME..."
        curl -u ":$PAT" -s \
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

        # Get the current branch's oldObjectId (required for pushing)
        OLD_OBJECT_ID=$(curl -s -u ":$PAT" \
            "$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories/$REPO_NAME/refs?filter=$BRANCH&api-version=$API_VERSION" \
            | jq -r '.value[0].objectId')

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
        echo "Pushing the modified file back to the repository: $REPO_NAME..."
        curl -u ":$PAT" \
            -H "Content-Type: application/json" \
            -X POST \
            --data-binary @payload.json \
            "$ORG_URL/$COLLECTION/$PROJECT/_apis/git/repositories/$REPO_NAME/pushes?api-version=$API_VERSION"

        # Clean up temporary files
        rm -f "$TMP_FILE" payload.json

    done <<< "$REPOS"

done

echo "Operation completed."
