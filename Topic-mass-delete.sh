#!/bin/bash

# Set Kafka home directory and broker address
KAFKA_HOME="/path/to/kafka"
BROKER="localhost:9092"

# File containing the list of topics to delete
TOPIC_LIST_FILE="topics_to_delete.txt"

# Check if the topic list file exists
if [ ! -f "$TOPIC_LIST_FILE" ]; then
    echo "Topic list file '$TOPIC_LIST_FILE' not found!"
    exit 1
fi

# Iterate over each topic in the file and delete it
while IFS= read -r topic; do
    if [ -n "$topic" ]; then
        echo "Deleting topic: $topic"
        $KAFKA_HOME/bin/kafka-topics.sh --delete --topic "$topic" --bootstrap-server "$BROKER"
        if [ $? -eq 0 ]; then
            echo "Successfully deleted topic: $topic"
        else
            echo "Failed to delete topic: $topic"
        fi
    fi
done < "$TOPIC_LIST_FILE"

echo "Mass delete operation completed."
