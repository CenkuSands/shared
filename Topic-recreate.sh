#!/bin/bash

# Set Kafka home directory and broker address
KAFKA_HOME="/path/to/kafka"
BROKER="localhost:9092"

# Number of partitions to use when recreating topics
PARTITIONS=3  # Adjust this number as needed

# File containing the list of topics to delete and recreate
TOPIC_LIST_FILE="topics_to_process.txt"

# Check if the topic list file exists
if [ ! -f "$TOPIC_LIST_FILE" ]; then
    echo "Topic list file '$TOPIC_LIST_FILE' not found!"
    exit 1
fi

# Iterate over each topic in the file and process it
while IFS= read -r topic; do
    if [ -n "$topic" ]; then
        echo "Processing topic: $topic"

        # Delete the topic
        $KAFKA_HOME/bin/kafka-topics.sh --delete --topic "$topic" --bootstrap-server "$BROKER"
        if [ $? -eq 0 ]; then
            echo "Successfully deleted topic: $topic"
        else
            echo "Failed to delete topic: $topic"
            continue
        fi

        # Recreate the topic with the specified number of partitions
        $KAFKA_HOME/bin/kafka-topics.sh --create --topic "$topic" --partitions "$PARTITIONS" --replication-factor 1 --bootstrap-server "$BROKER"
        if [ $? -eq 0 ]; then
            echo "Successfully recreated topic: $topic with $PARTITIONS partitions"
        else
            echo "Failed to recreate topic: $topic"
        fi
    else
        echo "Skipping empty line"
    fi
done < "$TOPIC_LIST_FILE"

echo "Topic processing completed."
