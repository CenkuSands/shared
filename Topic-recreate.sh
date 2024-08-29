#!/bin/bash

# Set Kafka home directory and broker address
KAFKA_HOME="/path/to/kafka"
BROKER="localhost:9092"

# File containing the list of topics to delete and recreate
TOPIC_LIST_FILE="topics_to_process.txt"

# Check if the topic list file exists
if [ ! -f "$TOPIC_LIST_FILE" ]; then
    echo "Topic list file '$TOPIC_LIST_FILE' not found!"
    exit 1
fi

# Iterate over each line in the file and process the topics
while IFS= read -r line; do
    # Extract topic name and partitions from the line
    topic=$(echo $line | awk '{print $1}')
    partitions=$(echo $line | awk '{print $2}')

    if [ -n "$topic" ] && [ -n "$partitions" ]; then
        echo "Processing topic: $topic with $partitions partitions"

        # Delete the topic
        $KAFKA_HOME/bin/kafka-topics.sh --delete --topic "$topic" --bootstrap-server "$BROKER"
        if [ $? -eq 0 ]; then
            echo "Successfully deleted topic: $topic"
        else
            echo "Failed to delete topic: $topic"
            continue
        fi

        # Recreate the topic with the specified number of partitions
        $KAFKA_HOME/bin/kafka-topics.sh --create --topic "$topic" --partitions "$partitions" --replication-factor 1 --bootstrap-server "$BROKER"
        if [ $? -eq 0 ]; then
            echo "Successfully recreated topic: $topic with $partitions partitions"
        else
            echo "Failed to recreate topic: $topic"
        fi
    else
        echo "Invalid line in file: $line"
    fi
done < "$TOPIC_LIST_FILE"

echo "Topic processing completed."
