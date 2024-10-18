!/usr/bin/env bash
cd /opt/confluent/confluent-7.1.1/bin
./kafka-topics --bootstrap-server 10.112.75.38:9093 --command-config /cp711/prod_installation/client.properties --list | xargs -I{} sh -c "echo -n '{} , ' && ./kafka-log-dirs --bootstrap-server 10.112.75.38:9093 --command-config /cp711/prod_installation/client.properties --topic-list {} --describe | grep '^{' | grep '^{' | jq '[.brokers[0].logDirs[0].partitions[].size | tonumber] | add'" | tee /root/kafka-topic-state/topics-by-size.$(date +%Y-%m-%d_%H%M).list

chmod 644 /root/kafka-topic-state/topics-by-size*.*

./kafka-topics --bootstrap-server 10.112.75.38:9093 --command-config /cp711/prod_installation/client.properties --list | xargs -I{} sh -c "echo -n '$(date +'%Y-%m-%d %H:%M:%S'), {} , ' && ./kafka-log-dirs --bootstrap-server 10.112.75.38:9093 --command-config /cp711/prod_installation/client.properties --topic-list {} --describe | grep '^{' | jq '[.brokers[0].logDirs[0].partitions[].size | tonumber] | add'" | tee /root/kafka-topic-state/topics-by-size.$(date +%Y-%m-%d_%H%M).list


#!/bin/bash

for file in Update-*; do
    # Remove the "Update-" prefix and store the new file name
    new_file="${file#Update-}"

    # Rename the file
    mv "$file" "$new_file"
done


input {
    file {
        path => "/mnt/kafka-topic-state/*.csv"  # Path to the mounted CSV files
        start_position => "beginning"
        sincedb_path => "/dev/null"
        codec => plain {
            charset => "UTF-8"
        }
    }
}

filter {
    csv {
        separator => ","
        columns => ["date", "topic", "size"]  # Define the columns as per your CSV file
    }
    mutate {
        convert => { "size" => "integer" }  # Convert 'size' field to integer
    }
}

output {
    elasticsearch {
        hosts => ["http://10.112.75.75:9200"]  # Elasticsearch server
        index => "kafka-topic-size-%{+YYYY.MM.dd}"
    }
    stdout { codec => rubydebug }  # Optional: Debug output to console
}
