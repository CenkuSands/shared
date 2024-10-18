!/usr/bin/env bash
cd /opt/confluent/confluent-7.1.1/bin
./kafka-topics --bootstrap-server 10.112.75.38:9093 --command-config /cp711/prod_installation/client.properties --list | xargs -I{} sh -c "echo -n '{} , ' && ./kafka-log-dirs --bootstrap-server 10.112.75.38:9093 --command-config /cp711/prod_installation/client.properties --topic-list {} --describe | grep '^{' | grep '^{' | jq '[.brokers[0].logDirs[0].partitions[].size | tonumber] | add'" | tee /root/kafka-topic-state/topics-by-size.$(date +%Y-%m-%d_%H%M).list

chmod 644 /root/kafka-topic-state/topics-by-size*.*
