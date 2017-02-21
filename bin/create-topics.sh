#!/usr/bin/env bash


KAFKA_HOME=$1
ZK_HOST=$2
ZK_PORT=$3

topics="src-topic patterns rewards purchases stocks stocks-out transaction-summary twitterData english french spanish"

for topic in ${topics}; do
     echo "attempting to create topic ${topic}"
     ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${topic} --partitions 1 --replication-factor 1 --zookeeper ${ZK_HOST}:${ZK_PORT}
done
