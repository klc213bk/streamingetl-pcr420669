#!/bin/bash

#KAFKA_HOME=/home/oracle/kafka_2.13-2.7.0
KAFKA_HOME=/home/feib/kafka_2.13-2.7.0

cd ${KAFKA_HOME}

./bin/kafka-server-stop.sh

echo "Kafka server is stopping..., Please wait !!!"

sleep 10s

echo "Kafka server is fully stopped !!!"

echo "Zookeeper server is stopping......"
./bin/zookeeper-server-stop.sh
echo "Zookeeper server is fully stopped......"

