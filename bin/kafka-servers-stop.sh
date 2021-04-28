#!/bin/bash

KAFKA_HOME=/home/oracle/kafka_2.13-2.7.0
#KAFKA_HOME=/home/feib/kafka_2.13-2.7.0

APP_HOME=/home/oracle/gitrepository/transglobe-logminer-kafka
#APP_HOME=/home/feib/gitrepository/transglobe-logminer-kafka

cd ${KAFKA_HOME}

./bin/kafka-server-stop.sh

echo "Kafka server is stopping..., Please wait !!!"

sleep 10s

echo "Kafka server is fully stopped !!!"

echo "Zookeeper server is stopping......"
./bin/zookeeper-server-stop.sh
echo "Zookeeper server is fully stopped......"

### consumer
java -cp ${APP_HOME}/lib/esp.kafka.consumer.ods-1.0-jar-with-dependencies.jar com.transglobe.esp.kafka.consumer.ods.Application

