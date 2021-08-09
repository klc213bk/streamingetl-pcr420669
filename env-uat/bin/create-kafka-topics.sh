#!/bin/bash

KAFKA_HOME=/home/kafka/kafka_2.13-2.7.0

cd ${KAFKA_HOME}

./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOPRD1.LS_EBAO.T_POLICY_HOLDER
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOPRD1.LS_EBAO.T_POLICY_HOLDER_LOG
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOPRD1.LS_EBAO.T_INSURED_LIST
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOPRD1.LS_EBAO.T_INSURED_LIST_LOG
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOPRD1.LS_EBAO.T_CONTRACT_BENE
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOPRD1.LS_EBAO.T_CONTRACT_BENE_LOG
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOPRD1.LS_EBAO.T_ADDRESS