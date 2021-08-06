#!/bin/bash

KAFKA_HOME=/home/steven/kafka_2.13-2.7.0

cd ${KAFKA_HOME}

./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.TGLMINER.TEST_T_POLICY_HOLDER
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.TGLMINER.TEST_T_POLICY_HOLDER_LOG
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.TGLMINER.TEST_T_INSURED_LIST
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.TGLMINER.TEST_T_INSURED_LIST_LOG
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.TGLMINER.TEST_T_CONTRACT_BENE
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.TGLMINER.TEST_T_CONTRACT_BENE_LOG
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.TGLMINER.TEST_T_ADDRESS
