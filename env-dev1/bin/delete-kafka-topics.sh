#!/bin/bash

KAFKA_HOME=/home/steven/kafka_2.13-2.7.0

cd ${KAFKA_HOME}

./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ebao.cdc.test_t_policy_holder.0
./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ebao.cdc.test_t_insured_list.0
./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ebao.cdc.test_t_contract_bene.0
./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ebao.cdc.test_t_policy_holder_log.0
./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ebao.cdc.test_t_insured_list_log.0
./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ebao.cdc.test_t_contract_bene_log.0
./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ebao.cdc.test_t_address.0
./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ebao.cdc.t_streaming_etl_health_cdc.0