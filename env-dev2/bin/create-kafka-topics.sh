#!/bin/bash

KAFKA_HOME=/home/feib/kafka_2.13-2.7.0

cd ${KAFKA_HOME}

./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic etl.pmuser.test_t_address
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic etl.pmuser.test_t_policy_holder
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic etl.pmuser.test_t_insured_list
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic etl.pmuser.test_t_contract_bene
