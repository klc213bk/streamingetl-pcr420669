#!/bin/bash

KAFKA_HOME=/home/kafka/kafka_2.13-2.7.0
APP_HOME=/home/kafka/streamingetl

${KAFKA_HOME}/bin/connect-standalone.sh ${APP_HOME}/env-sit/config/logminer_connect-standalone.properties ${APP_HOME}/env-sit/config/OracleSourceConnector.properties
