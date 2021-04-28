#!/bin/bash

APP_HOME=/home/oracle/gitrepository/transglobe/streamingetl-pcr420669
LOGMINER_HOME=/home/oracle/gitrepository/kafka-connect-oracle
#CONSUMER_HOME=/home/oracle/gitrepository/transglobe-esp-kafka-consumer

#APP_HOME=/home/feib/gitrepository/transglobe/streamingetl-pcr420669
#LOGMINER_HOME=/home/feib/gitrepository/kafka-connect-oracle
#CONSUMER_HOME=/home/feib/gitrepository/transglobe-esp-kafka-consumer

cd ${APP_HOME}

cp ${LOGMINER_HOME}/target/kafka-connect-oracle-1.0.jar "${APP_HOME}/lib"
cp ${CONSUMER_HOME}/target/esp.kafka.consumer.ods-1.0-jar-with-dependencies.jar "${APP_HOME}/lib"
cp ${APP_HOME}/target/ods-load-1.0-jar-with-dependencies.jar "${APP_HOME}/lib"
