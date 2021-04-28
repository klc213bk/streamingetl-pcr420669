#!/bin/bash

COMMON_HOME=/home/oracle/gitrepository/transglobe/streamingetl-common
APP_HOME=/home/oracle/gitrepository/transglobe/streamingetl-pcr420669
LOGMINER_HOME=/home/oracle/gitrepository/kafka-connect-oracle
#CONSUMER_HOME=/home/oracle/gitrepository/transglobe-esp-kafka-consumer

#COMMON_HOME=/home/oracle/gitrepository/transglobe/streamingetl-common
#APP_HOME=/home/feib/gitrepository/transglobe/streamingetl-pcr420669
#LOGMINER_HOME=/home/feib/gitrepository/kafka-connect-oracle
#CONSUMER_HOME=/home/feib/gitrepository/transglobe-esp-kafka-consumer

cd ${COMMON_HOME}
mvn clean package
cp ${COMMON_HOME}/target/streamingetl-common-1.0.jar "${APP_HOME}/lib"

cd ${APP_HOME}
mvn clean package

