#!/bin/bash

LOGMINER_HOME=/home/feib/gitrepository/kafka-connect-oracle
COMMON_HOME=/home/feib/gitrepository/transglobe/streamingetl-common
APP_HOME=/home/feib/gitrepository/transglobe/streamingetl-pcr420669
STREAMINGETL_HOME=/home/feib/gitrepository/transglobe/streamingetl

echo "start to build logminer"
cd ${LOGMINER_HOME}
mvn clean package
cp ${LOGMINER_HOME}/target/*.jar ${STREAMINGETL_HOME}/connectors/oracle-logminer-connector/

echo "start to build app"
cd ${APP_HOME}
mvn clean package
cp ${APP_HOME}/pcr420669-consumer/target/*.jar "${APP_HOME}/lib"
cp ${APP_HOME}/pcr420669-load/target/*.jar "${APP_HOME}/lib"
cp ${APP_HOME}/pcr420669-test/target/*.jar "${APP_HOME}/lib"
