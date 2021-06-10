#!/bin/bash

LOGMINER_HOME=/home/steven/gitrepo/transglobe/kafka-connect-oracle
COMMON_HOME=/home/steven/gitrepo/transglobe/streamingetl-common
APP_HOME=/home/steven/gitrepo/transglobe/streamingetl-pcr420669
STREAMINGETL_HOME=/home/steven/gitrepo/transglobe/streamingetl

echo "start to build logminer"
cd ${LOGMINER_HOME}
mvn clean package
cp ${LOGMINER_HOME}/target/*.jar ${STREAMINGETL_HOME}/connectors/oracle-logminer-connector/

echo "start to build common"
cd ${COMMON_HOME}
mvn clean package
cp ${COMMON_HOME}/target/*.jar "${APP_HOME}/lib"

echo "start to build app"
cd ${APP_HOME}
mvn clean package
cp ${APP_HOME}/pcr420669-consumer/target/*.jar "${APP_HOME}/lib"
cp ${APP_HOME}/pcr420669-load/target/*.jar "${APP_HOME}/lib"
cp ${APP_HOME}/pcr420669-test/target/*.jar "${APP_HOME}/lib"
