#!/bin/bash

LOGMINER_HOME=/home/oracle/gitrepository/kafka-connect-oracle
COMMON_HOME=/home/oracle/gitrepository/transglobe/streamingetl-common
APP_HOME=/home/oracle/gitrepository/transglobe/streamingetl-pcr420669
STREAMINGETL_HOME=/home/oracle/gitrepository/transglobe/streamingetl
SPRINGBOOT_HOME=/home/oracle/gitrepository/transglobe/streamingetl-springboot

echo "start to build logminer"
cd ${LOGMINER_HOME}
mvn clean package
cp ${LOGMINER_HOME}/target/kafka-connect-oracle-1.0.jar ${STREAMINGETL_HOME}/connectors/oracle-logminer-connector/kafka-connect-oracle-1.0.jar

echo "start to build common"
cd ${COMMON_HOME}
mvn clean package
cp ${COMMON_HOME}/target/streamingetl-common-1.0.jar "${APP_HOME}/lib"

echo "start to build app"
cd ${APP_HOME}
mvn clean package
cp ${APP_HOME}/pcr420669-consumer/target/pcr420669-consumer-1.0.jar "${APP_HOME}/lib"
cp ${APP_HOME}/pcr420669-load/target/pcr420669-load-1.0.jar "${APP_HOME}/lib"
cp ${APP_HOME}/pcr420669-test/target/pcr420669-test-1.0.jar "${APP_HOME}/lib"

echo "start to build springboot"
cd ${SPRINGBOOT_HOME}
mvn clean package