#!/bin/bash

#COMMON_HOME=/home/oracle/gitrepository/transglobe/streamingetl-common
#LOGMINER_HOME=/home/oracle/gitrepository/kafka-connect-oracle
#APP_HOME=/home/oracle/gitrepository/transglobe/streamingetl-pcr420669

COMMON_HOME=/home/feib/gitrepository/transglobe/streamingetl-common
LOGMINER_HOME=/home/feib/gitrepository/kafka-connect-oracle
APP_HOME=/home/feib/gitrepository/transglobe/streamingetl-pcr420669

cd ${COMMON_HOME}
mvn clean install
cp ${COMMON_HOME}/target/streamingetl-common-1.0.jar "${APP_HOME}/lib"

cd ${LOGMINER_HOME}
mvn clean install
cp ${LOGMINER_HOME}/target/kafka-connect-oracle-1.0.jar "${APP_HOME}/lib"
