#!/bin/bash

KAFKA_HOME=/home/steven/kafka_2.13-2.7.0
APP_HOME=/home/steven/gitrepo/transglobe/streamingetl-pcr420669

export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:${APP_HOME}/env-dev1/config/connect-log4j.properties"

${KAFKA_HOME}/bin/connect-standalone.sh ${APP_HOME}/env-dev1/config/logminer_connect-standalone.properties ${APP_HOME}/env-dev1/config/OracleSourceConnector.properties
