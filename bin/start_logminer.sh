#!/bin/bash

KAFKA_HOME=/home/oracle/kafka_2.13-2.7.0
APP_HOME=/home/oracle/gitrepository/transglobe/streamingetl-pcr420669

#KAFKA_HOME=/home/feib/kafka_2.13-2.7.0
#APP_HOME=/home/feib/gitrepository/transglobe/streamingetl-pcr420669

STATUS=-1
# execute initial load
if [ "$1" == "load" ]
then
  echo "with load"
  java -cp ${APP_HOME}/lib/ods-load-1.0-jar-with-dependencies.jar:${KAFKA_HOME}/libs/ojdbc8-12.2.0.1.jar com.transglobe.logminer.kafka.ods.InitialLoadApplication
  STATUS=$?	
  if [ ${STATUS} == 0 ]
  then
  	echo "Status=${STATUS}, load data [ OK ]"
  else 
  	echo "Status=${STATUS}, load data [ Fail ]"
  fi
else
  echo "without initial load"
  STATUS=0
fi

if [ ${STATUS} == 0 ] 
then
  echo "execute Kafka Connect program."
  ${KAFKA_HOME}/bin/connect-standalone.sh ${APP_HOME}/config/logminer_connect-standalone.properties ${APP_HOME}/config/OracleSourceConnector.properties
else
  echo "STOP!!! Something Wrong!!! status = ${STATUS}, Please check log for details." 
fi

