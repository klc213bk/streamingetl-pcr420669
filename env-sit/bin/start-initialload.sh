#!/bin/bash

APP_HOME=/home/kafka/streamingetl-pcr420669
STREAMINGETL_HOME=/home/kafka/streamingetl

cd ${STREAMINGETL_HOME}/env-sit
./bin/start-streaming-register-ebao.sh

java -cp "${APP_HOME}/lib/pcr420669-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-sit com.transglobe.streamingetl.pcr420669.load.InitialLoadApp2 $1
STATUS=$?	
if [ ${STATUS} == 0 ]
then
 echo "Status=${STATUS}, load data [ OK ]"
else 
 echo "Status=${STATUS}, load data [ Fail ]"
fi