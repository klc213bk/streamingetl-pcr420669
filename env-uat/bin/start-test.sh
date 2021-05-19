#!/bin/bash

APP_HOME=/home/kafka/streamingetl-pcr420669

java -cp ${APP_HOME}/lib/pcr420669-test-1.0.jar:${APP_HOME}/lib/jackson-core-2.9.1.jar com.transglobe.streamingetl.pcr420669.test.TestApp
STATUS=$?	
if [ ${STATUS} == 0 ]
then
 echo "Status=${STATUS}, load data [ OK ]"
else 
 echo "Status=${STATUS}, load data [ Fail ]"
fi