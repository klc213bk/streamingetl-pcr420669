#!/bin/bash

APP_HOME=/home/feib/gitrepository/transglobe/streamingetl-pcr420669

./start-initialload.sh noload

java -cp "${APP_HOME}/lib/pcr420669-test-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev2 com.transglobe.streamingetl.pcr420669.test.TestApp

STATUS=$?	
if [ ${STATUS} == 0 ]
then
 echo "Status=${STATUS}, load data [ OK ]"
else 
 echo "Status=${STATUS}, load data [ Fail ]"
fi