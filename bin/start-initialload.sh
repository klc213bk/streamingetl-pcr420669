#!/bin/bash

APP_HOME=/home/oracle/gitrepository/transglobe/streamingetl-pcr420669

#APP_HOME=/home/feib/gitrepository/transglobe/streamingetl-pcr420669


java -cp ${APP_HOME}/lib/pcr420669-load-1.0.jar:${APP_HOME}/lib/ojdbc8-12.2.0.1.jar com.transglobe.streamingetl.pcr420669.load.InitialLoadApp
STATUS=$?	
if [ ${STATUS} == 0 ]
then
 echo "Status=${STATUS}, load data [ OK ]"
else 
 echo "Status=${STATUS}, load data [ Fail ]"
fi



