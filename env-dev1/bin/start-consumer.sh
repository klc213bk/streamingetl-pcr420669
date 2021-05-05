#!/bin/bash

APP_HOME=/home/oracle/gitrepository/transglobe/streamingetl-pcr420669

java -cp ${APP_HOME}/lib/pcr420669-consumer-1.0.jar -Dprofile.active=env-dev1 com.transglobe.streamingetl.pcr420669.consumer.ConsumerApp
