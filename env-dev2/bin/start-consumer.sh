#!/bin/bash

APP_HOME=/home/feib/gitrepository/transglobe/streamingetl-pcr420669

java -cp ${APP_HOME}/lib/pcr420669-consumer-1.0.jar -Dprofile.active=env-dev2 com.transglobe.streamingetl.pcr420669.consumer.ConsumerApp
