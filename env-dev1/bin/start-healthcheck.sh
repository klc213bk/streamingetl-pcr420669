#!/bin/bash

APP_HOME=/home/steven/gitrepo/transglobe/streamingetl-pcr420669

java -cp "${APP_HOME}/lib/pcr420669-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev1 com.transglobe.streamingetl.pcr420669.load.HealthCheckApp
