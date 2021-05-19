#!/bin/bash

APP_HOME=/home/feib/gitrepository/transglobe/streamingetl-pcr420669

cd ${APP_HOME}
mvn clean package
cp ${APP_HOME}/pcr420669-consumer/target/pcr420669-consumer-1.0.jar "${APP_HOME}/lib"
cp ${APP_HOME}/pcr420669-load/target/pcr420669-load-1.0.jar "${APP_HOME}/lib"
cp ${APP_HOME}/pcr420669-test/target/pcr420669-test-1.0.jar "${APP_HOME}/lib"
