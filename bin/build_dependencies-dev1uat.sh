#!/bin/bash

COMMON_HOME=/home/oracle/gitrepository/transglobe/streamingetl-common
APP_HOME=/home/oracle/gitrepository/transglobe/streamingetl-pcr420669

cd ${COMMON_HOME}
mvn clean install
cp ${COMMON_HOME}/target/streamingetl-common-1.0.jar "${APP_HOME}/lib"

