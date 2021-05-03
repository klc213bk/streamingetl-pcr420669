#!/bin/bash

IGNITE_HOME=/home/oracle/apache-ignite-2.9.1-bin

APP_HOME=/home/oracle/gitrepository/transglobe/streamingetl-pcr420669/env-dev1

${IGNITE_HOME}/bin/ignite.sh ${APP_HOME}/ignite/config/pcr420669-ignite.xml

