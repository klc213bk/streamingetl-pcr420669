#!/bin/bash

IGNITE_HOME=/home/feib/apache-ignite-2.9.1-bin

APP_HOME=/home/feib/gitrepository/transglobe/streamingetl-pcr420669

${IGNITE_HOME}/bin/ignite.sh ${APP_HOME}/env-dev2/ignite/config/pcr420669-ignite.xml
