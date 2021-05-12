#!/bin/bash

IGNITE_HOME=/home/kafka/apache-ignite-2.9.1-bin

APP_HOME=/home/kafka/streamingetl-pcr420669/env-uat

${IGNITE_HOME}/bin/ignite.sh ${APP_HOME}/ignite/config/pcr420669-ignite.xml

