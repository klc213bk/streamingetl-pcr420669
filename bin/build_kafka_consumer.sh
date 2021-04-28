#!/bin/bash

KAFKA_CONSUMER_HOME=/home/oracle/gitrepository/transglobe/streamingetl-pcr420669
#KAFKA_CONSUMER_HOME=/home/feib/gitrepository/transglobe/streamingetl-pcr420669

cd ${KAFKA_CONSUMER_HOME}

mvn clean package

