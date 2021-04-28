#!/bin/bash

KAFKA_HOME=/home/oracle/kafka_2.13-2.7.0
APP_HOME=/home/oracle/gitrepository/transglobe/streamingetl-pcr420669

#KAFKA_HOME=/home/feib/kafka_2.13-2.7.0
#APP_HOME=/home/feib/gitrepository/transglobe/streamingetl-pcr420669

cd ${KAFKA_HOME}

gnome-terminal --title zookeeper --tab -e  "/bin/bash -c './bin/zookeeper-server-start.sh ./config/zookeeper.properties; bash'"

echo "Please Wait !!!"

sleep 3s

gnome-terminal --title kafka --tab -e  "/bin/bash -c './bin/kafka-server-start.sh ./config/server.properties; bash'"


