#!/bin/bash

APP_HOME=/home/kafka/partycontact/sit/streamingetl-pcr420669

java -cp "${APP_HOME}/lib/pcr420669-consumer-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-sit com.transglobe.streamingetl.pcr420669.consumer.ConsumerApp
