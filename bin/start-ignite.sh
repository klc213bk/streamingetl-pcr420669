#!/bin/bash

IGNITE_HOME=/home/oracle/apache-ignite-2.9.1-bin
#IGNITE_HOME=/home/feib/apache-ignite-2.9.1-bin

cd ${IGNITE_HOME}

./bin/ignite.sh ./examples/config/example-ignite.xml

