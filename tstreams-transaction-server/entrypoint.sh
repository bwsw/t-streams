#!/usr/bin/env bash

JVM_OPTS="-Dconfig=${CONFIG_FILE}"

if [[ -n ${LOG4J_PROPERTIES_FILE} ]]
then
    JVM_OPTS="${JVM_OPTS} -Dlog4j.configuration=file:${LOG4J_PROPERTIES_FILE}"
fi


java ${JVM_OPTS} -cp tstreams-transaction-server.jar:slf4j-log4j12.jar com.bwsw.tstreamstransactionserver.ServerLauncher