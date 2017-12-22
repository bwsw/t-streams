#!/usr/bin/env bash
# It expects 1 argument:
# $1 path to config file

# It expects 3 global arguments:
# TTS_VERSION    transaction server version
# SLF4J_VERSION  sfl4j version
# SCALA_VERSION  scala library version

EXTERNAL_CONFIG_PATH=$1

CONFIG='/tmp/config.properties'
PROJECT_PATH='/opt/bin/tts/'

cp ${EXTERNAL_CONFIG_PATH} ${CONFIG}

chmod u=rw ${CONFIG}


# Expects 2 arguments: property and value to set
#
# Returns: nothing
function change_property()
{
    local PROPERTY="$1="
    local NEW_VALUE=$2
    local RECORD=$(grep ${PROPERTY} ${CONFIG})
    sed -i s:${RECORD}:${PROPERTY}${NEW_VALUE}:g ${CONFIG}
}

change_property 'storage-model.file-prefix' '/storage'
change_property 'bootstrap.host' '127.0.0.1'
change_property 'bootstrap.port' 8080

exec java -Dconfig=${CONFIG} \
  -classpath ${PROJECT_PATH}tstreams-transaction-server_2.12-${TTS_VERSION}.jar:${PROJECT_PATH}slf4j-api-${SLF4J_VERSION}.jar:${PROJECT_PATH}slf4j-log4j12-${SLF4J_VERSION}.jar:${PROJECT_PATH}scala-library-${SCALA_VERSION}.jar \
  com.bwsw.tstreamstransactionserver.ServerLauncher
