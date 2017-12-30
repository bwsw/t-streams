FROM openjdk:8-jre-slim

LABEL maintainer='Bitworks Software info@bitworks.software'

EXPOSE 8071

ARG version

ENV CONFIG_FILE "/etc/conf/config.properties"
ENV SLF4J_VERSION "1.7.25"
ENV LOG4J_PROPERTIES_FILE ""

WORKDIR /opt/bin/tts

ADD "http://central.maven.org/maven2/org/slf4j/slf4j-log4j12/${SLF4J_VERSION}/slf4j-log4j12-${SLF4J_VERSION}.jar" "slf4j-log4j12.jar"
COPY "entrypoint.sh" .
COPY "target/scala-2.12/tstreams-transaction-server-${version}.jar" "tstreams-transaction-server.jar"

ENTRYPOINT ./entrypoint.sh