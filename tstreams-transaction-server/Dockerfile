FROM ubuntu:xenial

LABEL maintainer='Bitworks Software info@bitworks.software'

EXPOSE 8071

ENV TTS_VERSION 1.5.0-SNAPSHOT
ENV SLF4J_VERSION 1.7.25
ENV SCALA_VERSION 2.12.4

ENV DEBIAN_FRONTEND noninteractive

# Install Oracle JDK 8
RUN echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee /etc/apt/sources.list.d/webupd8team-java.list && \
    echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list && \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886 && \
    apt-get update && \
    apt-get install -y --no-install-recommends apt-utils && \
    apt-get install -y --no-install-recommends wget && \
    echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | debconf-set-selections && \
    apt-get install -y --no-install-recommends oracle-java8-installer && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./rundocker.sh /opt/bin/tts/

WORKDIR /opt/bin/tts

RUN wget http://central.maven.org/maven2/org/scala-lang/scala-library/${SCALA_VERSION}/scala-library-${SCALA_VERSION}.jar && \
    wget http://central.maven.org/maven2/org/slf4j/slf4j-api/${SLF4J_VERSION}/slf4j-api-${SLF4J_VERSION}.jar && \
    wget http://central.maven.org/maven2/org/slf4j/slf4j-log4j12/${SLF4J_VERSION}/slf4j-log4j12-${SLF4J_VERSION}.jar && \
    wget --no-check-certificate https://oss.sonatype.org/content/repositories/snapshots/com/bwsw/tstreams-transaction-server_2.12/${TTS_VERSION}/tstreams-transaction-server_2.12-${TTS_VERSION}.jar

WORKDIR /var/log/tts
ENTRYPOINT ["/bin/bash", "/opt/bin/tts/rundocker.sh", "/etc/conf/config.properties"]

