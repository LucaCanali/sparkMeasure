FROM sbtscala/scala-sbt:eclipse-temurin-alpine-17.0.15_6_1.11.3_2.13.16 AS builder
FROM sbtscala/scala-sbt:eclipse-temurin-alpine-17.0.15_6_1.11.3_2.13.16 AS builder


# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers SBT
COPY build.sbt ./
COPY project ./project

# Copier le code source
COPY src ./src

# Compiler le projet avec Scala 2.13.8
# ENV SCALA_VERSION=2.13.8
ENV SCALA_VERSION=2.12.18

RUN sbt ++${SCALA_VERSION} package

# Copier uniquement le JAR compilé



FROM docker.io/library/spark:3.5.6-scala2.12-java17-python3-ubuntu

USER root

# Setup for the Prometheus JMX exporter.
# Add the Prometheus JMX exporter Java agent jar for exposing metrics sent to the JmxSink to Prometheus.
ENV JMX_EXPORTER_AGENT_VERSION=1.1.0
ADD https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_AGENT_VERSION}.jar /opt/spark/jars
RUN chmod 644 /opt/spark/jars/jmx_prometheus_javaagent-${JMX_EXPORTER_AGENT_VERSION}.jar

RUN apt-get update -y && apt-get install apt-transport-https curl gnupg -yqq && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
    chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg && \
    apt-get update && \
    apt-get install sbt -y && \
    rm -rf /var/lib/apt/lists/*

ADD python /opt/src/python

RUN pip install /opt/src/python

ADD e2e/rootfs/ /

RUN mkdir -p /opt/spark/jars
COPY --from=builder /app/target/scala-2.12/*.jar /opt/spark/jars

ARG spark_uid=185
ENV spark_uid=${spark_uid}
USER ${spark_uid}

ENV PYTHONPATH=/opt/spark/python

# Exposer le port Spark
EXPOSE 4040

# Définir le point d'entrée
CMD ["/bin/bash"]
