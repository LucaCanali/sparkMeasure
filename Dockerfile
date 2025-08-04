ARG spark_image_tag=4.0.0-scala2.13-java17-python3-ubuntu
ARG scala_version

FROM sbtscala/scala-sbt:eclipse-temurin-alpine-17.0.15_6_1.11.3_2.13.16 AS builder
ARG scala_version

# Set the working directory
WORKDIR /app

# Copy the SBT configuration file
COPY build.sbt ./

# Copy the SBT project configuration directory
COPY project ./project

# Copy the application source code
COPY src ./src

# Compile the project with Scala
ENV SCALA_VERSION_LONG=${scala_version}.8
RUN sbt ++${SCALA_VERSION_LONG} package

# Build the spark image container sparkMeasure
FROM docker.io/library/spark:${spark_image_tag}
ARG scala_version

USER root

# Setup for the Prometheus JMX exporter.
# Add the Prometheus JMX exporter Java agent jar for exposing metrics sent to the JmxSink to Prometheus.
ENV JMX_EXPORTER_AGENT_VERSION=1.1.0
ADD https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_AGENT_VERSION}.jar /opt/spark/jars
RUN chmod 644 /opt/spark/jars/jmx_prometheus_javaagent-${JMX_EXPORTER_AGENT_VERSION}.jar

# Add the local sparkMeasure python code to the image
ADD python /opt/src/python

# Install the local sparkMeasure python package
RUN pip install /opt/src/python

# Add rootfs filesystem, it contains python scripts for runnning end-to-end tests
ADD e2e/rootfs/ /

# Copy the sparkMeasure compiled jar from the build stage
COPY --from=builder /app/target/scala-${scala_version}/*.jar /opt/spark/jars/

# Set the Spark user
ARG spark_uid=185
ENV spark_uid=${spark_uid}
USER ${spark_uid}

# Expose port 4040 for the Spark UI
EXPOSE 4040

# Set the default entrypoint
CMD ["/bin/bash"]
