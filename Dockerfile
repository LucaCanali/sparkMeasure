# Use a Scala SBT base image with Java 17 for building sparkMeasure jar files
FROM sbtscala/scala-sbt:eclipse-temurin-alpine-17.0.15_6_1.11.3_2.13.16 AS builder

# Set the working directory
WORKDIR /app

# Copy the SBT configuration file
COPY build.sbt ./
# Copy the SBT project configuration directory
COPY project ./project

# Copy the application source code
COPY src ./src

# Compile the project with Scala 2.12.18
ENV SCALA_VERSION=2.12.18
RUN sbt ++${SCALA_VERSION} package

# Use the official Spark image with Scala 2.12, Java 17, and Python 3 for runtime
FROM docker.io/library/spark:3.5.6-scala2.12-java17-python3-ubuntu

USER root

# Set up the Prometheus JMX exporter to expose metrics to Prometheus
ENV JMX_EXPORTER_AGENT_VERSION=1.1.0
ADD https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_AGENT_VERSION}.jar /opt/spark/jars
RUN chmod 644 /opt/spark/jars/jmx_prometheus_javaagent-${JMX_EXPORTER_AGENT_VERSION}.jar

# Add the local sparkMeasure python code to the image
ADD python /opt/src/python

# Install the local sparkMeasure python package
RUN pip install /opt/src/python

# Add rootfs filesystem, it contains python scripts for runnning end-to-end tests
ADD e2e/rootfs/ /

# Copy the compiled jar from the build stage
COPY --from=builder /app/target/scala-2.12/*.jar /opt/spark/jars/

# Set the Spark user
ARG spark_uid=185
ENV spark_uid=${spark_uid}
USER ${spark_uid}

# Expose port 4040 for the Spark UI
EXPOSE 4040

# Set the default entrypoint
CMD ["/bin/bash"]