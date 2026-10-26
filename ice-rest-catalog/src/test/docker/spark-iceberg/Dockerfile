# Spark + Iceberg runtime used by DockerEncryptionIT (built by the test via testcontainers).
# ice-encryption and ice-test-support jars are copied into /opt/spark/jars at container start.
ARG SPARK_IMAGE=apache/spark:4.1.1-java21-scala
FROM ${SPARK_IMAGE}

ARG ICEBERG_VERSION=1.11.0
ARG ICEBERG_SPARK_RUNTIME=iceberg-spark-runtime-4.1_2.13

USER root
ADD https://repo1.maven.org/maven2/org/apache/iceberg/${ICEBERG_SPARK_RUNTIME}/${ICEBERG_VERSION}/${ICEBERG_SPARK_RUNTIME}-${ICEBERG_VERSION}.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/${ICEBERG_SPARK_RUNTIME}-${ICEBERG_VERSION}.jar
USER spark
