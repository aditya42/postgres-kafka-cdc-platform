ARG DEBEZIUM_VERSION=3.7
ARG AIVEN_S3_CONNECTOR_VERSION=3.4.2
FROM eclipse-temurin:17-jdk AS s3-builder
ARG AIVEN_S3_CONNECTOR_VERSION
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git clone --depth 1 --branch v${AIVEN_S3_CONNECTOR_VERSION} \
    https://github.com/Aiven-Open/cloud-storage-connectors-for-apache-kafka.git
WORKDIR /src/cloud-storage-connectors-for-apache-kafka
RUN ./gradlew :s3-sink-connector:installDist --no-daemon
RUN mkdir -p /plugin && \
    find s3-sink-connector/build/install -type f -name '*.jar' -exec cp {} /plugin/ \;

FROM maven:3.9.11-eclipse-temurin-17 AS avro-builder
WORKDIR /build
COPY confluent-avro-pom.xml pom.xml
RUN mvn -q -DskipTests dependency:copy-dependencies -DoutputDirectory=/avro-libs

FROM quay.io/debezium/connect:${DEBEZIUM_VERSION}
USER root
RUN mkdir -p /kafka/connect/aiven-s3 /kafka/connect/confluent-avro
COPY --from=s3-builder /plugin/ /kafka/connect/aiven-s3/
COPY --from=avro-builder /avro-libs/ /kafka/connect/confluent-avro/
USER kafka
