# order-saga-orchestrator — multi-stage Docker build. Same pattern as
# order-service/Dockerfile, with ONE real variation: no `COPY avro-schemas`
# and no dependency on it — this is the one service in the reactor with
# no Avro/Kafka dependency at all (no avro-maven-plugin execution, see
# this module's own pom.xml), so there's nothing for that directory to
# feed here. Every other service's Dockerfile needs it because their
# avro-maven-plugin reads ../avro-schemas relative to their own pom.xml
# at build time; this one simply never runs that plugin.
#
# Build from the REPO ROOT:
#   docker build -f order-saga-orchestrator/Dockerfile -t orderflow/order-saga-orchestrator .

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml .
COPY order-service/pom.xml order-service/pom.xml
COPY inventory-service/pom.xml inventory-service/pom.xml
COPY payment-service/pom.xml payment-service/pom.xml
COPY shipment-service/pom.xml shipment-service/pom.xml
COPY fraud-detection-service/pom.xml fraud-detection-service/pom.xml
COPY analytics-service/pom.xml analytics-service/pom.xml
COPY order-saga-orchestrator/pom.xml order-saga-orchestrator/pom.xml
COPY search-indexer-service/pom.xml search-indexer-service/pom.xml

RUN mvn -pl order-saga-orchestrator -am dependency:go-offline -B

COPY order-saga-orchestrator/src order-saga-orchestrator/src

RUN mvn -pl order-saga-orchestrator -am -DskipTests package -B

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/order-saga-orchestrator/target/order-saga-orchestrator.jar app.jar

RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
