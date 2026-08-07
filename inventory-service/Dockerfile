# inventory-service — multi-stage Docker build. Same pattern as
# order-service/Dockerfile — see that file's comments for the full
# explanation of every decision here (multi-module reactor context,
# layer-cache ordering, why not -o on the package step, why a JRE +
# Alpine + non-root runtime stage).
#
# Build from the REPO ROOT:
#   docker build -f inventory-service/Dockerfile -t orderflow/inventory-service .

# ---- Stage 1: build ---------------------------------------------------
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

RUN mvn -pl inventory-service -am dependency:go-offline -B

COPY avro-schemas ./avro-schemas
COPY inventory-service/src inventory-service/src

RUN mvn -pl inventory-service -am -DskipTests package -B

# ---- Stage 2: runtime ---------------------------------------------------
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/inventory-service/target/inventory-service.jar app.jar

RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
