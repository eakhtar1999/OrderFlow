# fraud-detection-service — multi-stage Docker build. Same pattern as
# order-service/Dockerfile — see that file's comments for the full
# explanation of every decision here.
#
# Build from the REPO ROOT:
#   docker build -f fraud-detection-service/Dockerfile -t orderflow/fraud-detection-service .

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

RUN mvn -pl fraud-detection-service -am dependency:go-offline -B

COPY avro-schemas ./avro-schemas
COPY fraud-detection-service/src fraud-detection-service/src

RUN mvn -pl fraud-detection-service -am -DskipTests package -B

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/fraud-detection-service/target/fraud-detection-service.jar app.jar

RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
