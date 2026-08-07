# order-service — multi-stage Docker build.
#
# Two stages: "build" compiles this ONE module using the FULL Maven
# reactor context (this module belongs to a multi-module project, and
# its avro-maven-plugin reads ../avro-schemas relative to its own
# pom.xml — both things only exist if the whole reactor is present, not
# just this module's own directory). "runtime" then copies out only the
# resulting jar into a much smaller image — none of the build stage's
# JDK/Maven/downloaded-dependency weight (600MB+) makes it into the
# final image, only the ~40MB jar crosses the stage boundary.
#
# Build from the REPO ROOT, not from this directory — this Dockerfile
# needs sibling modules' pom.xml files and ../avro-schemas as build
# context:
#   docker build -f order-service/Dockerfile -t orderflow/order-service .

# ---- Stage 1: build ---------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# Every module's pom.xml, copied BEFORE any source code. Docker caches
# each instruction as its own layer, keyed by the exact bytes copied —
# as long as no pom.xml changes, this layer (and the dependency
# download below, easily the slowest step) stays cached across rebuilds
# even when Java source changes constantly. All 8 must be present: the
# root pom.xml's <modules> list references every one of them by
# directory, and Maven needs to at least parse the whole reactor's
# module list before it can build any single one of them with -pl.
COPY pom.xml .
COPY order-service/pom.xml order-service/pom.xml
COPY inventory-service/pom.xml inventory-service/pom.xml
COPY payment-service/pom.xml payment-service/pom.xml
COPY shipment-service/pom.xml shipment-service/pom.xml
COPY fraud-detection-service/pom.xml fraud-detection-service/pom.xml
COPY analytics-service/pom.xml analytics-service/pom.xml
COPY order-saga-orchestrator/pom.xml order-saga-orchestrator/pom.xml
COPY search-indexer-service/pom.xml search-indexer-service/pom.xml

# -pl order-service: build ONLY this module. -am ("also make"): also
# build any module IT DEPENDS ON. A no-op here specifically (none of
# OrderFlow's 8 services depend on each other), but the right habit for
# any multi-module project where they do — leaving it off is the kind
# of thing that works today and breaks silently the day someone adds a
# real inter-module dependency.
RUN mvn -pl order-service -am dependency:go-offline -B

# NOW copy real source — the layer that actually changes on every code
# edit, deliberately kept separate from the pom.xml layer above.
COPY avro-schemas ./avro-schemas
COPY order-service/src order-service/src

# NOT -o (offline) here, even though the layer above already ran
# dependency:go-offline — found live, the hard way: go-offline does NOT
# reliably fetch every artifact a full `package` lifecycle needs (this
# build failed on missing jackson-dataformat-csv, several netty jars,
# reactor-core, byte-buddy, hamcrest — a mix of test-scope and
# plugin-transitive dependencies go-offline is well known to sometimes
# miss). Leaving this step able to reach the network is what makes the
# build actually succeed; the go-offline layer above still does its job
# for Docker's layer CACHE (skipped entirely on rebuilds where no
# pom.xml changed), it just isn't a hard completeness guarantee.
RUN mvn -pl order-service -am -DskipTests package -B

# ---- Stage 2: runtime ---------------------------------------------------
# A JRE, not a full JDK — nothing at runtime needs a Java COMPILER.
# Alpine Linux, not the build stage's Debian-based Maven image — a much
# smaller base OS. Together with only copying the final jar (not the
# whole /workspace), this keeps the shipped image small.
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/order-service/target/order-service.jar app.jar

# A non-root user — real security practice (a compromised process
# running as root inside a container is a meaningfully worse blast
# radius than one running as an unprivileged user), not just style.
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
