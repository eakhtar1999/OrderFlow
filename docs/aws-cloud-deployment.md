# OrderFlow on AWS — Cloud Deployment Notes

A from-scratch, phase-by-phase build log for deploying OrderFlow to AWS
ECS — written the same way [`docs/git-github-workflow.md`](git-github-workflow.md)
was: real commands, real output, explained deeply enough that the
PATTERN transfers to a different project, not just this one. This is a
genuinely separate deliverable from the numbered Build Order in the root
README (that's the local Kafka/system-design tutorial; this is "take
that platform and actually run it on real cloud infrastructure,
enterprise-style"), so it isn't numbered as a Build Order step — but it
follows the exact same rule the rest of this project does: nothing gets
written here as a claim until it's been run for real and the output is
captured.

**The whole local `docker-compose.yml` + `mvn spring-boot:run` workflow
stays 100% untouched by everything in this document.** Every file this
work adds is new and additive — no existing file's behavior changes for
local development.

**Why this exists**: `docs/system-design.md`'s "known gaps" section
names two real, honest limitations of the local setup — Kafka only ever
ran as a single broker, so ISR/leader-election/broker-failure was never
demonstrated, only discussed; and no real throughput numbers were ever
captured across different partition counts. Both need MULTIPLE real
machines to demonstrate honestly. That's what this deployment is
actually for.

---

## Phase 1: Containerizing the 8 services (Dockerfiles)

### Why this has to happen before anything else

ECS schedules **containers**. Every other AWS decision — the cluster,
the networking, the service definitions — is downstream of "does a
Docker image of this service exist." Locally, every service runs via
`mvn spring-boot:run`, which needs a JDK, Maven, and this exact
checked-out source tree on the machine running it. A container image is
the opposite: a single, self-contained, portable artifact that runs
identically on your laptop, in CI, or on an EC2 instance in `us-east-1`
— with nothing installed on the HOST beyond a container runtime.

### The concept: multi-stage Docker builds

A naive Dockerfile for a Java app looks like this:

```dockerfile
FROM maven:3.9-eclipse-temurin-21
COPY . .
RUN mvn package
ENTRYPOINT ["java", "-jar", "target/app.jar"]
```

This WORKS, but the resulting image ships the entire Maven distribution,
the JDK's compiler, and every downloaded `.m2` dependency jar — none of
which the running application needs at 2am when it's just serving HTTP
requests. A **multi-stage build** splits this into two `FROM` blocks in
one Dockerfile:

- **Stage 1 ("build")**: a heavy image with Maven + a full JDK, used
  ONLY to compile and package the jar.
- **Stage 2 ("runtime")**: a much smaller image with just a JRE, which
  `COPY --from=build`s the ONE artifact (the jar) it actually needs out
  of stage 1. Everything else stage 1 downloaded or installed — gone,
  never part of the final image.

Docker's build engine is smart enough to discard stage 1 entirely once
the final image is exported — you get the small image's size, but the
convenience of building from source in one `docker build` command, no
separate CI step required.

### The real problem this project's structure creates: a multi-module Maven reactor

OrderFlow isn't 8 independent Maven projects — it's ONE multi-module
reactor (see the root `pom.xml`). `order-service/pom.xml` declares
`<parent><groupId>com.orderflow</groupId>...` with no `<relativePath>`
override, meaning Maven defaults to looking for the parent at
`../pom.xml` — one directory up from `order-service/`. And every
service's `avro-maven-plugin` execution reads schema files from
`../avro-schemas` relative to ITS OWN `pom.xml` (see any service's
`pom.xml` — the `<sourceDirectory>${project.basedir}/../avro-schemas</sourceDirectory>`
line). **Neither of those relative paths resolves correctly if you
build a Dockerfile using only `order-service/` as its context** — the
parent pom and the schemas directory simply wouldn't exist inside the
build.

The fix: build context = the REPO ROOT, not the service subdirectory.
Every Dockerfile in this project is invoked like this:

```bash
docker build -f order-service/Dockerfile -t orderflow/order-service:local .
```

Read that command carefully — `-f order-service/Dockerfile` says WHICH
file to use as the build recipe, but the trailing `.` says the build
CONTEXT (everything Docker is allowed to `COPY` from) is the current
directory (the repo root), not `order-service/`. The Dockerfile lives
inside `order-service/` because building this one service is that
service's own concern — same reasoning as `order-service/pom.xml`
living there and not at the root — but it's invoked from, and copies
from, the root.

### Layer-cache ordering: the actual reason `pom.xml` files get copied before source

Every `COPY` or `RUN` instruction in a Dockerfile becomes its own
cached layer, keyed by the exact bytes involved (for `COPY`, the copied
files' content; for `RUN`, the command string plus the state of the
layer before it). If NOTHING changed since the last build, Docker skips
re-running that instruction entirely and reuses the cached result.

`order-service/Dockerfile`'s build stage does this, in order:

```dockerfile
COPY pom.xml .
COPY order-service/pom.xml order-service/pom.xml
COPY inventory-service/pom.xml inventory-service/pom.xml
... (all 8 modules' pom.xml files)

RUN mvn -pl order-service -am dependency:go-offline -B

COPY avro-schemas ./avro-schemas
COPY order-service/src order-service/src

RUN mvn -pl order-service -am -DskipTests package -B
```

Why POMs first, source second? Because **editing Java source code is
the single most common thing that changes between builds, and pom.xml
files (declaring WHICH dependencies exist) change far less often.** If
source were copied before the dependency-download step, EVERY code
change would invalidate the pom-copy layer's cache key too (Docker
doesn't know the two are unrelated — it just sees "the files changed"),
forcing a full re-download of every Maven dependency on every single
rebuild. With POMs copied first, `dependency:go-offline` (a Maven goal
whose entire job is "download everything this reactor needs into the
local `.m2` cache, then stop") stays cached across every rebuild that
doesn't touch a `pom.xml` — the slowest part of the whole build
(network-bound dependency downloads) gets skipped entirely on the
common case of "I just changed one Java file."

**All 8 modules' `pom.xml` files get copied into EVERY service's
Dockerfile**, not just that one service's own — even though the 8
services never depend on each other. This is required, not incidental:
the ROOT `pom.xml`'s `<modules>` list names all 8 directories, and
Maven refuses to even parse the reactor if a listed module's `pom.xml`
is missing. `-pl order-service` (build only this module) plus `-am`
("also make" — also build whatever this module DEPENDS ON) doesn't
change that requirement; it only limits what actually gets COMPILED,
not what Maven needs to see in order to understand the reactor's shape
at all.

### A real bug, found live: `dependency:go-offline` doesn't guarantee an offline build actually works

The first attempt at `order-service/Dockerfile` used `-o` (offline mode)
on the final `package` step, assuming the earlier `go-offline` layer had
already downloaded everything needed. It hadn't:

```
[ERROR] dependency: com.fasterxml.jackson.dataformat:jackson-dataformat-csv:jar:2.17.2 (compile)
[ERROR] 	Cannot access confluent (...) in offline mode and the artifact ... has not been downloaded from it before.
[ERROR] dependency: io.netty:netty-common:jar:4.1.114.Final (compile)
[ERROR] dependency: net.bytebuddy:byte-buddy:jar:1.14.19 (test)
[ERROR] dependency: org.hamcrest:hamcrest-core:jar:2.2 (test)
... (several more)
```

This is a genuine, well-known Maven limitation, not a mistake in how it
was invoked: `dependency:go-offline` resolves the DECLARED dependency
graph, but misses some transitive dependencies pulled in by PLUGINS
(like `avro-maven-plugin` needing its own runtime deps) and some
test-scope transitives that only get resolved during the actual `test`
or `package` lifecycle phases, not during a standalone `go-offline`
goal invocation. There is no flag that makes `go-offline` 100% complete
— it's a best-effort optimization, not a guarantee.

**The fix**: drop `-o` from the final `package` command. The
`go-offline` layer STILL provides its caching benefit (Docker skips
re-running it whenever no `pom.xml` changed, regardless of whether it
was 100% complete), and the `package` step is now free to reach the
network for whatever the `go-offline` step missed — which, since most
of the dependency tree IS already cached in `.m2` from the previous
layer, ends up being a small, fast top-up rather than a full
re-download.

```
# Before the fix: BUILD FAILURE, offline mode, missing transitive deps
# After the fix:
[INFO] BUILD SUCCESS
[INFO] Total time:  45.958 s
```

### The runtime stage: JRE, Alpine, non-root

```dockerfile
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/order-service/target/order-service.jar app.jar

RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

Three deliberate choices, each with a real reason:

- **`eclipse-temurin:21-jre-alpine`, not `...-jdk-...`**: the runtime
  never compiles anything — a JRE (Java Runtime Environment) has
  everything needed to RUN a jar; a JDK (Java Development Kit) adds a
  compiler, debugger, and other build-time tools this container will
  never use. Shipping a JDK in a runtime image is pure unused weight.
- **`-alpine`**: Alpine Linux is a minimal Linux distribution built
  around `musl` libc instead of glibc, commonly a fraction of the size
  of a full Debian/Ubuntu base. The BUILD stage doesn't need this
  optimization (it's discarded entirely, size never matters there) —
  only the stage that actually ships.
- **A non-root user**: by default, a container's process runs as `root`
  INSIDE the container. If an attacker ever achieves code execution
  inside a compromised container, root privileges inside that container
  are a meaningfully larger blast radius (more of the container
  filesystem writable, more syscall surface available) than an
  unprivileged user's. `addgroup`/`adduser` then `USER spring:spring`
  costs three lines and closes that gap for free.

### The one real variation: `order-saga-orchestrator`

Seven of the eight Dockerfiles are byte-for-byte identical except for
the module name. `order-saga-orchestrator/Dockerfile` is the one
genuine exception — it has NO `COPY avro-schemas` line, because this is
the one service in the whole reactor with no Avro/Kafka dependency at
all (see its own `pom.xml` — no `avro-maven-plugin` execution, no
`kafka-avro-serializer`, no `spring-kafka`; it only talks to the other
three services over plain synchronous REST). Its Dockerfile doesn't
need what it doesn't use — a small thing, but worth noticing: a
copy-paste template is a starting point, not a rule to follow blindly
even when a specific case genuinely differs.

### Verified live: the containerized image actually works, against the real local infra

Building an image proves it COMPILES. It proves nothing about whether
it actually RUNS correctly. Two real, live checks:

**1. `order-service`, the most complex case (Postgres + Redis + Kafka +
the transactional outbox pattern)** — run on the SAME Docker network
`docker-compose.yml` already created (`ordernow_default`), configured
entirely through environment variables, zero code or config file
changes:

```bash
docker run -d --name order-service-container-test \
  --network ordernow_default \
  -p 8180:8080 \
  -e SPRING_DATASOURCE_URL="jdbc:postgresql://postgres:5432/orderflow" \
  -e SPRING_DATASOURCE_USERNAME="orderflow" \
  -e SPRING_DATASOURCE_PASSWORD="orderflow" \
  -e SPRING_DATA_REDIS_HOST="redis" \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS="kafka:29092" \
  -e SPRING_KAFKA_PRODUCER_PROPERTIES_SCHEMA_REGISTRY_URL="http://schema-registry:8085" \
  -e SPRING_KAFKA_CONSUMER_PROPERTIES_SCHEMA_REGISTRY_URL="http://schema-registry:8085" \
  orderflow/order-service:local
```

Then a real order, placed against the CONTAINER, not the usual
`mvn spring-boot:run` process:

```bash
curl -s -X POST localhost:8180/api/orders -H "Content-Type: application/json" \
  -d '{"customerId":"cust-docker-test","region":"us-east","items":[{"productId":"sku-42","quantity":2}]}'
# -> {"orderId":"88cebd27-3ab9-4a96-b88a-2daa2d9614e5","status":"ACCEPTED"}
```

And the real proof it wasn't just accepting HTTP requests but actually
completing the outbox pattern against the real Postgres and Kafka —
straight from the container's own logs:

```
📤 Relayed outbox row id=59 -> order-created + order-status for orderId=88cebd27-3ab9-4a96-b88a-2daa2d9614e5
```

**2. `payment-service`, the simplest case (Kafka only, no database)** —
confirms the pattern generalizes to a service with a completely
different dependency shape, not just a lucky result for one service:

```bash
docker run -d --name payment-service-container-test \
  --network ordernow_default -p 8187:8087 \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS="kafka:29092" \
  -e SPRING_KAFKA_PRODUCER_PROPERTIES_SCHEMA_REGISTRY_URL="http://schema-registry:8085" \
  -e SPRING_KAFKA_CONSUMER_PROPERTIES_SCHEMA_REGISTRY_URL="http://schema-registry:8085" \
  orderflow/payment-service:local
```

```
Started PaymentServiceApplication in 1.702 seconds
... Successfully joined group with generation Generation{generationId=3, ...}
... partitions assigned: [inventory-reserved-0, inventory-reserved-1]
```

**Why environment variables were enough, with zero Java code changes**:
every address these services need — `spring.datasource.url`,
`spring.data.redis.host`, `spring.kafka.bootstrap-servers`,
`spring.elasticsearch.uris`, `schema.registry.url`, and
`order-saga-orchestrator`'s `services.inventory-service.base-url` /
`payment-service.base-url` / `shipment-service.base-url` — was already
an externalized Spring Boot `application.yml` property with a
`localhost` default, from the very first Build Order step that
introduced it. Spring Boot's relaxed environment-variable binding
(uppercase the property path, replace every `.` and `-` with `_`) means
any of these can be overridden at deploy time without touching a single
line of source. This is the exact property this whole cloud deployment
leans on: point these at AWS-internal DNS names later, and the
identical jars — the SAME artifact, unmodified — run correctly there
too.

### Final image sizes

| Service | Size | Notes |
|---|---|---|
| `order-service` | 409MB | Postgres + Redis + Kafka + Avro |
| `inventory-service` | 406MB | Postgres + Redis + Kafka + Avro |
| `payment-service` | 382MB | Kafka + Avro only |
| `shipment-service` | 382MB | Kafka + Avro only |
| `fraud-detection-service` | 502MB | Kafka Streams (heaviest client library) |
| `analytics-service` | 502MB | Kafka Streams |
| `order-saga-orchestrator` | 343MB | No Kafka/Avro at all — smallest |
| `search-indexer-service` | 414MB | Kafka + Avro + Elasticsearch client |

The pattern holds exactly as expected: services carrying the Kafka
Streams library are the heaviest; the one service with no Kafka/Avro
dependency at all is the lightest.

---

## 🎓 Concepts learned in this phase

1. **Multi-stage builds decouple "what it takes to BUILD" from "what it
   takes to RUN."** The build stage's weight (a full JDK, Maven, every
   downloaded dependency jar) never has to be paid at runtime — only
   the final artifact crosses the stage boundary.
2. **Docker build context ≠ where the Dockerfile lives.** `-f
   order-service/Dockerfile .` says: use this file as the recipe, but
   everything it's allowed to `COPY` comes from `.` (wherever the
   command was RUN from). A multi-module project's Dockerfiles almost
   always need a build context wider than the one module's own
   directory.
3. **Layer cache ordering is a real design decision, not an
   afterthought.** Put the least-frequently-changing inputs (dependency
   manifests) in early layers, the most-frequently-changing inputs
   (source code) in late layers — every rebuild that doesn't touch an
   early layer skips re-running it entirely.
4. **`dependency:go-offline` is a best-effort optimization, not a
   completeness guarantee.** Relying on `-o` (true offline mode)
   immediately afterward is a real, reproducible way to break a build
   on plugin-transitive or test-scope dependencies it missed.
5. **A JRE-Alpine-non-root runtime image is the default responsible
   choice for a Spring Boot service**, not a micro-optimization to skip
   under time pressure — smaller attack surface, smaller image, faster
   pulls, for a few lines of Dockerfile.

## 🧠 Interview Q&A

**Q: Why use a multi-stage Docker build instead of just installing
Maven in your final image?**
A: Size and attack surface. A JDK/Maven/full-dependency-cache image can
easily be 3-4x the size of a JRE-only runtime image, and every extra
tool present in a running container is one more thing that could be
abused if the container is ever compromised. Multi-stage builds let you
use a heavy, convenient toolchain to BUILD the artifact, then discard
everything except the artifact itself.

**Q: You have a multi-module Maven project. How do you Dockerize one
module without dragging in the whole monorepo's build?**
A: You can't fully avoid the reactor's structure — Maven needs to at
least SEE every module's `pom.xml` to understand the module graph (the
parent's `<modules>` list references them), even if you only actually
COMPILE one via `-pl <module> -am`. The practical answer: build from
the repo root as Docker context, copy every module's `pom.xml` (cheap,
small files) before copying any actual source, then target the one
module you want with `-pl`.

**Q: What's the difference between `-pl` and `-am` in a Maven command,
and when do you need `-am`?**
A: `-pl <module>` restricts the build to just that module (or a
comma-separated list). `-am` ("also make") additionally builds any
module that the targeted one DEPENDS ON — without it, if your module
needs a sibling module's jar and that sibling hasn't been built/
installed yet, the build fails with a missing-artifact error. In
OrderFlow specifically, none of the 8 services depend on each other, so
`-am` is a no-op here — but it's the right habit for any multi-module
project where inter-module dependencies DO exist, since a build that
"happens to work" without it will break the exact day someone adds a
real dependency between two modules.

**Q: `dependency:go-offline` failed to make an offline build work. Is
that a bug?**
A: No — it's documented, known behavior, just widely misunderstood.
`go-offline`'s job is to warm the local `.m2` cache for the DECLARED
dependency graph; it does not guarantee it discovers every
plugin-transitive or lifecycle-phase-specific dependency a full `mvn
package` run might need. Treat it as "makes most rebuilds fast," not
"guarantees zero network access."

**Q: Why run a container as a non-root user?**
A: Defense in depth. Container isolation (namespaces, cgroups) is real
but not airtight — container escape vulnerabilities do exist. If a
process is compromised while running as root INSIDE the container,
an attacker has more privileges to work with (more of the filesystem
writable, more capabilities available) than if that same process were
running as an unprivileged user. It costs nothing to avoid and is a
standard checklist item in any container security review.

**Q: How did you verify these images actually work, not just that they
built successfully?**
A: A successful `docker build` only proves the code compiled. Real
verification meant running the container against real, already-running
infrastructure (the same Postgres/Redis/Kafka/Schema Registry Docker
Compose already stood up locally), on the same Docker network, and
checking for real side effects — an HTTP 202 response, a Kafka consumer
group actually joining and getting partitions assigned, and (for
order-service specifically) a log line proving the transactional
outbox relay genuinely published to the real broker. Two different
services were checked, deliberately chosen because they have different
dependency shapes (one needs Postgres+Redis+Kafka, the other needs only
Kafka), to make sure the environment-variable-override pattern
generalizes rather than happening to work for one lucky case.
