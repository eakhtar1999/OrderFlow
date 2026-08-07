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

---

## Phase 2: AWS CDK basics + the cost-safety net (`BudgetStack`)

### Why the very first thing deployed is a Budget, not any actual infrastructure

Nothing about a `docker build` costs money — every command in Phase 1
ran entirely on a laptop. Everything from this point on creates REAL
AWS resources with REAL, ongoing cost, against a hard, personal-money
budget. Before creating anything else, this phase deploys exactly one
thing: an automated early-warning system for that budget. If every
later phase in this project got skipped and ONLY this one thing shipped,
that would still be worth doing — it's the one piece of infrastructure
whose entire job is protecting against every OTHER mistake.

### Concept: what even is AWS CDK, and why not write raw CloudFormation or click through the console?

**CloudFormation** is AWS's native infrastructure-as-code format — you
write a JSON/YAML template describing every resource you want, AWS
creates/updates/deletes them to match. It's powerful but verbose: even
a simple resource often needs many lines of nested JSON, and there's no
programming-language tooling (loops, functions, type checking) — just a
declarative document.

**AWS CDK (Cloud Development Kit)** lets you write the SAME kind of
infrastructure description in a real programming language (TypeScript,
here) instead. You don't hand-write CloudFormation — you write CDK code
in TypeScript, and running `cdk synth` COMPILES it down into an actual
CloudFormation template (see `cloud/cdk/cdk.out/*.template.json` after
running it). CDK never talks to AWS directly to CREATE anything; it
only ever produces a CloudFormation template, then hands that template
to CloudFormation itself to actually execute. This matters for
understanding what "deploy" really does (see below) and for debugging —
when something goes wrong, the CloudFormation template CDK generated is
always available to inspect directly, in plain JSON, with no CDK-
specific knowledge required to read it.

Why not just click through the AWS Console instead? Because a
console click leaves no record anywhere except AWS's own audit log —
there's no file in this repo you could look at a year from now to know
EXACTLY what was created, and no reliable way to tear it all back down
except manually finding and deleting each thing by hand. Everything in
`cloud/cdk/` is the actual, complete, version-controlled specification
of every AWS resource this deployment creates — `cdk destroy` can
reverse ALL of it, precisely, because CDK/CloudFormation tracked
exactly what it created.

### CDK vocabulary, used precisely from here on

- **App**: the whole CDK program — one `cdk.App()` instance
  (`bin/cdk.ts`), which can contain multiple Stacks.
- **Stack**: a deployable unit — maps 1:1 to a single CloudFormation
  stack. `OrderFlowBudgetStack` is this project's first; later phases
  add more (`NetworkStack`, `EcsClusterStack`, etc.), each independently
  deployable and destroyable.
- **Construct**: any reusable building block INSIDE a stack — a stack
  itself is a construct, and everything you put inside one (like this
  phase's `budgets.CfnBudget`) is also a construct. Constructs come in
  three "levels":
  - **L1 ("Cfn...")**: a near-literal, auto-generated mapping of one
    CloudFormation resource type. `budgets.CfnBudget` (used in this
    phase) is L1 — AWS Budgets has no higher-level construct, so this
    is as good as it gets, and reading the underlying
    [`AWS::Budgets::Budget` CloudFormation docs](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-budgets-budget.html)
    directly is often unavoidable for these.
  - **L2**: a hand-written, higher-level wrapper around one or more L1s,
    with sane defaults and helper methods (e.g. `ec2.SecurityGroup`,
    coming in a later phase) — most CDK code you'll see in the wild is
    L2.
  - **L3 ("patterns")**: whole common ARCHITECTURES bundled as one
    construct (e.g. `ecs_patterns.ApplicationLoadBalancedFargateService`
    creates an ALB + target group + ECS service + task definition all
    at once) — a later phase for the app-tier services will likely lean
    on one of these.
- **Context**: per-deployment parameters, passed via `-c key=value` on
  the CLI or read in code via `this.node.tryGetContext('key')` (used in
  `BudgetStack` for the alert email). This is CDK's answer to "how do I
  parameterize a stack without hardcoding a value into source" —
  preferred over environment variables specifically because context
  values are RECORDED in `cdk.context.json` and the synthesized
  template's metadata, making a deployment's exact parameters
  reproducible/auditable later, which a plain env var read at synth
  time wouldn't be.

### The actual commands, and what each one does (and does NOT do)

```bash
npm run build      # tsc: TypeScript -> JavaScript. Catches type errors.
                    # Does NOT talk to AWS at all.

cdk synth           # Runs your CDK app, produces a CloudFormation
                    # template in cdk.out/. Still does NOT talk to AWS
                    # to CREATE anything — synth only needs enough AWS
                    # access to do CONTEXT LOOKUPS (e.g. "what AZs exist
                    # in this account/region"), not to change anything.

cdk diff             # Synthesizes, THEN compares the result against
                     # what's ACTUALLY deployed right now (queries
                     # CloudFormation for the current stack, if any).
                     # Read-only — shows you what WOULD change without
                     # changing anything. This is the step to run before
                     # every single `cdk deploy`, no exceptions, once
                     # real money is on the line.

cdk deploy           # Submits the synthesized template to
                     # CloudFormation as a "change set" (a preview of
                     # exact changes), then executes it — THIS is the
                     # one command in this list that actually creates/
                     # updates/deletes real AWS resources.
```

### Two real environment gotchas, found live, neither AWS-specific

**1. A corrupted npm cache from a stray `sudo npm install` at some
point in the past:**

```
npm error code EACCES
npm error Your cache folder contains root-owned files, due to a bug in
npm error previous versions of npm which has since been addressed.
npm error To permanently fix this problem, please run:
npm error   sudo chown -R 501:20 "/Users/eakhtar/.npm"
```

npm's own suggested fix needs `sudo` — a system-level privilege change,
not something to run without being asked to. The actual fix used here
sidesteps it entirely: point npm at a fresh, throwaway cache directory
for just this install (`npm install --cache /tmp/npm-cache-orderflow`),
never touching the corrupted global one at all. This is a real, general
technique worth knowing beyond this one project — `--cache <path>`
works on nearly every npm command, not just `install`.

**2. CDK CLI / library version skew:**

```
This CDK CLI is not compatible with the CDK library used by your application.
Cloud assembly schema version mismatch: Maximum schema version supported
is 53.x.x, but found 54.0.0. You need at least CLI version 2.1135.1 to
read this manifest.
```

`cdk init` pins the `aws-cdk` (CLI) devDependency to whatever version
was globally installed at scaffold time (2.1118.2 here) — but
`aws-cdk-lib` (the actual construct LIBRARY your code imports) was
declared with a caret range (`^2.248.0`), so `npm install` resolved it
to the newest compatible release (2.263.0), which needs a newer CLI
than what got pinned. **The CLI and the library are two SEPARATE npm
packages that must stay roughly in sync**, and nothing enforces that
automatically. Fixed by bumping the `aws-cdk` devDependency to match
(`2.1135.1`, the minimum the error message itself named) and
reinstalling.

This is also the real reason to prefer running the LOCAL,
project-pinned CDK CLI (`node_modules/.bin/cdk`, or `npx cdk` when it
behaves — see below) over a globally-installed `cdk` command: a global
install drifts out of sync with whatever `aws-cdk-lib` version any
GIVEN project's `package.json` happens to pin, silently, until you hit
exactly this error.

**3. An environment-specific quirk, worth naming even though it isn't
really a CDK problem**: `npx cdk <command>` produced completely silent,
empty output in this specific terminal/sandbox environment (exit code
0, but no visible stdout and no `cdk.out/` actually written) — while
`node_modules/.bin/cdk <command>` (the exact same locally-installed
binary `npx` would have found and run) worked correctly every time.
Likely an `npx`-specific process-spawning quirk in this particular
sandboxed shell, not a CDK issue — but the fix (call the local binary
directly) is simple, and worth trying first any time `npx <anything>`
produces suspiciously empty output with a "successful" exit code.

### Verified live

```bash
$ node_modules/.bin/cdk diff --no-color
Stack OrderFlowBudgetStack
Resources
[+] AWS::Budgets::Budget OrderFlowCloudBudget OrderFlowCloudBudget
✨  Number of stacks with differences: 1
```

```bash
$ node_modules/.bin/cdk deploy --no-color --require-approval never
OrderFlowBudgetStack | 3/3 | CREATE_COMPLETE | AWS::CloudFormation::Stack | OrderFlowBudgetStack
✅  OrderFlowBudgetStack
✨  Deployment time: 10.55s
```

And directly against the AWS Budgets API, not just trusting CDK's own
"success" message:

```bash
$ aws budgets describe-budget --account-id 383579119256 --budget-name orderflow-cloud-deployment
{
    "Budget": {
        "BudgetName": "orderflow-cloud-deployment",
        "BudgetLimit": { "Amount": "130.0", "Unit": "USD" },
        "TimeUnit": "MONTHLY",
        "CalculatedSpend": {
            "ActualSpend": { "Amount": "0.0", "Unit": "USD" },
            "ForecastedSpend": { "Amount": "0.125", "Unit": "USD" }
        },
        "HealthStatus": { "Status": "HEALTHY" }
    }
}

$ aws budgets describe-subscribers-for-notification --account-id 383579119256 \
    --budget-name orderflow-cloud-deployment \
    --notification '{"NotificationType":"ACTUAL","ComparisonOperator":"GREATER_THAN","Threshold":80.0,"ThresholdType":"PERCENTAGE"}'
{
    "Subscribers": [{ "SubscriptionType": "EMAIL", "Address": "eakhtar1999@gmail.com" }]
}
```

All 3 notification thresholds (`ACTUAL` 80%, `ACTUAL` 100%,
`FORECASTED` 100%) confirmed live and correctly subscribed — no
confirmation email needed at all, since AWS Budgets' native `EMAIL`
subscriber type (used here) skips SNS's usual subscription-confirmation
step entirely.

---

## 🎓 Concepts learned in Phase 2

1. **CDK is a code generator for CloudFormation, not a replacement
   execution engine.** `cdk synth` produces a plain CloudFormation
   template; `cdk deploy` hands that template to CloudFormation, which
   does the actual work. Understanding this makes "what does X command
   actually touch" a much easier question — `synth`/`diff` are
   read-only by construction (they can't be anything else, since they
   never invoke CloudFormation's create/update APIs), only `deploy`
   (and `destroy`) can.
2. **L1 vs. L2 vs. L3 constructs is a real spectrum, not a hierarchy
   you always climb.** Newer/less common AWS services often only HAVE
   an L1 mapping — reaching for `CfnXxx` and reading the underlying
   CloudFormation resource docs directly is completely normal CDK work,
   not a sign you're doing something wrong.
3. **CDK context values, not environment variables, are the idiomatic
   way to parameterize a stack per-deployment** — because context gets
   recorded alongside the synthesized template, making "what parameters
   was this deployed with" answerable later, which a value read
   silently from `process.env` at synth time isn't.
4. **The CLI and the construct library are separate, independently-
   versioned packages that must be kept roughly in sync** — a lesson
   that generalizes past CDK to plenty of other "CLI tool + library your
   code imports" pairings (this exact class of bug showed up earlier in
   this project too, with Testcontainers' CLI/library version pinning
   in Build Order Step 14).
5. **Deploy the cost-safety net FIRST, verify it independently of the
   tool that created it.** CDK reporting `✅ OrderFlowBudgetStack` is a
   claim; querying the AWS Budgets API directly for the actual budget
   amount, actual notification thresholds, and actual subscriber
   address is the verification — the same "don't just trust the tool,
   check the real state" discipline this whole project has followed
   since Build Order Step 1.

## 🧠 Interview Q&A

**Q: What's the actual relationship between AWS CDK and
CloudFormation?**
A: CDK is a code-generation layer on top of CloudFormation, not a
replacement for it. Writing CDK code and running `cdk synth` produces a
literal CloudFormation template (JSON/YAML); `cdk deploy` submits that
template to CloudFormation as a change set and executes it.
CloudFormation is what actually creates, tracks, updates, and can roll
back AWS resources — CDK never talks to individual AWS service APIs
directly to provision infrastructure.

**Q: What's the difference between an L1, L2, and L3 CDK construct?**
A: L1 ("Cfn" prefix) is an auto-generated, near-1:1 mapping of a single
CloudFormation resource type, with no added logic or sane defaults —
you set every property yourself, matching the CloudFormation docs
closely. L2 is a hand-authored wrapper around one or more L1s, adding
sensible defaults, validation, and convenience methods. L3 ("patterns")
bundles an entire common multi-resource architecture (e.g., a load-
balanced Fargate service — ALB + target group + service + task
definition) behind one construct. Not every AWS service has an L2 or
L3; plenty of real infrastructure code is written directly against L1s.

**Q: Why prefer CDK context values over environment variables for
parameterizing a stack?**
A: Context values passed via `-c key=value` (or read via
`this.node.tryGetContext(...)`) get recorded in the CDK app's own
`cdk.context.json` and are visible in the synthesized template's
metadata — so a deployment's exact parameters are auditable and
reproducible later. An environment variable read at synth time leaves
no such record; six months later, nobody (including future-you) can
tell what value was actually used just by looking at the repo.

**Q: `cdk diff` shows no changes but you know you edited a construct.
What would you check?**
A: First, whether the build step actually ran (`npm run build`, or
confirm your `cdk.json` app command runs TypeScript source directly via
`ts-node` rather than stale compiled JS) — a stale compiled artifact
sitting alongside fresh source is a classic silent-diff cause. Second,
whether `cdk.out/` itself is stale (delete it and re-synth fresh).
Third — and this is environment-specific, not a CDK concept — whether
the CLI invocation itself is actually running correctly at all; an
`npx`-wrapped command that exits 0 with suspiciously empty output,
with no error, is worth re-running via the local binary directly
(`node_modules/.bin/cdk`) before assuming the diff itself is wrong.

**Q: You deployed a CDK stack and it reported success. How do you
independently verify it actually did what you intended?**
A: Query the actual AWS service's own API directly, not just trust the
CDK/CloudFormation "success" message — a stack can report
`CREATE_COMPLETE` while still containing a subtly wrong property value
(wrong threshold, wrong email, wrong region) that only shows up if you
go look at the live resource. Here, that meant `aws budgets
describe-budget` and `aws budgets describe-subscribers-for-notification`
against the real, deployed budget, confirming the dollar amount, the
notification thresholds, and the subscriber address matched intent —
not just reading CDK's own deploy log.

**Q: Why does AWS Budgets' email notification not require a
subscription-confirmation step, unlike a typical SNS topic
subscription?**
A: AWS Budgets supports two distinct subscriber types:
`SubscriptionType: EMAIL` (Budgets sends the email itself, directly,
with no SNS topic involved at all) and `SubscriptionType: SNS` (routes
through an SNS topic you manage, which DOES require the usual
subscription-confirmation flow for email-protocol SNS subscriptions).
SNS-based subscriptions exist for programmatic use cases — triggering a
Lambda, forwarding to Slack/PagerDuty via a webhook — where you need
something other than a human reading an email. For a plain email
alert, the native `EMAIL` subscriber type is simpler and has one fewer
moving part.

---

## Phase 3: `NetworkStack` + `EcsClusterStack` — real, billable compute

### `NetworkStack`: reusing the default VPC, on purpose

`NetworkStack` does NOT create a new VPC. It calls
`ec2.Vpc.fromLookup(this, 'DefaultVpc', { isDefault: true })` — a real
AWS API call, made at `cdk synth`/`cdk diff` time (not deploy time),
that finds the account's EXISTING default VPC and reads back its
subnet IDs, AZs, and CIDR ranges. This is why `bin/cdk.ts` had to set
`env: { account, region }` explicitly back in Phase 2 — a lookup like
this is meaningless without knowing exactly which account/region to
look in.

The bigger decision this stack makes: every EC2 instance in this
project sits in a PUBLIC subnet, with NO NAT Gateway anywhere. The
"textbook" production pattern puts compute in PRIVATE subnets (no
direct route to the internet) behind a NAT Gateway for their outbound
access — but a NAT Gateway costs real, ongoing money (~$0.045/hr plus
per-GB data processing) for a benefit (network-layer isolation from
inbound internet traffic) this deployment doesn't actually need,
because **Security Groups already do that job** — a subnet being
"public" only means it has a route to an Internet Gateway; it says
NOTHING about which traffic is actually allowed to reach an instance
inside it. `NetworkStack`'s two Security Groups are what actually
enforces access control here: the ALB's group allows inbound HTTP from
anywhere (it's supposed to be the public entry point); the EC2
instances' group allows inbound ONLY from the ALB's group, plus a
self-referencing rule so instances in the SAME group (every Kafka
broker, every app service, every database) can reach each other. This
is a real, deliberate, budget-driven trade-off — not a security best
practice being recommended for a real production system, the same
honest framing this project already gives `xpack.security.enabled=false`
on local Elasticsearch.

### `EcsClusterStack`: two capacity pools, for one real architectural reason

The Kafka brokers need genuine failure isolation — killing one EC2
instance must only ever take down ONE broker, for the whole point of
this deployment (a real broker-failure/ISR/leader-election demo) to
mean anything. That requirement is fundamentally incompatible with
letting ECS bin-pack containers wherever it wants across an elastic
pool, which is exactly the right behavior for the OTHER 12 containers
(Postgres, Redis, Elasticsearch, Schema Registry, all 8 Spring Boot
services) that have no such isolation requirement. Two ECS "capacity
providers" — Kafka's own dedicated Auto Scaling Group (3 instances,
pinned, one broker per instance via a placement constraint applied
later when the actual Kafka services are created) and a second, shared
Auto Scaling Group for everything else — is the direct expression of
that one real requirement in infrastructure.

### A real bug, found live: this AWS account only permits Free-Tier-eligible instance types

The first deploy attempt used `t3.small` for Kafka's pool and
`t3.medium` for the app-tier pool. Kafka's 3 instances launched fine —
Watch:

```bash
$ aws autoscaling describe-auto-scaling-groups \
    --query "AutoScalingGroups[?contains(AutoScalingGroupName,'KafkaCapacityAsg')].{Desired:DesiredCapacity,Instances:length(Instances)}"
Desired: 3, Instances: 3   # healthy
```

but the app-tier pool never got a single instance, and the whole stack
sat in `CREATE_IN_PROGRESS` for 45+ minutes with NO new CloudFormation
events — genuinely stuck, not just slow. `describe-stack-events`
doesn't explain WHY an `AWS::AutoScaling::AutoScalingGroup` resource is
taking a long time (CloudFormation just waits for it to reach its
desired capacity, silently, up to its own internal timeout) — the
Auto Scaling Group's OWN activity log is what actually has the answer,
and it's a much more direct diagnostic tool for exactly this situation:

```bash
$ aws autoscaling describe-scaling-activities --auto-scaling-group-name <app-asg-name>
StatusCode: Failed
StatusMessage: The specified instance type is not eligible for Free Tier.
```

This AWS account has a Free-Tier-only guardrail on `ec2:RunInstances`
— almost certainly a Service Control Policy from whatever
Activate/Educate/promotional program granted the account's credits,
silently rejecting every `t3.medium` launch attempt (`t3.medium` isn't
in the free tier; `t3.small`, the type Kafka happened to use, IS —
pure coincidence that Kafka's pool worked and the app pool didn't).
The error message itself names the fix:

```bash
$ aws ec2 describe-instance-types --filters Name=free-tier-eligible,Values=true \
    --query 'InstanceTypes[].InstanceType'
c7i-flex.large  t3.micro  t4g.small  t4g.micro  t3.small  m7i-flex.large
```

`m7i-flex.large` (2 vCPU, 8GiB RAM) is the best fit of that set for
bin-packing 12 containers across 2 instances — 4x the RAM of `t3.small`,
the next-largest eligible option. CDK has a proper enum for it
(`ec2.InstanceClass.M7I_FLEX`), no raw string escape hatch needed.

**The real operational lesson, independent of this specific error**:
once a CloudFormation stack is `CREATE_IN_PROGRESS`, killing the LOCAL
`cdk deploy` process does nothing to the actual, ongoing AWS-side
operation — CloudFormation keeps retrying whatever it was doing
regardless of whether anything local is still watching. There's also
no direct "cancel a stuck CREATE" API the way `cancel-update-stack`
exists for updates — the only way out of a stuck CREATE is to let it
run to its own failure/timeout, at which point CloudFormation
automatically rolls back EVERYTHING the stack created so far (in this
case, terminating the 3 perfectly healthy Kafka instances too, since a
failed CREATE has no concept of "partial success" to preserve). The
practical response: fix the code immediately so the NEXT attempt
succeeds, rather than trying to intervene in the stuck one, then
redeploy fresh once the stack reaches a terminal (rolled-back) state.

### Verified live: the redeploy, once fixed

```bash
$ node_modules/.bin/cdk deploy OrderFlowEcsClusterStack --no-color --require-approval never
...
OrderFlowEcsClusterStack | 36/36 | CREATE_COMPLETE | AWS::CloudFormation::Stack | OrderFlowEcsClusterStack
✅  OrderFlowEcsClusterStack
✨  Deployment time: 212.77s
```

3.5 minutes with the correct instance type, versus 65 minutes to fail
with the wrong one — confirmed directly against both the ECS and EC2
APIs, not just CDK's own success message:

```bash
$ aws ecs describe-clusters --clusters orderflow-cluster \
    --query 'clusters[0].{status:status,registeredContainerInstances:registeredContainerInstancesCount}'
{ "status": "ACTIVE", "registeredContainerInstances": 5 }

$ aws ec2 describe-instances --filters Name=tag:aws:cloudformation:stack-name,Values=OrderFlowEcsClusterStack \
    Name=instance-state-name,Values=running --query 'Reservations[].Instances[].{Type:InstanceType,State:State.Name}'
t3.small        running   (x3, Kafka capacity)
m7i-flex.large  running   (x2, app-tier capacity)
```

All 5 EC2 instances running, all 5 registered as ECS container
instances, cluster `ACTIVE`.

### `cloud/scripts/up.sh` / `down.sh`: the actual cost lever for a fixed-budget window

Two scripts, deliberately asymmetric with what they touch:

- **`down.sh`** destroys ONLY `OrderFlowEcsClusterStack` — the 5 EC2
  instances, the only resources in this whole project that cost money
  so far. `OrderFlowBudgetStack` and `OrderFlowNetworkStack` are left
  running, because both cost $0 to exist, and leaving `NetworkStack` up
  means `up.sh` never has to recreate the VPC lookup/Security Groups.
- **`up.sh`** redeploys `NetworkStack` (fast no-op if nothing changed)
  then `EcsClusterStack` (recreates the 5 instances from scratch).

Both use `--force`/`--require-approval never` deliberately — the whole
point of these two scripts is a fast, unattended, repeatable up/down
cycle between work sessions, not a guarded one-off action. This is safe
specifically BECAUSE `EcsClusterStack` currently owns nothing stateful
worth protecting (no Kafka topic data, no database rows exist yet at
this phase) — `down.sh`'s own comment flags this explicitly as a
trade-off that stops being automatically safe once later phases add
real data to these instances' local disks.

Verified live, both directions, not just written and assumed to work:

```bash
$ ./cloud/scripts/down.sh
Tearing down OrderFlowEcsClusterStack (all 5 EC2 instances)...
... (34 resources, in dependency order: Lambda drain hooks -> capacity
    providers -> the 2 ASGs -> launch templates -> IAM roles -> the
    ECS cluster itself)
✅  OrderFlowEcsClusterStack: destroyed
# Confirmed separately: `aws ec2 describe-instances
# --filters Name=instance-state-name,Values=running` returned nothing.

$ ./cloud/scripts/up.sh
Bringing up OrderFlowNetworkStack + OrderFlowEcsClusterStack...
✅  OrderFlowEcsClusterStack
✨  Deployment time: 206.41s
```

### Mid-testing instance downsize, and a genuinely useful cancellation trick

While repeatedly cycling `down.sh`/`up.sh` for this exact verification,
it became clear the app-tier pool's `m7i-flex.large` (8GB RAM) is
overkill for a phase that's only validating the STACK MECHANICS (does
`up`/`down` actually work cleanly) rather than running the real
12-container app tier yet — swapped to `t3.small` (matching Kafka's
pool) for the remainder of this testing phase, to minimize cost during
what's likely to be many more up/down cycles before later phases
actually need the extra RAM.

The genuinely reusable technique from making that swap mid-deploy:
`up.sh` was already `CREATE_IN_PROGRESS` (past the point where killing
the LOCAL `cdk deploy` process does anything useful — see Phase 3's
earlier note on this) when the decision to downsize came in. Checking
progress first (`aws cloudformation describe-stack-events`) showed only
3 of 36 resources had been created — cheap ones (IAM roles, SNS topics),
no ASG, no EC2 instances yet. Rather than waiting for the whole
(expensive) creation to finish and then tearing it down again, a direct

```bash
aws cloudformation delete-stack --stack-name OrderFlowEcsClusterStack
```

against the STILL-`CREATE_IN_PROGRESS` stack cancelled it cleanly —
CloudFormation rolled back exactly the 3 resources that existed and
never proceeded to create anything costly. This is a real, useful
distinction from the earlier "can't cancel a stuck CREATE" finding: that
one was about a stack ALREADY STUCK waiting on a slow/failing resource
with no clean way out; THIS is proactively cancelling a healthy,
still-in-progress creation before it reaches the expensive part —
`delete-stack` accepts this immediately precisely because nothing
blocking has happened yet.

---

## 🎓 Concepts learned in Phase 3

1. **Public subnet + Security Groups can be a legitimate substitute for
   private subnet + NAT Gateway** when the thing you're protecting
   against is inbound access, not "does this instance have a route to
   the internet at all" — the two mechanisms answer genuinely different
   questions, and knowing which one you actually need saves real money.
2. **A resource-isolation requirement (Kafka's "one broker per
   instance") is a real architectural constraint that shapes
   infrastructure design directly** — it's the entire reason this
   stack has two capacity pools instead of one simpler shared one.
3. **`describe-stack-events` tells you WHAT CloudFormation is doing;
   it often doesn't tell you WHY something is stuck.** For a specific
   resource type stuck `IN_PROGRESS`, that resource's own service-level
   API (here, `autoscaling describe-scaling-activities`) is frequently
   the tool with the actual answer.
4. **A stuck `CREATE_IN_PROGRESS` CloudFormation operation cannot be
   safely cancelled locally** — the AWS-side state machine runs
   independently of whatever CLI process happened to kick it off.
   Fix the root cause, then let the current attempt fail/roll back on
   its own before retrying.
5. **AWS accounts under promotional/educational credit programs often
   carry invisible-until-you-hit-them guardrails** (here: a Free-Tier-
   only instance-type restriction) that a "standard" AWS account
   wouldn't have — worth checking `describe-instance-types
   --filters Name=free-tier-eligible,Values=true` EARLY in any project
   under this kind of account, not after a failed deploy.

## 🧠 Interview Q&A

**Q: Why put EC2 instances in a public subnet instead of the
"standard" private-subnet-plus-NAT-Gateway pattern?**
A: Because the actual threat being defended against (unwanted inbound
traffic) is a Security Group's job, not a subnet-routing job — a
public subnet only means "has a route to an Internet Gateway," it says
nothing about which traffic a Security Group actually permits to reach
an instance sitting in it. NAT Gateway's real value is giving PRIVATE
(no-inbound-route-at-all) instances outbound internet access without
exposing them to inbound connections — a genuinely different property,
worth its ongoing cost when you need true network-layer isolation
(e.g., compliance requirements, defense-in-depth for a real production
system), and reasonably skippable for a budget-constrained portfolio
deployment where Security Groups alone provide adequate access control.

**Q: When would you use two separate ECS capacity providers/Auto
Scaling Groups instead of one shared pool?**
A: When some subset of your workload has a placement requirement
ordinary bin-packing can't satisfy — most commonly, genuine per-
instance failure isolation (stateful, clustered systems like Kafka,
where you need to guarantee two replicas never land on the same
physical/virtual host) or a hardware requirement a subset of tasks
needs that the rest don't (GPU instances for one workload, standard
compute for everything else). Splitting capacity pools is the
mechanism; a real, specific reason to need isolation or specialized
hardware is the justification — doing it "just in case" adds
operational complexity (more instance types to manage, less efficient
overall utilization) without a concrete payoff.

**Q: A CloudFormation stack has been `CREATE_IN_PROGRESS` for far
longer than expected. What do you actually check, and can you cancel
it?**
A: Start with `describe-stack-events` to see the most recent resource
each event touched — but if the events go quiet with no new activity,
that resource's own service-specific API is usually the better next
step (an Auto Scaling Group's `describe-scaling-activities`, an ECS
service's `describe-services` events, etc.) — these often surface the
EXACT failure reason CloudFormation's own event stream doesn't. As for
cancelling: there's no direct "cancel a stuck CREATE" API — that only
exists for UPDATE operations (`cancel-update-stack`). A stuck CREATE
has to either succeed or fail/timeout on its own; CloudFormation then
automatically rolls back everything the stack created, with no partial-
success option. The practical move is fixing the root cause immediately
so the next attempt succeeds, then waiting for the current one to reach
a terminal state before retrying — not trying to force an in-flight
operation to stop.

**Q: Why did some instance types succeed and others fail with the same
error, in the same account, same region, same deploy?**
A: AWS's Free Tier eligibility is defined PER INSTANCE TYPE, not
account-wide — `t3.small` is free-tier-eligible, `t3.medium` is not, in
the exact same family. A Service-Control-Policy-style guardrail
restricting launches to free-tier-eligible types will therefore permit
some types and reject others within the same instance family,
depending purely on which specific type/size you request —
`describe-instance-types --filters Name=free-tier-eligible,Values=true`
against the target account/region is the authoritative way to know the
full eligible set ahead of time, rather than discovering it one failed
launch at a time.
