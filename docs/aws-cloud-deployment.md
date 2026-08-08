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

---

## Phase 4: `EcrStack` — pushing images to AWS

### Why this stack needs nothing from the other three

`EcrStack` doesn't reference the VPC, the Security Groups, or the ECS
cluster — an ECR (Elastic Container Registry) repository is just
storage for container images, with no networking or compute of its own.
It's the one stack so far with zero cross-stack dependencies, which
`bin/cdk.ts` reflects directly: no props are passed in from another
stack's outputs the way `EcsClusterStack` needed `NetworkStack`'s `vpc`
and `ecsInstanceSecurityGroup`.

One property matters more here than anywhere else so far:
`emptyOnDelete: true`. ECR refuses to delete a repository that still
contains images by default — a sensible guard for a registry you
depend on, but exactly the kind of thing that would make `cdk destroy`
FAIL partway through given this project's whole cost-conscious,
tear-down-between-sessions design. Same reasoning Build Order Step 16
already applied to a different AWS-adjacent problem
(`autoDeleteImages` on an Elasticsearch/S3-style resource), now applied
to ECR specifically.

### A real bug, found live, that would have been much more expensive to discover on AWS: architecture mismatch

`docker image inspect orderflow/order-service:local --format '{{.Architecture}}'`
came back `arm64`. Every one of Phase 1's 8 images does — this project's
development machine is Apple Silicon, and `docker build` matches the
HOST architecture by default unless told otherwise. The EC2 instances
Phase 3 deployed (`t3.small`, `m7i-flex.large`) are `x86_64`. Had this
gone unnoticed, `docker-push`-ing these images and pointing ECS at them
would have produced a real, live failure mode on AWS: ECS placing tasks
successfully, then every container immediately crash-looping with an
`exec format error` — the kind of failure that's genuinely confusing to
debug the FIRST time you hit it, because everything about the
deployment (task definition, networking, IAM) can be completely
correct, and it still won't run.

The first fix attempted was rebuilding for the "more standard" x86_64
target, via Docker's cross-platform build support:

```bash
docker buildx build --platform linux/amd64 -f order-service/Dockerfile -t orderflow/order-service:local --load .
```

This uses QEMU (a CPU emulator) to run x86_64 instructions on the
arm64 host during the build. Killed after **92 minutes**, still not
finished, for a build that takes under a minute natively. This is a
real, well-documented worst case for this class of emulation, not a
fluke: a JVM's JIT compiler generates NEW machine code continuously,
at runtime, as it decides which methods are hot enough to optimize —
and QEMU's own translation cache (the thing that makes emulation
tolerable for STATIC binaries, which only need translating once) gets
no benefit from code that's being freshly generated and immediately
discarded on every run. Compiling AND running Maven/Java under full
emulation compounds this twice over — the JVM compiling the Java source
is itself getting emulated, and then the JIT-compiled output it
produces adds a second, continuously-changing layer on top.

**The actual fix was cheaper than the failed one**: match the
INFRASTRUCTURE to the images that already exist, not the other way
around. AWS Graviton (ARM-based, `t4g`/`m7g`/etc. instance families) is
a first-class, fully-supported EC2 architecture — swapping
`EcsClusterStack`'s instance types from `T3` to `T4G` and its AMI
selection from the default `Standard` hardware type to
`AmiHardwareType.ARM` (both real, one-line CDK changes) means the
EXISTING arm64 images just work, unmodified, with zero rebuild:

```typescript
const machineImage = ecs.EcsOptimizedImage.amazonLinux2023(ecs.AmiHardwareType.ARM);
// ...
instanceType: ec2.InstanceType.of(ec2.InstanceClass.T4G, ec2.InstanceSize.SMALL),
```

`t4g.small` is also free-tier-eligible in this account (confirmed back
in Phase 3's own `describe-instance-types` check) — this fix didn't
cost anything extra either. The one real constraint it creates, flagged
for a later phase rather than solved now: this account's free-tier set
has NO Graviton option larger than `t4g.small` — growing the app-tier
pool's capacity later means either more `t4g.small` instances (not
bigger ones) or accepting the need to properly cross-compile the 8
Spring Boot images for x86_64 somewhere that isn't this laptop's local
QEMU (a real x86_64 machine, or a CI runner with native x86_64
hardware — GitHub Actions' standard runners, for instance).

### `cloud/scripts/build-and-push.sh`

Tags each already-built `orderflow/<service>:local` image with its ECR
repository's URI and pushes it — deliberately does NOT rebuild anything
(Phase 1's Dockerfiles already produce the exact artifact this script
ships). ECR authentication uses the modern, two-step pattern:

```bash
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"
```

rather than the older `aws ecr get-login` command, which used to print
a complete `docker login` command — PASSWORD INCLUDED — to stdout,
meaning it would land straight in your shell history the moment you
ran it verbatim. `get-login-password` + `--password-stdin` gets the
credential from AWS straight into `docker login`'s stdin, with the
token never appearing in a command line or history file at all.

Verified live — all 8 images pushed successfully:

```bash
$ ./cloud/scripts/build-and-push.sh
...
latest: digest: sha256:c935927f73538916f8e303434f36d9c4803806979e186103e25742f1a4b3f4cc size: 856
All 8 images pushed.
```

One genuinely surprising thing, checked directly rather than assumed —
`aws ecr describe-images` showed **3 images per repository**, not 1:

```bash
$ aws ecr describe-images --repository-name orderflow/order-service --query 'imageDetails[].{Tags:imageTags,Digest:imageDigest}'
Digest: sha256:d02edb...   Tags: None
Digest: sha256:342737...   Tags: None
Digest: sha256:2cd1f5...   Tags: ['latest']
```

Only ONE digest carries the `latest` tag — the actual image manifest
ECS will pull. The other two, untagged, are build **provenance/
attestation manifests** Docker Buildx attaches automatically (visible
during the Phase 1 build logs too, as "exporting attestation manifest"
— easy to have scrolled past without registering what it meant at the
time). ECR counts every distinct manifest object in a repository as an
"image," whether or not anything ever references it directly by tag —
this is normal, expected `docker push` behavior with a modern Buildx-
based Docker installation, not evidence of 3 separate builds or a
push gone wrong.

---

## 🎓 Concepts learned in Phase 4

1. **`docker build` targets the HOST machine's CPU architecture by
   default.** On Apple Silicon, that's `arm64` — a fact with zero
   consequence for purely local development (the same machine builds
   and runs the image), and a real, silent landmine the moment an
   image needs to run somewhere else, unless the target's architecture
   is checked explicitly.
2. **QEMU-based cross-platform emulation is not a universal drop-in
   fix** — it's genuinely excellent for many workloads and genuinely
   terrible for others, and a JIT-compiling language runtime doing a
   compile-heavy build (Maven, here) is close to the worst realistic
   case. Knowing WHEN to reach for emulation versus WHEN to just match
   the target platform to what you already have is a real, transferable
   piece of judgment, not a Docker trivia fact.
3. **Matching infrastructure to an existing artifact is often cheaper
   than re-deriving the artifact for existing infrastructure.** Once an
   arm64 image exists and works, Graviton EC2 instances are a
   first-class target, not a workaround — reaching for x86_64 "because
   it's more standard" would have been optimizing for a preference, not
   a real requirement.
4. **A repository's image count and its tagged-image count are
   different numbers**, once a modern Buildx-based Docker client is
   involved — attestation/provenance manifests are a real, automatic
   part of a normal push, not something to investigate as a bug the
   first time the numbers don't match 1:1.

## 🧠 Interview Q&A

**Q: You pushed a Docker image built on your Mac to a registry, and a
Linux server now fails to run it. What's the first thing you'd check?**
A: CPU architecture, before anything about the registry, the
orchestrator config, or IAM permissions. `docker build` targets the
host's own architecture by default — an Apple Silicon Mac produces
`arm64` images unless `--platform` is specified explicitly. `docker
image inspect <image> --format '{{.Architecture}}'` locally, compared
against the target server's actual CPU architecture, answers this in
one command, before spending time debugging something that LOOKS like
a deployment/config problem but is actually a fundamentally different
issue (the binary literally cannot execute on that CPU).

**Q: When would you use QEMU-based cross-platform Docker builds versus
just deploying to infrastructure matching your build machine's native
architecture?**
A: QEMU emulation is the right tool when you genuinely need artifacts
for MULTIPLE architectures from ONE build machine (publishing a public
image that other people's arm64 AND x86_64 machines both need to
pull), or when the workload itself is emulation-friendly (mostly static
binaries, limited dynamic code generation). It's the wrong tool when
you control BOTH the build machine and the deployment target and only
need ONE architecture — matching the deployment target to whatever the
build machine already produces natively is faster and simpler. It's
also specifically a poor fit for JIT-compiling runtimes (JVM, and
similar dynamic-compilation languages) doing compile-heavy work, since
the runtime's own continuously-regenerated machine code defeats
emulation's translation caching in a way static binaries don't suffer
from.

**Q: Why authenticate to ECR with `aws ecr get-login-password | docker
login --password-stdin` instead of simpler-looking alternatives?**
A: The older `aws ecr get-login` command printed a complete, ready-to-
run `docker login` command with the password embedded in plain text —
convenient to copy-paste, but if you ever ran it as a literal shell
command (rather than only eval-ing its output), that password landed
in your shell history file. `get-login-password` returns ONLY the
token, and piping it into `docker login`'s `--password-stdin` means the
credential is never a literal command-line argument or logged anywhere
— a real, if easy to overlook, credential-hygiene improvement in the
newer pattern.

**Q: Your container registry shows more images than you actually
pushed. Is that a bug?**
A: Not necessarily — check whether each extra entry has a tag. Modern
Docker Buildx attaches build attestations (SLSA provenance, SBOMs) as
separate, UNTAGGED manifests alongside the actual tagged image by
default. A registry that counts every distinct manifest as an "image"
(which ECR does) will show more entries than the number of `docker
push` commands you ran — the real signal is which digest actually
carries the tag you expect (`latest`, a version number, etc.); that's
the one anything will actually pull and run.

---

## Phase 5a: `ServicesStack` — 3 real Kafka brokers, a real KRaft quorum, Schema Registry

This is the actual point of the entire cloud deployment. Every prior
phase — the Dockerfiles, the CDK vocabulary, the budget guardrail, the
two capacity pools, the ECR repos — existed to make THIS phase possible:
3 genuinely independent Kafka brokers, each on its own EC2 instance,
forming one real KRaft controller quorum. Nothing about `docs/
system-design.md`'s "single broker, RF=1, no real failover" gap can be
closed without this actually running.

### The design: 3 `Ec2Service`s, not 1 service with 3 tasks

A single `Ec2Service` with `desiredCount: 3` was the wrong shape for
this, even though it's the more common ECS pattern. An `Ec2Service`
treats its tasks as interchangeable replicas of the SAME thing — fine
for 3 identical, stateless copies of an app server, wrong for 3 Kafka
brokers that each need a DIFFERENT, STABLE identity
(`KAFKA_NODE_ID=1/2/3`, a different Cloud Map DNS name, a different
slice of persistent local disk). `ServicesStack` instead creates 3
completely separate `Ec2Service` resources — `kafka-1`, `kafka-2`,
`kafka-3` — each with `desiredCount: 1` and its own task definition,
env vars, and Cloud Map registration. The one thing tying them together
is `KAFKA_CONTROLLER_QUORUM_VOTERS`, computed once and handed
identically to all 3:

```typescript
const controllerQuorumVoters = [1, 2, 3]
  .map((nodeId) => `${nodeId}@kafka-${nodeId}.${NAMESPACE_NAME}:9093`)
  .join(',');
// "1@kafka-1.orderflow.local:9093,2@kafka-2.orderflow.local:9093,3@kafka-3.orderflow.local:9093"
```

Each broker's own `Ec2Service` also carries
`placementConstraints: [ecs.PlacementConstraint.distinctInstances()]`
— this is what turns "3 tasks that COULD land on the same EC2 instance"
into "3 tasks GUARANTEED to land on 3 different instances," which is
the entire reason `EcsClusterStack` built a separate, dedicated,
exactly-3-instance Kafka capacity pool back in Phase 3. Without this
constraint, ECS's default bin-packing behavior could easily put 2 or
even all 3 brokers on the same physical instance — technically "3
Kafka processes," but a single EC2 instance failure would then take
down more than 1 broker at once, defeating the entire point of this
deployment.

### Cloud Map: how 3 tasks with dynamic IPs find each other by stable name

ECS tasks in `awsvpc` network mode get a fresh ENI (and fresh private
IP) every time they're placed or replaced — there's no way to hardcode
`KAFKA_CONTROLLER_QUORUM_VOTERS` with real IP addresses that would
survive a single task restart. AWS Cloud Map (`servicediscovery.
PrivateDnsNamespace`) solves this the same way Kubernetes' internal DNS
does: a private Route 53 hosted zone, scoped to the VPC, where
`kafka-1.orderflow.local` always resolves to whatever IP `kafka-1`'s
CURRENT task actually has, updated automatically as ECS starts/stops/
replaces tasks. `ServicesStack` creates one namespace
(`orderflow.local`) and each `Ec2Service` self-registers into it via
`cloudMapOptions` — no separate DNS management, no hardcoded IPs
anywhere in this stack.

The namespace is created directly inside `ServicesStack`, not added to
the already-deployed, already-merged `EcsClusterStack`'s cluster — a
deliberate choice to avoid touching a stable prior phase's stack for a
concern (service discovery for services that don't exist until THIS
phase) that belongs entirely to the new one.

### A real bug, caught before it ever touched AWS: `cdk synth` exposed a missing security group rule

Before deploying anything, `cdk synth OrderFlowServicesStack` was run
and the output inspected directly rather than assumed correct. It
showed something easy to miss: every `Ec2Service` in `awsvpc` mode
auto-creates its OWN security group by default, with an egress-only
rule (`0.0.0.0/0`, allow all outbound) and **zero ingress rules**:

```yaml
kafka1ServiceSecurityGroup...:
  Type: AWS::EC2::SecurityGroup
  Properties:
    GroupDescription: OrderFlowServicesStack/kafka-1Service/SecurityGroup
    SecurityGroupEgress:
      - CidrIp: 0.0.0.0/0
        IpProtocol: "-1"
    # no ingress rules at all
```

With 4 independently-created security groups like this (one per
service), `kafka-1` would have had no rule allowing `kafka-2` or
`kafka-3` to reach it on 9092/9093, and Schema Registry would have had
no rule allowing it to reach ANY broker — a KRaft quorum that could
never have formed, discoverable only after real EC2 instances were
already running and billing. The fix: one SHARED security group,
created explicitly, with a **self-referencing ingress rule** — anything
already wearing this security group may talk to anything else wearing
it, on exactly the ports these services need:

```typescript
this.internalSecurityGroup = new ec2.SecurityGroup(this, 'InternalSecurityGroup', {
  vpc: props.vpc,
  allowAllOutbound: true,
});
this.internalSecurityGroup.addIngressRule(this.internalSecurityGroup, ec2.Port.tcp(9092), '...');
this.internalSecurityGroup.addIngressRule(this.internalSecurityGroup, ec2.Port.tcp(9093), '...');
this.internalSecurityGroup.addIngressRule(this.internalSecurityGroup, ec2.Port.tcp(8085), '...');
```

...then passed explicitly to all 4 `Ec2Service`s via `securityGroups:
[this.internalSecurityGroup]`, which stops CDK from auto-creating the
broken per-service default. This is the same "Security Groups as the
access-control boundary" approach `NetworkStack` already used for the
ALB/instance split back in Phase 3, applied one level down for
service-to-service traffic. Catching this at `cdk synth` time — a
free, local, offline command — instead of after a live deploy is
exactly why `synth`/`diff` are run before every `deploy` in this
project, not a formality.

### A real bug, found live: non-root container + root-owned bind mount = broker can't write its own metadata

`cdk synth` catches wiring mistakes; it can't catch a bug that only
exists once a real container runs against a real host filesystem. The
first live deploy attempt had all 3 brokers crash-loop immediately:

```
===> User	uid=1000(appuser) gid=1000(appuser) groups=1000(appuser)
Formatting /var/lib/kafka/data with metadata.version 3.8-IV0.
Error while writing meta.properties file /var/lib/kafka/data: /var/lib/kafka/data/bootstrap.checkpoint.tmp
```

`apache/kafka:3.8.0` runs as a non-root user (`uid 1000`, `appuser`) —
good, standard image-hardening practice. But each broker task's
`Volume.host.sourcePath` (`/data/kafka` on the EC2 instance, bind-mounted
into the container at `/var/lib/kafka/data`, matching the
`KAFKA_LOG_DIRS` env var — the same fix Build Order Step 12 already
needed for the LOCAL docker-compose deployment, now needed again here
for the same underlying reason) gets auto-created by Docker on first
mount, owned by `root:root`. `appuser` inside the container has no
write permission to a root-owned host directory — the fix has to
happen ON THE HOST, before the container ever starts. `AutoScalingGroup
.addUserData()` appends shell commands to the Kafka pool's EC2 launch
template, run once at instance boot:

```typescript
kafkaAsg.addUserData(
  'mkdir -p /data/kafka',
  'chown -R 1000:1000 /data/kafka',
);
```

The subtlety that made this take longer to fully resolve than the code
change itself: an `AutoScalingGroup`'s launch template change does
**not** retroactively touch instances that are already running — it
only affects instances launched AFTER the change (new capacity, or an
instance the ASG replaces on its own). The 3 Kafka EC2 instances from
the FIRST deploy attempt already existed with the OLD, unfixed
UserData baked in. Redeploying `EcsClusterStack` alone was not enough;
the 3 existing instances had to be terminated directly —

```bash
aws ec2 terminate-instances --instance-ids i-0c6ec3c0552fe99b2 i-05fa2bf9afdd93717 i-054102b896d1b3be5
```

— so the ASG would launch 3 FRESH replacements from the corrected
launch template. This is the exact same trade-off `up.sh`'s own
comments already documented for a different reason (bind-mounted Kafka
data does not survive a `down.sh`/`up.sh` cycle) — here it was used
deliberately, as the actual fix mechanism, not an accepted side effect.

### A second real bug, found live: a stale ECS placement failure needed a manual nudge

Once the permission fix was in place, `kafka-1`, `kafka-3`, and
`schema-registry` all reached steady state — but `kafka-2` stayed at
`running: 0, desired: 1, pending: 0`, with its most recent event being
an old placement failure:

```
(service kafka-2) was unable to place a task because no container
instance met all of its requirements. The closest matching
(container-instance 8b7d...) has insufficient memory available.
```

This happened because `kafka-2`'s task tried to place WHILE the 3
replacement Kafka instances were still mid-registration — an entirely
transient condition (the 3rd instance simply wasn't in the cluster
yet). By the time all 3 instances were fully registered, one of them
sat completely idle with all 1846 MiB of reservable memory free — but
`kafka-2`'s service wasn't automatically retrying. `aws ecs
update-service --force-new-deployment` triggered an immediate new
placement attempt rather than waiting out ECS's own backoff:

```bash
aws ecs update-service --cluster orderflow-cluster --service kafka-2 --force-new-deployment
```

...after which `kafka-2` placed cleanly onto the empty instance and
joined the quorum.

### Verified live: a real 3-broker KRaft quorum, not 3 independent single-node brokers

This is the actual payoff. Broker 1's own CloudWatch logs show a real
Raft leader election, with `voters=[1, 2, 3]` — all 3 REAL broker node
IDs, running on 3 REAL, physically separate EC2 instances:

```
[RaftManager id=1] Completed transition to FollowerState(..., epoch=120,
  leader=kafka-3.orderflow.local:9093 (id: 3 ...), voters=[1, 2, 3], ...)
[RaftManager id=1] Completed transition to CandidateState(..., epoch=142,
  voteStates={1=GRANTED, 2=GRANTED, 3=UNRECORDED}, ...)
[RaftManager id=1] Completed transition to Leader(..., epoch=142, ...)
[KafkaRaftServer nodeId=1] Kafka Server started
```

Cross-checking the OTHER 2 brokers' independent logs confirms they
agree with broker 1's view — a real, consistent, distributed
election, not one broker's isolated opinion:

```
# kafka-2's own log:
[RaftManager id=2] Completed transition to Voted(epoch=142, votedKey=ReplicaKey(id=1, ...), ...)
[RaftManager id=2] Completed transition to FollowerState(..., epoch=142,
  leader=kafka-1.orderflow.local:9093 (id: 1 ...), voters=[1, 2, 3], ...)

# kafka-3's own log:
[RaftManager id=3] Completed transition to FollowerState(..., epoch=142,
  leader=kafka-1.orderflow.local:9093 (id: 1 ...), voters=[1, 2, 3], ...)
```

All 3 agree: epoch 142, broker 1 is leader. And this isn't just
controller-quorum bookkeeping — the internal `__consumer_offsets` topic
(auto-created despite `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, since
internal topics are a special case) shows real partition leadership
SPREAD ACROSS different brokers, visible in broker 1's own
`ReplicaFetcher` logs pulling from `leaderId=2` for some partitions and
`leaderId=3` for others — genuine multi-broker data replication, not
just 3 processes agreeing to elect one of themselves:

```
[ReplicaFetcher replicaId=1, leaderId=2, fetcherId=0] ... partition __consumer_offsets-8 ...
[ReplicaFetcher replicaId=1, leaderId=3, fetcherId=0] ... partition __consumer_offsets-16 ...
```

Schema Registry's own logs confirm it isn't just listening on port
8085 in isolation — it actually joined a Kafka consumer group backed
by this 3-broker cluster, elected itself leader (the only replica, so
trivially so), and read/caught up on its internal `_schemas` topic
before serving its first HTTP request:

```
[Schema registry clientId=sr-1, groupId=schema-registry] Successfully joined group ...
Finished rebalance with leader election result: ... isLeader=true
Wait to catch up until the offset at 2
Reached offset at 2
Server started, listening for requests... (io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain)
```

Final state, confirmed directly against the live AWS API rather than
inferred from CDK's own "it deployed" signal:

```bash
$ aws ecs describe-services --cluster orderflow-cluster --services kafka-1 kafka-2 kafka-3 schema-registry \
    --query 'services[].{name:serviceName,running:runningCount,desired:desiredCount,pending:pendingCount}' --output table
+---------+-------------------+----------+-----------+
| desired |       name        | pending  |  running  |
+---------+-------------------+----------+-----------+
|  1      |  kafka-1          |  0       |  1        |
|  1      |  kafka-2          |  0       |  1        |
|  1      |  kafka-3          |  0       |  1        |
|  1      |  schema-registry  |  0       |  1        |
+---------+-------------------+----------+-----------+

$ aws cloudformation describe-stacks --stack-name OrderFlowServicesStack --query 'Stacks[0].StackStatus'
"CREATE_COMPLETE"
```

### `up.sh` / `down.sh`: updated for the new stack dependency

`ServicesStack`'s 4 ECS services reference `EcsClusterStack`'s cluster
and capacity providers via CloudFormation `Fn::ImportValue` — AWS
refuses to delete a stack whose exports are still imported elsewhere.
`down.sh` now destroys `OrderFlowServicesStack` FIRST, releasing those
imports, before attempting `OrderFlowEcsClusterStack`; `up.sh` deploys
`ServicesStack` as a separate, LATER `cdk deploy` call (not bundled
into the same multi-stack command as `NetworkStack`/`EcsClusterStack`)
so the EC2 instances have time to register with the cluster before ECS
tries to place any broker task onto them.

### Real deployment timing, and why the honest number here is inflated

```
OrderFlowEcsClusterStack: 206.36s
OrderFlowServicesStack:  1983.68s   (~33 minutes)
```

The `ServicesStack` number is NOT a clean baseline — it includes the
real, live debugging time for both bugs above (waiting out the first
crash-loop, terminating and waiting for 3 replacement EC2 instances to
boot and register, waiting out `kafka-2`'s stale placement backoff).
Recorded honestly rather than replaced with a cleaner re-run, since the
whole point of this document is what actually happened, including the
detours — a clean redeploy with both fixes already in the CDK source
(the normal case after this phase merges) would be close to
`EcsClusterStack`'s own instance-boot-bound ~3-4 minutes, plus however
long the 4 ECS services take to reach steady state once real capacity
exists (observed here at under a minute once instances were actually
ready).

### ECS Exec: set up, not yet exercised

Every service in this phase has `enableExecuteCommand: true` — the
modern, SSH-free way to get an interactive shell inside a running
container for debugging (`aws ecs execute-command`), consistent with
this project's Security Groups containing no SSH ingress rule anywhere.
Actually running it locally requires the separate `session-manager-
plugin` binary, which needs an interactive `sudo` install this session
couldn't complete non-interactively — deferred, not blocking, since
CloudWatch Logs already provided everything needed to verify the
quorum above. Worth finishing before the live broker-kill/failover demo
in a later phase, where an interactive shell inside a SURVIVING broker
(to run `kafka-topics.sh`/`kafka-metadata-quorum.sh` by hand, live,
while the failure is happening) will matter more than log excerpts
after the fact.

---

## 🎓 Concepts learned in Phase 5a

1. **One `Ec2Service` per broker, not one `Ec2Service` with N tasks** —
   the right shape whenever "identical, interchangeable replicas" isn't
   true of the thing being deployed. Each Kafka broker needs a
   different, STABLE identity; ECS's normal service model assumes
   tasks are fungible.
2. **`PlacementConstraint.distinctInstances()`** is what actually
   guarantees per-instance failure isolation — without it, "3 tasks" and
   "3 tasks on 3 different machines" are NOT the same guarantee, even
   with a dedicated capacity pool sized exactly 3.
3. **Cloud Map (`PrivateDnsNamespace`) solves the "IP changes every
   restart" problem for peer-to-peer clustered systems on ECS** — the
   same fundamental problem Kubernetes' internal DNS solves, applied to
   ECS's `awsvpc` mode.
4. **`cdk synth` is a real bug-finding tool, not just a formality
   before `deploy`** — the missing-ingress-rule bug here was caught
   completely offline, before a single EC2 instance existed, simply by
   reading the generated CloudFormation template.
5. **A non-root container image + a host bind mount is a real,
   recurring class of bug**, not specific to Kafka — any time a
   container's process runs as a non-root user AND writes to a
   host-mounted path, the host-side ownership of that path (which
   Docker does NOT automatically match to the container's user) has to
   be arranged explicitly, usually via instance bootstrap tooling
   (`UserData` here; a Kubernetes `initContainer` or `fsGroup` in that
   world).
6. **Changing an Auto Scaling Group's launch template does not affect
   already-running instances** — only NEW ones. Fixing a bug baked into
   an instance's boot-time configuration requires actually replacing
   the instance, not just updating the template it was originally
   launched from.
7. **ECS service placement failures can be transient and stale** — a
   service can be stuck on an old failure event with nothing actively
   retrying, even once the underlying condition (insufficient capacity)
   has resolved. `--force-new-deployment` is the direct way to trigger
   an immediate retry rather than waiting out whatever backoff ECS is
   currently on.
8. **"The stack deployed successfully" and "the distributed system
   actually formed a working quorum" are two different claims** —
   CloudFormation's `CREATE_COMPLETE` only means the ECS service
   scheduler reported steady state (the right number of tasks running).
   Confirming the KRaft election actually happened, and that all 3
   brokers agree on the same leader/epoch, required reading the
   application's own logs directly.

## 🧠 Interview Q&A

**Q: You need to deploy a 3-node clustered system (Kafka, etcd,
ZooKeeper, Cassandra — anything where each node needs a distinct,
stable identity) on ECS. Would you use one service with `desiredCount:
3`, or something else?**
A: Something else — N separate services, one per node, each with
`desiredCount: 1`. A single ECS service models its tasks as
interchangeable replicas of the same thing, which is exactly wrong for
a cluster where each member needs a different node ID, a different
stable network identity, and often its own slice of persistent storage.
Separate services also make it possible to give each member its own
placement constraint, so failure isolation (one member per physical
host) can actually be guaranteed rather than left to chance.

**Q: How do you guarantee 3 ECS tasks land on 3 DIFFERENT EC2
instances, not just "3 tasks somewhere in the cluster"?**
A: `PlacementConstraint.distinctInstances()` on each service (or task
placement, depending on API surface) — without it, ECS's default
scheduler behavior (spread or binpack, depending on configured
placement strategy) is a preference, not a guarantee, and multiple
tasks CAN end up on the same host. A dedicated capacity pool sized to
match (exactly 3 instances for exactly 3 constrained tasks here) makes
the constraint satisfiable; the constraint itself is what actually
enforces the isolation.

**Q: A container process running as a non-root user can't write to a
directory you bind-mounted from the host. What's actually happening,
and how do you fix it?**
A: Docker bind mounts preserve the HOST-side ownership of the mounted
path — if Docker creates that path fresh on first mount (common when
nothing pre-creates it), it's owned by `root:root` on the host, and a
container process running as a non-root UID has no write permission to
it, regardless of what the container's own filesystem permissions look
like. The fix has to happen on the host side, before the container
starts: pre-create the directory and `chown` it to match the
container's UID (found via the image's own startup logs, or its
Dockerfile/documentation) — via instance bootstrap tooling (cloud-init/
UserData on EC2, an `initContainer` or `fsGroup` setting in Kubernetes),
not inside the container itself.

**Q: You updated an Auto Scaling Group's launch template to fix a
boot-time configuration bug. The bug is still happening. Why?**
A: A launch template change only affects instances launched AFTER the
change — it does not retroactively reconfigure instances that are
already running, since EC2 instances don't re-execute their boot-time
UserData on their own. The already-running instances need to actually
be replaced — either by terminating them directly (letting the ASG
launch replacements from the now-current template) or via a proper
EC2 instance refresh, if the ASG is configured for one.

**Q: How do you actually verify a Raft-based quorum (KRaft, etcd,
anything Raft-family) formed correctly, beyond "the process is
running"?**
A: Check for agreement across INDEPENDENT nodes' own logs/state, not
just one node's self-report. A real, healthy quorum shows: the same
epoch/term number reported by every member, the same leader identity
agreed on by every member, and — for systems with actual data
replication on top of the consensus layer (Kafka's partition leadership
being a clear example) — data/partition leadership genuinely
distributed across different nodes, not concentrated on the one you
happened to check. A single node reporting "I am the leader" is
necessary but not sufficient; the other members corroborating it is
what makes it a real, cluster-wide verified fact rather than one
process's possibly-stale opinion.

---

## Phase 5b: Postgres, Redis, Elasticsearch — the 3 stateful dependencies the app tier needs

Phase 5a proved the hard, genuinely-distributed part of this
deployment works. Phase 5b is comparatively simple: 3 more `Ec2Service`s
on the SAME `ServicesStack`, bin-packed onto the app-tier capacity pool
alongside Schema Registry, giving the 8 Spring Boot services (Phase 5c)
everywhere they need to read/write once they're deployed —
order-service's Postgres-backed outbox, the 4 shared Redis use cases,
and search-indexer-service's Elasticsearch target.

### Why none of these 3 need `distinctInstances`

Phase 5a's Kafka brokers needed `distinctInstances` because there were
3 REPLICAS of the same role, and losing more than 1 to a single
instance failure would defeat the whole point of the demo. Postgres,
Redis, and Elasticsearch here are each a SINGLE instance of their
role — there's nothing to "spread across instances" when there's only
one of a thing. They bin-pack onto the app-tier pool exactly like
Schema Registry already does, no placement constraint needed.

### Applying Phase 5a's lesson proactively instead of finding it live again

Phase 5a's real permission bug (non-root container + root-owned bind
mount) wasn't specific to Kafka — it's a property of ANY image that
runs as a non-root user and writes to a host-mounted path. Rather than
deploy all 3 new services and wait to see which one(s) crash-loop, each
image's actual container user was checked directly, first:

```bash
$ docker inspect postgres:16 --format '{{.Config.User}}'
                                            # empty — no USER directive
$ docker inspect redis:7 --format '{{.Config.User}}'
                                            # empty — no USER directive
$ docker inspect docker.elastic.co/elasticsearch/elasticsearch:8.15.0 --format '{{.Config.User}}'
1000:0
```

An empty `Config.User` doesn't mean "runs as root forever" — it means
the container's INITIAL process is root, which for both `postgres:16`
and `redis:7`'s official entrypoint scripts is deliberate: their
entrypoints run any needed setup (including `chown`-ing their own data
directory) AS root, then drop privileges internally before starting the
actual database/server process. Confirmed directly rather than assumed,
since Phase 5a already showed the cost of guessing wrong once:

```bash
$ docker run --rm postgres:16 id
uid=0(root) gid=0(root) groups=0(root)
$ docker run --rm redis:7 id
uid=0(root) gid=0(root) groups=0(root)
$ docker run --rm docker.elastic.co/elasticsearch/elasticsearch:8.15.0 id
uid=1000(elasticsearch) gid=0(root) groups=0(root)
```

Only Elasticsearch is in the same situation Kafka was: an explicit,
permanent non-root `USER 1000:0` Dockerfile directive, confirmed by
`id` reporting `uid=1000` even for the container's very first process.
`EcsClusterStack`'s app-tier Auto Scaling Group got the same kind of
`UserData` fix Phase 5a's Kafka pool already has, added BEFORE the
first Phase 5b deploy attempt rather than after a crash-loop:

```typescript
appAsg.addUserData(
  'mkdir -p /data/elasticsearch',
  'chown -R 1000:0 /data/elasticsearch',
);
```

Postgres and Redis got plain bind mounts with no `chown` step — adding
one would have been harmless but pointless, since their own entrypoints
already handle it. (Redis, matching the local `docker-compose.yml`
exactly, gets no persistent volume at all — see the code's own comment
for why all 4 of its use cases here are legitimately fine losing their
contents on a restart.)

### The other lesson from Phase 5a, also applied proactively this time: replace already-running instances

Phase 5a discovered, the hard way, that an Auto Scaling Group launch
template change doesn't retroactively affect instances that are
already running. This time, with that lesson already learned, the fix
was applied BEFORE deploying the new services, not after:

```bash
$ node_modules/.bin/cdk deploy OrderFlowEcsClusterStack ...   # ships the UserData fix
$ aws ec2 terminate-instances --instance-ids i-07846ff... i-0f9eed4...  # the 2 OLD app-tier instances
# wait for the ASG to launch 2 replacements from the now-current template
$ node_modules/.bin/cdk deploy OrderFlowServicesStack ...     # only now bring up Postgres/Redis/Elasticsearch
```

### Verified live: all 3 services healthy on the FIRST deploy attempt

```bash
$ aws ecs describe-services --cluster orderflow-cluster \
    --services kafka-1 kafka-2 kafka-3 schema-registry postgres redis elasticsearch \
    --query 'services[].{name:serviceName,running:runningCount,desired:desiredCount,pending:pendingCount}' --output table
+---------+-------------------+----------+-----------+
| desired |       name        | pending  |  running  |
+---------+-------------------+----------+-----------+
|  1      |  kafka-1          |  0       |  1        |
|  1      |  kafka-2          |  0       |  1        |
|  1      |  kafka-3          |  0       |  1        |
|  1      |  schema-registry  |  0       |  1        |
|  1      |  postgres         |  0       |  1        |
|  1      |  redis            |  0       |  1        |
|  1      |  elasticsearch    |  0       |  1        |
+---------+-------------------+----------+-----------+
```

No crash-loop this time — every service reached `CREATE_COMPLETE`
inside the same single `cdk deploy OrderFlowServicesStack` run,
confirmed against each service's own CloudWatch logs, not just
CloudFormation's steady-state signal:

```
# Postgres:
2026-08-08 05:42:01.424 UTC [1] LOG:  database system is ready to accept connections

# Redis:
1:M 08 Aug 2026 05:41:56.032 * Ready to accept connections tcp

# Elasticsearch: no permission/denied/exception lines anywhere in the
# full log (checked from the very first line, not just the tail), node
# started, and it immediately began writing real data — dozens of
# index lifecycle policies and index templates — to the bind-mounted
# /usr/share/elasticsearch/data, which is itself proof the mount is
# writable, not just that the process didn't immediately crash:
"message":"started {ip-172-31-75-193.ec2.internal}{...}{8.15.0}..."
"message":"adding index lifecycle policy [90-days-default]"
"message":"adding index lifecycle policy [180-days-default]"
# ... (dozens more, all successful)
```

Deployment time for the whole `ServicesStack` update (3 new services,
no rework needed): **144.29s** — a useful, honest contrast against
Phase 5a's inflated ~33-minute number, since this run had no live
debugging baked into it. This is close to what a "normal," bug-free
`ServicesStack` change actually costs: mostly the time for 3 ECS
services to place their tasks and reach steady state, once real
capacity already exists.

### One dead end, noted rather than pursued: SSM `send-command` to check the fix before deploying

Before deploying `ServicesStack`, an attempt was made to verify the
`chown` had actually run on the 2 new app-tier instances FIRST, via
`aws ssm send-command` (which — unlike `aws ecs execute-command`,
Phase 5a's ECS Exec — talks to the EC2 instance directly and doesn't
need the local `session-manager-plugin` binary):

```bash
$ aws ssm send-command --instance-ids i-060e43... --document-name AWS-RunShellScript ...
An error occurred (InvalidInstanceId): Instances not in a valid state for account
```

This project's EC2 instance IAM role has never been granted SSM's
managed-instance permissions (`AmazonSSMManagedInstanceCore` or
equivalent) — a real, sensible gap, not a bug: this project deliberately
has no SSH ingress rule anywhere (Phase 3's `NetworkStack`) and relies
on ECS Exec (scoped to the ECS TASK role, not the underlying EC2
instance role) for container-level debugging instead. Direct
host-level access was never a design goal here, so this was left
unaddressed rather than granting broader IAM permissions than the
project actually needs — the deploy-then-check-CloudWatch-logs
approach (used successfully in both Phase 5a and this phase) covers the
same verification need without it.

---

## 🎓 Concepts learned in Phase 5b

1. **`docker inspect --format '{{.Config.User}}'` and `docker run <image>
   id` answer different questions.** An empty `Config.User` means no
   Dockerfile `USER` directive was set — the container's very first
   process starts as root, but that says nothing about whether the
   actual SERVER process ends up running as root too. Many official
   database images (Postgres, Redis here) deliberately start as root,
   self-fix ownership on their data directory, then drop to an
   unprivileged user internally — checking with `docker run ... id`
   only shows the INITIAL process, not this internal handoff.
2. **A bug found once in one service is worth checking for in every
   OTHER service before deploying them, not just fixing where it was
   found.** Phase 5a's Kafka permission bug generalizes to "non-root
   image + host bind mount" — applying that lens to Postgres, Redis,
   and Elasticsearch BEFORE deploying (rather than after a second
   crash-loop) is what turned a repeat live-debugging session into a
   244-second, first-attempt-clean deploy.
3. **Not every persistent-seeming service needs a persistent volume.**
   Redis here intentionally has none — matching its actual role
   (cache, dedupe store, rate limiter, lock — all fine to reset) rather
   than defaulting to "give it a volume because it's a database-shaped
   thing."
4. **IAM scope should match what a system actually needs, not what
   would be convenient to have.** Not wiring up SSM managed-instance
   access (which would have made a nice-to-have verification step
   easier) was a deliberate non-decision, consistent with this
   project's existing "no SSH ingress rule anywhere, ECS Exec only"
   design — a gap surfaced by trying something and hitting a real IAM
   boundary, not a bug.

## 🧠 Interview Q&A

**Q: How do you know whether a Docker image's ACTUAL server process
runs as root, versus just its container's first process?**
A: `docker inspect --format '{{.Config.User}}'` and `docker run <image>
id` only show the INITIAL process's user — for many official database
images, that's deliberately root, so the entrypoint script can perform
root-only setup (like fixing ownership on a mounted data directory)
before internally dropping privileges (via `gosu`/`su-exec`) and
starting the real server process unprivileged. The only fully reliable
way to know the ACTUAL server process's user is to check the image's
entrypoint script directly, or observe the running container's process
list — an empty `Config.User` is evidence of "no enforced non-root
user," not proof the server ends up running as root.

**Q: You found and fixed a bug in one service (say, a permission issue
between a non-root container and a host volume). Do you need to check
your other services for the same bug?**
A: Yes — the fix for one instance of a general class of bug (non-root
container + host-owned mount, in this case) doesn't fix the class
itself. Before deploying similar services, checking each one's actual
container user against whether the fix was already applied (here: only
Elasticsearch needed it; Postgres and Redis didn't) turns a
"same bug, found again, live" repeat incident into a preventive check
that costs a few `docker inspect` commands.

**Q: Would you give every EC2 instance in your cluster SSM Session
Manager access, "just in case" you need to debug something on the
host?**
A: Only if there's an actual need for host-level access that
container-level tooling can't cover. This project deliberately has no
SSH ingress anywhere and relies on ECS Exec (scoped narrowly to the ECS
task role) for the debugging it actually needs — adding
`AmazonSSMManagedInstanceCore` to the EC2 instance role broadens what a
compromised task/instance could do, for a capability (arbitrary
host-level shell access) the project doesn't have an active need for.
The right call is scoping IAM to what's actually used, and treating "it
would be convenient to have" as insufficient justification on its own.

**Q: A stateful service (cache, queue, session store) doesn't have a
persistent volume in your deployment. Is that automatically a bug?**
A: Not automatically — it depends on whether losing that service's data
on restart is actually a problem for how it's used. A cache backed by a
real source of truth, a short-TTL dedupe/rate-limit store, or a lock
that just gets re-acquired are all legitimately fine without
persistence; a system of record (a database holding data with no other
copy) is not. The question to ask is "what actually breaks if this
process restarts with an empty disk," not "is this the kind of thing
that usually gets a volume."

---

## Phase 5c: `AppServicesStack` — the 8 Spring Boot services + ALB, and a real order end-to-end on AWS

The payoff phase. Every prior phase built infrastructure; this one runs
the actual application and proves it works — placing a real order
through a public URL and watching it complete the full choreography
saga (order-service → Kafka → inventory/payment/shipment-service →
back to order-service) on AWS, not a laptop. This phase also surfaced
more real, live bugs than 5a and 5b combined — each one left in this
document because the debugging process is the actual teaching material,
not just the final working state.

### The design: one shared ALB, path-based routing, 3 internal-only services

All 8 services are `Ec2Service`s on the app-tier capacity pool, same
pattern as Phase 5b's Postgres/Redis/Elasticsearch — no
`distinctInstances` constraint, since there's only ONE of each. 5 of
the 8 have their own REST API worth reaching from outside the VPC
(order-service, fraud-detection-service, analytics-service,
order-saga-orchestrator, search-indexer-service) and get an ALB
`ApplicationTargetGroup` + a path-based listener rule
(`/orders/*`, `/fraud/*`, `/analytics/*`, `/saga/*`, `/search/*`). The
other 3 (inventory-service, payment-service, shipment-service) are
internal-only — reached purely via Cloud Map by the orchestrator and
each other, exactly matching how they're used locally, where nothing
outside the platform ever calls them directly.

**Zero Java code changes anywhere in this phase.** Every address each
service needs was already an externalized Spring Boot property with a
`localhost` default (confirmed by reading all 8 `application.yml`
files directly before writing any CDK code, not assumed) —
`SPRING_DATASOURCE_URL`, `SPRING_DATA_REDIS_HOST`,
`SPRING_KAFKA_BOOTSTRAP_SERVERS`,
`SPRING_KAFKA_{PRODUCER,CONSUMER,STREAMS}_PROPERTIES_SCHEMA_REGISTRY_URL`,
`SPRING_ELASTICSEARCH_URIS`, and
`SERVICES_{INVENTORY,PAYMENT,SHIPMENT}_SERVICE_BASE_URL` for
order-saga-orchestrator's direct HTTP calls — all overridden purely via
environment variables in each ECS task definition, pointed at Cloud Map
DNS names instead of `localhost`.

### JVM memory tuning: `JAVA_TOOL_OPTIONS`, not a Dockerfile change

8 more JVMs need to safely bin-pack alongside Phase 5b's 4 backing
services on `t4g.small` instances. Rather than let JVM ergonomics size
each service's default heap off the HOST's full 2 GiB (visible to every
container, since only a SOFT `memoryReservationMiB` is set, not a hard
`memory` limit), every task gets `JAVA_TOOL_OPTIONS: "-Xmx<N>m
-Xss512k"` — a JVM launcher environment variable, picked up
automatically with zero Dockerfile change, since every one of Phase 1's
Dockerfiles ends in a direct `ENTRYPOINT ["java", "-jar", ...]` (checked
directly, not assumed). Kafka Streams services
(fraud-detection-service, analytics-service) get more headroom (384m
heap, 550 MiB reservation) than the plain consumer/producer services
(256m heap, 350-400 MiB reservation), reflecting RocksDB state store
overhead the plain services don't have.

### Bug 1, caught before it ever touched AWS: an invalid security-group rule description

`cdk deploy` failed immediately with:

```
Resource handler returned message: "Invalid rule description. Valid descriptions are strings less than 256 characters
from the following set:  a-zA-Z0-9. _-:/()#,@[]+=&;{}!$*
```

The culprit: `` `ALB -> ${spec.name}` `` — the `>` character isn't in
CloudFormation's allowed set for a security-group rule description.
The SAME class of bug as Phase 3's em-dash finding (a different
forbidden character, same underlying lesson: CloudFormation's string
validation for these fields is stricter than it looks, and it's cheap
to hit by accident with ordinary punctuation). Fixed by writing "ALB to
`${spec.name}`" instead. `ServicesStack` (which owns the shared
`internalSecurityGroup` these rules attach to) rolled back cleanly with
zero impact to the 7 already-running Phase 5a/5b services — a real,
practical benefit of CloudFormation's automatic rollback-on-failure
behavior, not a hazard, when the failure is caught this early.

### Bug 2, found live: the ENI-per-instance ceiling on `t4g.small`

With the description fixed, the deploy proceeded — 3 of 8 app services
placed and reached steady state, but 4 (order-service,
fraud-detection-service, analytics-service, order-saga-orchestrator)
sat permanently at `running: 0, pending: 0`, with a genuinely different
placement failure than anything seen before:

```
(service order-service) was unable to place a task because no container instance met all of its requirements.
The closest matching (container-instance ...) encountered error "RESOURCE:ENI".
```

`aws ec2 describe-instance-types --instance-types t4g.small` confirmed
the real, hard constraint: `t4g.small` supports a MAXIMUM of 3 network
interfaces — 1 is always the instance's own primary ENI, leaving only
**2** free for `awsvpc`-mode ECS tasks, regardless of how much CPU or
memory is still free on that instance. With 12 app-tier services (4
backing + 8 app) and only 4 instances × 2 ENI-limited slots = 8 slots
available, 4 services had nowhere to go — a capacity ceiling that has
nothing to do with the memory-driven 2 → 4 growth Phase 5c started
with.

**ENI trunking, tried and found not to help here.** ECS's
`awsvpcTrunking` account-level setting exists specifically to raise
this per-instance ceiling via "branch" ENIs riding on one "trunk" ENI:

```bash
aws ecs put-account-setting-default --name awsvpcTrunking --value enabled
```

New instances launched after enabling it DID show the
`ecs.capability.task-eni-trunking` attribute (confirmed via `aws ecs
describe-container-instances`). Empirically, though, it made no
difference: fresh instances still hit `RESOURCE:ENI` at exactly 2 tasks
each, with CPU and memory both still plentiful (`remainingResources`
showed 984+ MiB free). Left enabled anyway (harmless, account-wide,
might matter for a different instance type later) but not relied on —
the real fix was accepting the untrunked 2-tasks-per-instance ceiling
as fact and sizing the pool around it: 6 app-tier instances × 2 = 12
slots for the 12 services that needed one.

### Bug 3, found live: a real, hard EC2 vCPU account quota

Growing the pool to 7 (to comfortably fit 12 services with headroom)
hit an entirely different wall — a genuine AWS account-level service
quota, not anything this project's own code controlled:

```
You have requested more vCPU capacity than your current vCPU limit of 16 allows
for the instance bucket that the specified instance type belongs to.
```

Confirmed directly: `aws service-quotas get-service-quota --service-code ec2
--quota-code L-1216C47A` showed exactly 16 vCPU for "Running On-Demand
Standard (A, C, D, H, I, M, R, T, Z) instances" — a quota bucket broad
enough to cover essentially every general-purpose/compute/memory/
burstable EC2 family, Graviton included, so switching instance types
wasn't an escape hatch (confirmed `t4g.micro` is ALSO 2 vCPUs, not a
smaller number — Graviton's burstable sizes don't reduce vCPU count the
way they reduce memory). Empirically, 9 total `t4g.small` instances ran
successfully but a 10th consistently failed — the real usable ceiling
sits a bit above the naive `16 ÷ 2 = 8` math, but the boundary is real
and reproducible. With 3 Kafka instances fixed, app-tier tops out at
6 — which, conveniently, is exactly the 12 ENI-limited slots needed.

The stuck update (stuck trying to reach 7 forever) was cancelled with
`aws cloudformation cancel-update-stack` — the SAME "works cleanly on
an in-progress UPDATE" technique already proven in Phase 3, still true
here.

### Bug 3b: a genuine deadlock between draining instances and the same quota

Redeploying at the corrected count of 6 hit a subtler version of the
same problem: 2 instances from the earlier over-provisioning attempt
were stuck in `Terminating:Wait` (an ECS-managed lifecycle hook,
draining their tasks before allowing termination), while every ACTIVE
instance was already at its 2-task ENI ceiling — nowhere for the
draining tasks to go. Meanwhile, the 2 terminating instances STILL
counted against the account's vCPU quota, blocking their own
replacements from launching. A real deadlock: nothing could progress
without an outside nudge. `aws autoscaling complete-lifecycle-action
--lifecycle-action-result CONTINUE` on the ECS-managed drain hook broke
the cycle — forcing those 2 instances to finish terminating immediately
(their 4 tasks got stopped, not gracefully drained — acceptable here
since nothing important was uniquely running on them) freed the quota
for real replacements to launch.

### Bug 4, found live: the ALB does not strip the path it matched

With capacity finally correct, all 12 services reached
`running: 1`. The very first real test — an actual `curl` through the
ALB — 404'd:

```bash
$ curl -s -i -X POST http://<alb-dns>/orders/api/orders -d '...'
HTTP/1.1 404
{"timestamp":"...","status":404,"error":"Not Found","path":"/orders/api/orders"}
```

That's Spring Boot's OWN 404 JSON body (not the ALB's fixed-response
404), meaning the request DID reach `order-service` — it just didn't
match anything, because `OrderController` is mapped at `/api/orders`,
not `/orders/api/orders`. The real, easy-to-miss fact: **ALB path-based
routing rules match a PATTERN to pick a target group; they do not strip
that prefix before forwarding.** A request for `/orders/api/orders`
arrives at the container as literally `/orders/api/orders`.

Checking all 5 exposed services' controllers directly (not assumed)
showed they ALL already follow the same `/api/<domain>/...` convention
— `fraud-detection-service` at `/api/fraud/...`, `analytics-service` at
`/api/analytics/...`, and so on. That consistency made the fix a single
standard Spring Boot property, zero Java changes:
`server.servlet.context-path`, set to match each service's ALB prefix
(`/orders`, `/fraud`, `/analytics`, `/saga`, `/search`). It makes the
APPLICATION itself respond at the prefix the ALB actually forwards,
rather than trying (and failing, since ALB has no path-rewrite feature
at all) to strip the prefix in front of the app.

One easy-to-miss side effect: `server.servlet.context-path` moves
**every** endpoint under that prefix, Actuator included — the ALB
target group's own health check path had to move from
`/actuator/health` to `/orders/actuator/health` (etc.) in the SAME
change, or the fix for the app's routing would have broken its own
health checks.

### Recovery: a stalled `CREATE_IN_PROGRESS`, and the delete-and-recreate technique

After redeploying the context-path fix, `cdk deploy
OrderFlowAppServicesStack` immediately failed with `Stack ... is in
CREATE_IN_PROGRESS state and can not be updated` — the STACK's
original `CREATE`, from before the ENI/quota saga above, had never
actually reached `CREATE_COMPLETE`. `aws cloudformation
describe-stack-events` showed why: the last event was **nearly 3 hours
old**, with zero further activity, even though the underlying ECS
services were (by then) genuinely running fine. CloudFormation's own
stabilization polling for this stack had stalled completely, not just
slowly — a different failure mode from every "stuck but still
progressing" case seen earlier in this project.

Since the underlying issue (capacity/ENI/quota) was already fixed and
the stack couldn't accept an UPDATE in this state, `aws cloudformation
delete-stack` was the way out — and, unlike Phase 3's "only works if
caught EARLY, before expensive resources exist" finding, this delete
worked cleanly on a FULLY-built stack (78 resources, ECS services,
ALB, target groups, all included) specifically because CloudFormation's
DELETE path doesn't depend on whatever made the original CREATE's
polling stall. The deletion itself was fast and genuinely progressing
(fresh timestamps throughout, ECS services gone within ~2 minutes, ALB
gone shortly after) — confirming the underlying resources were healthy
all along; only CloudFormation's own bookkeeping for THIS stack had
gotten stuck. A fresh `cdk deploy` from a clean slate, with every fix
already baked into the CDK source from the start, completed cleanly in
**268.69s** — a useful, honest contrast against the many hours the
original, trouble-filled attempt took.

### Verified live: a real order, placed through a public URL, completing the full choreography saga on AWS

```bash
$ curl -s -i -X POST http://OrderF-Alb16-ptcO3kocwRrO-1495158311.us-east-1.elb.amazonaws.com/orders/api/orders \
    -H "Content-Type: application/json" \
    -d '{"customerId":"cust-cloud-verify-1","region":"us-east","items":[{"productId":"sku-42","quantity":2}]}'
HTTP/1.1 202
{"orderId":"949337fd-b08b-4287-9e4e-28032fa78ae7","status":"ACCEPTED"}
```

202 Accepted, a real `orderId` — the request crossed the ALB, matched
the `/orders/*` rule, landed at `order-service`'s
`server.servlet.context-path`-corrected `/orders/api/orders` mapping,
and was written into Postgres via the outbox pattern. But the REAL
proof this phase exists for is what happened next, entirely
asynchronously, visible only in `order-service`'s own logs — its
`OrderStatusUpdater` (a Kafka consumer reacting to the saga's own
events, publishing into the log-compacted `order-status` topic) shows
the order transitioning through all 3 real downstream states, produced
by 3 DIFFERENT services running on 3 different ECS tasks, in under one
second:

```
2026-08-08T09:19:05.381Z  📝 order-status[949337fd-...] -> RESERVED
2026-08-08T09:19:05.732Z  📝 order-status[949337fd-...] -> PAID
2026-08-08T09:19:06.288Z  📝 order-status[949337fd-...] -> SHIPPED
```

`RESERVED` only happens if `inventory-service` genuinely consumed the
`order-created` event, checked/reserved real stock in the shared
Postgres database, and published `inventory-reserved`. `PAID` only
happens if `payment-service` consumed THAT event and published
`payment-completed`. `SHIPPED` only happens if `shipment-service`
consumed THAT event and published `shipment-created`. This is the
entire choreography saga — order-service, inventory-service,
payment-service, shipment-service, all independently deployed, all
talking only through the 3-broker Kafka cluster Phase 5a built —
running correctly, end-to-end, on AWS, for the first time in this
project's history.

---

## 🎓 Concepts learned in Phase 5c

1. **A path-based ALB listener rule matches a pattern to pick a target
   group — it does not rewrite or strip that prefix.** Whatever path
   pattern matched is exactly what the backend receives; if the
   backend's own routes don't already expect that prefix, the fix has
   to happen on the APPLICATION side (a context path, a route alias),
   not by expecting the load balancer to do it.
2. **`awsvpc`-mode ECS tasks are capped by the underlying EC2 instance
   TYPE's network interface limit, independent of CPU/memory
   headroom.** A small instance can show plenty of free memory and
   still refuse to place a task with `RESOURCE:ENI` — a genuinely
   different failure mode from "insufficient memory," diagnosable only
   by reading the actual placement-failure message rather than
   assuming it's another capacity issue.
3. **A feature "being available" (an attribute, a capability flag) is
   not the same as it "changing observed behavior."** ENI trunking
   showed up as an advertised container-instance capability without
   empirically raising the actual task-placement ceiling — worth
   verifying the OUTCOME (can more tasks actually place?), not just the
   presence of the feature flag.
4. **EC2 vCPU service quotas are shared across broad instance-family
   buckets, not per instance type.** "Running On-Demand Standard (A, C,
   D, H, I, M, R, T, Z) instances" covers nearly every
   general-purpose/compute/memory/burstable family — switching
   instance TYPES within that bucket doesn't dodge the quota; only a
   fundamentally different category (GPU, FPGA) or an actual quota
   increase request does.
5. **A resource stuck "Terminating" can still count against capacity
   limits that block its own replacement** — a real deadlock pattern
   worth recognizing: if nothing is progressing and every party is
   waiting on something else, look for a resource that's simultaneously
   consuming a limited pool AND blocked from releasing it.
6. **CloudFormation's own stabilization polling can stall completely,
   independent of the underlying resources' actual health.** A stack
   stuck `CREATE_IN_PROGRESS` for hours with zero new events, while the
   real infrastructure underneath is fine, is a different failure mode
   from "slow but still working" — recognizable by checking event
   TIMESTAMPS, not just the current status string.
7. **`delete-stack` can be a clean recovery path even on a FULLY-BUILT
   stack**, not just an early, cheap `CREATE_IN_PROGRESS` — CloudFormation's
   DELETE path is independent of whatever made a specific stack's
   CREATE polling get stuck.

## 🧠 Interview Q&A

**Q: You configure an ALB listener rule to route `/api/v2/*` to a
service. The service's own routes are defined WITHOUT that prefix
(just `/users`, `/orders`, etc.). What happens, and how do you fix
it?**
A: Every request matching that rule arrives at the backend with the
FULL original path still intact — `/api/v2/users`, not `/users` — since
ALB has no path-rewriting capability; it only uses the path to DECIDE
which target group gets the request. The backend will 404 on every
request unless it's told to expect that prefix. The fix has to happen
on the application side: either configure the app to serve everything
under that prefix (a context path, e.g. Spring Boot's
`server.servlet.context-path`, or an equivalent base-path setting in
another framework), or design routes that already include the prefix
from the start.

**Q: An ECS task fails to place with `RESOURCE:ENI`, even though the
target instance shows plenty of free CPU and memory. What's actually
happening?**
A: `awsvpc` network mode gives every task its own Elastic Network
Interface, and EC2 instance TYPES have a hard maximum number of ENIs
they can attach — a small instance might support only 2-3 total,
leaving very few free for tasks after the instance's own primary ENI.
This is a completely separate constraint from CPU/memory reservations;
an instance can have gigabytes of free memory and still refuse a new
task purely because it's out of network interfaces. The fix is either
a larger instance type, more instances, or (where actually effective)
ECS's ENI trunking feature — verified empirically, not just assumed to
be working from the presence of a capability flag.

**Q: How would you find out whether an ECS/EC2 capacity problem is
really an account-level service quota rather than something in your
own configuration?**
A: Check the actual EC2/Auto Scaling API error message first — AWS
service quota errors are usually explicit ("you have requested more
vCPU capacity than your current limit of N allows"), not silent. Cross-
reference with `aws service-quotas get-service-quota` for the specific
quota code the error names. It's also worth knowing these quotas are
often shared across broad instance-family "buckets" (e.g., one quota
covering most general-purpose/compute/memory/burstable families
together) — switching to a "different" instance type doesn't help if
it's in the same bucket, which is a common trap when debugging this
kind of failure.

**Q: A CloudFormation stack has been `CREATE_IN_PROGRESS` for hours.
How do you tell whether it's still making genuine progress or
permanently stuck?**
A: Check `describe-stack-events` and look at the TIMESTAMPS of the most
recent events, not just the current status string — a stack that's
still actively working shows a steady trickle of new events; one whose
last event is hours old with nothing since is stalled, regardless of
what the top-level status says. For a stuck CREATE that can't accept an
UPDATE, `delete-stack` is a legitimate recovery path even on a
fully-built stack with real resources already created — CloudFormation's
delete path doesn't depend on whatever made the original create's
polling get stuck, so a fresh create afterward often succeeds cleanly
and quickly, especially once the root cause (in this case, capacity)
has already been fixed in the underlying template.

---

## Phase 6: killing a Kafka broker for real — the actual payoff of this entire deployment

Every phase before this one existed to make this moment possible.
`docs/system-design.md`'s own "Known gaps" section named it plainly:
*"ISR / leader election / broker-failure simulation is not
demonstrated... needs a 3-broker cluster."* Phase 5a built that
cluster. This phase kills one of its 3 brokers — for real, by
terminating the underlying EC2 instance, not by stopping a container
gracefully — and captures exactly what happens next, including the
parts that didn't go smoothly. Left in this document deliberately: the
messy parts are more honest, and more useful, than a demo edited down
to only the clean parts.

### The setup: kill the CURRENT LEADER, not just any broker

Before touching anything, the current KRaft controller leader was
identified directly from each broker's own logs — `kafka-1`, epoch
142, agreed by all 3 brokers. Killing a FOLLOWER would only prove
"a replica can disappear painlessly"; killing the LEADER forces an
actual, real election, the more convincing and more honest test:

```bash
$ aws ec2 terminate-instances --instance-ids i-0a4a69013fa84ef9f   # kafka-1's EC2 instance
```

A test order was placed through the ALB seconds later, DURING the
outage window, specifically to observe what a real client experiences
while a broker is actively dying — not before or after, but during.

### Verified live: server-side Raft recovery in 6 seconds

Both surviving brokers' own independent logs, cross-checked against
each other (not just trusting one broker's self-report):

```
# kafka-2's own log:
[09:27:09,581] Completed transition to CandidateState(localId=2, epoch=143, ...) from FollowerState(..., epoch=142, leader=kafka-1...)
[09:27:09,597] Completed transition to Leader(localId=2, epoch=143, ...)

# kafka-3's own log, same moment, independently:
[09:27:09,591] Completed transition to Unattached(epoch=143, ...) from FollowerState(..., epoch=142, leader=kafka-1...)
[09:27:09,596] Completed transition to Voted(epoch=143, votedKey=ReplicaKey(id=2, ...), ...)
[09:27:09,612] Completed transition to FollowerState(..., epoch=143, leader=kafka-2.orderflow.local:9093, ...)
```

Both survivors independently detected `kafka-1`'s disappearance,
elected `kafka-2`, and agree on the exact same epoch and leader — a
genuine, cross-verified Raft election recovering from a real node
failure in **6 seconds**. This is the architectural claim Phase 5a made
and this phase proves: the 3-broker design genuinely tolerates losing
1 node.

### A real, honest finding: server recovery ≠ client recovery

The test order placed during the outage got a `202 Accepted` from
`order-service`'s REST layer immediately — but its actual Kafka
publish, inside `OutboxRelay`'s transactional producer, started
repeatedly failing:

```
2026-08-08T09:28:14Z ERROR ... Failed to publish outbox row id=2 orderId=767a7707-...: Timeout expired after 60000ms while awaiting EndTxn(true)
2026-08-08T09:29:14Z ERROR ... Failed to publish outbox row id=2 orderId=767a7707-...: Timeout expired after 60000ms while awaiting EndTxn(true)
```

The reason: this specific producer's transaction COORDINATOR had been
assigned to `kafka-1` before it died. Kafka's transactional producers
pin to a specific coordinator broker at initialization; that pinning
doesn't automatically follow a Raft election the way the underlying
metadata/replication layer does — the client has to notice the
coordinator is gone and re-discover a new one, which costs a full
`transaction.timeout.ms` (60s here) per stuck attempt, not 6 seconds.
Separately, `order-service`'s own status-update CONSUMER kept trying to
reach "node 1" and got `DisconnectException` for a while too — a
second, related symptom of the same underlying fact: CLIENT-side
connection/metadata caches don't refresh instantly just because the
SERVER-side cluster already recovered. This is a genuinely important,
easy-to-miss distinction: "the cluster is healthy again" and "every
client has noticed" are different claims, on different timescales.

### The replacement instance's own recovery hit real, compounding operational problems

Getting `kafka-1` itself back — not just electing around its absence —
took far longer than 6 seconds, for reasons that had NOTHING to do with
Kafka or Raft:

1. **The same stuck lifecycle-hook deadlock from Phase 5c, again.** The
   dead instance sat in `Terminating:Wait`, still consuming the
   account's EC2 quota, blocking its own replacement — required the
   same `aws autoscaling complete-lifecycle-action --lifecycle-action-result
   CONTINUE` fix already documented in Phase 5c.
2. **The exact same 16 vCPU account quota from Phase 5c**, now blocking
   KAFKA's OWN replacement instance instead of an app-tier one. Between
   09:27 and 10:04 — **37 minutes** — repeated launch attempts failed,
   oscillating between the vCPU quota error and one transient
   "instance type not supported in this Availability Zone" error.
3. **Breaking through required trading one outage for a smaller one.**
   To free a vCPU slot, the app-tier pool was temporarily shrunk from
   6 to 5 instances — a user-authorized, deliberately disruptive
   action (blocked once, correctly, by this environment's own
   auto-mode safety classifier, since it stops 2 currently-running
   services; proceeded only after explicit confirmation). This ITSELF
   triggered 2 more stuck `Terminating:Wait` deadlocks on the app-tier
   side, each needing the same forced-completion fix.

`kafka-1`'s replacement instance finally launched at **10:04:01** — 37
minutes after the kill. The new broker (a genuinely fresh node, new
directory ID, nothing carried over from the dead one) rejoined the
SAME epoch-143 quorum as a follower of `kafka-2`, confirmed in its own
logs:

```
[10:05:23,859] Completed transition to FollowerState(..., epoch=143, leader=kafka-2.orderflow.local:9093, ...)
[10:05:25,481] Kafka Server started (kafka.server.KafkaRaftServer)
```

Full 3-broker health restored. The stuck test order's outbox row
finally published successfully 4 seconds after one last timeout:

```
2026-08-08T10:05:38Z ERROR ... Timeout expired after 60000ms while awaiting EndTxn(true)
2026-08-08T10:05:42Z  INFO ... 📤 Relayed outbox row id=2 -> order-created + order-status for orderId=767a7707-...
```

**Total elapsed time for this one stuck message to get through: ~38.5
minutes** — almost all of it capacity/quota churn, essentially none of
it Kafka itself (which, again, recovered in 6 seconds).

### The fix broke something else: a real "one outage causes another" moment

The temporary app-tier shrink (6 → 5 instances) had its own delayed
side effect. 5 instances × 2 ENI-limited task slots = 10 slots for 12
app-tier services — 2 services (`inventory-service` and
`elasticsearch`) got bumped and had nowhere to reschedule. This
directly blocked the STUCK ORDER's actual saga completion even after
its Kafka message got through: the message existed, but
`inventory-service` — the thing that actually reserves stock and
advances the saga — wasn't running to consume it.

### A real CloudFormation lesson, found live: `cdk deploy` reporting "no changes" does not correct drift

Restoring app-tier back to 6 via `cdk deploy OrderFlowEcsClusterStack`
reported `(no changes)` and deployed in 24 seconds — but the ASG's
actual live `desiredCapacity` stayed at 5. The reason: `desiredCapacity`
had been changed OUTSIDE of CloudFormation, via a direct `aws
autoscaling update-auto-scaling-group` call (the mechanism used to free
the vCPU slot above). CloudFormation's diffing compares the TEMPLATE
against its own last-applied state, not against the live resource's
actual current value — since the template genuinely hadn't changed
since the last real deploy, CloudFormation found nothing to do and
never re-asserted the property, even though the live resource had
drifted underneath it. This is a distinct, real lesson from anything
found earlier in this project: infrastructure-as-code only overwrites
drift when the CODE itself changes, not automatically on every deploy.
The direct fix — another `aws autoscaling update-auto-scaling-group`
call — corrected it immediately, but the CODE-level desired state
(`minCapacity/maxCapacity/desiredCapacity: 6` in `ecs-cluster-stack.ts`)
was never wrong; only the LIVE resource had silently diverged from it.

### Honest final state, as of writing this section

- **Kafka: fully proven, fully healthy.** All 3 brokers running,
  cross-verified quorum, genuine failover demonstrated end-to-end. This
  was the actual goal of this phase, and it is unambiguously achieved.
- **App-tier: partially recovered, genuinely still open.** The 6th
  app-tier instance has been retrying against the SAME 16 vCPU quota
  for over 50 minutes without resolving — noticeably longer than the
  two earlier times this exact quota wall cleared on its own (a few
  minutes in Phase 5c, 37 minutes for Kafka's own replacement above).
  `inventory-service` and `elasticsearch` remain down; the other 10 of
  12 app-tier services are healthy. This is being left as an honestly
  unresolved open item rather than papered over — a real illustration
  of running a real workload right at a tight account quota's edge with
  zero slack for even one unplanned replacement, let alone two
  overlapping ones.

---

## 🎓 Concepts learned in Phase 6

1. **Killing the current LEADER, not an arbitrary follower, is the
   more convincing failure test** — it's the only way to actually
   observe a real election happening, not just a replica's quiet
   disappearance.
2. **Cross-checking independent nodes' own logs, not just one node's
   self-report, is what makes a distributed-system claim verified
   rather than assumed** — this project's whole KRaft verification
   methodology (Phase 5a and here) leans on this deliberately.
3. **Server-side consensus recovery and client-side recovery happen on
   genuinely different timescales.** A Raft election can complete in
   single-digit seconds while a specific client (a pinned transaction
   coordinator, a stale connection) takes tens of seconds to tens of
   minutes to notice and recover — "the cluster is healthy" and "every
   caller has noticed" are different, separately-verifiable claims.
4. **Kafka transactional producers pin to a specific coordinator
   broker** — if that broker dies mid-transaction, the client doesn't
   instantly fail over; it times out (`transaction.timeout.ms`) before
   retrying against a newly-discovered coordinator. A real, concrete
   cost of exactly-once semantics under a real broker failure, not
   just a config knob.
5. **A capacity fix applied under pressure can create a second,
   smaller outage** — freeing quota by shrinking one pool to grow
   another is a real, sometimes-necessary trade-off, but it has
   consequences that need tracking down separately, not assumed away
   once the ORIGINAL problem is fixed.
6. **Infrastructure-as-code does not self-heal drift on every deploy —
   only when the CODE changes.** A live resource manually changed
   outside the IaC tool can silently persist through a `(no changes)`
   deploy, even though the tool's own template has always said
   something different. Detecting this requires checking the LIVE
   resource state directly, not trusting a clean `cdk deploy` output as
   proof the live infrastructure matches the code.
7. **An honestly-reported "still not fully resolved" is more valuable
   than a quietly-omitted one.** The core architectural claim (Kafka
   survives a broker failure) is fully proven; a separate, genuinely
   still-open operational issue (account quota exhaustion under
   compounding capacity pressure) is reported as such, not hidden
   behind a "everything's fine now" summary.

## 🧠 Interview Q&A

**Q: You want to demonstrate that your Kafka cluster tolerates a
broker failure. Would you kill a random broker, or does it matter
which one?**
A: It matters — killing the current controller/partition leader is the
more rigorous test, because it forces an actual leader election to
happen, observable and verifiable in the surviving brokers' own logs.
Killing an arbitrary follower only proves a replica can disappear
without the cluster crashing, which is a weaker claim than "the cluster
correctly elects a new leader and continues serving requests."

**Q: After a Kafka broker dies and a new leader is elected within
seconds, a client-side operation (a transaction, a consumer) is still
failing for several minutes. Is your failover broken?**
A: Not necessarily — broker-side Raft/ISR recovery and client-side
recovery are genuinely different processes on different timescales.
Kafka clients cache connections, broker metadata, and (for
transactional producers) a specific coordinator assignment; none of
that is guaranteed to refresh instantly just because the underlying
cluster already recovered. The right question isn't "why hasn't this
client recovered yet" in isolation — it's whether the client eventually
recovers within its own configured timeouts (here, `transaction.timeout.ms`),
which is a real, tunable trade-off, not a bug.

**Q: You fixed a capacity problem by shrinking one resource pool to
free room for another. What's the risk in that approach?**
A: The freed-up pool may not have had slack to spare — shrinking it
can bump whatever was already running there, creating a SECOND,
smaller-scope outage while fixing the first. The fix isn't "don't do
this" (sometimes it's the only lever available under a hard external
constraint like an account quota); it's to explicitly check what got
displaced afterward, rather than assuming the original problem being
fixed means everything is fine again.

**Q: You changed a live AWS resource's setting directly via the AWS
CLI, then ran your IaC tool's deploy command expecting it to restore
the "correct" value from your code. It reported no changes and did
nothing. Why?**
A: Most IaC tools (CloudFormation included) diff the TEMPLATE against
their own LAST-APPLIED state, not against the resource's actual live
value in the cloud. If the template hasn't changed since the last real
deploy, the tool has nothing new to apply and won't re-touch a resource
just because it might have drifted underneath it. Detecting this kind
of drift needs either an explicit drift-detection feature (CloudFormation
has one) or checking the live resource directly — a clean, "no
changes" deploy is not proof the live infrastructure still matches the
code.
process's possibly-stale opinion.
