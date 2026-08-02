# inventory-service

Through Build Order Step 7, a pure Kafka consumer — no REST API of its
own. Step 8 changes that on two fronts: it now actually PUBLISHES its
reservation decision (`inventory-reserved` / `inventory-failed`) instead
of only logging it, compensates by releasing stock when it hears a
downstream `payment-failed`, and exposes its first-ever REST endpoints —
the orchestration saga's entry point into this exact same stock logic.

Build Order Step 9 replaces the in-memory stock map — flagged as a
placeholder since Step 1 — with Postgres as the real source of truth, a
Redis cache-aside layer in front of it, a Redis distributed lock that
FIXES the overselling race condition (verified live across two real
running instances), and a Redis-backed idempotent-consumer dedupe store
closing the "redelivery can double-process" gap flagged since Step 1 too.

## Deep dives

More get added here as the tutorial goes deeper on specific topics —
check back after later Build Order steps.

- [`docs/consumer-group-lifecycle.md`](docs/consumer-group-lifecycle.md) —
  the full lifecycle (spin up → scale out → instance dies → instance
  rejoins), staged, with every protocol-level component involved
  (heartbeats, coordinator, JoinGroup/SyncGroup, generations, offset
  semantics), plus an interview-style Q&A designed to catch the
  misconceptions this stuff tends to hide.
- [`docs/avro-basics.md`](docs/avro-basics.md) — Avro from zero: what it
  actually is, the wire format, Schema Registry's role, the full
  producer/consumer flow — annotated line-by-line against a real captured
  log from a poison-message run (WARN about a non-existent partition,
  the lazily-created retry producer, the `KafkaAvroSerializerConfig`
  dump, all of it decoded).
- [`docs/avro-flow-visual.md`](docs/avro-flow-visual.md) — the calmer
  companion to the above: one simple happy-path order, drawn out step by
  step with ASCII diagrams, plus a 3-thing mental model for keeping the
  whole producer/registry/consumer relationship straight in your head.

## What this module demonstrates

| Concept | Where | Trade-off |
|---|---|---|
| Consumer groups (fan-out vs. scale-out) | `consumer/OrderEventListener.java` | Same `group-id` across instances splits partitions between them (scale out); different `group-id`s each get an independent full copy (fan-out) — the mechanism every future consumer of this topic relies on |
| Manual offset commit (`AckMode.MANUAL_IMMEDIATE`) | `config/KafkaConsumerConfig.java` | Precise control over exactly when "processed" is recorded, at the cost of writing the ack call yourself instead of trusting a timer |
| At-least-once delivery | `consumer/OrderEventListener.java` | A crash before `acknowledge()` means safe redelivery, not silent loss — but the same message CAN be processed twice |
| The overselling race condition — FIXED in Build Order Step 9 | `service/StockService.java`, `lock/DistributedLock.java` | Through Step 8, reproducible: each instance's map was genuinely separate, so two instances could both "win" a reservation for the last unit. Now every instance shares the SAME Postgres `stock` table AND the check-then-write is wrapped in a Redis `SET NX PX` distributed lock — verified live across two real instances: 10 concurrent requests against 5 units of stock landed on exactly 0, never negative, with one of the 5 rejections decided by a genuinely separate JVM |
| Cache-aside (Postgres + Redis) | `service/StockService.java` | Reads check Redis first, populate it on a miss from Postgres; writes go to Postgres and INVALIDATE the cache rather than trying to update it in place — the cost is one guaranteed cache miss right after every write, traded for never needing two code paths to agree on "the new value" |
| Distributed lock (`SET key value NX PX ttl` + Lua compare-and-delete) | `lock/DistributedLock.java` | A single-Redis-instance simplification of real Redlock (which coordinates a MAJORITY of independent Redis nodes specifically to survive one crashing) — a real, documented trade-off: this Redis instance is now a single point of failure for reservations, not a free upgrade |
| Idempotent-consumer dedupe store | `idempotency/IdempotencyStore.java` | A TTL-backed Redis marker keyed by orderId, checked BEFORE any reservation logic runs and set AFTER it fully completes — closes the exact gap Step 1's version of `OrderEventListener` flagged as unaddressed: a redelivered message now short-circuits instead of double-decrementing stock. Verified live with a REAL Kafka consumer-group offset reset forcing genuine redelivery, not a simulated retry |
| **Rebalancing, made visible** (`ConsumerAwareRebalanceListener`) | `config/KafkaConsumerConfig.java` | `onPartitionsAssigned`/`Revoked`/`Lost` cost nothing at runtime — pure observability — but distinguishing a graceful REVOKED from an ungraceful LOST is exactly what you'd alert on in production |
| Per-instance `client.id` (`${random.uuid}`) | `application.yml`, `InventoryServiceApplication.java` | Free instance identity for logs/Kafka UI once you scale out; zero config to remember per terminal |
| Static group membership (`group.instance.id`) — discussed, not enabled | `config/KafkaConsumerConfig.java` | Would suppress rebalancing on a routine restart (good for rolling deploys) but ALSO suppresses the rebalance Step 2 wants you to see — left off by default, exercise below shows the contrast |
| Avro consumer + `SPECIFIC_AVRO_READER_CONFIG` (replaces `TRUSTED_PACKAGES`/`VALUE_DEFAULT_TYPE`) | `config/KafkaConsumerConfig.java` | Typed generated class back instead of a raw `GenericRecord`; no Jackson-style class-instantiation attack surface to close because messages carry a schema ID, never a class name |
| Live schema evolution (verified, not simulated) | `/avro-schemas/order-created.avsc`, root README | This exact module, running with its OLD compiled Avro class, correctly processed a message shaped by a NEWER schema — zero code change, zero restart, because Avro schema resolution just ignores fields the reader doesn't ask for |
| Retry topics + Dead Letter Topic (`@RetryableTopic`) | `consumer/OrderEventListener.java` | One poison message no longer blocks its whole partition forever — instead it hops to dedicated retry topics with exponential backoff, then a DLT — at the cost of extra auto-created topics AND, we found by testing, extra Schema Registry subjects (one per retry/DLT topic) |
| `@DltHandler` — the "what happens when it lands in DLT" answer | `consumer/OrderEventListener.java` | Build Order Step 11 wired real alerting on top of this exact hook: a Micrometer `Counter` (`inventory.dlt.messages`) increments right alongside the log line, giving Prometheus/Grafana something to actually query and alert on instead of a log line nobody's necessarily watching |
| Hand-built `ConsumerFactory` silently opts out of automatic observability | `config/KafkaConsumerConfig.java` | Found live, Build Order Step 11: this service's consumer-lag Prometheus metrics were completely absent, while every other service (relying on Spring Boot's auto-configured `ConsumerFactory`) got them for free. `@ConditionalOnMissingBean` backs off Spring Boot's automatic `MicrometerConsumerListener` wiring the instant this class defines its OWN `ConsumerFactory` bean (done since Step 2, for AckMode control) — fixed with one explicit `addListener(...)` call, but the silent gap itself is the real lesson: a hand-built bean opts out of EVERYTHING Spring Boot would otherwise wire up automatically, not just the one thing you meant to control |
| Local compensation, inside one method | `consumer/OrderEventListener.java` | A partial reservation (some items reserved, one fails) rolls back everything it already reserved, via `StockService.release()`, BEFORE publishing `inventory-failed` — cheap because it's all in-process, no cross-service call needed |
| Cross-service compensation, via a listener | `consumer/PaymentFailedCompensationListener.java` | Reacts to `payment-failed` — a fact published for an unrelated reason (payment-service telling the world what happened) — by releasing the stock reserved for that order. Nobody told this service to compensate; it just happens to be listening |
| This service's first REST endpoints | `internal/InternalReservationController.java` | `POST /internal/reserve` / `POST /internal/release` reuse the EXACT SAME `StockService` the Kafka listener uses — the orchestration saga's entry point into this service, with no separate/duplicated business logic |
| Real event publishing (was log-only through Step 7) | `consumer/OrderEventListener.java` | `inventory-reserved` / `inventory-failed` are now genuine Kafka events, keyed by `customerId` (matching `order-created`'s key, deliberately, after Step 6's partition-key regression precedent) — `payment-service` and `order-service`'s `OrderStatusUpdater` both consume them |
| A REST endpoint costs the "zero-config multi-instance" story | `application.yml` | Step 2's "run this same jar again, zero config" promise held for the Kafka-consumer side forever — but a synchronous REST entry point always needs SOME single address, so two instances would now collide on port 8086. The Kafka consumer side scales exactly as it always did; only the NEW synchronous side picked up this constraint |

## Try the hands-on exercises

Recommended order — this is Build Order **Step 2**, so this time actually
run it multi-instance:

1. Bring up infra + order-service (see root README), then start
   `inventory-service` **twice**, in two terminals:
   `mvn spring-boot:run`. Watch each terminal's identity banner
   (`InventoryServiceApplication.java`) print a different `client.id`.
2. Place a handful of orders with different `customerId`s (the partition
   key). Watch each instance's 🔀 `ASSIGNED` log line — between the two
   instances, all 3 partitions of `order-created` are covered, with no
   overlap. Confirm the split in Kafka UI (`http://localhost:8081` →
   Consumers → `inventory-service-group`).
3. Start a **third** instance. Watch both existing instances log 🔀
   `REVOKED (graceful)` for some of their partitions, then all three log
   `ASSIGNED` again with the 3 partitions now spread across three owners.
4. Find one instance's OS process id and `kill -9` it (not Ctrl-C — a hard
   kill, simulating a crash). Watch a SURVIVING instance log 🔀
   `REVOKED (graceful)` then 🔀 `ASSIGNED` with the dead instance's
   partitions folded back in — **not** `LOST`. That's not a typo: we
   assumed it WOULD log `LOST` until we actually ran this experiment.
   `onPartitionsLost` fires on a client's own consumer when THAT client
   gets fenced out before it can revoke cleanly — it's not a notification
   survivors get about a peer's death. See `KafkaConsumerConfig`'s
   `onPartitionsLost` Javadoc for how to trigger a genuine one (hint:
   make an instance's OWN listener stall past `max.poll.interval.ms`).
   Place an order that would have gone to the dead instance's old
   partitions and confirm it's still processed — no message loss, just a
   brief reassignment delay. Then bring that instance back up and watch
   it rejoin — for the full staged breakdown of both directions (die AND
   rejoin) plus what each log line actually means, see
   [`docs/consumer-group-lifecycle.md`](docs/consumer-group-lifecycle.md).
5. `StockService.java` — hammer `sku-7` (stock=5) with concurrent orders
   of quantity 5, confirm only one wins.
6. Build Order **Step 4**: place an order for productId `sku-poison` (see
   `OrderEventListener`'s `POISON_PRODUCT_ID`) and watch your own logs
   walk through the main topic, then `retry-0`, `retry-1`, `retry-2`, each
   attempt further apart (~1s, ~2s, ~4s), then `💀 DEAD LETTER` from
   `onDeadLetter`. While that's happening, place a NORMAL order too and
   confirm it processes immediately — the poison message isn't blocking
   anything else. Then run `docker exec orderflow-kafka
   /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092
   --list` and `curl -s http://localhost:8085/subjects` to see the
   auto-created topics and their (surprisingly numerous) schema subjects
   for yourself.
7. Per `KafkaConsumerConfig.java`'s TRY IT YOURSELF: add
   `GROUP_INSTANCE_ID_CONFIG`, then Ctrl-C and restart ONE instance
   normally. Confirm no rebalance fires this time — contrast with step 4.
8. Build Order **Step 3**: follow the root README's schema evolution
   walkthrough. Start this service, then — WITHOUT restarting it — add a
   field to `/avro-schemas/order-created.avsc`, rebuild only order-service,
   and place an order. Watch THIS already-running instance (still holding
   its old-compiled Avro class) process the new-shaped message without
   any error. Then try the root README's breaking-change test directly
   against Schema Registry's `/compatibility` endpoint — no restart
   needed for that one at all.
9. **Build Order Step 8**: follow the root README's choreography
   walkthrough — place an order, confirm `📥 inventory-reserved`
   publishes. Then trigger a payment decline (26 x `sku-42`) and watch
   `PaymentFailedCompensationListener` fire `↩️  Released 26 x sku-42` —
   confirm via a second reservation attempt that the stock genuinely came
   back (or just re-run the same order and watch it succeed again).
10. Call `POST localhost:8086/internal/reserve` directly with curl,
    bypassing Kafka entirely — confirm it's the exact same `StockService`
    the listener uses, just reached synchronously (this is exactly what
    `order-saga-orchestrator` does).
11. **Build Order Step 9, the overselling fix, proven not asserted**:
    start a SECOND instance on a different REST port
    (`-Dspring-boot.run.arguments="--server.port=8090"`), then fire 10
    concurrent `POST /api/orders` for `sku-7` (stock=5) across different
    `customerId`s (a bash loop with `&` and `wait`, see root README).
    Confirm via `SELECT quantity FROM stock WHERE product_id='sku-7'`
    that it lands on exactly 0, never negative, and that BOTH instances'
    logs together show exactly 5 `📦 Reserved` and 5 `❌ Insufficient
    stock` lines.
12. Cache-aside, made visible: restart with
    `-Dspring-boot.run.arguments="--logging.level.com.orderflow.inventory.service.StockService=DEBUG"`,
    place an order for a product whose cache entry doesn't exist yet, and
    watch `❌ Cache MISS ... reading Postgres` in your own logs, then
    place another order for the SAME product and watch `🎯 Cache HIT`
    instead — no Postgres query the second time.
13. Idempotent-consumer dedupe, proven with a REAL redelivery: place an
    order, let it fully process, then reset THIS consumer group's offset
    back by one on the partition that order landed on
    (`kafka-consumer-groups.sh --group inventory-service-group --topic
    order-created:N --reset-offsets --shift-by -1 --execute`, group must
    be inactive — stop this service first) and restart. Watch `♻️  ...
    already processed ... skipping` fire instead of a second reservation,
    and confirm stock is unchanged.

## What's deliberately NOT here yet

- The DLT handler only logs — no real alerting/incident-tracking
  integration yet (Build Order Step 11)
- `InternalReservationController`'s endpoints have no auth/network
  restriction — in a real deployment, "internal" REST endpoints like these
  would sit behind a service mesh or network policy, not be reachable from
  wherever `order-saga-orchestrator`'s `base-url` config happens to point
- The distributed lock is single-Redis-instance, not real Redlock — see
  `DistributedLock.java`'s Javadoc for exactly what that trade-off means
  (this Redis instance is a single point of failure for reservations now)
- No cache-stampede protection — if a hot product's cache entry expires
  (or is invalidated by a write) at the exact moment many concurrent
  reads arrive, all of them miss and hit Postgres simultaneously rather
  than one request repopulating the cache while others wait; the
  distributed lock happens to serialize WRITES to the same product but
  doesn't currently do anything for a stampede of pure reads on
  DIFFERENT products
- No Flyway/Liquibase for `schema.sql` — same deliberate simplification
  order-service made in Build Order Step 5, now repeated here for the new
  `stock` table
