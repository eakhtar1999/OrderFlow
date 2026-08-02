# inventory-service

Through Build Order Step 7, a pure Kafka consumer — no REST API of its
own. Step 8 changes that on two fronts: it now actually PUBLISHES its
reservation decision (`inventory-reserved` / `inventory-failed`) instead
of only logging it, compensates by releasing stock when it hears a
downstream `payment-failed`, and exposes its first-ever REST endpoints —
the orchestration saga's entry point into this exact same stock logic.

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
| The overselling race condition | `service/StockService.java` | Deliberately reproducible today: works fine single-instance, breaks the moment you run two instances against "shared" state (each instance's map is actually separate — an even more obvious version of the bug that Redis + a distributed lock fixes in Build Order Step 9) |
| **Rebalancing, made visible** (`ConsumerAwareRebalanceListener`) | `config/KafkaConsumerConfig.java` | `onPartitionsAssigned`/`Revoked`/`Lost` cost nothing at runtime — pure observability — but distinguishing a graceful REVOKED from an ungraceful LOST is exactly what you'd alert on in production |
| Per-instance `client.id` (`${random.uuid}`) | `application.yml`, `InventoryServiceApplication.java` | Free instance identity for logs/Kafka UI once you scale out; zero config to remember per terminal |
| Static group membership (`group.instance.id`) — discussed, not enabled | `config/KafkaConsumerConfig.java` | Would suppress rebalancing on a routine restart (good for rolling deploys) but ALSO suppresses the rebalance Step 2 wants you to see — left off by default, exercise below shows the contrast |
| Avro consumer + `SPECIFIC_AVRO_READER_CONFIG` (replaces `TRUSTED_PACKAGES`/`VALUE_DEFAULT_TYPE`) | `config/KafkaConsumerConfig.java` | Typed generated class back instead of a raw `GenericRecord`; no Jackson-style class-instantiation attack surface to close because messages carry a schema ID, never a class name |
| Live schema evolution (verified, not simulated) | `/avro-schemas/order-created.avsc`, root README | This exact module, running with its OLD compiled Avro class, correctly processed a message shaped by a NEWER schema — zero code change, zero restart, because Avro schema resolution just ignores fields the reader doesn't ask for |
| Retry topics + Dead Letter Topic (`@RetryableTopic`) | `consumer/OrderEventListener.java` | One poison message no longer blocks its whole partition forever — instead it hops to dedicated retry topics with exponential backoff, then a DLT — at the cost of extra auto-created topics AND, we found by testing, extra Schema Registry subjects (one per retry/DLT topic) |
| `@DltHandler` — the "what happens when it lands in DLT" answer | `consumer/OrderEventListener.java` | Just a log line here; a real deployment pages someone or opens an incident — Build Order Step 11 wires that up on top of this exact hook |
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

## What's deliberately NOT here yet

- No real database — stock is an in-memory map that resets on restart and
  is NOT shared across instances (Build Order Step 9 fixes both with
  Postgres + Redis) — this also means the overselling race condition
  above is now reachable via TWO different paths (concurrent Kafka
  consumers, or a concurrent REST call racing a Kafka consumer), neither
  fixed yet
- The DLT handler only logs — no real alerting/incident-tracking
  integration yet (Build Order Step 11)
- No idempotent-consumer dedupe — a message redelivered after a crash (or
  reprocessed via a retry topic) can still double-decrement stock (Build
  Order Step 9)
- `InternalReservationController`'s endpoints have no auth/network
  restriction — in a real deployment, "internal" REST endpoints like these
  would sit behind a service mesh or network policy, not be reachable from
  wherever `order-saga-orchestrator`'s `base-url` config happens to point
