# order-service

The only synchronous, HTTP-facing entry point in OrderFlow. Accepts
`POST /api/orders` and durably records it — Kafka publishing happens
out of band, on its own schedule, not on the request thread.

## What this module demonstrates

| Concept | Where | Trade-off |
|---|---|---|
| Sync edge / async core split | whole module vs. everything downstream | Fast API response vs. having to communicate "accepted" ≠ "completed" to the client (202, not 200) |
| API contract ≠ event contract | `dto/PlaceOrderRequest` vs the Avro `OrderCreatedEvent` | A little duplication now buys freedom to evolve the internal event without breaking the public API later |
| Topics declared as code, retention vs. compaction | `config/KafkaTopicConfig.java` | `order-created` (retention) answers "what happened"; `order-status` (compacted) answers "what's true now" — same cluster, opposite cleanup policies, chosen per use case |
| Avro + Schema Registry | `pom.xml` (avro-maven-plugin), `/avro-schemas/*.avsc` | An enforced, evolvable contract instead of hand-copied Java classes — at the cost of a codegen build step and a registry to run |
| **The transactional outbox pattern** (replaces Steps 1-4's direct publish) | `outbox/OutboxWriter.java`, `outbox/OutboxRelay.java` | The dual-write problem, actually solved: one ACID Postgres transaction guarantees the order and the intent-to-publish exist together, always — Kafka publishing becomes "will happen, at-least-once, eventually" instead of "might silently never happen" |
| Kafka transactions spanning two topics | `outbox/OutboxRelay.java` | `order-created` and `order-status` become visible together or not at all — real Kafka-to-Kafka atomicity, which is a DIFFERENT guarantee than Postgres-to-Kafka atomicity (nothing provides that; see the outbox pattern above for why) |
| Two `PlatformTransactionManager` beans, one ambiguous `@Transactional` | `config/TransactionManagerConfig.java` | Found by running it, not by reading docs: mixing a JDBC datasource with a Kafka transactional producer means `@ConditionalOnMissingBean` auto-configuration races can silently skip creating the JDBC transaction manager at all — fixed by defining it explicitly rather than trusting bean-registration order |
| Partition key regression, found two steps late | `outbox/OutboxRelay.java` | Originally keyed `order-created` by `orderId` (copy-paste from the `order-status` send right above it) — silently broke Step 1's "same customerId, same partition" promise for the entire lifetime of Step 5. Nothing in Step 5 depended on that key, so nothing caught it; Build Order Step 6's fraud-detection velocity check does, and it did, immediately |
| This service's first-ever consumer | `status/OrderStatusUpdater.java` | Everything through Step 7 flowed OUT of this service; Step 8's saga changes that — 5 `@KafkaListener` methods react to every event the saga produces, translating each into an `order-status` write. It makes NO decisions and triggers nothing else — a pure "read model updater," the same idea Build Order Step 9's Redis-backed "track my order" endpoint will eventually read FROM |
| Compacted `order-status` finally does real work | `status/OrderStatusUpdater.java` | Through Step 7 every order wrote `order-status` exactly ONCE ("CREATED"), so log compaction (Step 5's whole justification for this topic) had nothing to discard. Now an order genuinely transitions through multiple statuses (RESERVED → PAID → SHIPPED, or RESERVED → PAYMENT_FAILED), and only the latest survives compaction — verified live in the root README's Step 8 walkthrough |
| Avro consumer deserializer, found missing live | `application.yml` | This service's brand-new `OrderStatusUpdater` consumer hit the exact same `StringDeserializer`-by-default bug `payment-service` did — see the root README's "A real bug, found live" section |
| Redis-backed token-bucket rate limiting | `rate/TokenBucketRateLimiter.java`, `controller/OrderController.java` | Per-customerId, atomic via a Redis Lua script (read-refill-consume-write as one uninterruptible operation) — checked BEFORE Bean Validation even runs, so an over-quota customer costs this service nothing beyond a Redis round trip. Verified live: exactly 5 of 10 rapid-fire requests succeeded (capacity=5), the rest got 429 instantly; a different customerId was completely unaffected |

## Try the hands-on exercises

Every source file ends with a `🔧 TRY IT YOURSELF` block. Recommended
order for this module:

1. `OrderController.java` — send the exact same request twice, notice you
   get two different `orderId`s. Still true after Step 5 — this is an
   HTTP-layer concern, orthogonal to the outbox pattern.
2. `KafkaTopicConfig.java` — drop `order-created`'s partitions to 1, then
   try scaling inventory-service (see that module's README) and watch
   only one instance ever get work.
3. Follow the root README's Step 3 schema evolution walkthrough — add a
   field to `/avro-schemas/order-created.avsc`, rebuild ONLY this module,
   and watch inventory-service (untouched, still running old code) keep
   working.
4. **Build Order Step 5, the real payoff**: follow the root README's
   crash-resilience walkthrough yourself — override
   `OUTBOX_RELAY_POLL_INTERVAL_MS` to something huge, place an order,
   `kill -9` this service immediately, and confirm with your own
   `SELECT * FROM orders;` / `SELECT * FROM outbox;` that the order
   survived a total crash of the only service that knew about it.
   Restart normally and watch it get relayed within moments.
5. `OutboxWriter.java` — make `writeAsJson()` always throw, place an
   order, then check Postgres: the `orders` row is gone too, even though
   its own INSERT never failed. `@Transactional` rolling back the WHOLE
   method, not just the failing statement, made concrete.
6. `OutboxRelay.java` — set `outbox.relay.poll-interval-ms` to something
   huge, place an order, and watch the row just sit in `outbox`,
   visibly waiting. Nothing shows up in Kafka UI until the relay actually
   runs. That visible gap IS the durability guarantee working, not a bug.
7. **Build Order Step 8**: follow the root README's choreography
   walkthrough — place an order, then watch THIS service's own console
   print `📝 order-status[...] -> RESERVED`, then `PAID`, then `SHIPPED`,
   arriving asynchronously, well after the original 202 response. Then
   trigger a payment decline and watch `PAYMENT_FAILED` arrive instead of
   `PAID`, with `SHIPPED` never appearing.
8. `docker exec orderflow-kafka /opt/kafka/bin/kafka-topics.sh
   --bootstrap-server localhost:9092 --list` — confirm no new topics were
   needed for Step 8's `OrderStatusUpdater`; it only ever reads existing
   topics and writes to `order-status`, already declared back in Step 5.
9. **Build Order Step 9**: fire 10 `POST /api/orders` requests for the
   SAME `customerId` in a tight loop (a bash `for` loop, no delay) —
   confirm the first 5 return `202`, the rest `429`. Then fire the same
   burst for a DIFFERENT `customerId` and confirm it's unaffected. Wait
   ~5 seconds and retry the first customer — confirm it succeeds again,
   refilled roughly 1 token/second, not all 5 at once.
10. `application.yml` — change `order.rate-limit.capacity` to `1` and
    restart. Confirm even a SINGLE burst of 2 rapid requests now gets one
    `202` and one `429` — the smallest possible token bucket, useful for
    seeing the rejection trigger without needing 5+ requests.

## Testing

`src/test/java/.../CoreOrderFlowIntegrationTest.java` (Build Order Step
14) — `mvn test`. Real Testcontainers Kafka/Postgres/Redis, not H2 or an
embedded broker. Places an order over REST, then asserts BOTH the
`order-created` and `order-status=CREATED` Kafka messages and that the
Postgres outbox row was actually relayed (deleted) — proving the whole
outbox pattern ran, not just that the HTTP endpoint returned 202.

`src/test/java/.../schema/SchemaCompatibilityContractTest.java` (Build
Order Step 18) turns the root README's manual `curl` checks against
Schema Registry's `/compatibility` endpoint into a real, automated
contract test — a real Testcontainers Kafka + Schema Registry, reading
the ACTUAL `../avro-schemas/order-created.avsc` file off disk (not a
hardcoded copy). Three tests: adding an optional field with a default is
backward compatible; removing a field entirely is ALSO backward
compatible (the genuinely surprising result the root README documents
finding live); adding a required field with no default is the one that
actually gets rejected, with Schema Registry's own
`READER_FIELD_MISSING_DEFAULT_VALUE` reason verified in the response.
Lives here rather than in a dedicated module because `order-created.avsc`
is this service's own event and Step 3's original live demo was done
against it specifically — no Spring context needed at all, just Schema
Registry's real compatibility engine over HTTP.

## What's deliberately NOT here yet

- No REST-layer idempotency key handling (flagged in `OrderController`)
- No retry-limit or DLT-equivalent on the relay side — a genuinely
  malformed outbox row retries every poll cycle, forever (see
  `OutboxRelay`'s comments — the same "no limit" gap Build Order Step 1
  had on the consumer side, now on the producer side, just as
  deliberately unaddressed here)
- Debezium CDC, the alternative outbox-relay implementation `claude.md`
  suggests comparing against — documented (see the root README's
  comparison table and docker-compose.yml's commented `kafka-connect`
  block) but not built; the polling relay is what actually runs
- Flyway/Liquibase — `schema.sql` re-runs `CREATE TABLE IF NOT EXISTS`
  on every startup instead of versioned migrations, a deliberate
  simplification to keep this step focused on the outbox pattern itself
- No REST endpoint to actually READ `order-status` — `OrderStatusUpdater`
  writes it, but nothing in this service exposes a "track my order"
  endpoint yet
- The rate limiter is per-customerId only — no separate global or
  per-IP limit, so a customer with many legitimate concurrent devices
  and an attacker cycling fake customerIds are treated identically
  (unaddressed, a real gap a production deployment would need a second
  layer for)
