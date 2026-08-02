# fraud-detection-service

OrderFlow's first Kafka Streams application. Not a `@KafkaListener`
consumer, not a REST-to-Kafka producer — a standing TOPOLOGY that Kafka
Streams runs continuously against `order-created`, scoring every order
for two independent fraud signals in real time.

## What this module demonstrates

| Concept | Where | Trade-off |
|---|---|---|
| Kafka Streams DSL vs. plain `@KafkaListener` | whole module vs. `inventory-service` | A continuously-running topology graph instead of "react to one record" — still a consumer group underneath (Build Order Step 2's mechanics all still apply), just orchestrated by the Streams runtime |
| Stateless transformation (Branch A) | `topology/FraudDetectionTopology.java` | A pure function of one record — no state store, nothing to restore on restart, nothing to get out of sync |
| KStream-KTable join, `leftJoin` not `join` | `topology/FraudDetectionTopology.java` | An inner join would silently DROP every order from a customer with no seeded profile yet — leftJoin evaluates every order regardless, treating "no profile" as "not blocklisted" rather than "invisible" |
| Stateful windowed aggregation (Branch B) | `topology/FraudDetectionTopology.java` | "How many orders has this customer placed recently" has no answer from a single record — needs a real state store, with real restore-on-restart behavior and real operational gotchas (see below) |
| `Materialized.as(name)` — naming a state store | `topology/FraudDetectionTopology.java` | The one line that makes internal state queryable from outside the topology at all |
| `exactly_once_v2` | `application.yml` | One config line gets the whole topology (both branches, the join, the windowed changelog) the same atomicity guarantee `order-service`'s `OutboxRelay` had to hand-write for just two Kafka sends in Build Order Step 5 |
| Interactive queries over REST | `query/FraudQueryController.java` | Answers in microseconds, no Kafka round trip — but only correct for keys THIS instance's partitions own; not yet safe to scale to multiple instances (flagged, not solved) |
| `@Profile("seed")` test fixture | `seed/CustomerProfileSeeder.java` | Real reference data with no real customer-service to produce it — an explicit, labeled stand-in instead of pretending the gap away |

## A real regression this module caught, two steps late

`order-service`'s `OutboxRelay` (Build Order Step 5) keyed `order-created`
by `orderId` instead of `customerId` — a silent break of Step 1's
documented partition-key promise. Nothing in Step 5's own testing caught
it. This module's velocity branch (`groupByKey()` on customerId) did,
immediately — every customer's count came back as 1, always. Fixed in
`order-service/.../OutboxRelay.java`; see the root README for the full
story. Left in as a real example of an unenforced design invariant
eroding silently until something downstream actually depends on it.

## Try the hands-on exercises

1. Follow the root README's Step 6 walkthrough end to end — seed
   profiles, place the three test orders, query the interactive-queries
   endpoint.
2. `FraudDetectionTopology.java` — switch `leftJoin` to `join` (inner),
   place an order for a customerId with no seeded profile, and confirm
   it never reaches `fraud-alerts` even when clearly over the high-value
   threshold. Switch back and watch it reappear.
3. Place a 5th, 6th order for the same velocity-test customer within the
   same window and watch `fraud-alerts` get a NEW alert every time, not
   just once — confirms the "no dedup within a window" behavior
   documented in the topology's comments.
4. Change the topology's shape (add or remove a `.peek()` call), restart,
   and watch the `customer-profile` KTable come back empty even though
   the compacted topic still has the data — the exact live gotcha found
   while building this module. Re-run the seeder to fix it; read
   `FraudDetectionTopology.java`'s concept footer for why it happened.
5. `query/FraudQueryController.java` — stop this service mid-window,
   restart it, and query velocity again immediately. Watch the count
   correctly resume from the restored state store rather than starting
   over at 0.

## What's deliberately NOT here yet

- No multi-instance interactive queries — `KafkaStreams.metadataForKey(...)`
  query-forwarding isn't implemented, so scaling this service (the way
  `inventory-service` scales in Build Order Step 2) would make
  `FraudQueryController` return wrong (empty) answers for keys owned by
  a different instance, not an error
- No suppression of repeated velocity alerts within the same window —
  every order past the threshold re-alerts; deduping would need its own
  state, not built here
- No customer-service — `customer-profile` is seeded by hand
  (`seed/CustomerProfileSeeder.java`), standing in for a real upstream
  service this tutorial doesn't build
- `fraud-alerts` isn't consumed by anything yet — no
  notification-service exists to act on these alerts (see the root
  README's Build Order for what's still roadmap)
