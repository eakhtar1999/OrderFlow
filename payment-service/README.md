# payment-service

Build Order Step 8's second saga participant. Consumes `inventory-reserved`,
decides (deterministically) whether to approve or decline, publishes
`payment-completed` / `payment-failed`. Also exposes a plain internal REST
endpoint — the same `PaymentProcessor` backs both the choreography listener
AND the orchestration saga's entry point, so there's exactly one place the
actual "approve or decline" decision is made.

## What this module demonstrates

| Concept | Where | Trade-off |
|---|---|---|
| A second choreography saga participant | `consumer/InventoryReservedListener.java` | Reacts only to `inventory-reserved` — has no idea `order-service` or `inventory-service` even exist, only that a topic with this shape occasionally has new records |
| Deterministic business decision (not a real payment gateway) | `service/PaymentProcessor.java` | A real gateway's approval logic is opaque and probabilistic; this one is neither, on purpose — `totalAmount > 250.0` always declines, so triggering a decline for testing is a one-line curl, not luck |
| Event-carried state transfer | `PaymentCompleted`/`PaymentFailed` Avro schemas | `payment-failed` carries the full item list (`DeclinedItem`), not just an orderId — so inventory-service's compensation listener can act without querying anything back |
| Default (auto-commit) ack mode | `application.yml` | Manual offset commit was already taught in depth on `inventory-service`'s `OrderEventListener` (Build Order Step 1) — repeating it here would teach nothing new; the same at-least-once trade-offs still apply regardless of ack strategy |
| One decision, two entry points | `internal/PaymentController.java` vs. `consumer/InventoryReservedListener.java` | Both call the same `PaymentProcessor.charge(...)` — choreography reaches it via a Kafka listener, orchestration via `POST /internal/charge`; the actual business logic doesn't know or care which one |
| Avro consumer deserializer, found missing live | `application.yml` | This service's `application.yml` was the FIRST place this bug surfaced — see the root README's "A real bug, found live" section for the full story and fix |

## Try the hands-on exercises

1. Follow the root README's Step 8 choreography walkthrough — place a
   normal order, watch this service's console log the charge decision in
   real time.
2. Trigger a decline directly: place an order whose `quantity × 9.99`
   exceeds `payment.decline-threshold-amount` (250.0 by default,
   `application.yml`) and watch `🚫 Payment declined` fire, followed by
   `inventory-service`'s compensation listener releasing the stock.
3. Change `payment.decline-threshold-amount` to `0` and restart — every
   order now declines. Confirm compensation still fires correctly even
   when it's the FIRST order ever placed for a customer (no prior
   successful state to protect).
4. Call `POST localhost:8087/internal/charge` directly with curl,
   bypassing Kafka entirely — confirm it's the exact same
   `PaymentProcessor` making the exact same decision as the listener
   does, just reached synchronously.
5. Stop this service entirely and place an order through
   `order-saga-orchestrator` (`POST localhost:8089/api/saga/orders`) —
   watch the root README's circuit-breaker walkthrough play out for
   yourself, including the real retry timing and the CLOSED → OPEN →
   HALF_OPEN → CLOSED lifecycle.

## Testing

`src/test/java/.../CoreOrderFlowIntegrationTest.java` (Build Order Step
14) — `mvn test`. Real Testcontainers Kafka (no Postgres/Redis — this
service is stateless). Two tests, one per branch of the decline-threshold
rule: an `inventory-reserved` message under $250 gets `payment-completed`;
one over $250 gets `payment-failed` carrying the same items forward. Both
subscribe to BOTH output topics and assert on whichever one is expected,
so an inverted threshold would time out waiting on the wrong topic
instead of silently passing.

## What's deliberately NOT here yet

- No real payment gateway integration — `PaymentProcessor`'s deterministic
  threshold is the whole "gateway," permanently, for this tutorial
- No idempotent-consumer dedupe — a redelivered `inventory-reserved`
  message (after a crash, or a retry-topic hop) can charge twice (Build
  Order Step 9 fixes this project-wide with a Redis-backed dedupe store)
- No Kafka transactions on the producer side — `payment-completed` /
  `payment-failed` are plain at-least-once sends, unlike order-service's
  transactional outbox producer (Build Order Step 5)
- No retry topics / DLT for this service's own consumer — a message this
  listener can't process retries via Spring Kafka's default error handler
  only, not the dedicated retry-topic chain `inventory-service` has (Build
  Order Step 4)
