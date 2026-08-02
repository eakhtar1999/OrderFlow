# shipment-service

Build Order Step 8's terminal saga step. Consumes `payment-completed`,
creates a shipment, publishes `shipment-created`. The simplest of the
three new saga services on purpose — its entire job is to demonstrate
that a saga NEEDS a natural terminal step, and to give both choreography
and orchestration something unambiguous to end on.

## What this module demonstrates

| Concept | Where | Trade-off |
|---|---|---|
| A saga's terminal step | `consumer/PaymentCompletedListener.java` | Nothing downstream reacts to `shipment-created` yet — this is genuinely the END of the choreography chain, not an arbitrary stopping point for the tutorial |
| A deliberately unconditional operation | `service/ShipmentCreator.java` | `create(orderId)` ALWAYS succeeds — a real shipment step has failure modes (no carrier capacity, invalid address), deliberately scoped out so this saga's compensation story stays bounded to the ONE failure mode (payment decline) it fully demonstrates, rather than building a half-finished second compensation path |
| Same logic, two entry points | `internal/ShipmentController.java` vs. `consumer/PaymentCompletedListener.java` | Both call the same `ShipmentCreator.create(...)` — the exact same "orchestration reuses choreography's own service class" pattern `payment-service` and `inventory-service` also use |
| No compensating call exists for this step | `order-saga-orchestrator`'s `ShipmentServiceClient.java` | Directly follows from `ShipmentCreator` always succeeding — there's nothing to compensate FOR, so the saga's compensation logic never needs a "undo the shipment" branch at all |

## Try the hands-on exercises

1. Follow the root README's Step 8 happy-path walkthrough (either saga
   style) — watch this service's console log the shipment creation as the
   final event in the chain.
2. Confirm the negative case directly: trigger a payment decline (either
   saga style) and grep this service's console for the orderId — nothing
   will be there. This service is never even called when an earlier step
   fails, choreography OR orchestration.
3. As an extension exercise (not built here): add a `SHIPMENT_FAILED`
   branch to `ShipmentCreator` (e.g. decline a specific "no-carrier" SKU)
   and work through what a SECOND compensation hop would require — both a
   new choreography listener (something reacting to `shipment-failed` to
   undo payment) AND a new branch in `SagaOrchestrator.run()`. This is the
   most direct way to feel why real sagas often need MULTIPLE
   compensating steps, one per already-completed prior step, not just one.

## Testing

`src/test/java/.../CoreOrderFlowIntegrationTest.java` (Build Order Step
14) — `mvn test`. Real Testcontainers Kafka (no Postgres/Redis). One
test: a raw `payment-completed` message in, `shipment-created` out — this
service's only behavior, by design (see `ShipmentCreator`'s Javadoc), so
the test doesn't invent a second case just to look symmetrical with its
siblings.

## What's deliberately NOT here yet

- No actual failure mode — see `ShipmentCreator`'s Javadoc; this is the
  single most deliberate scope boundary in the whole Step 8 saga work
- No idempotent-consumer dedupe — a redelivered `payment-completed`
  message creates a second, distinct shipment (Build Order Step 9)
- No carrier integration, tracking numbers, or delivery estimates — a
  shipment here is just a generated ID, nothing more
