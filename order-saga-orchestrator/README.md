# order-saga-orchestrator

Build Order Step 8's orchestration-based saga — the direct counterpart to
the choreography saga running across `order-service` /
`inventory-service` / `payment-service` / `shipment-service`. One
service, one class (`SagaOrchestrator.java`), calls all three downstream
services directly and synchronously over REST, in an explicit sequence,
with explicit compensation logic. Standing beside the choreography saga,
not replacing it — both run against the same infrastructure at once,
triggered by structurally different requests.

## What this module demonstrates

| Concept | Where | Trade-off |
|---|---|---|
| Orchestration's defining property, made literal | `orchestration/SagaOrchestrator.java` | The entire sequence — reserve, charge, ship, or fail-and-compensate — is readable top to bottom in ONE method, at the cost of this class knowing all 3 downstream services' exact request/response shapes at compile time |
| A saga's sequence AND its state, both first-class | `orchestration/SagaOrchestrator.java`, `schema.sql` | Every transition writes a row to a `saga` Postgres table this service owns — `SELECT * FROM saga WHERE order_id = ...` answers "what happened to this order" directly, no log-reconstruction needed |
| A saga is NOT one distributed transaction | `schema.sql`'s comment | Each `updateStatus()` call is its own independent, immediately-committed statement — wrapping the WHOLE saga (including 3 blocking HTTP calls) in one open DB transaction would hold a connection for the saga's full duration and misrepresent what a saga actually is |
| Compensation as a direct method call | `SagaOrchestrator.run()` | `inventoryClient.release(...)` is an ordinary call the orchestrator makes on purpose, in response to a decision it made — contrast with choreography's `PaymentFailedCompensationListener`, which reacts to a fact published for an unrelated reason |
| `@CircuitBreaker` + `@Retry`, correctly ordered | `client/*.java` | Resilience4j-spring6's default stacking makes `@Retry` the OUTER decorator, `@CircuitBreaker` the INNER one — verified from a live stack trace, not assumed (see root README's "found live" section). `fallbackMethod` MUST live on whichever annotation is outermost, or it silently swallows every failure before the inner one is even reached |
| `retry.ignore-exceptions` | `application.yml` | Once `@CircuitBreaker` is OPEN, it throws `CallNotPermittedException` instantly — telling `@Retry` to ignore that specific exception means an open-circuit call fails on its FIRST attempt instead of wasting 600ms retrying against a breaker that's already refused |
| Resilience4j reacts to exceptions, not business outcomes | `client/*.java` | A payment decline is a normal 200 OK as far as HTTP is concerned — only genuine unavailability (connection refused, timeout, 5xx) trips retry or the breaker |
| Same request shape as choreography's entry point | `orchestration/StartSagaRequest.java` | Deliberately identical to `order-service`'s `PlaceOrderRequest` — makes the side-by-side comparison in the root README an honest one, not an apples-to-oranges test |
| Database-per-service, deliberately violated | `application.yml` | Shares `order-service`'s Postgres DATABASE (not its tables) — a real "each service owns its own database" deployment would give this its own; sharing here is a documented infra-simplicity trade-off, safe only because neither service's tables reference the other's |
| No shared JAR between services, extended to internal DTOs | `client/*.java` | Each client class defines its OWN private `ReserveApiRequest`/`ChargeApiRequest`/etc. records rather than importing a shared library — a little duplication now, in exchange for each service being free to change its internal shape without a lockstep release |

## Try the hands-on exercises

1. Follow the root README's Step 8 orchestration walkthrough — place a
   happy-path order via `POST localhost:8089/api/saga/orders`, watch the
   entire sequence (`STARTED` → `INVENTORY_RESERVED` → `PAYMENT_COMPLETED`
   → `SHIPPED`) print from ONE service's console, then confirm the `saga`
   table's row matches.
2. Trigger the compensation path (`quantity: 26` of `sku-42` exceeds the
   $250 decline threshold) and confirm `inventory-service`'s log shows
   BOTH the reservation and the compensating release, triggered by a
   direct call from this service, not a listener.
3. **The circuit breaker, the real payoff of this module**: `kill -9`
   payment-service's process, then fire 5-6 orchestrated saga calls back
   to back. Time them yourself. The first 1-2 should take ~600-900ms
   (real retries, 300ms apart); once the breaker trips, the rest should
   return in well under 50ms. Real captured evidence of exactly this test
   — including the DEBUG-log state transitions and their timestamps — is
   in the root README.
4. Restart payment-service and place 1-2 more orchestrated orders — watch
   the breaker's automatic `HALF_OPEN → CLOSED` recovery, confirmed live
   in the root README.
5. `application.yml` — change `resilience4j.circuitbreaker.instances.payment-service.sliding-window-size`
   to something larger (e.g. 20) and re-run the circuit breaker test.
   Confirm it now takes noticeably MORE failed calls before the breaker
   opens — the window size directly controls how much evidence the
   breaker needs before it trusts a failure-rate calculation.
6. `SagaOrchestrator.java` — add a 4th step (e.g. a fake notification
   call) and work through what changes: a new client class, a new step in
   `run()`, and — if that new step can fail — a new compensation branch
   undoing payment AND inventory. Compare how much code that requires here
   versus what the equivalent choreography change would need (a new
   listener + producer pair, nothing else).

## Testing

`src/test/java/.../SagaOrchestratorIntegrationTest.java` (Build Order
Step 17) — `mvn test`. Real Testcontainers Postgres for the actual
`saga` table, plus ONE WireMock server standing in for
inventory-service/payment-service/shipment-service (no path collisions
between `/internal/reserve`, `/internal/charge`, `/internal/ship`, and
`/internal/release`, so one stub server covers all three real
services). Five tests: the happy path reaching `SHIPPED`; payment
declined compensating by genuinely calling `/internal/release` with the
right orderId (verified against WireMock's own request log, not
inferred from the saga's status); inventory declined failing immediately
with no compensation or payment attempt; a WireMock Scenario proving
`@Retry` actually retries (fail, fail, succeed on the 3rd attempt,
exactly matching `application.yml`'s `max-attempts: 3`); and a test that
asserts against the real `CircuitBreakerRegistry` bean's state
(`CLOSED` → `OPEN`) after sustained failures, then confirms a
subsequent call makes NO further HTTP request at all — the actual proof
`ignore-exceptions` is working, not just configured.

## What's deliberately NOT here yet

- No idempotency on `POST /api/saga/orders` — calling it twice with
  identical parameters starts two completely independent sagas, two
  different `orderId`s (Build Order Step 9's territory, project-wide)
- No compensation for a shipment failure — `shipment-service` always
  succeeds (see its own README), so this saga's compensation logic never
  needs a "undo the shipment" branch
- No Bean Validation on `StartSagaRequest` — jakarta validation wasn't
  added to keep this module's `pom.xml` lean; a malformed request fails
  later, inside the saga, rather than at the boundary
- No multi-instance concerns — this service assumes exactly one running
  instance; two instances racing to update the same `saga` row isn't
  addressed
- No Debezium/CDC-style saga log — the `saga` table is queried directly,
  not replayed as an event stream the way `order-status` (Build Order
  Step 5) is
