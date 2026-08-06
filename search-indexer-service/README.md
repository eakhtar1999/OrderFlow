# search-indexer-service

Build Order Step 10. Consumes `order-created` plus all five saga events
(Build Order Step 8) and builds ONE denormalized order document per
orderId in Elasticsearch — the CQRS read side for a question no other
service in this platform can answer directly: "everything about order X,
in one query." A second, independent consumer feeds analytics-service's
windowed aggregates into their own indices for Kibana.

## What this module demonstrates

| Concept | Where | Trade-off |
|---|---|---|
| Denormalized read model / CQRS | `document/OrderDocument.java`, `search/SearchController.java` | One document answers what would otherwise need a Postgres join plus cross-referencing two Kafka Streams apps' state stores — at the cost of eventual consistency and a second copy of the data to keep in sync |
| Dual-write problem, avoided | `consumer/OrderDocumentIndexer.java` | Writes to Elasticsearch happen from a KAFKA CONSUMER, never directly from order-service or any saga participant — same fix order-service's outbox pattern (Build Order Step 5) applies to Postgres+Kafka, applied here to Kafka+Elasticsearch |
| Partial updates via `docAsUpsert` | `consumer/OrderDocumentIndexer.java` | Six independently-ordered listeners safely build ONE document with no coordination between them — whichever event arrives first creates it, every later one merges in, server-side, atomically per document |
| Index mapping, found broken live | `config/ElasticsearchIndexInitializer.java` | `@Field(type = Keyword)` annotations silently did nothing until this class existed — Elasticsearch auto-creates an index with DYNAMIC mapping on first write, and this service's first write is a partial `UpdateQuery`, which doesn't go through Spring Data Elasticsearch's own mapping-creation path. Confirmed via `GET orders/_mapping` showing `text` fields instead of `keyword` |
| Concurrent-writer version conflicts, found live | `consumer/OrderDocumentIndexer.java` | `docAsUpsert` alone isn't safe under real concurrency — a backfill replay produced genuine HTTP 409 `version_conflict_engine_exception`s across six listener threads racing the same document. `withRetryOnConflict(3)` fixes it server-side — the same CLASS of problem inventory-service's distributed lock solves (Build Order Step 9), solved here with Elasticsearch's built-in retry instead of an app-level lock |
| CQRS made literal | `search/SearchController.java` vs. `consumer/OrderDocumentIndexer.java` | The read side never touches Kafka or Postgres; the write side never handles HTTP — two files, zero shared code, not an abstraction layer pretending to separate them |
| Kibana as a second, independent consumer group | `consumer/AnalyticsMetricsIndexer.java` | A THIRD independent reader of the same `order-created` lineage fraud-detection-service and analytics-service already consume — consumer groups (Build Order Step 1) make this free; no coordination with either Kafka Streams app required |

## Try the hands-on exercises

1. Place an order, then immediately `curl localhost:9200/orders/_doc/<orderId>` before the saga finishes — confirm `status: CREATED` with no `shipmentId` yet. Poll again a few seconds later and watch `status` progress to `SHIPPED`, `shipmentId` finally appearing — a real partial merge each time, not a full re-index.
2. `curl "localhost:8090/api/search/orders?region=us-east&status=SHIPPED"` — then place a fresh order in `us-east` and re-run the SAME query immediately, then again a few seconds later. Watch the new order be absent, then appear — that gap IS the eventual-consistency lag, made observable.
3. `GET localhost:9200/orders/_mapping` — confirm `customerId`/`region`/`status`/etc. all come back as plain `keyword`, not `text` with a `.keyword` sub-field. If you comment out `ElasticsearchIndexInitializer` and delete the index (`DELETE localhost:9200/orders`), restart, and place an order, watch the mapping regress to Elasticsearch's dynamic default — the exact bug this class was built to fix, reproducible on demand.
4. Open Kibana (`localhost:5601`) and browse the `orders`, `orders-per-minute`, and `revenue-by-region` data views (already created via Kibana's Data Views API, not manually through the UI) — build a simple line chart of `orderCount` over `windowStart` to visualize analytics-service's own windowed aggregation, this time as a chart instead of a JSON response.
5. Stop this service, place a few orders, wait, then restart it — confirm it picks up exactly where its consumer group left off (no re-indexing of orders it already processed), the same at-least-once/offset-commit story every other consumer in this platform has followed since Build Order Step 1.

## Testing

`src/test/java/.../CoreOrderFlowIntegrationTest.java` (Build Order Step
16) — `mvn test`. Real Testcontainers Kafka AND Elasticsearch (the same
`xpack.security.enabled=false` single-node image docker-compose.yml
runs). Five tests: the full happy-path saga building one document
incrementally across four partial updates, with the FIRST event's fields
confirmed to survive three later, unrelated merges; the inventory-failed
path setting `status`/`reason`; the faceted search endpoint filtering by
region and status; `AnalyticsMetricsIndexer`'s simpler full-document save
behavior; and — the standout one — a test that doesn't hide this
module's own documented "cross-topic reordering" limitation but
reproduces it directly: publishing `shipment-created` before
`order-created` for the same orderId, and asserting the exact status
regression (`SHIPPED` back to `CREATED`) the README below already
describes in prose. The bug stays unfixed on purpose; the test exists so
it can't silently get WORSE unnoticed.

## What's deliberately NOT here yet

- **Cross-topic reordering during a cold backfill/replay is a real, found gap, not just a theoretical one.** In normal live operation, `order-created` always arrives before any downstream saga event for the same order (nothing downstream can fire before the order exists) — but Kafka only guarantees ordering WITHIN a topic-partition, never ACROSS different topics. Resetting this service's consumer group to `earliest` (done live, to verify the mapping fix) fired all six listeners' backlogs concurrently, and for at least one historical order, `order-created`'s upsert (which unconditionally writes `status: CREATED`) was processed AFTER that order's `shipment-created` event, silently regressing its indexed status backward from `SHIPPED` to `CREATED` — permanently, since no further real events exist to correct it. The professional fix is a scripted (Painless) conditional update comparing each write's own timestamp against the document's currently stored `updatedAt` and no-op'ing if the incoming write is older — deliberately not built here, flagged instead, since it's a genuine step up in complexity for what is specifically a cold-replay edge case, not a live-traffic bug.
- No pagination or result-count limits on `GET /api/search/orders` — Elasticsearch's own default page size caps it today, but nothing here is deliberate about it
- No full-text search — every field is `Keyword` (exact-match facets), not `Text` (tokenized/analyzed); this service answers "orders matching these exact filters," not "orders whose description contains roughly these words"
- No audit/event-log index (claude.md's optional "what happened to order #1234" stretch goal) — only the CURRENT denormalized state is indexed, not a queryable history of every event that built it
- Elasticsearch runs single-node with security disabled — a dev-only simplification (see docker-compose.yml's comment), not representative of a production ES deployment
