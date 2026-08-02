# OrderFlow — Real-Time Order Processing Platform
### Project Requirements Document (for Claude / Claude Code)

## 1. Project Purpose
Build a Spring Boot microservices project called **OrderFlow** that showcases advanced Apache Kafka
concepts alongside core distributed system design principles. This is a personal revision/portfolio
project — code should be clean, well-commented, and each module's README should explicitly call out
which Kafka/system-design concept it demonstrates and why that choice was made (trade-offs included).

Domain: an e-commerce order pipeline — order placed → inventory checked/reserved → payment processed →
shipment created → notifications sent, with fraud detection and analytics running in parallel.

---

## 2. Services to Build

1. **order-service** — REST API to place orders, produces `order-created` events
2. **inventory-service** — consumes orders, checks/reserves stock, produces `inventory-reserved` /
   `inventory-failed` events
3. **payment-service** — consumes reserved orders, processes payment, produces `payment-completed` /
   `payment-failed` events
4. **shipment-service** — consumes paid orders, creates shipment, produces `shipment-created` events
5. **notification-service** — consumes events from all topics, sends notifications (log/mock email)
6. **fraud-detection-service** — Kafka Streams app, scores orders in real time, flags risky orders
7. **analytics-service** — Kafka Streams app for windowed aggregations (orders/min, revenue/region)
8. **search-indexer-service** — consumes order/payment/shipment events, indexes denormalized order
   documents into Elasticsearch
9. **cache/read-model updater** — can be embedded in inventory-service or standalone — keeps Redis
   updated with latest stock counts and order status for fast reads

Each service should be its own Spring Boot module (multi-module Maven/Gradle repo).

---

## 3. Kafka Concepts to Implement

### Producer side
- Custom partitioning strategy (key by customer ID or region) — explain ordering vs parallelism trade-off
- Idempotent producer (`enable.idempotence=true`)
- Compare `acks=all` vs `acks=1` configurations
- Custom serializers — Avro with Schema Registry (or Apicurio)
- Synchronous vs asynchronous sends with callbacks
- Producer interceptors (e.g., inject tracing headers)

### Consumer side
- Consumer groups — scale inventory-service horizontally, demonstrate rebalancing
- Manual vs auto offset commit
- Static group membership
- Consumer rebalance listeners
- Compare exactly-once vs at-least-once semantics (config + code)
- Dead Letter Topics (DLT) for poison messages
- Retry topics with exponential backoff (Spring Kafka `DefaultErrorHandler` +
  `RetryTopicConfiguration`)
- Batch listeners vs single-record listeners
- Idempotent consumers (dedupe using order ID, backed by Redis)

### Kafka Streams
- Stateless transformations (filter/map) in fraud-detection-service
- Stateful aggregations (windowed counts, KTable) in analytics-service
- KStream–KTable joins (enrich order with customer data)
- Interactive queries (expose current Streams state via REST endpoint)
- Exactly-once-v2 processing guarantee

### Schema Management
- Avro schemas + Schema Registry
- Schema evolution demo (backward/forward compatibility — add/remove a field, show it still works)

### Reliability & Transactions
- Kafka transactions (transactional producer spanning order + payment topics)
- Outbox pattern to avoid the dual-write problem between DB and Kafka (consider Debezium CDC as
  an alternative implementation to compare)
- Topic configs: retention-based vs log-compacted topics (compacted topic for "latest order status"
  lookup)
- Experiment with partition counts + replication factor, document throughput differences

### Monitoring/Operations
- Kafka UI or AKHQ for topic/consumer-group visibility
- Micrometer metrics exported to Prometheus + Grafana
- Consumer lag as a first-class monitored metric

### Testing
- Embedded Kafka / Testcontainers for integration tests
- Contract tests for Avro schema compatibility

---

## 4. System Design Concepts to Include

### Scalability & Partitioning
- Horizontal scaling of consumers within a group; show partition assignment/rebalancing
- Hot partition simulation (skewed key) and fix (composite key or salting)

### Reliability & Consistency
- Discuss CAP theorem in context of Kafka's tunable consistency (acks/ISR)
- Saga pattern for the distributed order transaction:
    - Implement **choreography-based saga** (services react to each other's events)
    - Also implement/document **orchestration-based saga** (a coordinator service) for comparison
- Circuit breaker + retry with backoff (Resilience4j) between synchronous service calls, paired with
  Kafka-native retry topics for async failures

### Data Modeling & Storage
- Event sourcing — Kafka log as source of truth; ability to rebuild state by replaying events
- CQRS — separate write path (commands) from read path (materialized views in Redis/Elasticsearch)
- Log compaction vs retention topics, applied appropriately per use case

### Performance
- Backpressure handling — slow consumer scenario, batch listener tuning, `max.poll.records`
- Caching with Redis, cache invalidation driven by Kafka events
- Basic load testing — throughput benchmarks across different partition counts, documented results

### Distributed Systems Fundamentals
- Kafka's ISR (in-sync replicas) / leader election — simulate broker failure, show failover
- KRaft mode (Kafka's Raft-based metadata quorum) instead of Zookeeper
- Idempotency at the REST API layer too (safe order-placement retries)

### Observability
- Distributed tracing — propagate trace/correlation IDs via Kafka headers (OpenTelemetry +
  Jaeger/Zipkin)
- Dashboards for consumer lag, throughput, error rates
- Dead-letter-topic alerting story (what happens operationally when messages land in DLT)

### API & Contract Design
- Schema evolution/backward compatibility (ties Kafka Avro work into API-versioning discussion)
- Rate limiting on order-service's REST endpoint (Redis-backed token bucket)

---

## 5. Redis Usage

- Cache-aside pattern for inventory stock lookups (Postgres as source of truth, Redis as cache)
- Cache invalidation/update triggered by Kafka consumer on stock-change events
- Idempotency/dedup store — track processed message IDs with TTL for idempotent consumers
- Rate limiting (token bucket or sliding window) for order-service API
- Distributed locking (Redlock or `SETNX`) to prevent overselling when multiple inventory-service
  instances process reservations concurrently
- Read-optimized materialized view: Kafka Streams pushes current order status into Redis for a fast
  "track my order" endpoint

---

## 6. Elasticsearch Usage

- search-indexer-service consumes order/payment/shipment events, builds a denormalized order document,
  indexes into Elasticsearch — supports full-text/faceted search (e.g., "delayed orders for customer X
  in region Y last week")
- Feed windowed analytics aggregates into Elasticsearch, visualize with Kibana (orders/min, revenue by
  region, fraud-flag rate)
- Optional: index every event (with correlation/trace IDs) for audit/event-log search — "what happened
  to order #1234"
- (Optional stretch) centralized logging via ELK/EFK, using Kafka as the log transport

---

## 7. Cross-Cutting Design Notes to Call Out Explicitly in Docs

- Dual-write problem: writing to Elasticsearch/Redis from a Kafka consumer (not directly from the
  originating service) avoids the same dual-write issue solved by the outbox pattern for Postgres+Kafka
- Eventual consistency: Elasticsearch/Redis views lag behind Postgres — document how this is
  communicated (e.g., a version/timestamp on search/read results)
- Consumer lag as a UX concern: if search-indexer-service falls behind, users see stale search results

---

## 8. Infrastructure (Docker Compose)

Include the following services in `docker-compose.yml`:
- Kafka (KRaft mode, no Zookeeper)
- Schema Registry (Confluent or Apicurio)
- Kafka UI / AKHQ
- Postgres (per-service databases or shared, your call — document the choice)
- Redis
- Elasticsearch + Kibana
- Prometheus + Grafana
- Jaeger or Zipkin (for tracing)

---

## 9. Suggested Build Order (incremental revision arc)

1. Single producer/consumer with plain JSON — basic flow working end-to-end
2. Add consumer groups + manual offset commits, scale inventory-service
3. Introduce Avro + Schema Registry, demonstrate schema evolution
4. Add error handling: retry topics + Dead Letter Topics
5. Add Kafka transactions + outbox pattern (order-service → Postgres + Kafka)
6. Build fraud-detection-service as a Kafka Streams app (stateless + stateful processing)
7. Build analytics-service with windowed aggregations + interactive queries
8. Implement saga pattern (choreography first, then orchestration variant for comparison)
9. Add Redis: cache-aside, idempotency store, rate limiting, distributed lock
10. Add Elasticsearch: search-indexer-service + Kibana dashboards
11. Add observability: Micrometer/Prometheus/Grafana, distributed tracing with OpenTelemetry
12. Wrap everything in Docker Compose; write per-module READMEs mapping code to concepts
13. Write a system design doc: requirements, architecture diagram, data flow, and a
    "trade-offs considered" section (partitioning key choice, saga style, exactly-once vs
    at-least-once, etc.)

---

## 10. Deliverables Expected From Claude

- Multi-module Spring Boot project (Maven or Gradle — specify preference before starting)
- `docker-compose.yml` with all infra listed above
- Per-service README explaining the Kafka/system-design concept(s) it demonstrates
- A top-level architecture diagram (can be Mermaid in README)
- A short system design doc summarizing requirements, architecture, and trade-off decisions
- Basic integration tests using Testcontainers/Embedded Kafka for at least the core order flow