# analytics-service

OrderFlow's second Kafka Streams application. Where
`fraud-detection-service` (Build Order Step 6) was built around a
KStream-KTable join and a single windowed count, this module is purely
about windowed AGGREGATION in the more general sense — computing metrics
along dimensions `order-created` was never partitioned by.

## What this module demonstrates

| Concept | Where | Trade-off |
|---|---|---|
| `groupBy` vs. `groupByKey` | `topology/AnalyticsTopology.java` | `groupByKey` is free — the key isn't changing. `groupBy` lets you aggregate along ANY dimension (a global constant, a field like region) but costs a real, auto-created repartition topic to physically re-shuffle data by the new key first |
| `.aggregate()` beyond `.count()` | `topology/AnalyticsTopology.java` | `.count()` is really just `.aggregate()` with "add 1 every time" baked in — reaching for the general form (initializer + adder) is how you compute a sum, an average, or anything else a window needs to fold multiple records into |
| Two independent aggregations, one shared source stream | `topology/AnalyticsTopology.java` | Kafka Streams doesn't re-read `order-created` twice for two branches — both consume the one physical read |
| Publishing a windowed KTable back to a topic | `topology/AnalyticsTopology.java` | Turns an aggregation into a first-class Kafka topic other services (Build Order Step 10's Elasticsearch indexer, eventually) can consume — not just something queryable from this service's own state store |
| Retention, not compaction, for metrics topics | `config/KafkaTopicConfig.java` | A time series needs its full history (every window's value) — compacting would silently keep only the latest window, throwing away the trend line a dashboard actually wants |
| Interactive queries: `fetch` vs. `fetchAll` | `query/AnalyticsQueryController.java` | `fetch(key, ...)` answers "what's the value for a key I already know"; `fetchAll(...)` is the only way to discover which keys even exist — there's no separate "list distinct keys" API on a windowed store |

## Try the hands-on exercises

1. Follow the root README's Step 7 walkthrough — place orders across
   multiple regions, query both interactive endpoints, watch the numbers
   update live.
2. Wait past a window boundary (60s by default) and place one more
   order — confirm via `curl localhost:8084/api/analytics/orders-per-minute`
   that the count resets rather than continuing to climb. Real captured
   evidence of this exact test is in the root README.
3. `docker exec orderflow-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep analytics-service`
   — find the auto-created repartition and changelog topics, then
   compare against `fraud-detection-service`'s topic list (which has
   NONE, because its velocity branch uses `groupByKey`, not `groupBy`).
4. Change `analytics.window-size-seconds` to something much larger
   (e.g. 600) and restart. Place several orders spread a minute apart —
   confirm they all land in the SAME window now instead of separate ones.
5. `topology/AnalyticsTopology.java` — add a third aggregation (e.g.
   average order value per region, using `.aggregate()` with a
   `(sum, count)` pair as the accumulator instead of a plain `Double`) as
   a genuine extension exercise.

## What's deliberately NOT here yet

- **`fraud-flag rate`** — `claude.md`'s suggested third analytics metric
  (correlating `order-created` volume against `fraud-alerts` volume) is
  NOT built here. It would need a KStream-KStream join (with a real
  `JoinWindows`, unlike the KStream-KTable join `fraud-detection-service`
  uses) across two independently-produced streams — a meaningfully
  different, more complex technique than the `groupBy` re-keying this
  module focuses on. Scoped out deliberately rather than half-built; a
  strong candidate TRY IT YOURSELF for a later pass.
- No multi-instance interactive queries — same caveat as
  `FraudQueryController`, unaddressed here for the same reason
  (`KafkaStreams.metadataForKey(...)` forwarding not implemented)
- Nothing consumes `orders-per-minute` or `revenue-by-region` yet — they
  exist as real topics with real data, waiting for Build Order Step 10's
  Elasticsearch indexer to actually visualize them
