package com.orderflow.analytics.topology;

import com.orderflow.avro.OrderCreatedEvent;
import com.orderflow.avro.OrdersPerMinute;
import com.orderflow.avro.RevenueByRegion;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Map;

import static com.orderflow.analytics.config.KafkaTopicConfig.ORDERS_PER_MINUTE_TOPIC;
import static com.orderflow.analytics.config.KafkaTopicConfig.REVENUE_BY_REGION_TOPIC;

/**
 * Two independent windowed aggregations over {@code order-created}, both
 * re-keyed away from the topic's native {@code customerId} key — the
 * defining difference from fraud-detection-service's velocity branch
 * (Build Order Step 6), which aggregated using {@code groupByKey()}
 * because it needed the EXISTING key (per-customer counting). Neither
 * metric here is "per customer," so both need {@code groupBy()} instead,
 * which lets you supply a BRAND NEW key — at a real cost: {@code groupBy}
 * triggers an internal REPARTITION topic (data gets physically re-shuffled
 * across partitions by the new key before aggregating), where
 * {@code groupByKey} needed none, since nothing about the key changed.
 */
@Configuration
public class AnalyticsTopology {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsTopology.class);

    public static final String ORDERS_PER_MINUTE_STORE = "orders-per-minute-store";
    public static final String REVENUE_BY_REGION_STORE = "revenue-by-region-store";

    private static final String ORDER_CREATED_TOPIC = "order-created";
    // The re-key target for the global count — every order maps to this
    // SAME constant key, so groupBy funnels every record into one logical
    // group regardless of which customer or partition it came from. This
    // is the standard Kafka Streams pattern for "aggregate across
    // everything" — there's no built-in "no key" grouping; you fake it
    // with a key that's always the same value. Public because
    // AnalyticsQueryController needs this exact string to query the
    // orders-per-minute-store by the right key.
    public static final String GLOBAL_KEY = "ALL";

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${analytics.window-size-seconds}")
    private long windowSizeSeconds;

    @Bean
    public KStream<String, OrderCreatedEvent> buildTopology(StreamsBuilder streamsBuilder) {
        KStream<String, OrderCreatedEvent> orders = streamsBuilder
                .stream(ORDER_CREATED_TOPIC, Consumed.with(Serdes.String(), orderCreatedSerde()))
                .peek((customerId, order) -> log.info(
                        "📥 Analyzing order {} customerId={} region={} totalAmount={}",
                        order.getOrderId(), customerId, order.getRegion(), order.getTotalAmount()));

        buildOrdersPerMinute(orders);
        buildRevenueByRegion(orders);

        return orders;
    }

    private void buildOrdersPerMinute(KStream<String, OrderCreatedEvent> orders) {
        KTable<Windowed<String>, Long> counts = orders
                // groupBy, not groupByKey: the topic's real key
                // (customerId) is irrelevant to a GLOBAL count, so every
                // record gets remapped to the same GLOBAL_KEY before
                // grouping. This is what actually triggers the
                // repartition topic — Kafka Streams can't guarantee
                // records with the new key are already co-located on the
                // right partition, so it re-shuffles through an internal
                // topic first.
                .groupBy((customerId, order) -> GLOBAL_KEY,
                        Grouped.with(Serdes.String(), orderCreatedSerde()))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(windowSizeSeconds), Duration.ZERO))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(ORDERS_PER_MINUTE_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        counts.toStream()
                .peek((windowedKey, count) -> log.info("📊 [orders/min] window starting {} -> count={}",
                        windowedKey.window().startTime(), count))
                .map((windowedKey, count) -> KeyValue.pair(
                        String.valueOf(windowedKey.window().start()),
                        OrdersPerMinute.newBuilder()
                                .setWindowStart(windowedKey.window().start())
                                .setWindowEnd(windowedKey.window().end())
                                .setOrderCount(count)
                                .build()))
                .to(ORDERS_PER_MINUTE_TOPIC, Produced.with(Serdes.String(), ordersPerMinuteSerde()));
    }

    private void buildRevenueByRegion(KStream<String, OrderCreatedEvent> orders) {
        KTable<Windowed<String>, Double> revenue = orders
                // Re-keyed to region instead of a constant — same
                // groupBy mechanism as above, but funneling into MULTIPLE
                // groups (one per distinct region value) instead of one.
                .groupBy((customerId, order) -> order.getRegion(),
                        Grouped.with(Serdes.String(), orderCreatedSerde()))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(windowSizeSeconds), Duration.ZERO))
                // .aggregate(), not .count() — the DSL has no built-in
                // "sum"; you supply an initializer (0.0, revenue starts
                // at nothing) and an adder (fold each order's totalAmount
                // into the running total). count() is really just
                // aggregate() with "add 1 every time" baked in.
                .aggregate(
                        () -> 0.0,
                        (region, order, totalSoFar) -> totalSoFar + order.getTotalAmount(),
                        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as(REVENUE_BY_REGION_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double()));

        revenue.toStream()
                .peek((windowedKey, total) -> log.info("📊 [revenue] region={} window starting {} -> total={}",
                        windowedKey.key(), windowedKey.window().startTime(), total))
                .map((windowedKey, total) -> KeyValue.pair(
                        windowedKey.key(),
                        RevenueByRegion.newBuilder()
                                .setRegion(windowedKey.key())
                                .setWindowStart(windowedKey.window().start())
                                .setWindowEnd(windowedKey.window().end())
                                .setTotalRevenue(total)
                                .build()))
                .to(REVENUE_BY_REGION_TOPIC, Produced.with(Serdes.String(), revenueByRegionSerde()));
    }

    private SpecificAvroSerde<OrderCreatedEvent> orderCreatedSerde() {
        SpecificAvroSerde<OrderCreatedEvent> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(), false);
        return serde;
    }

    private SpecificAvroSerde<OrdersPerMinute> ordersPerMinuteSerde() {
        SpecificAvroSerde<OrdersPerMinute> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(), false);
        return serde;
    }

    private SpecificAvroSerde<RevenueByRegion> revenueByRegionSerde() {
        SpecificAvroSerde<RevenueByRegion> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(), false);
        return serde;
    }

    private Map<String, String> schemaRegistryConfig() {
        return Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. groupBy vs. groupByKey: groupByKey is "free" — the key isn't
 *    changing, so Kafka Streams knows every record is already on the
 *    right partition for aggregation. groupBy lets you aggregate along
 *    ANY dimension, including one the source topic was never partitioned
 *    by, but it costs a real repartition topic — data physically moves
 *    across partitions before the aggregation can even start.
 * 2. .aggregate() is the general form; .count() is a special case of it.
 *    Reaching for .aggregate() with your own initializer + adder is how
 *    you compute a sum, an average, a running max, or anything else a
 *    single window needs to fold multiple records into.
 * 3. Two entirely independent aggregations (global count, per-region sum)
 *    can live in the SAME topology, sharing the same source KStream —
 *    Kafka Streams doesn't re-read the topic twice; both branches consume
 *    the one physical read.
 * 4. Publishing a windowed KTable back out via .toStream().to(topic) is
 *    what makes an aggregation a first-class Kafka topic other services
 *    can consume later — not just something queryable from THIS
 *    service's own state store.
 * 5. Verified live: repartition topics register their OWN Schema
 *    Registry subjects too
 *    ("analytics-service-orders-per-minute-store-repartition-value",
 *    etc.) — Kafka Streams still has to serialize data to write it to an
 *    internal repartition topic, and with Avro as the value type, that
 *    means a real schema gets registered for a topic you never declared
 *    and will likely never look at directly. One more subject that
 *    exists per topic-with-a-different-name, same lesson as Build Order
 *    Step 4's retry/DLT topics each getting their own subject.
 *
 * 🔧 TRY IT YOURSELF
 * After running this once, check for auto-created internal topics:
 *   docker exec orderflow-kafka /opt/kafka/bin/kafka-topics.sh \
 *     --bootstrap-server localhost:9092 --list | grep analytics-service
 * You'll see repartition topics (one per groupBy call) and changelog
 * topics (one per Materialized store) that nothing in THIS file declared
 * directly — Kafka Streams creates and names them itself from the
 * application-id + an internal processor name.
 * ════════════════════════════════════════════════════════════════════════
 */
