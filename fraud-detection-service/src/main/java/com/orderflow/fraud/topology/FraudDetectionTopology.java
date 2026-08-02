package com.orderflow.fraud.topology;

import com.orderflow.avro.CustomerProfile;
import com.orderflow.avro.FraudAlert;
import com.orderflow.avro.OrderCreatedEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
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
import java.time.Instant;
import java.util.Map;

import static com.orderflow.fraud.config.KafkaTopicConfig.CUSTOMER_PROFILE_TOPIC;
import static com.orderflow.fraud.config.KafkaTopicConfig.FRAUD_ALERTS_TOPIC;

/**
 * The whole topology, in two independent branches feeding the same
 * output topic — deliberately not merged into one combined verdict.
 * Real fraud-detection systems are usually built this way too: a
 * collection of independent rules, each free to evolve on its own,
 * rather than one monolithic scoring function.
 *
 * <p><b>Branch A</b> ({@link #buildStatelessAndEnrichmentBranch}):
 * stateless rules, enriched via a KStream-KTable join. Every decision is
 * a pure function of one order plus its customer's current profile — no
 * memory of anything that came before it.
 *
 * <p><b>Branch B</b> ({@link #buildStatefulVelocityBranch}): stateful,
 * windowed order-velocity detection — a question a single record can
 * never answer on its own ("has this customer placed an unusual NUMBER
 * of orders recently"), which is exactly what makes it stateful.
 */
@Configuration
public class FraudDetectionTopology {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionTopology.class);

    public static final String ORDER_VELOCITY_STORE = "order-velocity-store";
    private static final String ORDER_CREATED_TOPIC = "order-created";

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${fraud.high-value.threshold-amount}")
    private double highValueThreshold;

    @Value("${fraud.velocity.window-minutes}")
    private long velocityWindowMinutes;

    @Value("${fraud.velocity.threshold-count}")
    private long velocityThresholdCount;

    /**
     * The return type matters less than it looks like it should — Spring
     * just needs this method INVOKED once, against the shared
     * StreamsBuilder it injects, to register both branches on the same
     * topology. Returning the raw order stream is as good as returning
     * anything else.
     *
     * Named {@code buildTopology}, deliberately NOT
     * {@code fraudDetectionTopology} — this class is itself a
     * {@code @Configuration} bean, auto-registered under the bean name
     * "fraudDetectionTopology" (its own simple class name,
     * decapitalized). Naming this method the same string is a genuine
     * bean-name collision, found immediately on startup:
     * {@code BeanDefinitionOverrideException}, not a subtle runtime bug —
     * Spring refuses to silently pick one.
     */
    @Bean
    public KStream<String, OrderCreatedEvent> buildTopology(StreamsBuilder streamsBuilder) {
        KStream<String, OrderCreatedEvent> orders = streamsBuilder.stream(
                        ORDER_CREATED_TOPIC, Consumed.with(Serdes.String(), orderCreatedSerde()))
                .peek((customerId, order) -> log.info("📥 Scoring order {} customerId={} totalAmount={}",
                        order.getOrderId(), customerId, order.getTotalAmount()));

        KTable<String, CustomerProfile> customerProfiles = streamsBuilder.table(
                CUSTOMER_PROFILE_TOPIC, Consumed.with(Serdes.String(), customerProfileSerde()));

        buildStatelessAndEnrichmentBranch(orders, customerProfiles);
        buildStatefulVelocityBranch(orders);

        return orders;
    }

    private void buildStatelessAndEnrichmentBranch(
            KStream<String, OrderCreatedEvent> orders,
            KTable<String, CustomerProfile> customerProfiles) {

        // leftJoin, not join. An INNER join would silently DROP every
        // order from a customer with no profile in the table yet — a
        // brand-new customer would be invisible to this entire branch,
        // which is backwards for a fraud check (an unknown customer is
        // not automatically a safe one). leftJoin still fires for every
        // order; `profile` is simply null when there's no match, and
        // EnrichedOrder treats that as "not blocklisted" rather than
        // failing.
        //
        // Also notable for what it DOESN'T need: no time window. A
        // stream-TABLE join always compares against "whatever the table
        // says right now" — only stream-STREAM joins need a JoinWindows
        // to bound how far apart two records can be and still match.
        KStream<String, EnrichedOrder> enriched = orders.leftJoin(
                customerProfiles,
                EnrichedOrder::new,
                Joined.with(Serdes.String(), orderCreatedSerde(), customerProfileSerde())
        );

        // The actual stateless step: filter + map, a pure function of
        // one enriched record, touching no state store at all.
        enriched
                .filter((customerId, e) -> e.isHighValue(highValueThreshold) || e.isBlocklisted())
                .mapValues(e -> e.toAlert(highValueThreshold))
                .peek((customerId, alert) -> log.warn("🚨 [Branch A] {} orderId={} customerId={} severity={}",
                        alert.getReason(), alert.getOrderId(), alert.getCustomerId(), alert.getSeverity()))
                .to(FRAUD_ALERTS_TOPIC, Produced.with(Serdes.String(), fraudAlertSerde()));
    }

    private void buildStatefulVelocityBranch(KStream<String, OrderCreatedEvent> orders) {
        KTable<Windowed<String>, Long> velocityCounts = orders
                .groupByKey(Grouped.with(Serdes.String(), orderCreatedSerde()))
                .windowedBy(TimeWindows.ofSizeAndGrace(
                        Duration.ofMinutes(velocityWindowMinutes), Duration.ZERO))
                // Materialized.as(...) is what makes this state store
                // NAMED — and therefore queryable from outside the
                // topology. Without a name, Kafka Streams still keeps
                // this state internally (it has to, to compute the
                // count at all), but nothing else could ever ask it a
                // question. This exact name is what
                // FraudQueryController.java looks up for interactive
                // queries.
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(ORDER_VELOCITY_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        velocityCounts
                .toStream()
                // Every order past the threshold, within the SAME
                // window, re-triggers this filter — count keeps
                // climbing (4, 5, 6...) and each crossing re-emits.
                // That's not a bug we're suppressing: once a customer is
                // over the line, alerting on every further order in that
                // window is a defensible default for a fraud signal.
                // Suppressing repeats to "alert once per window" would
                // need its OWN state (have I already alerted for this
                // window-key?) — a good exercise, not built here.
                .filter((windowedKey, count) -> count != null && count > velocityThresholdCount)
                .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.key(),
                        FraudAlert.newBuilder()
                                .setOrderId("N/A")
                                .setCustomerId(windowedKey.key())
                                .setReason("ORDER_VELOCITY")
                                .setSeverity(count >= velocityThresholdCount * 2 ? "HIGH" : "MEDIUM")
                                .setDetectedAt(Instant.now().toEpochMilli())
                                .build()))
                .peek((customerId, alert) -> log.warn("🚨 [Branch B] ORDER_VELOCITY customerId={} severity={}",
                        alert.getCustomerId(), alert.getSeverity()))
                .to(FRAUD_ALERTS_TOPIC, Produced.with(Serdes.String(), fraudAlertSerde()));
    }

    /**
     * Enrichment result of Branch A's leftJoin — deliberately a plain
     * Java record, not another Avro schema. This value never touches
     * Kafka; it exists only inside this one topology stage, between the
     * join and the filter/map that consume it. Not everything needs a
     * schema — only things that cross a process boundary do.
     */
    private record EnrichedOrder(OrderCreatedEvent order, CustomerProfile profile) {

        boolean isHighValue(double threshold) {
            return order.getTotalAmount() > threshold;
        }

        boolean isBlocklisted() {
            return profile != null && "BLOCKLISTED".equals(profile.getRiskTier());
        }

        FraudAlert toAlert(double highValueThreshold) {
            String reason = isBlocklisted() ? "BLOCKLISTED_CUSTOMER" : "HIGH_VALUE";
            String severity = isBlocklisted() || order.getTotalAmount() > highValueThreshold * 2
                    ? "HIGH" : "MEDIUM";
            return FraudAlert.newBuilder()
                    .setOrderId(order.getOrderId())
                    .setCustomerId(order.getCustomerId())
                    .setReason(reason)
                    .setSeverity(severity)
                    .setDetectedAt(Instant.now().toEpochMilli())
                    .build();
        }
    }

    // Manually-constructed Serdes need their OWN configure() call — the
    // streams.properties.schema.registry.url in application.yml only
    // auto-applies to DEFAULT serdes Kafka Streams builds for you, not
    // to Serde objects your own code instantiates and passes explicitly
    // to Consumed/Produced/Grouped/Joined/Materialized, which is every
    // Serde in this file.

    private SpecificAvroSerde<OrderCreatedEvent> orderCreatedSerde() {
        SpecificAvroSerde<OrderCreatedEvent> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(), false);
        return serde;
    }

    private SpecificAvroSerde<CustomerProfile> customerProfileSerde() {
        SpecificAvroSerde<CustomerProfile> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(), false);
        return serde;
    }

    private SpecificAvroSerde<FraudAlert> fraudAlertSerde() {
        SpecificAvroSerde<FraudAlert> serde = new SpecificAvroSerde<>();
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
 * 1. Stateless vs. stateful, made concrete: Branch A never needs a state
 *    store — everything it decides comes from the current record (and a
 *    table lookup that's always "right now"). Branch B fundamentally
 *    CANNOT work without one — "how many orders in the last N minutes"
 *    has no answer without remembering previous orders somewhere.
 * 2. KStream-KTable join vs. KStream-KStream join: a stream-table join
 *    needs no JoinWindows because a KTable has no concept of "how long
 *    ago" — it's always the latest value. Stream-stream joins need a
 *    window because BOTH sides are unbounded logs with no "current
 *    value" to fall back on.
 * 3. leftJoin vs. join (inner): the difference is whether a record with
 *    no match on the other side survives at all. Getting this wrong here
 *    would mean brand-new customers (no profile yet) never get evaluated
 *    by this branch — an inner join silently drops them, not silently
 *    "trusts" them.
 * 4. Materialized.as(storeName) is the line that turns internal state
 *    into a queryable interactive-queries store — everything else about
 *    the aggregation works identically without it, only the "can I ask
 *    it a question from outside the topology" property changes.
 * 5. A @Configuration class registers itself as a bean under its own
 *    decapitalized class name — found immediately, the first time this
 *    ran: naming the @Bean method inside it the SAME as the class name
 *    ("fraudDetectionTopology" on both) collided with that self-
 *    registration and failed startup outright
 *    (BeanDefinitionOverrideException). Renamed the method to
 *    buildTopology to fix it — a naming collision easy to hit any time a
 *    @Configuration class's name and its main @Bean's purpose sound like
 *    the same word.
 * 6. Changing this topology's SHAPE (we added .peek() calls between two
 *    test runs) shifts Kafka Streams' auto-generated internal state
 *    store names (e.g. "customer-profile-STATE-STORE-0000000001"
 *    becomes "...-0000000002") — found live, mid-testing: the
 *    KTable came back empty after a restart even though the seeded data
 *    was still sitting in the compacted topic untouched. The new store
 *    instance has no changelog history under ITS name, and the
 *    underlying source-topic consumer had already committed offsets
 *    PAST the seed data from the previous topology shape, so neither
 *    path repopulated it. Kafka Streams ships
 *    `kafka-streams-application-reset.sh` specifically for this
 *    scenario — resetting an app's consumer offsets and internal state
 *    so it rebuilds cleanly after a topology change. Not something you'd
 *    guess from the DSL alone; only shows up once you've actually
 *    changed a running topology's shape and watched state go missing.
 *
 * 🔧 TRY IT YOURSELF
 * Comment out the .leftJoin(...) call's fallback to join(...) (inner)
 * instead, place an order for a customerId you never seeded a profile
 * for, and watch it never produce a HIGH_VALUE/BLOCKLISTED alert even
 * when the amount clearly exceeds the threshold — confirm branch A
 * silently skipped it, then switch back to leftJoin and watch it appear.
 * ════════════════════════════════════════════════════════════════════════
 */
