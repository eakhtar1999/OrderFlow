package com.orderflow.analytics.topology;

import com.orderflow.avro.OrderCreatedEvent;
import com.orderflow.avro.OrderItem;
import com.orderflow.avro.OrdersPerMinute;
import com.orderflow.avro.RevenueByRegion;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Build Order Step 15. A real UNIT test — {@link TopologyTestDriver}
 * runs {@link AnalyticsTopology}'s actual DSL topology in-process,
 * against no broker at all. See fraud-detection-service's sibling test
 * for the full explanation of why this is a genuinely different testing
 * tier from Step 14's Testcontainers tests (topology LOGIC here, real
 * broker wiring there), and why {@link AnalyticsTopology} is
 * instantiated directly rather than through Spring.
 *
 * Both aggregations here use {@code .groupBy(...)} (see this class's own
 * Javadoc on why, versus fraud-detection-service's {@code groupByKey()})
 * — from a test's point of view that's invisible: {@code
 * TopologyTestDriver} runs the internal repartition topic transparently,
 * the same as a real broker would, so these tests exercise the SAME
 * re-keying + repartitioning path production traffic does.
 */
class AnalyticsTopologyTest {

    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://analytics-topology-test";
    private static final long WINDOW_SIZE_SECONDS = 60;

    private TopologyTestDriver driver;
    private TestInputTopic<String, OrderCreatedEvent> orderCreatedTopic;
    private TestOutputTopic<String, OrdersPerMinute> ordersPerMinuteTopic;
    private TestOutputTopic<String, RevenueByRegion> revenueByRegionTopic;

    @BeforeEach
    void setUp() {
        AnalyticsTopology topology = new AnalyticsTopology();
        ReflectionTestUtils.setField(topology, "schemaRegistryUrl", MOCK_SCHEMA_REGISTRY_URL);
        ReflectionTestUtils.setField(topology, "windowSizeSeconds", WINDOW_SIZE_SECONDS);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topology.buildTopology(streamsBuilder);
        Topology builtTopology = streamsBuilder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "analytics-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        driver = new TopologyTestDriver(builtTopology, props);

        orderCreatedTopic = driver.createInputTopic(
                "order-created", Serdes.String().serializer(), orderCreatedSerde().serializer());
        ordersPerMinuteTopic = driver.createOutputTopic(
                "orders-per-minute", Serdes.String().deserializer(), ordersPerMinuteSerde().deserializer());
        revenueByRegionTopic = driver.createOutputTopic(
                "revenue-by-region", Serdes.String().deserializer(), revenueByRegionSerde().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void ordersWithinSameWindow_accumulateIntoOneGlobalCount() {
        // A fixed epoch-relative base, not Instant.now() — TimeWindows
        // are tumbling windows aligned to absolute epoch boundaries, not
        // relative to whenever this test happens to run. See
        // fraud-detection-service's sibling test for the full
        // explanation of why this avoids a rare, hard-to-reproduce
        // flaky failure.
        Instant base = Instant.EPOCH.plusSeconds(10);

        orderCreatedTopic.pipeInput("cust-1", orderOf("o-1", "cust-1", "us-east", 10.0), base);
        orderCreatedTopic.pipeInput("cust-2", orderOf("o-2", "cust-2", "eu-west", 20.0), base.plusSeconds(10));
        orderCreatedTopic.pipeInput("cust-3", orderOf("o-3", "cust-3", "us-east", 30.0), base.plusSeconds(20));

        // The windowed count re-emits on EVERY input (a KTable changelog,
        // not just a final answer) — three orders means three emitted
        // updates for the SAME window, counts climbing 1, 2, 3. Only the
        // LAST one is the answer a real consumer would see as "current."
        List<OrdersPerMinute> updates = ordersPerMinuteTopic.readValuesToList();
        assertThat(updates).hasSize(3);
        assertThat(updates.get(2).getOrderCount()).isEqualTo(3);

        // All three landed in the exact same window — global count is
        // deliberately blind to region/customer (see GLOBAL_KEY).
        assertThat(updates.get(0).getWindowStart()).isEqualTo(updates.get(2).getWindowStart());
        assertThat(updates.get(2).getWindowEnd() - updates.get(2).getWindowStart())
                .isEqualTo(Duration.ofSeconds(WINDOW_SIZE_SECONDS).toMillis());
    }

    @Test
    void ordersInDifferentWindows_getSeparateCounts_countResetsPerWindow() {
        Instant base = Instant.EPOCH.plusSeconds(10);

        orderCreatedTopic.pipeInput("cust-1", orderOf("o-1", "cust-1", "us-east", 10.0), base);
        // 70s later: past the 60s window boundary — guaranteed a new
        // window since the gap exceeds the window size.
        orderCreatedTopic.pipeInput("cust-2", orderOf("o-2", "cust-2", "us-east", 10.0),
                base.plusSeconds(70));

        List<OrdersPerMinute> updates = ordersPerMinuteTopic.readValuesToList();
        assertThat(updates).hasSize(2);
        assertThat(updates.get(0).getOrderCount()).isEqualTo(1);
        // The second order's window did NOT inherit the first window's
        // count — it starts fresh at 1, not 2, proving windows are
        // genuinely independent buckets, not a running total.
        assertThat(updates.get(1).getOrderCount()).isEqualTo(1);
        assertThat(updates.get(1).getWindowStart()).isGreaterThan(updates.get(0).getWindowStart());
    }

    @Test
    void ordersInSameRegionSameWindow_revenueSumsCorrectly() {
        Instant base = Instant.EPOCH.plusSeconds(10);

        orderCreatedTopic.pipeInput("cust-1", orderOf("o-1", "cust-1", "us-east", 10.00), base);
        orderCreatedTopic.pipeInput("cust-2", orderOf("o-2", "cust-2", "us-east", 25.50), base.plusSeconds(5));
        orderCreatedTopic.pipeInput("cust-3", orderOf("o-3", "cust-3", "us-east", 4.49), base.plusSeconds(10));

        List<RevenueByRegion> updates = revenueByRegionTopic.readValuesToList();
        assertThat(updates).hasSize(3);
        // Only the final cumulative value matters for "what's the
        // answer right now" — .aggregate()'s adder folds each order's
        // totalAmount in one at a time, same emit-per-update behavior as
        // the count aggregation above.
        assertThat(updates.get(2).getRegion()).isEqualTo("us-east");
        // Offset comparison, not exact equality — 10.00 + 25.50 + 4.49
        // summed as doubles isn't guaranteed to land on the EXACT same
        // bit pattern as the literal 39.99, since none of these decimals
        // has an exact binary floating-point representation.
        assertThat(updates.get(2).getTotalRevenue())
                .isCloseTo(39.99, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    void ordersInDifferentRegions_trackedAsCompletelySeparateSums() {
        // .aggregate() is the general form .count() specializes — this
        // test is the direct proof it correctly sums doubles instead of
        // just counting records, and that groupBy's re-key genuinely
        // splits traffic into independent per-region groups rather than
        // one shared bucket.
        Instant base = Instant.EPOCH.plusSeconds(10);

        orderCreatedTopic.pipeInput("cust-1", orderOf("o-1", "cust-1", "us-east", 10.0), base);
        orderCreatedTopic.pipeInput("cust-2", orderOf("o-2", "cust-2", "eu-west", 99.0), base.plusSeconds(5));

        List<RevenueByRegion> updates = revenueByRegionTopic.readValuesToList();
        assertThat(updates).hasSize(2);

        RevenueByRegion usEast = updates.stream().filter(r -> r.getRegion().equals("us-east")).findFirst().orElseThrow();
        RevenueByRegion euWest = updates.stream().filter(r -> r.getRegion().equals("eu-west")).findFirst().orElseThrow();

        assertThat(usEast.getTotalRevenue()).isEqualTo(10.0);
        assertThat(euWest.getTotalRevenue()).isEqualTo(99.0);
    }

    private static OrderCreatedEvent orderOf(String orderId, String customerId, String region, double totalAmount) {
        return OrderCreatedEvent.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setRegion(region)
                .setItems(List.of(OrderItem.newBuilder().setProductId("sku-42").setQuantity(1).build()))
                .setTotalAmount(totalAmount)
                .setCreatedAt(Instant.now().toEpochMilli())
                .setGiftMessage("")
                .build();
    }

    private static SpecificAvroSerde<OrderCreatedEvent> orderCreatedSerde() {
        return configuredSerde();
    }

    private static SpecificAvroSerde<OrdersPerMinute> ordersPerMinuteSerde() {
        return configuredSerde();
    }

    private static SpecificAvroSerde<RevenueByRegion> revenueByRegionSerde() {
        return configuredSerde();
    }

    private static <T extends org.apache.avro.specific.SpecificRecord> SpecificAvroSerde<T> configuredSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(java.util.Map.of(
                io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                MOCK_SCHEMA_REGISTRY_URL), false);
        return serde;
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. A windowed KTable-to-stream conversion (.toStream()) emits ONE
 *    record per UPDATE, not one record per closed window — piping 3
 *    orders into the same window produces 3 output records (running
 *    counts 1, 2, 3), not 1. Every test here reads the LAST emitted
 *    value as "the current answer," exactly like a real downstream
 *    consumer of these topics would have to.
 * 2. .groupBy(...) genuinely re-keys, verified two ways: the global
 *    count is blind to which region/customer an order came from
 *    (GLOBAL_KEY collapses everything into one bucket), while the
 *    per-region sum splits the SAME source stream into as many
 *    independent buckets as there are distinct region values — both
 *    aggregations read the same one KStream, proven by
 *    ordersInDifferentRegions_trackedAsCompletelySeparateSums having
 *    NO interaction with ordersWithinSameWindow_accumulate...'s count.
 * 3. TopologyTestDriver exercises groupBy's internal repartition topic
 *    transparently — nothing in this test file even mentions it exists,
 *    the same way a real broker's repartitioning is invisible to a
 *    downstream consumer reading the final aggregation output.
 *
 * 🔧 TRY IT YOURSELF
 * Change ordersInDifferentWindows_getSeparateCounts_countResetsPerWindow's
 * second pipeInput offset from 70 seconds to 30 seconds (still inside the
 * same 60-second window) and watch the LAST assertion fail — orderCount
 * becomes 2, not 1, since both orders now land in the one window this
 * test was built to prove they'd be split across.
 * ════════════════════════════════════════════════════════════════════════
 */
