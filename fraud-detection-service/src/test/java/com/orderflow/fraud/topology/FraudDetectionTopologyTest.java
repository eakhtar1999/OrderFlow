package com.orderflow.fraud.topology;

import com.orderflow.avro.CustomerProfile;
import com.orderflow.avro.FraudAlert;
import com.orderflow.avro.OrderCreatedEvent;
import com.orderflow.avro.OrderItem;
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

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Build Order Step 15. A real UNIT test — {@link TopologyTestDriver}
 * runs {@link FraudDetectionTopology}'s actual DSL topology in-process,
 * against no broker at all, feeding records directly into (mocked) input
 * topics and reading them back from (mocked) output topics. This is a
 * genuinely different testing tier from Step 14's Testcontainers tests:
 * those proved a real broker + real service wiring works end to end;
 * this proves the TOPOLOGY'S OWN LOGIC is correct — both branches of a
 * fraud rule, the leftJoin's exact behavior on an unknown customer, and
 * the windowed velocity count's exact threshold-crossing point — in
 * milliseconds per test, not seconds waiting on Awaitility polls.
 *
 * {@link FraudDetectionTopology} is instantiated directly here, NOT via
 * a Spring context — its {@code @Value}-injected fields
 * (schemaRegistryUrl, highValueThreshold, velocityWindowMinutes,
 * velocityThresholdCount) are set via {@link ReflectionTestUtils}
 * instead, the same values application.yml supplies in production. The
 * topology class itself doesn't know or care whether Spring or a test
 * set those fields — {@code buildTopology(StreamsBuilder)} is a plain
 * method that only needs a StreamsBuilder to run.
 */
class FraudDetectionTopologyTest {

    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://fraud-detection-topology-test";
    private static final double HIGH_VALUE_THRESHOLD = 200.0;
    private static final long VELOCITY_WINDOW_MINUTES = 5;
    private static final long VELOCITY_THRESHOLD_COUNT = 3;

    private TopologyTestDriver driver;
    private TestInputTopic<String, OrderCreatedEvent> orderCreatedTopic;
    private TestInputTopic<String, CustomerProfile> customerProfileTopic;
    private TestOutputTopic<String, FraudAlert> fraudAlertsTopic;

    @BeforeEach
    void setUp() {
        FraudDetectionTopology topology = new FraudDetectionTopology();
        ReflectionTestUtils.setField(topology, "schemaRegistryUrl", MOCK_SCHEMA_REGISTRY_URL);
        ReflectionTestUtils.setField(topology, "highValueThreshold", HIGH_VALUE_THRESHOLD);
        ReflectionTestUtils.setField(topology, "velocityWindowMinutes", VELOCITY_WINDOW_MINUTES);
        ReflectionTestUtils.setField(topology, "velocityThresholdCount", VELOCITY_THRESHOLD_COUNT);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topology.buildTopology(streamsBuilder);
        Topology builtTopology = streamsBuilder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-topology-test");
        // Never actually dialed — TopologyTestDriver runs everything
        // in-process, but StreamsConfig still requires SOME value here.
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        driver = new TopologyTestDriver(builtTopology, props);

        orderCreatedTopic = driver.createInputTopic(
                "order-created", Serdes.String().serializer(), orderCreatedSerde().serializer());
        customerProfileTopic = driver.createInputTopic(
                "customer-profile", Serdes.String().serializer(), customerProfileSerde().serializer());
        fraudAlertsTopic = driver.createOutputTopic(
                "fraud-alerts", Serdes.String().deserializer(), fraudAlertSerde().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void highValueOrder_forCustomerWithNoProfile_stillGetsFlagged() {
        // No CustomerProfile ever piped in for this customerId — the
        // exact scenario FraudDetectionTopology's own class Javadoc and
        // "TRY IT YOURSELF" section call out: an inner join would
        // silently DROP this order from Branch A entirely. leftJoin
        // must still evaluate it.
        String customerId = "cust-unknown";
        orderCreatedTopic.pipeInput(customerId,
                orderOf("order-1", customerId, 250.0), Instant.now());

        List<FraudAlert> alerts = fraudAlertsTopic.readValuesToList();

        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getReason()).isEqualTo("HIGH_VALUE");
        assertThat(alerts.get(0).getSeverity()).isEqualTo("MEDIUM");
        assertThat(alerts.get(0).getOrderId()).isEqualTo("order-1");
        assertThat(alerts.get(0).getCustomerId()).isEqualTo(customerId);
    }

    @Test
    void veryHighValueOrder_severityEscalatesToHigh() {
        // > 2x the threshold (200.0) — EnrichedOrder.toAlert escalates
        // severity to HIGH even for a customer with no blocklist status
        // at all.
        String customerId = "cust-unknown-2";
        orderCreatedTopic.pipeInput(customerId,
                orderOf("order-2", customerId, 500.0), Instant.now());

        List<FraudAlert> alerts = fraudAlertsTopic.readValuesToList();

        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getReason()).isEqualTo("HIGH_VALUE");
        assertThat(alerts.get(0).getSeverity()).isEqualTo("HIGH");
    }

    @Test
    void blocklistedCustomer_getsFlagged_evenForASmallOrder() {
        String customerId = "cust-blocklisted";
        customerProfileTopic.pipeInput(customerId,
                profileOf(customerId, "BLOCKLISTED"), Instant.now());

        // Well under the high-value threshold — this alert can ONLY be
        // explained by the blocklist check, not the amount.
        orderCreatedTopic.pipeInput(customerId,
                orderOf("order-3", customerId, 15.0), Instant.now());

        List<FraudAlert> alerts = fraudAlertsTopic.readValuesToList();

        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getReason()).isEqualTo("BLOCKLISTED_CUSTOMER");
        // Blocklisted is ALWAYS severity HIGH, regardless of amount —
        // see EnrichedOrder.toAlert's isBlocklisted() || amount>2x check.
        assertThat(alerts.get(0).getSeverity()).isEqualTo("HIGH");
    }

    @Test
    void trustedCustomer_smallOrder_producesNoAlertAtAll() {
        // The negative case: a KNOWN, non-blocklisted customer placing a
        // small order should be COMPLETELY invisible to Branch A —
        // proving the leftJoin enrichment doesn't itself manufacture
        // false positives just because a profile was found.
        String customerId = "cust-trusted";
        customerProfileTopic.pipeInput(customerId,
                profileOf(customerId, "TRUSTED"), Instant.now());
        orderCreatedTopic.pipeInput(customerId,
                orderOf("order-4", customerId, 25.0), Instant.now());

        assertThat(fraudAlertsTopic.isEmpty()).isTrue();
    }

    @Test
    void fourthOrderInWindow_triggersOrderVelocityAlert_thirdDoesNot() {
        // velocityThresholdCount=3 means the filter is count > 3 — the
        // 3rd order in a window does NOT cross it, the 4th does. Low
        // order values (well under the 200.0 threshold) and no profile
        // keep Branch A silent throughout, so every alert seen here can
        // only have come from Branch B.
        //
        // A FIXED epoch-relative base, not Instant.now(): TimeWindows
        // are tumbling windows aligned to absolute epoch boundaries (a
        // 5-minute window here always starts at a multiple of 300s from
        // epoch), not relative to whenever this test happens to run. Using
        // wall-clock "now" would make this test's pass/fail depend on
        // whether it happens to execute near a window boundary — flaky in
        // a way that would show up rarely and be maddening to reproduce.
        // Starting comfortably inside a known window (60s past an epoch
        // multiple of 300s) keeps all 4 events, spread across 30s, safely
        // inside the SAME window every single run.
        String customerId = "cust-fast-shopper";
        Instant base = Instant.EPOCH.plusSeconds(60);

        orderCreatedTopic.pipeInput(customerId, orderOf("v-1", customerId, 10.0), base);
        orderCreatedTopic.pipeInput(customerId, orderOf("v-2", customerId, 10.0), base.plusSeconds(10));
        orderCreatedTopic.pipeInput(customerId, orderOf("v-3", customerId, 10.0), base.plusSeconds(20));

        assertThat(fraudAlertsTopic.isEmpty()).isTrue();

        orderCreatedTopic.pipeInput(customerId, orderOf("v-4", customerId, 10.0), base.plusSeconds(30));

        List<FraudAlert> alerts = fraudAlertsTopic.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getReason()).isEqualTo("ORDER_VELOCITY");
        assertThat(alerts.get(0).getCustomerId()).isEqualTo(customerId);
        // count(4) is not yet >= threshold*2 (6), so MEDIUM, not HIGH —
        // see buildStatefulVelocityBranch's severity expression.
        assertThat(alerts.get(0).getSeverity()).isEqualTo("MEDIUM");
        // Branch B has no orderId to report (a windowed COUNT has no
        // single record backing it) — FraudDetectionTopology hardcodes
        // "N/A" rather than inventing one, see buildStatefulVelocityBranch.
        assertThat(alerts.get(0).getOrderId()).isEqualTo("N/A");
    }

    @Test
    void ordersInDifferentWindows_neverAccumulateTowardVelocity() {
        // Same customer, same low value, but spread far enough apart
        // (6 minutes, more than one full 5-minute window) that each
        // event is GUARANTEED to land in a later window than the one
        // before it, regardless of absolute epoch alignment — proves the
        // count genuinely resets per window instead of growing forever
        // per customer.
        String customerId = "cust-slow-shopper";
        Instant base = Instant.EPOCH.plusSeconds(60);

        orderCreatedTopic.pipeInput(customerId, orderOf("s-1", customerId, 10.0), base);
        orderCreatedTopic.pipeInput(customerId, orderOf("s-2", customerId, 10.0),
                base.plus(java.time.Duration.ofMinutes(6)));
        orderCreatedTopic.pipeInput(customerId, orderOf("s-3", customerId, 10.0),
                base.plus(java.time.Duration.ofMinutes(12)));
        orderCreatedTopic.pipeInput(customerId, orderOf("s-4", customerId, 10.0),
                base.plus(java.time.Duration.ofMinutes(18)));

        // 4 orders total, but never more than 1 per window — nowhere
        // near velocityThresholdCount=3 in any single window.
        assertThat(fraudAlertsTopic.isEmpty()).isTrue();
    }

    private static OrderCreatedEvent orderOf(String orderId, String customerId, double totalAmount) {
        return OrderCreatedEvent.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setRegion("us-east")
                .setItems(List.of(OrderItem.newBuilder().setProductId("sku-42").setQuantity(1).build()))
                .setTotalAmount(totalAmount)
                .setCreatedAt(Instant.now().toEpochMilli())
                .setGiftMessage("")
                .build();
    }

    private static CustomerProfile profileOf(String customerId, String riskTier) {
        return CustomerProfile.newBuilder()
                .setCustomerId(customerId)
                .setRiskTier(riskTier)
                .setUpdatedAt(Instant.now().toEpochMilli())
                .build();
    }

    private static SpecificAvroSerde<OrderCreatedEvent> orderCreatedSerde() {
        return configuredSerde();
    }

    private static SpecificAvroSerde<CustomerProfile> customerProfileSerde() {
        return configuredSerde();
    }

    private static SpecificAvroSerde<FraudAlert> fraudAlertSerde() {
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
 * 1. TopologyTestDriver vs. Testcontainers (Build Order Step 14): no
 *    broker, no network, no Awaitility polling — every assertion here
 *    runs synchronously, in the same thread, milliseconds after
 *    pipeInput() returns. This is the RIGHT tier for topology LOGIC;
 *    Step 14's tests are the right tier for "does this actually work
 *    against a real Kafka broker with real serialization over the
 *    wire."
 * 2. Explicit Instants passed to every pipeInput() call, rather than
 *    relying on wall-clock time — makes the window-boundary tests
 *    (fourthOrderInWindow..., ordersInDifferentWindows...) fully
 *    deterministic instead of depending on how fast the JVM happens to
 *    execute four method calls in a row.
 * 3. readValuesToList() drains everything produced since the last read
 *    — calling it after 3 orders (asserting empty) and then again after
 *    a 4th (asserting one alert) is what makes the exact
 *    threshold-crossing point (3 doesn't fire, 4 does) directly
 *    observable, not just "eventually an alert appears somewhere."
 * 4. FraudDetectionTopology never touches Spring in this test —
 *    ReflectionTestUtils sets its @Value fields directly. Proof the
 *    topology's actual DECISION LOGIC has zero real dependency on
 *    Spring's DI container, even though production wires it in via one.
 *
 * 🔧 TRY IT YOURSELF
 * Change velocityThresholdCount's test value from 3 to 1 and rerun —
 * watch fourthOrderInWindow_triggersOrderVelocityAlert_thirdDoesNot FAIL
 * on its own "3rd order produces no alert" assertion, since count=2 now
 * exceeds a threshold of 1. That failure is the test correctly proving
 * it's actually exercising the CONFIGURED threshold, not a hardcoded "4."
 * ════════════════════════════════════════════════════════════════════════
 */
