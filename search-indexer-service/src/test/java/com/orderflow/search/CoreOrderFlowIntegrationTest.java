package com.orderflow.search;

import com.orderflow.avro.InventoryFailed;
import com.orderflow.avro.InventoryReserved;
import com.orderflow.avro.OrderCreatedEvent;
import com.orderflow.avro.OrderItem;
import com.orderflow.avro.OrdersPerMinute;
import com.orderflow.avro.PaymentCompleted;
import com.orderflow.avro.PaidItem;
import com.orderflow.avro.ReservedItem;
import com.orderflow.avro.ShipmentCreated;
import com.orderflow.search.document.OrderDocument;
import com.orderflow.search.document.OrdersPerMinuteDocument;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Build Order Step 16. Covers search-indexer-service's real Kafka +
 * Elasticsearch contract — {@link com.orderflow.search.consumer.OrderDocumentIndexer}'s
 * partial-upsert-merge across six independent listeners, and
 * {@link com.orderflow.search.consumer.AnalyticsMetricsIndexer}'s
 * simpler full-document saves. See order-service's Step 14 sibling test
 * for why this project's integration tests are structured per-service,
 * driven by a raw Kafka producer test harness, rather than one mega-test
 * spanning multiple services' Spring contexts.
 *
 * Real Testcontainers-managed Kafka AND Elasticsearch — the same
 * docker.elastic.co image and xpack.security.enabled=false setting
 * docker-compose.yml runs, so a mapping bug like the one
 * {@code ElasticsearchIndexInitializer} exists to fix would be just as
 * reproducible here as it was found live in Build Order Step 10.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class CoreOrderFlowIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"))
            .withKraft();

    @Container
    static ElasticsearchContainer elasticsearch =
            new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.15.0"))
                    .withEnv("discovery.type", "single-node")
                    .withEnv("xpack.security.enabled", "false");

    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://search-indexer-core-flow-test";

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.consumer.properties.schema.registry.url", () -> MOCK_SCHEMA_REGISTRY_URL);
        registry.add("spring.elasticsearch.uris", () -> "http://" + elasticsearch.getHttpHostAddress());
    }

    @LocalServerPort
    int port;

    @Autowired
    ElasticsearchOperations elasticsearchOperations;

    @Test
    void orderDocument_buildsIncrementally_acrossTheHappyPathSagaEvents() {
        String orderId = UUID.randomUUID().toString();
        String customerId = "cust-it-" + System.nanoTime();

        try (Producer<String, Object> producer = newRawProducer()) {
            producer.send(new ProducerRecord<>("order-created", customerId,
                    orderOf(orderId, customerId, "us-east", 19.98)));
            producer.flush();

            Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                OrderDocument doc = elasticsearchOperations.get(orderId, OrderDocument.class);
                assertThat(doc).isNotNull();
                assertThat(doc.getStatus()).isEqualTo("CREATED");
            });

            producer.send(new ProducerRecord<>("inventory-reserved", customerId,
                    InventoryReserved.newBuilder()
                            .setOrderId(orderId).setCustomerId(customerId).setRegion("us-east")
                            .setItems(List.of(ReservedItem.newBuilder().setProductId("sku-42").setQuantity(2).build()))
                            .setTotalAmount(19.98).setReservedAt(Instant.now().toEpochMilli())
                            .build()));
            producer.send(new ProducerRecord<>("payment-completed", customerId,
                    PaymentCompleted.newBuilder()
                            .setOrderId(orderId).setCustomerId(customerId).setRegion("us-east")
                            .setItems(List.of(PaidItem.newBuilder().setProductId("sku-42").setQuantity(2).build()))
                            .setTotalAmount(19.98).setPaidAt(Instant.now().toEpochMilli())
                            .build()));
            producer.send(new ProducerRecord<>("shipment-created", customerId,
                    ShipmentCreated.newBuilder()
                            .setOrderId(orderId).setCustomerId(customerId).setShipmentId("SHIP-TEST-1")
                            .setCreatedAt(Instant.now().toEpochMilli())
                            .build()));
            producer.flush();
        }

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            OrderDocument doc = elasticsearchOperations.get(orderId, OrderDocument.class);
            assertThat(doc).isNotNull();
            assertThat(doc.getStatus()).isEqualTo("SHIPPED");
            assertThat(doc.getShipmentId()).isEqualTo("SHIP-TEST-1");
            // Fields from the FIRST event (order-created) must still be
            // present after three more PARTIAL updates merged in on top
            // — proving this is a merge, not each listener overwriting
            // the whole document.
            assertThat(doc.getCustomerId()).isEqualTo(customerId);
            assertThat(doc.getRegion()).isEqualTo("us-east");
            assertThat(doc.getTotalAmount()).isEqualTo(19.98);
        });
    }

    @Test
    void inventoryFailed_setsStatusAndReason() {
        String orderId = UUID.randomUUID().toString();
        String customerId = "cust-it-" + System.nanoTime();

        try (Producer<String, Object> producer = newRawProducer()) {
            producer.send(new ProducerRecord<>("order-created", customerId,
                    orderOf(orderId, customerId, "us-east", 9.99)));
            producer.send(new ProducerRecord<>("inventory-failed", customerId,
                    InventoryFailed.newBuilder()
                            .setOrderId(orderId).setCustomerId(customerId).setRegion("us-east")
                            .setReason("Insufficient stock for one or more items")
                            .setFailedAt(Instant.now().toEpochMilli())
                            .build()));
            producer.flush();
        }

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            OrderDocument doc = elasticsearchOperations.get(orderId, OrderDocument.class);
            assertThat(doc).isNotNull();
            assertThat(doc.getStatus()).isEqualTo("INVENTORY_FAILED");
            assertThat(doc.getReason()).isEqualTo("Insufficient stock for one or more items");
        });
    }

    @Test
    void eventArrivingBeforeOrderCreated_createsDocumentFromWhicheverArrivedFirst_thenOrderCreatedRegressesStatus() {
        // Reproduces search-indexer-service/README.md's own documented,
        // deliberately-NOT-fixed limitation: "Cross-topic reordering
        // during a cold backfill/replay is a real, found gap." Kafka
        // only guarantees ordering WITHIN a topic-partition, never
        // ACROSS different topics — this test simulates exactly that by
        // publishing shipment-created for an orderId BEFORE order-created
        // ever arrives, the same way a cold consumer-group replay could
        // deliver them out of the order a live saga always produces them
        // in.
        String orderId = UUID.randomUUID().toString();
        String customerId = "cust-it-" + System.nanoTime();

        try (Producer<String, Object> producer = newRawProducer()) {
            producer.send(new ProducerRecord<>("shipment-created", customerId,
                    ShipmentCreated.newBuilder()
                            .setOrderId(orderId).setCustomerId(customerId).setShipmentId("SHIP-TEST-2")
                            .setCreatedAt(Instant.now().toEpochMilli())
                            .build()));
            producer.flush();

            // docAsUpsert means shipment-created, arriving with nothing
            // ahead of it, CREATES the document — with only the fields
            // IT knows about. This is the documented, intentional half
            // of the behavior (see OrderDocumentIndexer's own Javadoc:
            // "whichever event happens to arrive FIRST for a given
            // orderId creates the document with just its own fields").
            Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                OrderDocument doc = elasticsearchOperations.get(orderId, OrderDocument.class);
                assertThat(doc).isNotNull();
                assertThat(doc.getStatus()).isEqualTo("SHIPPED");
                assertThat(doc.getShipmentId()).isEqualTo("SHIP-TEST-2");
                // order-created hasn't arrived yet — these fields don't
                // exist on the document at all yet.
                assertThat(doc.getRegion()).isNull();
            });

            // order-created finally arrives, "late" relative to
            // shipment-created — its upsert() unconditionally writes
            // status="CREATED", with no timestamp comparison against
            // what's already there.
            producer.send(new ProducerRecord<>("order-created", customerId,
                    orderOf(orderId, customerId, "us-east", 19.98)));
            producer.flush();
        }

        // THE BUG, locked in as an assertion rather than left as prose:
        // status genuinely regresses backward from SHIPPED to CREATED,
        // permanently — no further real event exists to correct it. This
        // is not a new discovery; it's the exact, already-documented gap
        // from search-indexer-service/README.md's "What's deliberately
        // NOT here yet" section, now pinned down as reproducible,
        // automated proof instead of only a paragraph of prose. The
        // professional fix (a Painless scripted conditional update
        // comparing timestamps) remains deliberately unbuilt — this test
        // exists to make the gap impossible to silently regress FURTHER,
        // not to fix it.
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            OrderDocument doc = elasticsearchOperations.get(orderId, OrderDocument.class);
            assertThat(doc).isNotNull();
            assertThat(doc.getStatus()).isEqualTo("CREATED");
            // shipmentId, written by the EARLIER (in arrival order)
            // shipment-created event, survives the merge untouched —
            // order-created's partial update never mentions that field,
            // so docAsUpsert's merge leaves it alone. Only `status` (a
            // field BOTH events write) actually regresses.
            assertThat(doc.getShipmentId()).isEqualTo("SHIP-TEST-2");
        });
    }

    @Test
    void searchEndpoint_filtersOnRegionAndStatus() {
        String customerId = "cust-it-" + System.nanoTime();
        String matchingOrderId = UUID.randomUUID().toString();
        String nonMatchingOrderId = UUID.randomUUID().toString();

        try (Producer<String, Object> producer = newRawProducer()) {
            // Matches region=us-east, status=CREATED.
            producer.send(new ProducerRecord<>("order-created", customerId,
                    orderOf(matchingOrderId, customerId, "us-east", 9.99)));
            // Different region — must NOT show up in a us-east filter.
            producer.send(new ProducerRecord<>("order-created", customerId,
                    orderOf(nonMatchingOrderId, customerId, "eu-west", 9.99)));
            producer.flush();
        }

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(elasticsearchOperations.get(matchingOrderId, OrderDocument.class)).isNotNull();
            assertThat(elasticsearchOperations.get(nonMatchingOrderId, OrderDocument.class)).isNotNull();
        });

        TestRestTemplate rest = new TestRestTemplate();
        List<?> results = rest.getForObject(
                "http://localhost:" + port + "/api/search/orders?region=us-east&status=CREATED",
                List.class);

        assertThat(results).isNotEmpty();
        boolean containsMatching = results.stream()
                .anyMatch(r -> ((java.util.Map<?, ?>) r).get("orderId").equals(matchingOrderId));
        boolean containsNonMatching = results.stream()
                .anyMatch(r -> ((java.util.Map<?, ?>) r).get("orderId").equals(nonMatchingOrderId));

        assertThat(containsMatching).isTrue();
        assertThat(containsNonMatching).isFalse();
    }

    @Test
    void analyticsMetricsIndexer_savesOrdersPerMinuteAsAFullDocument() {
        long windowStart = 60_000L;
        long windowEnd = 120_000L;

        try (Producer<String, Object> producer = newRawProducer()) {
            producer.send(new ProducerRecord<>("orders-per-minute", String.valueOf(windowStart),
                    OrdersPerMinute.newBuilder()
                            .setWindowStart(windowStart).setWindowEnd(windowEnd).setOrderCount(7L)
                            .build()));
            producer.flush();
        }

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            OrdersPerMinuteDocument doc = elasticsearchOperations.get(
                    String.valueOf(windowStart), OrdersPerMinuteDocument.class);
            assertThat(doc).isNotNull();
            assertThat(doc.getOrderCount()).isEqualTo(7L);
            assertThat(doc.getWindowEnd()).isEqualTo(windowEnd);
        });
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

    private static Producer<String, Object> newRawProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        return new KafkaProducer<>(props);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Real Testcontainers Elasticsearch, not a mock — the exact same
 *    xpack.security.enabled=false, single-node image docker-compose.yml
 *    runs. ElasticsearchIndexInitializer's ApplicationRunner (which
 *    fixed a real found-live mapping bug in Build Order Step 10) runs
 *    automatically as part of Spring context startup here too — this
 *    test would have caught THAT bug, had it existed when this test was
 *    written.
 * 2. A known, documented, deliberately-unfixed bug turned into a real,
 *    passing assertion — eventArrivingBeforeOrderCreated_... doesn't
 *    hide the cross-topic reordering gap, it PROVES it, on purpose. A
 *    future fix (the Painless scripted conditional update named in the
 *    README) would need to update THIS test's expectations too — which
 *    is exactly the point: the test is a specification of CURRENT
 *    behavior, not an aspirational one.
 * 3. Partial-update merging verified directly, not inferred: the happy-
 *    path test confirms fields from the FIRST event survive three more
 *    listeners' unrelated partial writes — proof this is docAsUpsert's
 *    merge semantics working, not each listener quietly overwriting the
 *    whole document.
 * 4. Two structurally different indexers tested with two different
 *    assertions styles: OrderDocumentIndexer's partial-merge documents
 *    are checked field-by-field as they accumulate; AnalyticsMetricsIndexer's
 *    documents (see analyticsMetricsIndexer_savesOrdersPerMinute...) are
 *    checked as one complete save, matching how each actually writes.
 *
 * 🔧 TRY IT YOURSELF
 * Comment out ElasticsearchIndexInitializer's @Component annotation and
 * rerun this test file — watch orderDocument_buildsIncrementally... still
 * PASS (the assertions here never check field TYPES, only values), while
 * a real `GET orders/_mapping` against the container would show `text`
 * fields instead of `keyword` — a reminder that this test proves
 * FUNCTIONAL correctness, not mapping correctness; the two are genuinely
 * different properties, both worth checking, only one of which this file
 * covers.
 * ════════════════════════════════════════════════════════════════════════
 */
