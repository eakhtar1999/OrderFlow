package com.orderflow.order;

import com.orderflow.avro.OrderCreatedEvent;
import com.orderflow.avro.OrderStatus;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Build Order Step 14. Covers order-service's own real contract with the
 * outside world — the ONLY thing this service can meaningfully guarantee
 * on its own, since inventory-service/payment-service/shipment-service
 * are entirely separate deployable modules, not classes this test could
 * import (see docs/system-design.md's note on why this project's
 * integration tests are structured per-service, chained by real Kafka
 * messages, rather than one mega-test booting all 4 services' Spring
 * contexts in a single JVM).
 *
 * Deliberately uses REAL Testcontainers-managed Kafka + Postgres + Redis,
 * not Spring Kafka's embedded broker or an H2 in-memory database. An
 * embedded broker is a DIFFERENT implementation of the Kafka protocol —
 * it cannot reproduce a bug like Build Order Step 12's KAFKA_LOG_DIRS
 * volume-mount mismatch, because it never touches a real broker's log
 * directory at all. This test's entire value proposition is running
 * against the actual thing that runs in production (well, in
 * docker-compose.yml), not a stand-in for it.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class CoreOrderFlowIntegrationTest {

    // Same images docker-compose.yml runs, so "works in the test" and
    // "works in docker compose up" mean the same thing.
    @org.testcontainers.junit.jupiter.Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("orderflow")
            .withUsername("orderflow")
            .withPassword("orderflow");

    @org.testcontainers.junit.jupiter.Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"))
            .withKraft();

    @org.testcontainers.junit.jupiter.Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7"))
            .withExposedPorts(6379);

    // Confluent's KafkaAvroSerializer/Deserializer treat a schema.registry.url
    // starting with "mock://" as a request for an in-JVM
    // MockSchemaRegistryClient (io.confluent.kafka.schemaregistry.testutil.
    // MockSchemaRegistry) instead of an HTTP call — already on the classpath
    // transitively via kafka-avro-serializer, no new dependency needed. The
    // scope name after "mock://" is the registry's identity: this service's
    // own producer AND this test's raw consumer both need the SAME scope to
    // see each other's schemas, which is why it's a shared constant here
    // rather than a random UUID per use.
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://order-service-core-flow-test";

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.producer.properties.schema.registry.url", () -> MOCK_SCHEMA_REGISTRY_URL);
        registry.add("spring.kafka.consumer.properties.schema.registry.url", () -> MOCK_SCHEMA_REGISTRY_URL);

        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));

        // Faster than the 500ms production default (application.yml) so
        // this test doesn't spend its whole runtime waiting on a poll
        // interval that exists to be gentle on a real Postgres, not to be
        // realistic in a test.
        registry.add("outbox.relay.poll-interval-ms", () -> "100");
    }

    @LocalServerPort
    int port;

    @Autowired
    JdbcTemplate jdbcTemplate;

    private Consumer<String, Object> rawConsumer;

    @BeforeAll
    static void beforeAll() {
        // No-op — @Container fields above are started automatically by
        // @Testcontainers before @DynamicPropertySource runs. Documented
        // here only so the ordering (containers up -> properties resolved
        // -> Spring context built) is explicit, not implicit in annotation
        // processing order a future reader has to already know.
    }

    @AfterAll
    static void afterAll() {
        // Containers are stopped automatically by @Testcontainers at the
        // end of the test class.
    }

    @Test
    void placingAnOrder_publishesOrderCreatedAndOrderStatus_andRelaysTheOutboxRow() {
        String customerId = "cust-it-" + System.nanoTime();

        Map<String, Object> requestBody = Map.of(
                "customerId", customerId,
                "region", "us-east",
                "items", List.of(Map.of("productId", "sku-42", "quantity", 2))
        );

        TestRestTemplate rest = new TestRestTemplate();
        ResponseEntity<Map> response = rest.postForEntity(
                "http://localhost:" + port + "/api/orders", requestBody, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String orderId = (String) response.getBody().get("orderId");
        assertThat(orderId).isNotBlank();

        rawConsumer = newRawConsumer();
        rawConsumer.subscribe(List.of("order-created", "order-status"));

        AtomicReference<OrderCreatedEvent> orderCreated = new AtomicReference<>();
        AtomicReference<OrderStatus> orderStatus = new AtomicReference<>();

        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    ConsumerRecords<String, Object> records = rawConsumer.poll(Duration.ofMillis(200));
                    for (ConsumerRecord<String, Object> record : records) {
                        if (record.value() instanceof OrderCreatedEvent event
                                && event.getOrderId().equals(orderId)) {
                            orderCreated.set(event);
                        }
                        if (record.value() instanceof OrderStatus status
                                && status.getOrderId().equals(orderId)) {
                            orderStatus.set(status);
                        }
                    }
                    assertThat(orderCreated.get()).isNotNull();
                    assertThat(orderStatus.get()).isNotNull();
                });

        // The event published to Kafka should be exactly what was
        // submitted over REST — proving OutboxWriter -> OutboxRelay
        // carried the request through unchanged, not just that SOME
        // event landed on the topic.
        assertThat(orderCreated.get().getCustomerId()).isEqualTo(customerId);
        assertThat(orderCreated.get().getRegion()).isEqualTo("us-east");
        assertThat(orderCreated.get().getItems()).hasSize(1);
        assertThat(orderCreated.get().getItems().get(0).getProductId()).isEqualTo("sku-42");
        assertThat(orderCreated.get().getItems().get(0).getQuantity()).isEqualTo(2);
        assertThat(orderCreated.get().getTotalAmount()).isEqualTo(19.98);

        // order-status started this order's compacted-topic history at
        // CREATED — Build Order Step 8's saga is what later overwrites
        // this same key with RESERVED/PAID/SHIPPED, but that's
        // inventory-service/payment-service/shipment-service's own
        // contract, not order-service's (see the sibling tests in each
        // of those modules).
        assertThat(orderStatus.get().getStatus()).isEqualTo("CREATED");

        // Proves OutboxRelay actually ran, not just that the Kafka
        // consumer eventually caught up — the outbox row for this order
        // must be gone by the time the Kafka messages above are visible,
        // since the relay deletes it only AFTER a successful publish.
        // Reuses the Spring context's own JdbcTemplate bean (already
        // pointed at the Postgres container via @DynamicPropertySource
        // above), the same datasource order-service's own code writes
        // through — not a second, separately configured connection.
        Integer remainingOutboxRows = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox WHERE order_id = ?", Integer.class, orderId);
        assertThat(remainingOutboxRows).isZero();

        rawConsumer.close();
    }

    private static Consumer<String, Object> newRawConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "core-order-flow-test-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new KafkaConsumer<>(props);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Testcontainers vs. embedded/in-memory test doubles: this test talks
 *    to the SAME Kafka/Postgres/Redis Docker images docker-compose.yml
 *    runs, via real network sockets on randomly assigned host ports — not
 *    an in-process fake with its own, different implementation quirks.
 * 2. @DynamicPropertySource resolves container-assigned ports (Postgres,
 *    Kafka, Redis all pick a random free host port so parallel test runs
 *    never collide) AFTER containers start but BEFORE the Spring context
 *    boots — the only point in the JUnit 5 lifecycle where both are true.
 * 3. schema.registry.url=mock://<scope> is a genuinely supported testing
 *    feature of kafka-schema-registry-client (MockSchemaRegistryClient),
 *    not a hack — it lets Avro (de)serialization work correctly in tests
 *    without running a real Schema Registry container, as long as every
 *    producer/consumer in the same test uses the identical scope name.
 * 4. This test proves the FULL outbox pattern (Build Order Step 5), not
 *    just "an HTTP endpoint returns 202": a real Postgres row is written,
 *    a real background poller reads and deletes it, and a real Kafka
 *    transaction publishes two topics atomically — the same crash-
 *    resilience story the root README's Step 5 walkthrough demonstrated
 *    manually, now re-checked automatically on every build.
 *
 * 🔧 TRY IT YOURSELF
 * Comment out the `outbox.relay.poll-interval-ms` override in
 * @DynamicPropertySource above (falls back to application.yml's 500ms)
 * and watch the test still pass, just slower — Awaitility's 15-second
 * budget has room either way. Then try setting Awaitility's atMost to
 * 50ms: watch it fail with a clear "condition was not fulfilled" message
 * instead of hanging forever, which is the whole point of bounding it.
 * ════════════════════════════════════════════════════════════════════════
 */
