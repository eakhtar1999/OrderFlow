package com.orderflow.inventory;

import com.orderflow.avro.DeclinedItem;
import com.orderflow.avro.InventoryReserved;
import com.orderflow.avro.OrderCreatedEvent;
import com.orderflow.avro.OrderItem;
import com.orderflow.avro.PaymentFailed;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Build Order Step 14. Covers inventory-service's real Kafka + Postgres +
 * Redis contract in BOTH directions the choreography saga uses this
 * service for: {@link OrderEventListener}'s forward hop (order-created in,
 * inventory-reserved out, stock actually decremented in Postgres) and
 * {@link PaymentFailedCompensationListener}'s compensating hop (payment-failed
 * in, stock actually released back). See order-service's sibling test for
 * why this is a per-service test driven by a raw Kafka producer/consumer
 * rather than one mega-test spanning multiple services' Spring contexts.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class CoreOrderFlowIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("orderflow")
            .withUsername("orderflow")
            .withPassword("orderflow");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"))
            .withKraft();

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7"))
            .withExposedPorts(6379);

    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://inventory-service-core-flow-test";

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
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    void orderCreated_getsReserved_andStockIsActuallyDecremented() {
        // sku-42 is seeded to 50 units by schema.sql on every startup —
        // see that file's own comment for why this is safe to rely on
        // rather than a magic number this test invented.
        Integer stockBefore = jdbcTemplate.queryForObject(
                "SELECT quantity FROM stock WHERE product_id = ?", Integer.class, "sku-42");

        String orderId = UUID.randomUUID().toString();
        String customerId = "cust-it-" + System.nanoTime();

        OrderCreatedEvent orderCreated = OrderCreatedEvent.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setRegion("us-east")
                .setItems(List.of(OrderItem.newBuilder().setProductId("sku-42").setQuantity(3).build()))
                .setTotalAmount(29.97)
                .setCreatedAt(Instant.now().toEpochMilli())
                .setGiftMessage("")
                .build();

        try (Producer<String, Object> producer = newRawProducer();
             Consumer<String, Object> consumer = newRawConsumer()) {

            producer.send(new ProducerRecord<>("order-created", customerId, orderCreated));
            producer.flush();

            consumer.subscribe(List.of("inventory-reserved"));
            AtomicReference<InventoryReserved> reserved = new AtomicReference<>();

            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(200));
                        for (ConsumerRecord<String, Object> record : records) {
                            if (record.value() instanceof InventoryReserved event
                                    && event.getOrderId().equals(orderId)) {
                                reserved.set(event);
                            }
                        }
                        assertThat(reserved.get()).isNotNull();
                    });

            assertThat(reserved.get().getCustomerId()).isEqualTo(customerId);
            assertThat(reserved.get().getItems()).hasSize(1);
            assertThat(reserved.get().getItems().get(0).getQuantity()).isEqualTo(3);
        }

        // The real proof this isn't just a listener echoing the event back
        // unchanged: stock in Postgres genuinely went down by exactly the
        // reserved quantity — the same StockService.tryReserve(...) call
        // Build Order Step 9's distributed-lock test also exercises.
        Integer stockAfter = jdbcTemplate.queryForObject(
                "SELECT quantity FROM stock WHERE product_id = ?", Integer.class, "sku-42");
        assertThat(stockAfter).isEqualTo(stockBefore - 3);
    }

    @Test
    void paymentFailed_compensatesByReleasingStockBackToPostgres() {
        // Reserve first, the same way the happy-path test does, so there's
        // real reserved stock for the compensation to release — a
        // compensation test that never actually reserved anything first
        // would prove nothing about whether release() undoes a real
        // reservation.
        Integer stockBefore = jdbcTemplate.queryForObject(
                "SELECT quantity FROM stock WHERE product_id = ?", Integer.class, "sku-7");

        String orderId = UUID.randomUUID().toString();
        String customerId = "cust-it-" + System.nanoTime();

        OrderCreatedEvent orderCreated = OrderCreatedEvent.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setRegion("us-east")
                .setItems(List.of(OrderItem.newBuilder().setProductId("sku-7").setQuantity(2).build()))
                .setTotalAmount(19.98)
                .setCreatedAt(Instant.now().toEpochMilli())
                .setGiftMessage("")
                .build();

        try (Producer<String, Object> producer = newRawProducer();
             Consumer<String, Object> consumer = newRawConsumer()) {

            producer.send(new ProducerRecord<>("order-created", customerId, orderCreated));
            producer.flush();

            consumer.subscribe(List.of("inventory-reserved"));
            Awaitility.await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(200));
                        boolean seenReservation = false;
                        for (ConsumerRecord<String, Object> record : records) {
                            if (record.value() instanceof InventoryReserved event
                                    && event.getOrderId().equals(orderId)) {
                                seenReservation = true;
                            }
                        }
                        assertThat(seenReservation).isTrue();
                    });

            Integer stockAfterReserve = jdbcTemplate.queryForObject(
                    "SELECT quantity FROM stock WHERE product_id = ?", Integer.class, "sku-7");
            assertThat(stockAfterReserve).isEqualTo(stockBefore - 2);

            // Now simulate payment-service declining the charge — carries
            // the SAME items InventoryReserved carried (see
            // PaymentFailedCompensationListener's Javadoc on why that's
            // what makes this compensation possible at all without a
            // lookup).
            PaymentFailed paymentFailed = PaymentFailed.newBuilder()
                    .setOrderId(orderId)
                    .setCustomerId(customerId)
                    .setRegion("us-east")
                    .setItems(List.of(DeclinedItem.newBuilder().setProductId("sku-7").setQuantity(2).build()))
                    .setTotalAmount(19.98)
                    .setReason("Simulated decline for compensation test")
                    .setFailedAt(Instant.now().toEpochMilli())
                    .build();
            producer.send(new ProducerRecord<>("payment-failed", customerId, paymentFailed));
            producer.flush();
        }

        // Compensation runs asynchronously too — poll Postgres until the
        // released quantity is back, instead of asserting immediately.
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    Integer stockAfterCompensation = jdbcTemplate.queryForObject(
                            "SELECT quantity FROM stock WHERE product_id = ?", Integer.class, "sku-7");
                    assertThat(stockAfterCompensation).isEqualTo(stockBefore);
                });
    }

    private static Producer<String, Object> newRawProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        return new KafkaProducer<>(props);
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
 * 1. A raw test-harness KafkaProducer stands in for order-service (test 1)
 *    and payment-service (test 2) — this test never boots either of
 *    those services' Spring contexts, only produces the exact Avro-shaped
 *    message they would have produced. That's enough to genuinely
 *    exercise inventory-service's OWN consumer code, which has no way to
 *    tell the difference between a message from the real service and one
 *    from this test.
 * 2. Both tests assert against Postgres, not just "a Kafka message with
 *    the right shape arrived" — a listener that received the event but
 *    threw before calling StockService would still let a naive
 *    Kafka-only assertion pass. Checking the actual stock row is what
 *    proves the reservation/release LOGIC ran, not just that the
 *    listener method was invoked.
 * 3. The compensation test (test 2) needed to reserve stock FIRST, for
 *    the same reason a real payment-failed event never arrives without a
 *    real inventory-reserved event preceding it — testing compensation
 *    in isolation, without ever having reserved anything, would exercise
 *    release() against a starting state a real system never produces.
 *
 * 🔧 TRY IT YOURSELF
 * Change test 2's DeclinedItem quantity to something that doesn't match
 * what was actually reserved (e.g. 99 instead of 2) and watch the final
 * assertion fail with a stock count that's now WRONG in the other
 * direction — a concrete demonstration of why PaymentFailedCompensationListener's
 * Javadoc calls carrying the exact reserved items forward "not just
 * convenient... the only thing that makes this compensation possible."
 * ════════════════════════════════════════════════════════════════════════
 */
