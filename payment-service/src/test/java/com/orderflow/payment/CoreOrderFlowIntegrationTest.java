package com.orderflow.payment;

import com.orderflow.avro.InventoryReserved;
import com.orderflow.avro.PaymentCompleted;
import com.orderflow.avro.PaymentFailed;
import com.orderflow.avro.ReservedItem;
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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
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
 * Build Order Step 14. Covers payment-service's real Kafka contract —
 * both branches of {@link InventoryReservedListener}'s deterministic
 * threshold rule (see payment-service/application.yml's
 * payment.decline-threshold-amount=250.0), not just the happy path. See
 * order-service's sibling test for why this project's integration tests
 * are structured per-service rather than one mega-test spanning multiple
 * services' Spring contexts.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class CoreOrderFlowIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"))
            .withKraft();

    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://payment-service-core-flow-test";

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.producer.properties.schema.registry.url", () -> MOCK_SCHEMA_REGISTRY_URL);
        registry.add("spring.kafka.consumer.properties.schema.registry.url", () -> MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void inventoryReserved_underThreshold_getsPaymentCompleted() {
        String orderId = UUID.randomUUID().toString();
        String customerId = "cust-it-" + System.nanoTime();

        // 2 x 9.99 = 19.98 — comfortably under the 250.0 decline
        // threshold, same FAKE_UNIT_PRICE order-service's own
        // CoreOrderFlowIntegrationTest happy path lands on.
        InventoryReserved reserved = InventoryReserved.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setRegion("us-east")
                .setItems(List.of(ReservedItem.newBuilder().setProductId("sku-42").setQuantity(2).build()))
                .setTotalAmount(19.98)
                .setReservedAt(Instant.now().toEpochMilli())
                .build();

        try (Producer<String, Object> producer = newRawProducer();
             Consumer<String, Object> consumer = newRawConsumer()) {

            producer.send(new ProducerRecord<>("inventory-reserved", customerId, reserved));
            producer.flush();

            consumer.subscribe(List.of("payment-completed", "payment-failed"));
            AtomicReference<PaymentCompleted> completed = new AtomicReference<>();

            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(200));
                        for (ConsumerRecord<String, Object> record : records) {
                            if (record.value() instanceof PaymentCompleted event
                                    && event.getOrderId().equals(orderId)) {
                                completed.set(event);
                            }
                            // If this shows up instead, the test SHOULD
                            // fail loudly rather than time out silently —
                            // untilAsserted below does that via
                            // completed.get() staying null.
                        }
                        assertThat(completed.get()).isNotNull();
                    });

            assertThat(completed.get().getCustomerId()).isEqualTo(customerId);
            assertThat(completed.get().getTotalAmount()).isEqualTo(19.98);
            assertThat(completed.get().getItems()).hasSize(1);
            assertThat(completed.get().getItems().get(0).getProductId()).isEqualTo("sku-42");
        }
    }

    @Test
    void inventoryReserved_overThreshold_getsPaymentFailed() {
        String orderId = UUID.randomUUID().toString();
        String customerId = "cust-it-" + System.nanoTime();

        // 30 x 9.99 = 299.70 — over the 250.0 decline threshold, the
        // exact scenario the root README's Step 8 compensation-path
        // walkthrough triggers manually via curl.
        InventoryReserved reserved = InventoryReserved.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setRegion("us-east")
                .setItems(List.of(ReservedItem.newBuilder().setProductId("sku-42").setQuantity(30).build()))
                .setTotalAmount(299.70)
                .setReservedAt(Instant.now().toEpochMilli())
                .build();

        try (Producer<String, Object> producer = newRawProducer();
             Consumer<String, Object> consumer = newRawConsumer()) {

            producer.send(new ProducerRecord<>("inventory-reserved", customerId, reserved));
            producer.flush();

            consumer.subscribe(List.of("payment-completed", "payment-failed"));
            AtomicReference<PaymentFailed> failed = new AtomicReference<>();

            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(200));
                        for (ConsumerRecord<String, Object> record : records) {
                            if (record.value() instanceof PaymentFailed event
                                    && event.getOrderId().equals(orderId)) {
                                failed.set(event);
                            }
                        }
                        assertThat(failed.get()).isNotNull();
                    });

            assertThat(failed.get().getCustomerId()).isEqualTo(customerId);
            // Carries the exact reserved items forward — this is what
            // makes inventory-service's PaymentFailedCompensationListener
            // able to release the right stock without a lookup (see that
            // class's own Javadoc, and its sibling test in
            // inventory-service).
            assertThat(failed.get().getItems()).hasSize(1);
            assertThat(failed.get().getItems().get(0).getProductId()).isEqualTo("sku-42");
            assertThat(failed.get().getItems().get(0).getQuantity()).isEqualTo(30);
        }
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
 * 1. payment-service is the first service in this Build Order with NO
 *    database and NO Redis — its Testcontainers footprint here is Kafka
 *    alone, proportional to what the service actually depends on, not a
 *    copy-pasted container list from a sibling test.
 * 2. Both branches of a deterministic business rule tested explicitly,
 *    not just the happy path — same principle the root README's Step 8
 *    walkthrough demonstrates manually (an order under $250 approves, an
 *    order over it declines), now checked automatically. A rule this
 *    testable had no excuse to only have half its behavior covered.
 * 3. Subscribing to BOTH payment-completed AND payment-failed in each
 *    test, then asserting on whichever ONE is expected, is deliberate:
 *    if the threshold logic ever inverted (an over-threshold order
 *    approved, or vice versa), this test would time out waiting for an
 *    event that never arrives on the topic it's polling for — a clear
 *    failure — rather than silently reading the wrong topic and passing
 *    for the wrong reason.
 * ════════════════════════════════════════════════════════════════════════
 */
