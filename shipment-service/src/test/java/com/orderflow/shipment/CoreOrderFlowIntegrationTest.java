package com.orderflow.shipment;

import com.orderflow.avro.PaidItem;
import com.orderflow.avro.PaymentCompleted;
import com.orderflow.avro.ShipmentCreated;
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
 * Build Order Step 14. Covers shipment-service's real Kafka contract —
 * the choreography saga's final, happy-path-only hop (see
 * ShipmentCreator's Javadoc for why this service has no failure mode by
 * design, unlike inventory-service/payment-service). See order-service's
 * sibling test for why this project's integration tests are structured
 * per-service rather than one mega-test spanning multiple services'
 * Spring contexts.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class CoreOrderFlowIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"))
            .withKraft();

    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://shipment-service-core-flow-test";

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.producer.properties.schema.registry.url", () -> MOCK_SCHEMA_REGISTRY_URL);
        registry.add("spring.kafka.consumer.properties.schema.registry.url", () -> MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void paymentCompleted_getsShipmentCreated() {
        String orderId = UUID.randomUUID().toString();
        String customerId = "cust-it-" + System.nanoTime();

        PaymentCompleted paymentCompleted = PaymentCompleted.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setRegion("us-east")
                .setItems(List.of(PaidItem.newBuilder().setProductId("sku-42").setQuantity(2).build()))
                .setTotalAmount(19.98)
                .setPaidAt(Instant.now().toEpochMilli())
                .build();

        try (Producer<String, Object> producer = newRawProducer();
             Consumer<String, Object> consumer = newRawConsumer()) {

            producer.send(new ProducerRecord<>("payment-completed", customerId, paymentCompleted));
            producer.flush();

            consumer.subscribe(List.of("shipment-created"));
            AtomicReference<ShipmentCreated> created = new AtomicReference<>();

            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(200));
                        for (ConsumerRecord<String, Object> record : records) {
                            if (record.value() instanceof ShipmentCreated event
                                    && event.getOrderId().equals(orderId)) {
                                created.set(event);
                            }
                        }
                        assertThat(created.get()).isNotNull();
                    });

            assertThat(created.get().getCustomerId()).isEqualTo(customerId);
            // ShipmentCreated does NOT carry items/totalAmount forward —
            // unlike every earlier event in this saga (see this event's
            // .avsc doc comment on why: nothing consumes it to take
            // further saga action). Only shipmentId is new information.
            assertThat(created.get().getShipmentId()).isNotBlank();
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
 * 1. This is the fourth and final of a chain of four per-service tests
 *    (order-service, inventory-service, payment-service, this one) that
 *    together cover the SAME choreography saga this project's root
 *    README walks through manually — every hop now re-verified on every
 *    build instead of only the one time it was tested by hand.
 * 2. Only one test method here, not two: unlike payment-service (a real
 *    approve/decline branch to cover) or inventory-service (a forward
 *    AND a compensating hop), shipment-service has exactly one behavior
 *    by design — see ShipmentCreator's own Javadoc for why this service
 *    deliberately has no failure path, which is also why its test file
 *    doesn't invent one just to look symmetrical with its siblings.
 * ════════════════════════════════════════════════════════════════════════
 */
