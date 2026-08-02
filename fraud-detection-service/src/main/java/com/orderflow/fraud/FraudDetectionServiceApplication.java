package com.orderflow.fraud;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Entry point of fraud-detection-service — Build Order Step 6, and the
 * first module in this repo that ISN'T a plain {@code @KafkaListener}
 * consumer or a REST-to-Kafka producer.
 *
 * Every other service so far reacts to ONE record at a time
 * (order-service publishes one, inventory-service consumes one). This
 * module defines a standing TOPOLOGY — a graph of transformations — that
 * Kafka Streams runs continuously against the order-created topic. The
 * topology itself lives in {@code topology/FraudDetectionTopology.java};
 * this class only needs {@code @EnableKafkaStreams} to tell Spring
 * "build and manage a KafkaStreams instance from that topology, and tie
 * its lifecycle to this application's."
 */
@SpringBootApplication
@EnableKafkaStreams
public class FraudDetectionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FraudDetectionServiceApplication.class, args);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Kafka Streams apps are a different SHAPE of Kafka client entirely —
 *    not "consume a record, do some work, maybe produce a record" like
 *    every previous module, but "describe a topology once, let the
 *    framework run it continuously." @EnableKafkaStreams is the one line
 *    that switches Spring Kafka into that mode.
 * 2. A Kafka Streams app IS a consumer group underneath (its
 *    application-id doubles as the group-id — see application.yml) — all
 *    the mechanics from Build Order Step 2 (partition assignment,
 *    rebalancing, offset commits) still apply here, just orchestrated by
 *    the Streams runtime instead of your own @KafkaListener method.
 *
 * 🔧 TRY IT YOURSELF
 * Once this is running, browse Kafka UI's consumer groups list — you'll
 * find "fraud-detection-service" there, exactly like
 * "inventory-service-group" was in Step 2, with partition assignments
 * and lag, because underneath the DSL, it genuinely is one.
 * ════════════════════════════════════════════════════════════════════════
 */
