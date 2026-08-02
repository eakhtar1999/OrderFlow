package com.orderflow.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Entry point of analytics-service — Build Order Step 7, OrderFlow's
 * second Kafka Streams app.
 *
 * Where fraud-detection-service (Step 6) was built around a KStream-KTable
 * JOIN and a windowed COUNT, this module is purely about windowed
 * AGGREGATION in the more general sense — re-keying a stream (via
 * {@code groupBy}, not {@code groupByKey}) to compute metrics along a
 * dimension the topic wasn't originally partitioned by, and combining
 * values with something other than a plain count. See
 * {@code topology/AnalyticsTopology.java} for both.
 */
@SpringBootApplication
@EnableKafkaStreams
public class AnalyticsServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsServiceApplication.class, args);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Two Kafka Streams apps in the same platform, same @EnableKafkaStreams
 *    annotation, completely independent consumer groups
 *    (application-id: analytics-service vs. fraud-detection-service) —
 *    both read order-created from the beginning, in parallel, neither
 *    aware the other exists. This is the fan-out property from Build
 *    Order Step 1 (different group-id = independent full copy), now
 *    demonstrated with two entire Streams topologies instead of two
 *    plain consumers.
 * ════════════════════════════════════════════════════════════════════════
 */
