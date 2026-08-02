package com.orderflow.fraud.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the two topics this module owns. {@code order-created} — the
 * one this whole topology reads from — is declared by order-service, not
 * here; owning a topic's config is a producer-side responsibility in
 * this project (see order-service's KafkaTopicConfig.java), and
 * fraud-detection-service is purely a consumer of it.
 */
@Configuration
public class KafkaTopicConfig {

    public static final String CUSTOMER_PROFILE_TOPIC = "customer-profile";
    public static final String FRAUD_ALERTS_TOPIC = "fraud-alerts";

    @Bean
    public NewTopic customerProfileTopic() {
        return TopicBuilder.name(CUSTOMER_PROFILE_TOPIC)
                .partitions(3)
                .replicas(1)
                // Compacted — this is a KTable source (see
                // FraudDetectionTopology.java), and a KTable is
                // fundamentally "latest value per key." A retention-based
                // topic here would mean replaying every historical
                // profile change on every app restart before the table
                // reflects reality; compaction keeps that rebuild fast
                // and bounded regardless of how long this topic has
                // existed.
                .compact()
                .build();
    }

    @Bean
    public NewTopic fraudAlertsTopic() {
        return TopicBuilder.name(FRAUD_ALERTS_TOPIC)
                .partitions(3)
                .replicas(1)
                // Retention-based (the default) — an alert is a fact
                // that happened, not a piece of current state. We want
                // the full history of every alert ever raised, not just
                // the latest one per customer.
                .build();
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Topic ownership as a convention: the SERVICE THAT PRODUCES to a
 *    topic declares its config, not every service that happens to
 *    consume it. order-service owns order-created's NewTopic bean even
 *    though this module (and inventory-service) both read from it.
 * 2. Compaction choice mirrors the DATA'S shape, not the topic's name or
 *    vibe: customer-profile is compacted because it backs a KTable
 *    (state, "latest per key"); fraud-alerts is retention-based because
 *    it's an event log (facts, "everything that happened"). Same
 *    reasoning as order-status vs. order-created in order-service — this
 *    is the third time this exact decision has come up, which is the
 *    point: it's a real, recurring design question, not a one-off.
 * ════════════════════════════════════════════════════════════════════════
 */
