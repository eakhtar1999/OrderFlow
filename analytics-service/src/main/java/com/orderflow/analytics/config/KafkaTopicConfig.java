package com.orderflow.analytics.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Both topics here are RETENTION-based — the default cleanup policy,
 * declared explicitly anyway so the choice reads as deliberate, not
 * forgotten. Worth contrasting directly with order-service's
 * order-status and fraud-detection-service's customer-profile, both
 * compacted: those topics answer "what's true RIGHT NOW for this key,"
 * where the history of how they got there doesn't matter once
 * superseded. These two topics answer a completely different question —
 * "what did this metric look like AT EACH POINT IN TIME" — a time
 * series, where the whole point is keeping every window's value, not
 * just the newest one. Compacting a metrics topic would silently throw
 * away every window except the last, which is exactly the data you'd
 * want for a dashboard showing a trend line.
 */
@Configuration
public class KafkaTopicConfig {

    public static final String ORDERS_PER_MINUTE_TOPIC = "orders-per-minute";
    public static final String REVENUE_BY_REGION_TOPIC = "revenue-by-region";

    @Bean
    public NewTopic ordersPerMinuteTopic() {
        return TopicBuilder.name(ORDERS_PER_MINUTE_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic revenueByRegionTopic() {
        return TopicBuilder.name(REVENUE_BY_REGION_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Retention vs. compaction is a per-topic DATA SHAPE decision, not a
 *    default you either remember or forget — this is the fourth time
 *    this exact choice has come up in this repo (order-created vs.
 *    order-status in Step 5, customer-profile vs. fraud-alerts in Step
 *    6, now orders-per-minute/revenue-by-region here), and every single
 *    time the right answer follows directly from one question: "does a
 *    consumer of this topic ever need history, or only the latest value
 *    per key?"
 * ════════════════════════════════════════════════════════════════════════
 */
