package com.orderflow.order.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the "order-created" topic as code instead of relying on Kafka's
 * auto-topic-creation (which is disabled in real clusters for a good
 * reason: a typo in a topic name shouldn't silently spin up a brand new
 * topic with default settings nobody chose).
 *
 * Spring's KafkaAdmin bean (auto-configured from application.yml's
 * bootstrap-servers) picks up every NewTopic bean in the context at
 * startup and reconciles the cluster to match — creating what's missing,
 * leaving existing topics alone.
 */
@Configuration
public class KafkaTopicConfig {

    public static final String ORDER_CREATED_TOPIC = "order-created";

    @Bean
    public NewTopic orderCreatedTopic() {
        return TopicBuilder.name(ORDER_CREATED_TOPIC)
                // Partitions = the unit of parallelism. 3 partitions means
                // up to 3 consumer instances in inventory-service's group
                // can process order-created concurrently. More partitions
                // = more parallelism headroom, but also more open file
                // handles/replication traffic per broker and — critically
                // — events for DIFFERENT keys can be reordered relative to
                // each other (never within the same key, see the producer
                // for why that matters).
                .partitions(3)
                // Replication factor 1 because our docker-compose runs a
                // single broker for this tutorial step. In production this
                // would be 3, so losing one broker doesn't lose data — see
                // the root README's "ISR / leader election" section for
                // where we simulate that failure later in the build order.
                .replicas(1)
                .build();
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Topics as code: declared in version control, reviewed in PRs, applied
 *    automatically on startup — instead of "someone ran a kafka-topics.sh
 *    command once and nobody remembers the settings."
 * 2. Partition count is a real capacity-planning decision with trade-offs
 *    (parallelism vs. ordering vs. per-broker overhead), not a default you
 *    ignore. Section 4 of the root README ("Hot partition simulation")
 *    revisits this exact bean.
 * 3. Replication factor is Kafka's core durability knob — it's what "ISR"
 *    (in-sync replicas) governs. RF=1 has zero fault tolerance; we call
 *    that out explicitly rather than hiding it.
 *
 * 🔧 TRY IT YOURSELF
 * Change `.partitions(3)` to `.partitions(1)`, restart order-service, place
 * a few orders for different customerIds, then start two instances of
 * inventory-service pointed at the same consumer group. Watch (via Kafka
 * UI, once wired up) that only ONE of the two instances ever gets work —
 * you can't have more active consumers in a group than partitions.
 * ════════════════════════════════════════════════════════════════════════
 */
