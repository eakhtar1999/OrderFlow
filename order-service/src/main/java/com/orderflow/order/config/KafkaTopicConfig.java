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

    // Build Order Step 3 aside: Schema Registry's default "TopicNameStrategy"
    // derives the schema subject from this exact topic name plus "-value"
    // — i.e. "order-created-value". Rename this constant and, without
    // touching a single line of Avro config, your next deploy registers
    // an entirely new, empty subject with no compatibility history at all.
    public static final String ORDER_CREATED_TOPIC = "order-created";

    // Build Order Step 5: "what's the CURRENT status of order X" —
    // answered by a log-COMPACTED topic instead of a regular one. Same
    // TopicNameStrategy subject-naming gotcha as above applies:
    // "order-status-value" is derived from this exact string.
    public static final String ORDER_STATUS_TOPIC = "order-status";

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
                // single broker. In production this would be 3, so losing
                // one broker doesn't lose data — but a single-broker
                // cluster has no second replica to fail over TO, so this
                // project never actually simulates that failure. Flagged,
                // not demonstrated — see docs/system-design.md §8.
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderStatusTopic() {
        return TopicBuilder.name(ORDER_STATUS_TOPIC)
                .partitions(3)
                .replicas(1)
                // The one line that makes this topic fundamentally
                // different from order-created: cleanup.policy=compact
                // instead of the default cleanup.policy=delete.
                //
                // A retention-based topic (order-created) keeps EVERY
                // record until it ages out (or disk fills) — it's an
                // immutable log of every event that ever happened, which
                // is exactly what you want for "replay history" or
                // "feed every future consumer the full sequence."
                //
                // A compacted topic keeps only the MOST RECENT record per
                // KEY, forever, discarding superseded ones in the
                // background (Kafka's log cleaner thread, on its own
                // schedule — not instant). That only makes sense because
                // every record here is keyed by orderId (see
                // OutboxRelay.java) — compaction without a meaningful key
                // would be nonsensical, since "latest per key" needs a
                // key worth deduplicating on.
                //
                // This is the trade-off in one sentence: order-created
                // answers "what happened, in order"; order-status answers
                // "what's true right now" — same platform, two different
                // questions, two different topic configs.
                .compact()
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
 *    ignore. The hot-partition risk this creates for a per-customer key
 *    (docs/system-design.md §7.1) is discussed there, not fixed here.
 * 3. Replication factor is Kafka's core durability knob — it's what "ISR"
 *    (in-sync replicas) governs. RF=1 has zero fault tolerance; we call
 *    that out explicitly rather than hiding it.
 * 4. Retention vs. compaction is a per-topic choice, not a global Kafka
 *    setting — order-created and order-status sit in the SAME cluster
 *    with opposite cleanup policies, because they answer different
 *    questions ("what happened" vs. "what's true now"). Picking the
 *    wrong one for a given use case either wastes disk forever (compact
 *    on something you needed full history for) or loses the "current
 *    state" query you actually wanted (delete on something you needed
 *    deduplicated by key).
 *
 * 🔧 TRY IT YOURSELF
 * Change `.partitions(3)` to `.partitions(1)`, restart order-service, place
 * a few orders for different customerIds, then start two instances of
 * inventory-service pointed at the same consumer group. Watch (via Kafka
 * UI, once wired up) that only ONE of the two instances ever gets work —
 * you can't have more active consumers in a group than partitions.
 * ════════════════════════════════════════════════════════════════════════
 */
