package com.orderflow.inventory.idempotency;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Closes the gap flagged since Build Order Step 1: at-least-once delivery
 * (manual offset commit, see {@code OrderEventListener}) guarantees a
 * message that crashed mid-processing gets redelivered, not lost — but it
 * says nothing about what happens the SECOND time that same message
 * arrives. Without this class, a redelivered order-created would run
 * {@code tryReserve} again and double-decrement stock for an order that
 * was already fully handled.
 *
 * A TTL-backed marker in Redis, keyed by orderId — "have I already made a
 * reservation decision for this exact order?" A plain boolean, not a
 * counter or a timestamp: this class doesn't need to know HOW MANY times
 * a message was redelivered, only whether the first successful attempt
 * already happened.
 */
@Component
public class IdempotencyStore {

    private static final String PROCESSED_KEY_PREFIX = "processed:order:";

    private final StringRedisTemplate redisTemplate;
    private final Duration ttl;

    public IdempotencyStore(
            StringRedisTemplate redisTemplate,
            @Value("${inventory.dedupe.ttl-hours}") long ttlHours
    ) {
        this.redisTemplate = redisTemplate;
        this.ttl = Duration.ofHours(ttlHours);
    }

    public boolean alreadyProcessed(String orderId) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(PROCESSED_KEY_PREFIX + orderId));
    }

    /**
     * Marked AFTER the reservation decision has fully run — same "only
     * commit once the work is actually done" principle as
     * {@code acknowledgment.acknowledge()} being the LAST line of
     * {@code onOrderCreated}, not the first. A crash between deciding and
     * marking means this key never gets set AND the Kafka offset never
     * gets committed — on redelivery, both are consistently "not done
     * yet," and reprocessing from scratch is exactly correct.
     */
    public void markProcessed(String orderId) {
        redisTemplate.opsForValue().set(PROCESSED_KEY_PREFIX + orderId, "1", ttl);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. At-least-once delivery and idempotent consumption are two DIFFERENT,
 *    complementary responsibilities. Kafka's manual-ack guarantees a
 *    message isn't silently lost on crash — it says nothing about
 *    duplicate processing being safe. That safety is the APPLICATION's
 *    job, not the broker's, and this class is where inventory-service
 *    finally does it.
 * 2. Why the marker is set AFTER processing, not before: setting it
 *    first would mean a message that crashes mid-processing looks
 *    "already handled" on redelivery and gets silently skipped — the
 *    exact opposite of what at-least-once delivery promises. Order
 *    matters here as much as the Redis call itself does.
 * 3. A TTL, not a permanent record: this store only needs to remember an
 *    orderId for as long as REDELIVERY is realistically possible (a
 *    retry-topic hop, a rebalance-driven reprocess) — not forever. An
 *    unbounded audit trail of every order ever processed is a
 *    completely different concern (event sourcing via the Kafka log
 *    itself, or Build Order Step 10's Elasticsearch indexer), not what
 *    this class is for.
 *
 * 🔧 TRY IT YOURSELF
 * Place a normal order, let it fully process. Then manually replay the
 * SAME message (e.g., reset this consumer group's offset for
 * order-created back to before that message with kafka-consumer-groups.sh
 * --reset-offsets, or just restart inventory-service with a fresh
 * group-id pointed at `earliest` temporarily) and watch the ♻️ skip log
 * line fire instead of a second reservation — confirm stock in Postgres
 * is unchanged by the replay.
 * ════════════════════════════════════════════════════════════════════════
 */
