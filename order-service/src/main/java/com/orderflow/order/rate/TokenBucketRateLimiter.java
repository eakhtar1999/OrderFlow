package com.orderflow.order.rate;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * A distributed token bucket, backed by one Redis hash per rate-limited
 * key (here: per customerId — see {@code OrderController}). "Distributed"
 * matters the moment order-service ever runs more than one instance (the
 * same story inventory-service's Step 2 scaling already told): a
 * per-JVM counter would let each instance independently grant its own
 * full quota to the same customer, defeating the limit entirely. Every
 * instance sharing this Redis sees and updates the SAME bucket.
 *
 * The read-refill-consume-write sequence below has to be ATOMIC, or two
 * concurrent requests for the same customer could both read "3 tokens
 * left," both decide to allow, and both write back "2 tokens left" —
 * silently granting one more request than the bucket actually had. A
 * Lua script is how Redis gives you that atomicity: the whole script
 * runs as a single, uninterruptible operation on the Redis server,
 * exactly like the distributed lock's compare-and-delete script in
 * inventory-service (see DistributedLock.java) uses the same trick for a
 * different reason.
 */
@Component
public class TokenBucketRateLimiter {

    private static final String BUCKET_KEY_PREFIX = "ratelimit:order-api:";

    // Bucket state (current token count + last refill timestamp) lives in
    // a Redis HASH so both fields update together in one HMSET, then the
    // whole key gets a short expiry — an idle customer's bucket doesn't
    // need to occupy Redis memory forever, and a fresh bucket at full
    // capacity is exactly what SHOULD happen if nobody's requested
    // anything from this customerId in a while anyway.
    private static final DefaultRedisScript<Long> TOKEN_BUCKET_SCRIPT = new DefaultRedisScript<>(
            """
            local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'timestamp')
            local capacity = tonumber(ARGV[1])
            local refill_per_sec = tonumber(ARGV[2])
            local now_ms = tonumber(ARGV[3])
            local requested = tonumber(ARGV[4])

            local tokens = tonumber(bucket[1])
            local timestamp = tonumber(bucket[2])
            if tokens == nil then
                tokens = capacity
                timestamp = now_ms
            end

            local elapsed_ms = math.max(0, now_ms - timestamp)
            local refilled = elapsed_ms * refill_per_sec / 1000
            tokens = math.min(capacity, tokens + refilled)

            local allowed = 0
            if tokens >= requested then
                tokens = tokens - requested
                allowed = 1
            end

            redis.call('HMSET', KEYS[1], 'tokens', tokens, 'timestamp', now_ms)
            redis.call('EXPIRE', KEYS[1], 60)

            return allowed
            """,
            Long.class
    );

    private final StringRedisTemplate redisTemplate;
    private final int capacity;
    private final double refillPerSecond;

    public TokenBucketRateLimiter(
            StringRedisTemplate redisTemplate,
            @Value("${order.rate-limit.capacity}") int capacity,
            @Value("${order.rate-limit.refill-per-second}") double refillPerSecond
    ) {
        this.redisTemplate = redisTemplate;
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
    }

    /**
     * Returns true if a request for {@code key} is allowed right now
     * (and atomically consumes one token if so), false if the bucket is
     * currently empty.
     */
    public boolean tryConsume(String key) {
        List<String> keys = Collections.singletonList(BUCKET_KEY_PREFIX + key);
        Long allowed = redisTemplate.execute(
                TOKEN_BUCKET_SCRIPT,
                keys,
                String.valueOf(capacity),
                String.valueOf(refillPerSecond),
                String.valueOf(Instant.now().toEpochMilli()),
                "1"
        );
        return allowed != null && allowed == 1L;
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Token bucket vs. fixed-window counters: a fixed window (e.g. "max 5
 *    requests per clock-minute") allows a burst of up to 2x the limit
 *    right at a window boundary (5 at the end of minute N, 5 more at the
 *    start of minute N+1, 10 in a few seconds). A token bucket refills
 *    continuously instead of resetting on a clock edge, so the WORST
 *    case burst is exactly the bucket's capacity, never more, regardless
 *    of timing.
 * 2. Lazy refill, not a background job: this script never runs on a
 *    timer — every call recomputes "how many tokens should exist NOW,
 *    given how much time passed since the last call" from the stored
 *    timestamp. No scheduler, no idle buckets consuming CPU between
 *    requests, and it's correct even if a customer's bucket goes
 *    completely untouched for hours.
 * 3. Why this needs to be a single Lua script and not several separate
 *    Redis calls from Java: read-then-write with any gap in between is a
 *    race condition the moment two requests for the same key arrive
 *    concurrently — exactly the same class of bug the distributed lock
 *    exists to prevent in inventory-service, solved here with atomicity
 *    instead of mutual exclusion, because a rate limiter's job is
 *    "count correctly under concurrency," not "let only one caller in
 *    at a time."
 *
 * 🔧 TRY IT YOURSELF
 * Fire 10 POST /api/orders requests for the SAME customerId back to back
 * (a tight loop, no delay). With capacity=5, refill=1/sec, expect the
 * first 5 to succeed and the rest to get 429 immediately. Then wait ~5
 * seconds and try again — some requests succeed again, refilled at
 * roughly 1 per second, not all 5 at once. Finally, fire the same burst
 * for a DIFFERENT customerId concurrently and confirm it's completely
 * unaffected — this is a PER-CUSTOMER bucket, not a global one.
 * ════════════════════════════════════════════════════════════════════════
 */
