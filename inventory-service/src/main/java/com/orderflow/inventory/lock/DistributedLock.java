package com.orderflow.inventory.lock;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

/**
 * A single-Redis-instance distributed lock via {@code SET key value NX PX
 * ttl} — the mechanism the actual Redlock algorithm builds on, simplified
 * down to one instance because this project runs exactly one Redis
 * container. Real Redlock coordinates a MAJORITY of several independent
 * Redis instances specifically to survive one of them crashing or
 * partitioning away; that failure mode doesn't exist here, so this class
 * doesn't pretend to solve it. What it DOES solve, correctly: two
 * inventory-service instances racing to reserve the same product can no
 * longer both "win" the check-then-decrement race that plain Postgres
 * transactions alone don't prevent when the check and the write are two
 * separate statements.
 */
@Component
public class DistributedLock {

    private static final String LOCK_KEY_PREFIX = "lock:stock:";

    // Compare-and-delete, atomically. Without the Lua script, "check the
    // value matches my token, then delete" would be TWO round trips —
    // and between them, this exact lock could have already expired (TTL)
    // and been re-acquired by a completely different caller. Deleting
    // unconditionally at that point would release a lock you no longer
    // own, letting a THIRD caller in while the second caller still
    // believes it's holding the lock. The script makes "is it still
    // mine?" and "delete it" a single atomic Redis operation.
    private static final DefaultRedisScript<Long> SAFE_RELEASE_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                    "return redis.call('del', KEYS[1]) " +
                    "else return 0 end",
            Long.class
    );

    private final StringRedisTemplate redisTemplate;

    public DistributedLock(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Attempts to acquire the lock for {@code resourceId}, returning a
     * unique token on success or {@code null} if someone else already
     * holds it. The token is what makes {@link #release} safe — see its
     * Javadoc.
     */
    public String tryLock(String resourceId, Duration ttl) {
        String token = UUID.randomUUID().toString();
        // setIfAbsent = Redis SET ... NX: only succeeds if the key
        // doesn't already exist. That single atomic check-and-set is
        // the entire mechanism — no separate "check if locked" call
        // that could race against another caller's SET in between.
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(LOCK_KEY_PREFIX + resourceId, token, ttl);
        return Boolean.TRUE.equals(acquired) ? token : null;
    }

    /**
     * Releases the lock ONLY if {@code token} matches what's currently
     * stored — i.e., only if this caller still actually holds it. A
     * caller whose lock already expired (TTL) and was re-acquired by
     * someone else calling this after its own work finished would
     * otherwise silently steal that someone else's lock out from under
     * them.
     */
    public void unlock(String resourceId, String token) {
        redisTemplate.execute(SAFE_RELEASE_SCRIPT,
                Collections.singletonList(LOCK_KEY_PREFIX + resourceId),
                token);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. SET key value NX PX ttl is the WHOLE lock primitive: atomic
 *    check-and-set (NX = only if absent) plus a self-expiring safety net
 *    (PX = expire after N ms) in one Redis command, so a crashed
 *    lock-holder can never wedge a resource forever.
 * 2. The unique token is what turns "delete the lock key" into "delete
 *    MY lock, if I still hold it" — without it, a caller whose TTL
 *    already expired could delete a DIFFERENT caller's now-active lock,
 *    since both would be deleting the same key name with no way to tell
 *    whose turn it currently is.
 * 3. Real Redlock (the actual Redis-endorsed algorithm) runs this same
 *    NX/PX primitive against an ODD number of INDEPENDENT Redis
 *    instances and requires a majority to agree — specifically to
 *    survive one Redis node crashing or network-partitioning away mid-
 *    lock. This class doesn't do that: one Redis instance is a single
 *    point of failure for locking, a real and known trade-off, not an
 *    oversight, for a tutorial running one Redis container.
 *
 * 🔧 TRY IT YOURSELF
 * See StockService.java's TRY IT YOURSELF for the actual overselling
 * experiment this class exists to fix — run it once WITHOUT the lock
 * (comment out the tryLock/unlock calls) and watch two concurrent
 * reservations both succeed against stock that only had enough for one;
 * then run it again WITH the lock and watch the second one correctly
 * fail.
 * ════════════════════════════════════════════════════════════════════════
 */
