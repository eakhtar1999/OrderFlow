package com.orderflow.inventory.service;

import com.orderflow.inventory.lock.DistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;

/**
 * Stock storage and reservation logic. Through Build Order Step 8 this was
 * an in-memory {@code ConcurrentHashMap} — per-instance, resets on
 * restart, and (the actual bug this class exists to fix) each running
 * instance had its OWN, DIFFERENT view of stock, so two instances could
 * both "successfully" reserve the last unit of the same product.
 *
 * Build Order Step 9 replaces that with three cooperating pieces:
 *   1. Postgres (the {@code stock} table) as the real, shared source of
 *      truth — every instance reads and writes the SAME data now.
 *   2. Redis as a cache-aside read layer in front of it, so a hot
 *      product's stock count doesn't need a database round trip on every
 *      single reservation attempt.
 *   3. A Redis-backed {@link DistributedLock} wrapping the whole
 *      check-then-write, so two instances (or two threads in one
 *      instance) can no longer both read "yes, enough stock" before
 *      either has written its decrement — the exact race Postgres alone
 *      does NOT prevent when the read and the write are two separate
 *      statements, even inside a transaction, without an explicit lock
 *      or {@code SELECT ... FOR UPDATE}.
 */
@Service
public class StockService {

    private static final Logger log = LoggerFactory.getLogger(StockService.class);

    private static final String STOCK_CACHE_KEY_PREFIX = "stock:";

    // Bounds on how long a caller will wait for the distributed lock
    // before giving up. Contention on the SAME product from two
    // concurrent requests is normal, expected behavior — worth briefly
    // retrying, not instantly failing the whole reservation over. A
    // request that exhausts this budget throws, which (for the Kafka
    // listener path) lands straight in Build Order Step 4's existing
    // retry-topic machinery — two previously-separate features
    // cooperating without any new code written to connect them.
    private static final long LOCK_ACQUIRE_TIMEOUT_MS = 2000;
    private static final long LOCK_RETRY_DELAY_MS = 50;

    private final JdbcTemplate jdbcTemplate;
    private final StringRedisTemplate redisTemplate;
    private final DistributedLock distributedLock;
    private final Duration cacheTtl;
    private final Duration lockTtl;

    public StockService(
            JdbcTemplate jdbcTemplate,
            StringRedisTemplate redisTemplate,
            DistributedLock distributedLock,
            @Value("${inventory.cache.ttl-seconds}") long cacheTtlSeconds,
            @Value("${inventory.lock.ttl-ms}") long lockTtlMs
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.redisTemplate = redisTemplate;
        this.distributedLock = distributedLock;
        this.cacheTtl = Duration.ofSeconds(cacheTtlSeconds);
        this.lockTtl = Duration.ofMillis(lockTtlMs);
    }

    /**
     * Attempts to reserve the given quantity. Returns true and decrements
     * stock (in Postgres, then invalidates the cache) if enough was
     * available; returns false (leaving stock untouched) otherwise.
     *
     * The lock is what makes this safe across concurrent callers, whether
     * they're two threads in this JVM or two separate inventory-service
     * instances — see the class Javadoc and {@link DistributedLock}.
     */
    public boolean tryReserve(String productId, int quantity) {
        String lockToken = acquireLockWithRetry(productId);
        try {
            int currentStock = readThroughCache(productId);
            if (currentStock < quantity) {
                return false;
            }
            jdbcTemplate.update(
                    "UPDATE stock SET quantity = quantity - ? WHERE product_id = ?",
                    quantity, productId);
            invalidateCache(productId);
            return true;
        } finally {
            distributedLock.unlock(productId, lockToken);
        }
    }

    /**
     * Build Order Step 8: the compensating half of tryReserve — undoes a
     * reservation that turned out not to be needed after all, because
     * something LATER in the saga failed (payment declined; see
     * PaymentFailedCompensationListener.java). This is the mechanical
     * core of a choreographed saga's rollback: there's no database
     * transaction spanning "reserve" and "release," just two independent
     * calls, triggered by two independent events, that happen to net out
     * to the stock level being correct again — eventually, not
     * instantaneously.
     *
     * Deliberately NOT validating that `quantity` matches what was
     * actually reserved for this exact product — the stock table has no
     * concept of "reservations," only current totals, so releasing MORE
     * than was ever reserved would silently over-credit stock. Acceptable
     * here because the only caller passes back exactly what a prior
     * InventoryReserved event said was reserved; a real inventory system
     * would track reservations as their own record, not just a running
     * total, specifically to make this operation self-validating.
     *
     * Locked the same way tryReserve is — a release racing a reserve for
     * the same product is exactly the kind of interleaving the lock
     * exists to serialize, not just the reserve-vs-reserve case.
     */
    public void release(String productId, int quantity) {
        String lockToken = acquireLockWithRetry(productId);
        try {
            jdbcTemplate.update(
                    "UPDATE stock SET quantity = quantity + ? WHERE product_id = ?",
                    quantity, productId);
            invalidateCache(productId);
        } finally {
            distributedLock.unlock(productId, lockToken);
        }
    }

    /**
     * Cache-aside: check Redis first; on a miss, read Postgres (the
     * source of truth) and populate the cache before returning. The
     * cache is only ever POPULATED here, on a read miss — writes
     * (tryReserve/release) never write through to Redis directly, they
     * just invalidate it (see {@link #invalidateCache}) and let the NEXT
     * read repopulate it with a fresh value. That's the "aside" in
     * cache-aside: the cache sits beside the source of truth, not
     * between the application and every write.
     */
    private int readThroughCache(String productId) {
        String cacheKey = STOCK_CACHE_KEY_PREFIX + productId;
        String cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            log.debug("🎯 Cache HIT for {}", productId);
            return Integer.parseInt(cached);
        }

        log.debug("❌ Cache MISS for {} — reading Postgres", productId);
        List<Integer> rows = jdbcTemplate.query(
                "SELECT quantity FROM stock WHERE product_id = ?",
                (rs, rowNum) -> rs.getInt("quantity"),
                productId);
        int quantity = rows.isEmpty() ? 0 : rows.get(0);

        redisTemplate.opsForValue().set(cacheKey, String.valueOf(quantity), cacheTtl);
        return quantity;
    }

    /**
     * Invalidate-on-write, not update-on-write: deleting the key (rather
     * than writing the new value directly into Redis here) means the
     * very next reader always does a real Postgres read to repopulate
     * it — slightly more database traffic right after a write, in
     * exchange for never having to prove this method computed the
     * post-write value correctly. Simpler to reason about, and this
     * project only writes to `stock` from inside this class, so there's
     * no OTHER writer whose changes an update-on-write approach would
     * need to coordinate with anyway.
     */
    private void invalidateCache(String productId) {
        redisTemplate.delete(STOCK_CACHE_KEY_PREFIX + productId);
    }

    private String acquireLockWithRetry(String productId) {
        long deadline = System.currentTimeMillis() + LOCK_ACQUIRE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            String token = distributedLock.tryLock(productId, lockTtl);
            if (token != null) {
                return token;
            }
            try {
                Thread.sleep(LOCK_RETRY_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for stock lock on " + productId, e);
            }
        }
        throw new IllegalStateException(
                "Could not acquire stock lock for " + productId + " within " + LOCK_ACQUIRE_TIMEOUT_MS + "ms");
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Cache-aside, precisely: reads check the cache first and populate it
 *    on a miss; writes go to the source of truth and INVALIDATE the
 *    cache rather than trying to keep it correct in place. The
 *    read-after-write cost (one guaranteed cache miss right after every
 *    write) is the trade for never having two different code paths that
 *    both need to compute "the new value" correctly.
 * 2. The overselling race, actually fixed this time: two concurrent
 *    tryReserve calls for the same product now serialize through the
 *    distributed lock — the SECOND caller's readThroughCache only runs
 *    after the FIRST caller's write has already committed and
 *    invalidated the cache, so it sees the true post-decrement stock,
 *    not a stale pre-decrement read. Compare Build Order Step 1's
 *    `synchronized` keyword, which only ever serialized calls WITHIN one
 *    JVM — this serializes across every instance sharing this Redis.
 * 3. A distributed lock adds real latency and a real new failure mode
 *    (what happens if Redis itself is down? every reservation call now
 *    fails, where the old in-memory version would have kept working) —
 *    not a free upgrade. Worth sitting with: correctness and
 *    availability traded against each other here, not simultaneously
 *    improved.
 *
 * 🔧 TRY IT YOURSELF
 * The experiment Step 1's version of this file promised you'd come back
 * for: place two orders for "sku-7" (stock=5) with quantity 5 each, back
 * to back, from TWO DIFFERENT running inventory-service instances (see
 * Build Order Step 2 for how to run a second one). Both should NOT
 * succeed — exactly one order gets the stock, the other is correctly
 * refused, verified against ONE shared Postgres table this time, not two
 * separate in-memory maps that never had a chance to disagree honestly.
 *
 * To see the bug this class fixes, not just the fix: temporarily
 * short-circuit acquireLockWithRetry to always return a fresh token
 * without ever calling distributedLock.tryLock, restart both instances,
 * and re-run the same experiment. Watch both orders "succeed," and
 * `SELECT quantity FROM stock WHERE product_id='sku-7'` come back
 * negative — a debt the old in-memory map could never even represent,
 * since ConcurrentHashMap.put doesn't stop you from writing a negative
 * number either.
 * ════════════════════════════════════════════════════════════════════════
 */
