package com.orderflow.inventory.service;

import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory stand-in for "real" inventory storage. This is deliberately
 * the crudest possible thing that could work: a ConcurrentHashMap seeded
 * with a few fake products.
 *
 * This is a placeholder, not the final design — flagging that explicitly
 * rather than pretending it's production-ready:
 *   - It's per-instance. Run two inventory-service processes (which
 *     Build Order Step 2 asks you to do, to see consumer-group
 *     rebalancing) and each has its OWN, DIFFERENT view of stock. That's
 *     obviously wrong for a real system — Build Order Step 9 replaces
 *     this with Postgres as the source of truth plus a Redis cache, and
 *     adds a distributed lock (Redlock/SETNX) specifically because
 *     "multiple instances decrementing the same counter concurrently" is
 *     a real overselling bug otherwise.
 *   - It has no persistence: restart the app, stock resets. Fine for a
 *     tutorial, not fine for a warehouse.
 * We're keeping it this simple ON PURPOSE for Step 1 so the Kafka
 * consumer-side concepts (manual ack, consumer groups, at-least-once)
 * aren't buried under a database setup.
 */
@Service
public class StockService {

    private final Map<String, Integer> stockByProductId = new ConcurrentHashMap<>(Map.of(
            "sku-42", 50,
            "sku-7", 5,
            "sku-99", 0
    ));

    /**
     * Attempts to reserve the given quantity. Returns true and decrements
     * stock if enough was available; returns false (and leaves stock
     * untouched) otherwise.
     *
     * `synchronized` gives us an atomic check-and-decrement within THIS
     * process — good enough to prove the "insufficient stock" branch
     * works correctly under concurrent requests to a single instance. See
     * the class comment above for why "within this process" stops being
     * good enough the moment you run a second instance, and what replaces
     * this method then.
     */
    public synchronized boolean tryReserve(String productId, int quantity) {
        Integer currentStock = stockByProductId.get(productId);
        if (currentStock == null || currentStock < quantity) {
            return false;
        }
        stockByProductId.put(productId, currentStock - quantity);
        return true;
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
     * actually reserved for this exact product — this map has no concept
     * of "reservations," only current totals, so releasing MORE than was
     * ever reserved would silently over-credit stock. Acceptable here
     * because the only caller passes back exactly what a prior
     * InventoryReserved event said was reserved; a real inventory system
     * would track reservations as their own record, not just a running
     * total, specifically to make this operation self-validating.
     */
    public synchronized void release(String productId, int quantity) {
        stockByProductId.merge(productId, quantity, Integer::sum);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. "Good enough for the concept being taught right now" is a legitimate
 *    engineering decision, as long as it's LABELED as a placeholder with a
 *    concrete plan for what replaces it and why (see the class comment).
 *    Silent placeholders masquerading as finished code are what cause
 *    production incidents.
 * 2. The overselling race condition: two threads (or two service
 *    instances) both reading currentStock=1, both deciding "yes there's
 *    enough for my order of 1", both decrementing — you've now sold the
 *    same last unit twice. ConcurrentHashMap.computeIfPresent makes the
 *    read-modify-write atomic WITHIN one JVM, which is necessary but not
 *    sufficient once you scale out.
 * 3. Compensating transactions (Build Order Step 8) aren't a database
 *    ROLLBACK — there's no transaction spanning the original reserve and
 *    this release; they're two entirely separate operations, connected
 *    only by both reacting to facts about the SAME order over time. The
 *    system passes through a real intermediate state (genuinely
 *    reserved, stock genuinely lower) before eventually correcting
 *    itself — that window is the defining trade-off of a saga versus a
 *    single ACID transaction.
 *
 * 🔧 TRY IT YOURSELF
 * Place two orders for "sku-7" with quantity 5 each, back to back, as fast
 * as you can (script a loop). Only one should succeed (stock=5, first
 * order takes all 5, second is refused) — verify that in the logs. Then
 * come back after Build Order Step 9 and do the SAME experiment with two
 * inventory-service instances running against shared Postgres+Redis
 * state, without a distributed lock, to actually witness overselling
 * before you fix it.
 * ════════════════════════════════════════════════════════════════════════
 */
