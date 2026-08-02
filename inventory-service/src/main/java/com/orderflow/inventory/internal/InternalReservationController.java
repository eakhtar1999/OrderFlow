package com.orderflow.inventory.internal;

import com.orderflow.inventory.service.StockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * Build Order Step 8's second saga-coordination style, entering
 * inventory-service through a completely different door than
 * {@code OrderEventListener}. Same {@link StockService} underneath, same
 * business rules — but {@code order-saga-orchestrator} calls this
 * SYNCHRONOUSLY over HTTP and blocks waiting for an answer, where
 * choreography's Kafka listener reacts to an event asynchronously and
 * "answers" by publishing a fact nobody in particular is waiting on. Two
 * coordination protocols, one domain capability, reused rather than
 * duplicated.
 */
@RestController
public class InternalReservationController {

    private static final Logger log = LoggerFactory.getLogger(InternalReservationController.class);

    private final StockService stockService;

    public InternalReservationController(StockService stockService) {
        this.stockService = stockService;
    }

    @PostMapping("/internal/reserve")
    public ResponseEntity<ReserveResponse> reserve(@RequestBody ReserveRequest request) {
        List<ReserveRequest.Item> reservedSoFar = new ArrayList<>();
        for (ReserveRequest.Item item : request.items()) {
            boolean reserved = stockService.tryReserve(item.productId(), item.quantity());
            if (reserved) {
                reservedSoFar.add(item);
            } else {
                // Same local-rollback rule OrderEventListener applies on
                // the choreography path — a partial reservation isn't a
                // valid end state no matter which door it came through.
                for (ReserveRequest.Item r : reservedSoFar) {
                    stockService.release(r.productId(), r.quantity());
                }
                log.warn("❌ [orchestrated] Order {} could not be fully reserved", request.orderId());
                return ResponseEntity.ok(new ReserveResponse(false, "Insufficient stock"));
            }
        }
        log.info("📦 [orchestrated] Order {} fully reserved", request.orderId());
        return ResponseEntity.ok(new ReserveResponse(true, "Reserved"));
    }

    @PostMapping("/internal/release")
    public ResponseEntity<Void> release(@RequestBody ReleaseRequest request) {
        for (ReleaseRequest.Item item : request.items()) {
            stockService.release(item.productId(), item.quantity());
        }
        log.info("↩️  [orchestrated] Released {} item(s) for order {} (compensation)",
                request.items().size(), request.orderId());
        return ResponseEntity.ok().build();
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Same business logic, two entry points: OrderEventListener (async,
 *    Kafka, choreography) and this controller (sync, REST, orchestration)
 *    both ultimately call StockService.tryReserve/release. The
 *    COORDINATION style is what differs, not the domain capability
 *    itself — a real, common pattern for services that need to support
 *    both styles during a migration or for different callers.
 * 2. This response is synchronous and blocking from the CALLER's
 *    perspective (order-saga-orchestrator's thread waits here) — directly
 *    contrasting with order-service's `POST /api/orders`, which returns
 *    202 before anything downstream has even started. That's the
 *    orchestration-vs-choreography latency trade-off made concrete: one
 *    caller waits for one step to finish; the other never waits for
 *    anything past its own database write.
 * ════════════════════════════════════════════════════════════════════════
 */
