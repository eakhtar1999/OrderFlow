package com.orderflow.saga.orchestration;

import com.orderflow.saga.client.InventoryServiceClient;
import com.orderflow.saga.client.PaymentServiceClient;
import com.orderflow.saga.client.ShipmentServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * The entire saga, in one method — this class IS the orchestration
 * pattern. Contrast this directly with the choreography saga, which has
 * no equivalent: no file anywhere lists "reserve, then charge, then
 * ship" as an explicit sequence; it only exists as an emergent property
 * of four services' independent listeners. Here, the sequence, the
 * compensation logic, and the saga's current state are all first-class,
 * readable, right here.
 */
@Service
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    // Same fake per-unit price order-service's controller uses — kept
    // identical so a saga started here and one started through
    // order-service's normal POST /api/orders compute the SAME
    // totalAmount for the same items, making payment-service's decline
    // threshold behave identically regardless of which saga style
    // triggered it.
    private static final double FAKE_UNIT_PRICE = 9.99;

    private final JdbcTemplate jdbcTemplate;
    private final InventoryServiceClient inventoryClient;
    private final PaymentServiceClient paymentClient;
    private final ShipmentServiceClient shipmentClient;

    public SagaOrchestrator(JdbcTemplate jdbcTemplate, InventoryServiceClient inventoryClient,
                             PaymentServiceClient paymentClient, ShipmentServiceClient shipmentClient) {
        this.jdbcTemplate = jdbcTemplate;
        this.inventoryClient = inventoryClient;
        this.paymentClient = paymentClient;
        this.shipmentClient = shipmentClient;
    }

    public SagaResult run(StartSagaRequest request) {
        String orderId = UUID.randomUUID().toString();
        double totalAmount = request.items().stream()
                .mapToDouble(item -> item.quantity() * FAKE_UNIT_PRICE)
                .sum();

        insertSaga(orderId, request, totalAmount);
        log.info("🎬 [saga {}] STARTED", orderId);

        List<InventoryServiceClient.Item> items = request.items().stream()
                .map(i -> new InventoryServiceClient.Item(i.productId(), i.quantity()))
                .toList();

        // STEP 1 — reserve inventory. Blocks here until inventory-service
        // answers (or Resilience4j gives up and falls back).
        InventoryServiceClient.ReserveResult reserveResult = inventoryClient.reserve(orderId, items);
        if (!reserveResult.reserved()) {
            updateStatus(orderId, "FAILED");
            log.warn("🎬 [saga {}] FAILED at inventory reservation — {}", orderId, reserveResult.message());
            return new SagaResult(orderId, "FAILED", "Inventory: " + reserveResult.message());
        }
        updateStatus(orderId, "INVENTORY_RESERVED");
        log.info("🎬 [saga {}] INVENTORY_RESERVED", orderId);

        // STEP 2 — charge payment.
        PaymentServiceClient.ChargeResult chargeResult =
                paymentClient.charge(orderId, request.customerId(), totalAmount);
        if (!chargeResult.approved()) {
            // COMPENSATE — explicitly, right here, as a direct method
            // call to the exact step that needs undoing. This is
            // orchestration's version of choreography's
            // PaymentFailedCompensationListener — same underlying
            // action (release stock), triggered by a direct call
            // instead of a listener reacting to a published fact.
            log.warn("🎬 [saga {}] payment declined — {} — compensating (releasing inventory)",
                    orderId, chargeResult.message());
            inventoryClient.release(orderId, items);
            updateStatus(orderId, "FAILED");
            log.warn("🎬 [saga {}] FAILED at payment, compensated", orderId);
            return new SagaResult(orderId, "FAILED", "Payment: " + chargeResult.message());
        }
        updateStatus(orderId, "PAYMENT_COMPLETED");
        log.info("🎬 [saga {}] PAYMENT_COMPLETED", orderId);

        // STEP 3 — create shipment. Always succeeds (see
        // ShipmentCreator's Javadoc) — no compensation branch needed
        // here, deliberately, to keep this saga's rollback story bounded
        // to the one failure mode it fully demonstrates.
        ShipmentServiceClient.ShipResult shipResult = shipmentClient.ship(orderId, request.customerId());
        updateStatus(orderId, "SHIPPED");
        log.info("🎬 [saga {}] SHIPPED — shipmentId={}", orderId, shipResult.shipmentId());

        return new SagaResult(orderId, "SHIPPED", "Shipment " + shipResult.shipmentId());
    }

    private void insertSaga(String orderId, StartSagaRequest request, double totalAmount) {
        jdbcTemplate.update(
                "INSERT INTO saga (order_id, customer_id, region, total_amount, status) VALUES (?, ?, ?, ?, ?)",
                orderId, request.customerId(), request.region(), totalAmount, "STARTED");
    }

    private void updateStatus(String orderId, String status) {
        // Each call is its OWN statement/transaction, not part of one
        // big transaction wrapping the whole saga — see schema.sql's
        // comment for why that's deliberate, not an oversight.
        jdbcTemplate.update("UPDATE saga SET status = ?, updated_at = ? WHERE order_id = ?",
                status, Timestamp.from(Instant.now()), orderId);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Orchestration's defining property, made completely literal: the
 *    saga's sequence, its compensation logic, and its state transitions
 *    are all readable top-to-bottom in ONE method. The coupling cost is
 *    real too — this class has compile-time knowledge of all three
 *    downstream services and their exact request/response shapes, where
 *    choreography's services know nothing about each other at all.
 * 2. Compensation here is an ordinary method call
 *    (inventoryClient.release(...)), not a message anyone reacts to —
 *    the orchestrator decides to compensate and makes it happen directly,
 *    rather than publishing a fact and hoping the right listener exists
 *    somewhere to interpret it.
 * 3. Saga state lives in a database this class owns and writes to
 *    directly and synchronously, one statement per transition — visible,
 *    queryable (`SELECT * FROM saga WHERE order_id = ...`), and NOT
 *    wrapped in one transaction spanning the whole saga (see
 *    updateStatus's comment) — a saga is fundamentally NOT a single ACID
 *    transaction, even in the orchestrated style.
 * ════════════════════════════════════════════════════════════════════════
 */
