package com.orderflow.shipment.internal;

/** Body for {@code POST /internal/ship} — order-saga-orchestrator's synchronous equivalent of payment-completed. */
public record ShipRequest(String orderId, String customerId) {
}
