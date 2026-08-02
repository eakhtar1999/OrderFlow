package com.orderflow.payment.internal;

/** Body for {@code POST /internal/charge} — order-saga-orchestrator's synchronous equivalent of inventory-reserved. */
public record ChargeRequest(String orderId, String customerId, double totalAmount) {
}
