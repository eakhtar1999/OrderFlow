package com.orderflow.inventory.internal;

import java.util.List;

/** Body for {@code POST /internal/release} — order-saga-orchestrator's compensating call, the REST equivalent of PaymentFailedCompensationListener. */
public record ReleaseRequest(String orderId, List<Item> items) {
    public record Item(String productId, int quantity) {
    }
}
