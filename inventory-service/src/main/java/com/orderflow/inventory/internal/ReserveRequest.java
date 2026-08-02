package com.orderflow.inventory.internal;

import java.util.List;

/** Body for {@code POST /internal/reserve} — order-saga-orchestrator's synchronous equivalent of order-created. */
public record ReserveRequest(String orderId, List<Item> items) {
    public record Item(String productId, int quantity) {
    }
}
