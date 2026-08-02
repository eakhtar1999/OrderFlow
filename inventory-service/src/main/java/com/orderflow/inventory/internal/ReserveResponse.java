package com.orderflow.inventory.internal;

/** Response for {@code POST /internal/reserve}. */
public record ReserveResponse(boolean reserved, String message) {
}
