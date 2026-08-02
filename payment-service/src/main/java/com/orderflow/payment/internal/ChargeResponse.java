package com.orderflow.payment.internal;

/** Response for {@code POST /internal/charge}. */
public record ChargeResponse(boolean approved, String message) {
}
