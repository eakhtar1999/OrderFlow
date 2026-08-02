package com.orderflow.saga.orchestration;

/** What {@code POST /api/saga/orders} returns — only once the WHOLE saga has finished, one way or the other. */
public record SagaResult(String orderId, String status, String message) {
}
