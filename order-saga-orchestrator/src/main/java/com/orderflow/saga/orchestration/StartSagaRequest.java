package com.orderflow.saga.orchestration;

import java.util.List;

/** Same shape as order-service's PlaceOrderRequest, deliberately — this is orchestration's entry point, so it should look like the SAME kind of order the choreography saga starts from, to make comparing the two honest. */
public record StartSagaRequest(String customerId, String region, List<Item> items) {
    public record Item(String productId, int quantity) {
    }
}
