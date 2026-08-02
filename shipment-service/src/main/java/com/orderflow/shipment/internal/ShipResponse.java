package com.orderflow.shipment.internal;

/** Response for {@code POST /internal/ship}. */
public record ShipResponse(String shipmentId) {
}
