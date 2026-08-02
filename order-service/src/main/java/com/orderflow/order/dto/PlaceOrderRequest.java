package com.orderflow.order.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/**
 * The HTTP request body for "place an order" — what the client sends us.
 *
 * Notice this is a DIFFERENT type from the Kafka event we'll publish
 * (OrderCreatedEvent). That's deliberate: the shape a client finds
 * convenient to send (no orderId — we generate that; no timestamp — the
 * server stamps that) is not the same shape the rest of the platform needs
 * to consume. Collapsing "API contract" and "event contract" into one
 * class is a trap: the day you need to add an internal-only field to the
 * event (a fraud score, say) you'd be leaking it into the public API too.
 */
public record PlaceOrderRequest(

        @NotBlank(message = "customerId is required")
        String customerId,

        @NotBlank(message = "region is required")
        String region,

        @NotEmpty(message = "an order needs at least one item")
        @Valid
        List<OrderItem> items,

        String notes

) {
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. API contract ≠ event contract. Keep them as separate types even when
 *    they look almost identical today — they will diverge over time, and
 *    untangling a merged type later is far more painful than the small
 *    duplication of keeping them apart from day one.
 * 2. `region` is captured here on purpose: later (Build Order Step 9+,
 *    analytics-service) we aggregate "revenue by region" with Kafka
 *    Streams windowed joins. Capturing the field now, at the source of
 *    truth, means we never have to backfill it.
 *
 * 🔧 TRY IT YOURSELF
 * Add a `notes` field to this record and send it in a request. Notice
 * OrderEventProducer/OrderCreatedEvent don't automatically get it — you
 * have to deliberately decide to carry it into the event. That decision
 * point is exactly where API/event coupling bugs are prevented.
 * ════════════════════════════════════════════════════════════════════════
 */
