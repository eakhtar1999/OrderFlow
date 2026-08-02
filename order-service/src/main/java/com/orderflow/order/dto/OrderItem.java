package com.orderflow.order.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

/**
 * A single line item on an order: "2 units of product SKU-42".
 *
 * This is a Java record, not a class with getters/setters hand-written out.
 * Records give us an immutable, final, constructor-validated value type in
 * one line — exactly what an event-carrying DTO should be. You never want
 * a mutable object floating around after you've already handed a copy of
 * its data to Kafka.
 */
public record OrderItem(

        @NotBlank(message = "productId must not be blank")
        String productId,

        @Positive(message = "quantity must be at least 1")
        int quantity

) {
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Records for DTOs: immutability by default maps naturally onto "events
 *    are facts that happened" — a fact doesn't change after the fact.
 * 2. Bean Validation (@NotBlank, @Positive) fails fast at the API boundary
 *    instead of letting garbage data flow deep into the Kafka pipeline
 *    where it's much harder to trace back to "who sent this?".
 *
 * 🔧 TRY IT YOURSELF
 * POST an order with quantity: -1 and watch Spring return a 400 before a
 * single byte reaches Kafka. Validation at the edge is cheaper than a
 * "bad event" cleanup job three services downstream.
 * ════════════════════════════════════════════════════════════════════════
 */
