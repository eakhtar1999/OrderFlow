package com.orderflow.order.outbox;

import java.util.List;

/**
 * The shape written into (and read back out of) the outbox table's
 * JSONB {@code payload} column.
 *
 * Deliberately its OWN type, not the Avro-generated
 * {@code com.orderflow.avro.OrderCreatedEvent}. Writing an Avro object
 * straight into Postgres would mean deciding on the Avro schema — and
 * therefore talking to Schema Registry — at REQUEST time, on the HTTP
 * thread. That's exactly the Kafka-adjacent work Build Order Step 5 is
 * trying to keep OUT of the request path. This plain record only knows
 * how to be JSON; {@code OutboxRelay} is where Avro enters the picture,
 * safely out of band, on its own schedule.
 */
public record OutboxOrderPayload(
        String orderId,
        String customerId,
        String region,
        List<Item> items,
        double totalAmount,
        long createdAt,
        String giftMessage
) {
    public record Item(String productId, int quantity) {
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. The outbox's stored shape doesn't have to match the eventual Kafka
 *    event's shape — it just has to carry enough information for
 *    OutboxRelay to BUILD that event later. Decoupling "what we
 *    durably remember" from "what we eventually publish" is what lets
 *    the publish step change independently (new Avro field, different
 *    topic, whatever) without touching how orders get written.
 * ════════════════════════════════════════════════════════════════════════
 */
