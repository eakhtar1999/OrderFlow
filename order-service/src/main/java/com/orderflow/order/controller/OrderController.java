package com.orderflow.order.controller;

import com.orderflow.order.dto.PlaceOrderRequest;
import com.orderflow.order.outbox.OutboxOrderPayload;
import com.orderflow.order.outbox.OutboxWriter;
import com.orderflow.order.rate.TokenBucketRateLimiter;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The only HTTP endpoint in the whole OrderFlow platform (for now). Every
 * other service reacts to Kafka events — this is where the outside world
 * gets in.
 *
 * Build Order Step 5: this class no longer imports anything Kafka-shaped
 * at all — no {@code KafkaTemplate}, no Avro type, nothing. Compare to
 * Steps 1-4, where this method built an Avro event and handed it to
 * {@code OrderEventProducer} directly, on this same thread. Now it talks
 * to exactly one thing — {@link OutboxWriter}, i.e. Postgres — and
 * that's the entire architectural shift Step 5 makes: Kafka moved
 * strictly downstream and out of band, see {@code OutboxRelay}.
 */
@RestController
public class OrderController {

    // A fake unit price so we can compute a totalAmount without standing
    // up a whole product-catalog service just for this tutorial step. A
    // real system would call (or cache-read, see the Redis build-order
    // steps) a pricing/catalog service here instead.
    private static final double FAKE_UNIT_PRICE = 9.99;

    private final OutboxWriter outboxWriter;
    private final TokenBucketRateLimiter rateLimiter;

    public OrderController(OutboxWriter outboxWriter, TokenBucketRateLimiter rateLimiter) {
        this.outboxWriter = outboxWriter;
        this.rateLimiter = rateLimiter;
    }

    @PostMapping("/api/orders")
    public ResponseEntity<Map<String, String>> placeOrder(@Valid @RequestBody PlaceOrderRequest request) {
        // Build Order Step 9: rate-limited PER CUSTOMER, checked before
        // anything else — a customer over their limit shouldn't cost us
        // a Postgres write attempt just to be told no. See
        // TokenBucketRateLimiter's Javadoc for why this needs to be
        // atomic and what algorithm it actually implements.
        if (!rateLimiter.tryConsume(request.customerId())) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(Map.of(
                            "status", "RATE_LIMITED",
                            "message", "Too many orders placed too quickly for customer " + request.customerId()
                    ));
        }

        // We generate the orderId here, server-side, rather than trusting
        // a client-supplied one. This is also the natural place a REST
        // idempotency key would plug in later (Section 4, "Idempotency at
        // the REST API layer") — a client retrying a timed-out POST with
        // the same Idempotency-Key header should get the SAME orderId
        // back, not a duplicate order. Step 1 doesn't implement that yet;
        // flagging where it belongs.
        String orderId = UUID.randomUUID().toString();

        double totalAmount = request.items().stream()
                .mapToDouble(item -> item.quantity() * FAKE_UNIT_PRICE)
                .sum();

        List<OutboxOrderPayload.Item> items = request.items().stream()
                .map(item -> new OutboxOrderPayload.Item(item.productId(), item.quantity()))
                .toList();

        OutboxOrderPayload payload = new OutboxOrderPayload(
                orderId,
                request.customerId(),
                request.region(),
                items,
                totalAmount,
                Instant.now().toEpochMilli(),
                // Same hardcoded demo value Build Order Step 3 introduced
                // to prove schema evolution live — carried through here
                // unchanged. A real feature would surface this from the
                // request instead.
                "🎁 Thanks for shopping with OrderFlow!"
        );

        // This is now the WHOLE interaction with anything downstream —
        // one call, into Postgres, nothing Kafka-shaped anywhere near
        // this thread. See OutboxWriter's Javadoc for what happens
        // inside that call, and OutboxRelay for what happens after this
        // method has already returned to the client.
        outboxWriter.save(payload);

        // 202 Accepted, not 200/201: we're telling the truth about what
        // just happened. We have NOT confirmed stock is reserved, payment
        // is captured, shipment exists, OR that Kafka has even seen this
        // order yet — only that it's durably recorded and WILL be
        // published. The "/track my order" endpoint that tells the
        // customer what's actually happened comes later (Build Order
        // Step 9, Redis-backed read model).
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of(
                        "orderId", orderId,
                        "status", "ACCEPTED"
                ));
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. HTTP status honesty: 202 Accepted communicates "queued for async
 *    work", distinct from 200/201 which would imply the whole saga
 *    finished. This is a small but important API design signal for any
 *    event-driven system's synchronous entry point.
 * 2. Server-generated IDs at the boundary set up (but don't yet implement)
 *    REST-layer idempotency — a client-visible concern that's independent
 *    of, and complementary to, Kafka's own idempotent-producer feature.
 * 3. Controller stays thin: validation is on the DTO, the durability
 *    guarantee is in OutboxWriter, the actual Kafka publish is in
 *    OutboxRelay — three separate concerns, three separate files, none
 *    of which this class needs to know the internals of.
 * 4. Build Order Step 5's real shift, visible right in this file's
 *    imports: through Step 4, this class imported Avro types and built a
 *    Kafka event directly. Now it imports neither — the request handler
 *    is 100% Postgres, 0% Kafka. That's not a refactor for its own sake;
 *    it's the mechanism the dual-write fix depends on.
 * 5. Build Order Step 9: rate limiting checked FIRST, before validation
 *    even runs — a customer who's over quota shouldn't cost this service
 *    a Bean Validation pass or a Postgres round trip just to be told no.
 *    429 Too Many Requests, not a 4xx that implies something was wrong
 *    with the request body itself.
 *
 * 🔧 TRY IT YOURSELF
 * curl -s -X POST localhost:8080/api/orders \
 *   -H "Content-Type: application/json" \
 *   -d '{"customerId":"cust-1","region":"us-east","items":[{"productId":"sku-42","quantity":2}]}'
 * Then send the exact same curl command again. Notice you get back TWO
 * different orderIds for what a flaky network might consider "the same
 * request, retried." That's the idempotency gap called out above, made
 * concrete — still true after Step 5, since it's an HTTP-layer concern,
 * completely orthogonal to whether the outbox pattern is solving the
 * Kafka side reliably.
 *
 * For the rate limiter specifically: fire the SAME curl command above 10
 * times in a tight loop for the same customerId — see
 * TokenBucketRateLimiter's own TRY IT YOURSELF for exactly what to expect.
 * ════════════════════════════════════════════════════════════════════════
 */
