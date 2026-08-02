package com.orderflow.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point of order-service.
 *
 * order-service is the "front door" of OrderFlow: it's the only service in
 * the whole platform that talks HTTP to the outside world. Every other
 * service (inventory, payment, shipment, ...) only ever talks Kafka. That
 * split matters: HTTP is synchronous and the caller waits; Kafka is
 * asynchronous and the caller doesn't. Putting the boundary here means a
 * customer's browser gets a fast "order accepted" response, while the slow,
 * multi-step fulfillment saga happens in the background across services
 * that don't even need to know about each other.
 *
 * Build Order Step 5 sharpens that split further: even Kafka itself moved
 * behind an extra layer. The request thread now only ever touches
 * Postgres (via OutboxWriter) — Kafka publishing happens entirely inside
 * OutboxRelay's {@code @Scheduled} method, which is why this class needs
 * {@code @EnableScheduling}: without it, that method exists in the
 * codebase but Spring never actually calls it.
 */
@SpringBootApplication
@EnableScheduling
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Synchronous edge vs asynchronous core: REST at the boundary, Kafka
 *    everywhere behind it. This is what lets you scale/replace/upgrade
 *    inventory-service, payment-service, etc. independently of the API.
 * 2. @SpringBootApplication = @Configuration + @EnableAutoConfiguration +
 *    @ComponentScan. Auto-configuration is what wires up Kafka AND
 *    datasource/transaction beans just from application.yml properties —
 *    no manual bean definitions needed for the basic case, even once two
 *    entirely different transaction managers are in play (see
 *    OutboxWriter's Javadoc).
 * 3. @EnableScheduling is opt-in, not automatic — Spring Boot won't
 *    invoke ANY @Scheduled method anywhere in the app unless this
 *    annotation is present somewhere in the context. A very easy thing to
 *    forget and get a silently-never-running relay instead of an error.
 *
 * 🔧 TRY IT YOURSELF
 * Remove @EnableScheduling, restart, and place an order. The HTTP
 * response still comes back 202 immediately (OutboxWriter only needs
 * Postgres) and the order row appears in Postgres fine — but
 * `SELECT * FROM outbox;` shows the row sitting there forever, and
 * nothing ever shows up on the order-created topic. No error, no log
 * line, nothing — OutboxRelay's method is just never invoked. A quiet
 * failure mode worth seeing once on purpose.
 * ════════════════════════════════════════════════════════════════════════
 */
