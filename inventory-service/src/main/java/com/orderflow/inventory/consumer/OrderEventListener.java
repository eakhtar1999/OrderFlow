package com.orderflow.inventory.consumer;

import com.orderflow.avro.OrderCreatedEvent;
import com.orderflow.avro.OrderItem;
import com.orderflow.inventory.service.StockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Reacts to every order-created event. This is the "single consumer" half
 * of Build Order Step 1's "single producer/consumer, plain JSON, basic
 * flow end-to-end" — order-service publishes, this class is the first
 * thing in the platform to receive and act on it.
 *
 * For now, a "real" downstream reaction (publishing inventory-reserved,
 * letting a saga compensate on failure) doesn't exist — this listener
 * only decides and logs. That's Build Order Step 8's job.
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final StockService stockService;

    public OrderEventListener(StockService stockService) {
        this.stockService = stockService;
    }

    /**
     * groupId is also set in application.yml — repeating it here (Spring
     * Kafka lets either win, the listener's own attribute takes
     * precedence) makes it undeniable, right at the call site, which
     * consumer group this listener belongs to. That matters because
     * PARTITIONS ARE ASSIGNED PER GROUP: two listeners with the same
     * group-id split the topic's partitions between them; two listeners
     * with DIFFERENT group-ids each get an independent full copy of every
     * message. That single string is the whole "fan-out vs. scale-out"
     * decision in Kafka.
     */
    @KafkaListener(
            topics = "order-created",
            groupId = "inventory-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCreated(OrderCreatedEvent event, Acknowledgment acknowledgment) {
        log.info("📥 Received order-created orderId={} customerId={} region={}",
                event.getOrderId(), event.getCustomerId(), event.getRegion());

        boolean allReserved = true;
        for (OrderItem item : event.getItems()) {
            boolean reserved = stockService.tryReserve(item.getProductId(), item.getQuantity());
            if (reserved) {
                log.info("📦 Reserved {} x {} for order {}", item.getQuantity(), item.getProductId(), event.getOrderId());
            } else {
                log.warn("❌ Insufficient stock for {} (wanted {}) on order {}",
                        item.getProductId(), item.getQuantity(), event.getOrderId());
                allReserved = false;
            }
        }

        if (allReserved) {
            log.info("✅ Order {} fully reserved.", event.getOrderId());
        } else {
            // A real system would now publish an inventory-failed event
            // and let the saga compensate anything already reserved. For
            // Step 1 we just stop here — that's Build Order Step 8's job.
            log.warn("⚠️  Order {} could not be fully reserved.", event.getOrderId());
        }

        // We only acknowledge (commit the offset) AFTER the reservation
        // decision above has fully run. If this process crashed midway
        // through the loop, the offset would NOT be committed, and on
        // restart this exact message would be redelivered — we'd retry
        // the stock check from scratch. That's at-least-once delivery: a
        // message may be processed more than once, but never silently
        // dropped. (Notice tryReserve isn't itself idempotent — reprocessing
        // a message whose reservation partly succeeded before the crash
        // could double-decrement stock. Build Order Step 9's Redis-backed
        // idempotent-consumer pattern, deduping by orderId, is what closes
        // that gap.)
        acknowledgment.acknowledge();
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Consumer groups control fan-out vs. scale-out. Same group-id across
 *    instances = share the work (scale out). Different group-ids = each
 *    gets every message independently (fan-out) — this is exactly how
 *    payment-service, fraud-detection-service, and analytics-service will
 *    ALL read order-created without stepping on each other later.
 * 2. At-least-once delivery, made concrete: ack after work, not before.
 *    The trade-off it accepts (possible reprocessing) versus what it
 *    prevents (silent message loss on crash) is the central reliability
 *    decision of this whole file.
 * 3. Idempotent consumers are a DIFFERENT, complementary concept to
 *    at-least-once delivery: delivery guarantees are Kafka's job, but
 *    making REPROCESSING safe is your application's job. This listener
 *    doesn't do that yet (see the acknowledge() comment) — that arrives
 *    with the Redis dedupe store in Build Order Step 9.
 *
 * 🔧 TRY IT YOURSELF
 * Kill this listener's error handling entirely (throw a RuntimeException
 * partway through) and watch Spring Kafka's default error handler retry
 * the SAME record against the SAME partition forever, blocking every
 * message behind it — the exact problem Build Order Step 4's retry
 * topics + Dead Letter Topic exist to fix.
 * ════════════════════════════════════════════════════════════════════════
 */
