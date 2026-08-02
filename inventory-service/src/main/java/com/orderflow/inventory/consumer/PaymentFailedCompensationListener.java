package com.orderflow.inventory.consumer;

import com.orderflow.avro.DeclinedItem;
import com.orderflow.avro.PaymentFailed;
import com.orderflow.inventory.service.StockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * The compensating half of the choreography saga, from inventory-service's
 * side. {@link OrderEventListener} only ever moves FORWARD — order-created
 * in, inventory-reserved/-failed out, done. This class is what makes the
 * saga able to move BACKWARD: payment-service declining a charge (see
 * payment-service's own listener) means the stock this service reserved
 * earlier needs to become available again, and nothing tells inventory-
 * service that directly — it finds out by listening to payment-failed,
 * the same way any other consumer of that topic would.
 *
 * This is choreography's defining shape: there's no coordinator calling
 * "please release the stock for order X." inventory-service reacts to a
 * fact (a payment was declined) that happens to imply an action it should
 * take, because THIS class was written to interpret it that way — the
 * coordination logic lives distributed across every service's own
 * listeners, not in one place.
 */
@Component
public class PaymentFailedCompensationListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentFailedCompensationListener.class);

    private final StockService stockService;

    public PaymentFailedCompensationListener(StockService stockService) {
        this.stockService = stockService;
    }

    @KafkaListener(
            topics = "payment-failed",
            groupId = "inventory-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentFailed(PaymentFailed event, Acknowledgment acknowledgment) {
        log.warn("↩️  Compensating: payment declined for order {} ({}) — releasing {} item(s) back to stock",
                event.getOrderId(), event.getReason(), event.getItems().size());

        for (DeclinedItem item : event.getItems()) {
            stockService.release(item.getProductId(), item.getQuantity());
            log.info("↩️  Released {} x {} for order {}", item.getQuantity(), item.getProductId(), event.getOrderId());
        }

        // Same manual-ack discipline as OrderEventListener — commit only
        // after every item has actually been released, so a crash mid-
        // loop means this event redelivers and finishes the job, rather
        // than silently leaving some stock un-released.
        acknowledgment.acknowledge();
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Choreography's rollback path is just... another consumer. There's no
 *    special "compensation" mechanism in Kafka — this class is a
 *    completely ordinary @KafkaListener, structurally identical to
 *    OrderEventListener, that happens to subtract instead of reserve.
 * 2. This listener depends entirely on PaymentFailed carrying the exact
 *    items that were reserved (see that schema's doc comment) — if
 *    payment-service's event only carried orderId, this class would have
 *    no way to know WHAT to release without querying inventory-service's
 *    own past decisions somewhere, which doesn't exist as a queryable
 *    thing yet. Event-carried-state-transfer isn't just convenient here,
 *    it's the only thing that makes this compensation possible at all.
 * 3. group-id reused from OrderEventListener ("inventory-service-group")
 *    — but for a DIFFERENT topic (payment-failed, not order-created).
 *    Kafka consumer groups track offsets per (group, topic, partition),
 *    so this is safe: it doesn't compete with OrderEventListener for
 *    order-created's partitions, it just happens to share the same
 *    logical "this is inventory-service's consumption" identity.
 *
 * 🔧 TRY IT YOURSELF
 * Place an order large enough to exceed payment-service's decline
 * threshold (see that service's README). Watch, in order: inventory-
 * service reserves and logs 📦, payment-service declines, THIS listener
 * fires and logs ↩️, and a final stock check shows the reservation is
 * gone — the order failed, but nothing about it lingers as phantom
 * reserved stock.
 * ════════════════════════════════════════════════════════════════════════
 */
