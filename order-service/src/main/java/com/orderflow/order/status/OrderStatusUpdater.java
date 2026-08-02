package com.orderflow.order.status;

import com.orderflow.avro.InventoryFailed;
import com.orderflow.avro.InventoryReserved;
import com.orderflow.avro.OrderStatus;
import com.orderflow.avro.PaymentCompleted;
import com.orderflow.avro.PaymentFailed;
import com.orderflow.avro.ShipmentCreated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

import static com.orderflow.order.config.KafkaTopicConfig.ORDER_STATUS_TOPIC;

/**
 * order-service's first-ever consumer. Everything through Build Order
 * Step 7 flowed OUT of this service (order-created, order-status=CREATED)
 * and nothing ever flowed back in. Step 8's choreography saga changes
 * that: this class listens to every event the saga produces about an
 * order it originally created, and republishes each one as a status
 * transition on the (compacted — see KafkaTopicConfig.java) order-status
 * topic.
 *
 * order-service itself takes NO action here beyond recording what
 * happened — it doesn't decide anything, doesn't trigger anything else.
 * That's deliberate: this class is a spectator on the saga, not a
 * participant in it, which is exactly the role the "current status"
 * read model should play.
 */
@Component
public class OrderStatusUpdater {

    private static final Logger log = LoggerFactory.getLogger(OrderStatusUpdater.class);

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public OrderStatusUpdater(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "inventory-reserved", groupId = "order-service-status-updater")
    public void onInventoryReserved(InventoryReserved event) {
        publishStatus(event.getOrderId(), "RESERVED");
    }

    @KafkaListener(topics = "inventory-failed", groupId = "order-service-status-updater")
    public void onInventoryFailed(InventoryFailed event) {
        publishStatus(event.getOrderId(), "INVENTORY_FAILED");
    }

    @KafkaListener(topics = "payment-completed", groupId = "order-service-status-updater")
    public void onPaymentCompleted(PaymentCompleted event) {
        publishStatus(event.getOrderId(), "PAID");
    }

    @KafkaListener(topics = "payment-failed", groupId = "order-service-status-updater")
    public void onPaymentFailed(PaymentFailed event) {
        publishStatus(event.getOrderId(), "PAYMENT_FAILED");
    }

    @KafkaListener(topics = "shipment-created", groupId = "order-service-status-updater")
    public void onShipmentCreated(ShipmentCreated event) {
        publishStatus(event.getOrderId(), "SHIPPED");
    }

    private void publishStatus(String orderId, String status) {
        OrderStatus update = OrderStatus.newBuilder()
                .setOrderId(orderId)
                .setStatus(status)
                .setUpdatedAt(Instant.now().toEpochMilli())
                .build();

        // Same transactional producer OutboxRelay uses (see
        // application.yml's transaction-id-prefix) — a bare
        // kafkaTemplate.send() here would throw, because a producer
        // built from a transactional ProducerFactory refuses to send
        // ANYTHING outside an explicit transaction boundary. That's not
        // extra ceremony for its own sake — see OutboxRelay's Javadoc for
        // why this producer is transactional in the first place.
        kafkaTemplate.executeInTransaction(kt -> {
            kt.send(ORDER_STATUS_TOPIC, orderId, update);
            return true;
        });

        log.info("📝 order-status[{}] -> {}", orderId, status);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. order-status finally does REAL work: through Step 7 it only ever
 *    got written once per order ("CREATED"), so compaction (Step 5's
 *    whole justification for this topic) had nothing to discard. Now
 *    every order writes multiple records to the same key over its
 *    lifetime, and only the latest survives compaction — the concept
 *    that was previously just correctly-configured infrastructure
 *    becomes something you can actually observe doing its job.
 * 2. A "read model updater" pattern: this class makes no decisions and
 *    triggers nothing — it only TRANSLATES saga events into a simpler,
 *    queryable current-state view. That's a materialized view, built
 *    from an event log, which is the same idea Build Order Step 9's
 *    Redis-backed "track my order" endpoint will read FROM once it
 *    exists.
 * 3. Five separate @KafkaListener methods, five different Avro types,
 *    one shared group-id — Kafka tracks committed offsets per
 *    (group, topic, partition), so these don't compete with each other
 *    despite sharing a group-id string; they're just five independent
 *    subscriptions bundled under one logical consumer identity.
 *
 * 🔧 TRY IT YOURSELF
 * Place a normal order, then watch order-service's own logs show
 * 📝 order-status[...] lines arriving asynchronously, well after the
 * original 202 response — proof that "order accepted" and "here's what
 * eventually happened to it" are genuinely two different moments in
 * time, not two views of the same synchronous call.
 * ════════════════════════════════════════════════════════════════════════
 */
