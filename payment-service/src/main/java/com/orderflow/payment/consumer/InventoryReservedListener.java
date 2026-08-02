package com.orderflow.payment.consumer;

import com.orderflow.avro.DeclinedItem;
import com.orderflow.avro.InventoryReserved;
import com.orderflow.avro.PaidItem;
import com.orderflow.avro.PaymentCompleted;
import com.orderflow.avro.PaymentFailed;
import com.orderflow.avro.ReservedItem;
import com.orderflow.payment.service.PaymentProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

import static com.orderflow.payment.config.KafkaTopicConfig.PAYMENT_COMPLETED_TOPIC;
import static com.orderflow.payment.config.KafkaTopicConfig.PAYMENT_FAILED_TOPIC;

/**
 * Choreography's second hop: reacts to inventory-service's
 * inventory-reserved fact, same way inventory-service reacted to
 * order-service's order-created. This class has no idea inventory-
 * service exists as anything other than "whoever produces
 * inventory-reserved" — and no idea shipment-service exists at all. It
 * just tells the truth about what payment-service itself decided.
 */
@Component
public class InventoryReservedListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryReservedListener.class);

    private final PaymentProcessor paymentProcessor;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public InventoryReservedListener(PaymentProcessor paymentProcessor, KafkaTemplate<Object, Object> kafkaTemplate) {
        this.paymentProcessor = paymentProcessor;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "inventory-reserved", groupId = "payment-service-group")
    public void onInventoryReserved(InventoryReserved event) {
        log.info("📥 Charging order {} customerId={} totalAmount={}",
                event.getOrderId(), event.getCustomerId(), event.getTotalAmount());

        PaymentProcessor.ChargeResult result = paymentProcessor.charge(event.getOrderId(), event.getTotalAmount());

        if (result.approved()) {
            log.info("💳 Payment approved for order {}", event.getOrderId());
            publishPaymentCompleted(event);
        } else {
            log.warn("🚫 Payment declined for order {} — {}", event.getOrderId(), result.message());
            publishPaymentFailed(event, result.message());
        }
    }

    private void publishPaymentCompleted(InventoryReserved event) {
        List<PaidItem> items = event.getItems().stream()
                .map(item -> PaidItem.newBuilder()
                        .setProductId(item.getProductId())
                        .setQuantity(item.getQuantity())
                        .build())
                .toList();

        PaymentCompleted completed = PaymentCompleted.newBuilder()
                .setOrderId(event.getOrderId())
                .setCustomerId(event.getCustomerId())
                .setRegion(event.getRegion())
                .setItems(items)
                .setTotalAmount(event.getTotalAmount())
                .setPaidAt(Instant.now().toEpochMilli())
                .build();

        kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, event.getCustomerId(), completed);
    }

    private void publishPaymentFailed(InventoryReserved event, String reason) {
        // The whole point of this event: it carries the SAME items
        // InventoryReserved carried, forward, one more hop — this is
        // literally what makes PaymentFailedCompensationListener (in
        // inventory-service) able to release exactly the right stock
        // without needing to look anything up.
        List<DeclinedItem> items = event.getItems().stream()
                .map(item -> DeclinedItem.newBuilder()
                        .setProductId(item.getProductId())
                        .setQuantity(item.getQuantity())
                        .build())
                .toList();

        PaymentFailed failed = PaymentFailed.newBuilder()
                .setOrderId(event.getOrderId())
                .setCustomerId(event.getCustomerId())
                .setRegion(event.getRegion())
                .setItems(items)
                .setTotalAmount(event.getTotalAmount())
                .setReason(reason)
                .setFailedAt(Instant.now().toEpochMilli())
                .build();

        kafkaTemplate.send(PAYMENT_FAILED_TOPIC, event.getCustomerId(), failed);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Choreography chains by CONVENTION, not configuration: nothing links
 *    inventory-service's publish to this listener except that they both
 *    independently agreed on a topic name and an event shape. There's no
 *    central registry saying "inventory-reserved feeds payment-service."
 *    That's the whole trade-off — zero coupling in the code, but the
 *    overall saga's shape only exists in your head (or in a diagram),
 *    never in one file you could point to. Compare directly to
 *    order-saga-orchestrator, where the ENTIRE sequence lives in one
 *    method.
 * 2. Every event in this chain re-declares the SAME three nested item
 *    record types (ReservedItem, PaidItem, DeclinedItem) with identical
 *    fields — Avro's per-schema-file isolation (see InventoryReserved's
 *    doc comment) means these can't share one type across files without
 *    real fragility, so a small amount of repetition is the honest cost
 *    of that safety.
 * ════════════════════════════════════════════════════════════════════════
 */
