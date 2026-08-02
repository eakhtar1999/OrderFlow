package com.orderflow.shipment.consumer;

import com.orderflow.avro.PaymentCompleted;
import com.orderflow.avro.ShipmentCreated;
import com.orderflow.shipment.service.ShipmentCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

import static com.orderflow.shipment.config.KafkaTopicConfig.SHIPMENT_CREATED_TOPIC;

/**
 * Choreography's third and final hop. Same shape as every listener
 * before it in this saga — react to a fact, publish a fact — and, like
 * all of them, with zero code-level awareness of who's downstream (in
 * this case: order-service's OrderStatusUpdater, the only consumer of
 * shipment-created).
 */
@Component
public class PaymentCompletedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentCompletedListener.class);

    private final ShipmentCreator shipmentCreator;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public PaymentCompletedListener(ShipmentCreator shipmentCreator, KafkaTemplate<Object, Object> kafkaTemplate) {
        this.shipmentCreator = shipmentCreator;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "payment-completed", groupId = "shipment-service-group")
    public void onPaymentCompleted(PaymentCompleted event) {
        log.info("📥 Creating shipment for order {} customerId={}", event.getOrderId(), event.getCustomerId());

        String shipmentId = shipmentCreator.create(event.getOrderId());
        log.info("🚚 Shipment {} created for order {}", shipmentId, event.getOrderId());

        ShipmentCreated created = ShipmentCreated.newBuilder()
                .setOrderId(event.getOrderId())
                .setCustomerId(event.getCustomerId())
                .setShipmentId(shipmentId)
                .setCreatedAt(Instant.now().toEpochMilli())
                .build();

        kafkaTemplate.send(SHIPMENT_CREATED_TOPIC, event.getCustomerId(), created);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. The choreography saga, traced end to end across four files now
 *    (OrderEventListener in inventory-service, InventoryReservedListener
 *    in payment-service, and this class): the SAME shape repeats at every
 *    hop — consume a fact, do local work, publish a new fact — with no
 *    file anywhere containing the word "saga" or describing the full
 *    sequence. The saga is an EMERGENT property of four independently
 *    deployed services agreeing on topic names, not a thing that exists
 *    in code.
 * ════════════════════════════════════════════════════════════════════════
 */
