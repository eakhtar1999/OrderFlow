package com.orderflow.search.consumer;

import com.orderflow.avro.InventoryFailed;
import com.orderflow.avro.InventoryReserved;
import com.orderflow.avro.OrderCreatedEvent;
import com.orderflow.avro.OrderItem;
import com.orderflow.avro.PaymentCompleted;
import com.orderflow.avro.PaymentFailed;
import com.orderflow.avro.ShipmentCreated;
import com.orderflow.search.document.OrderDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Six {@code @KafkaListener} methods, one Elasticsearch document per
 * orderId, built up incrementally — the direct search-side counterpart of
 * order-service's {@code OrderStatusUpdater} (Build Order Step 8), which
 * does the same "translate every saga event into one materialized field"
 * job for Postgres/Kafka's compacted `order-status` topic instead.
 *
 * Every listener does a PARTIAL UPDATE (upsert-merge), never a full
 * document overwrite — see {@link #upsert} — specifically because these
 * six listeners consume six INDEPENDENT topics/partitions with no
 * ordering guarantee relative to each other. `order-created` usually
 * arrives first in practice, but nothing enforces that at the Kafka
 * level, and this service's own consumer group re-reading from
 * `earliest` on first startup could plausibly deliver them in any order.
 * `docAsUpsert` means whichever event happens to arrive FIRST for a given
 * orderId creates the document with just its own fields; every
 * subsequent event merges its fields in, regardless of arrival order.
 */
@Component
public class OrderDocumentIndexer {

    private static final Logger log = LoggerFactory.getLogger(OrderDocumentIndexer.class);
    private final ElasticsearchOperations elasticsearchOperations;

    public OrderDocumentIndexer(ElasticsearchOperations elasticsearchOperations) {
        this.elasticsearchOperations = elasticsearchOperations;
    }

    @KafkaListener(topics = "order-created", groupId = "search-indexer-service-group")
    public void onOrderCreated(OrderCreatedEvent event) {
        List<Map<String, Object>> items = event.getItems().stream()
                .map(this::toItemMap)
                .toList();

        upsert(event.getOrderId(), Map.of(
                "orderId", event.getOrderId(),
                "customerId", event.getCustomerId(),
                "region", event.getRegion(),
                "items", items,
                "totalAmount", event.getTotalAmount(),
                "status", "CREATED",
                "createdAt", event.getCreatedAt(),
                "updatedAt", event.getCreatedAt()
        ));
        log.info("🔎 Indexed order-created for {}", event.getOrderId());
    }

    @KafkaListener(topics = "inventory-reserved", groupId = "search-indexer-service-group")
    public void onInventoryReserved(InventoryReserved event) {
        upsert(event.getOrderId(), Map.of(
                "status", "RESERVED",
                "updatedAt", now()
        ));
        log.info("🔎 Indexed inventory-reserved for {}", event.getOrderId());
    }

    @KafkaListener(topics = "inventory-failed", groupId = "search-indexer-service-group")
    public void onInventoryFailed(InventoryFailed event) {
        upsert(event.getOrderId(), Map.of(
                "status", "INVENTORY_FAILED",
                "reason", event.getReason(),
                "updatedAt", now()
        ));
        log.info("🔎 Indexed inventory-failed for {}", event.getOrderId());
    }

    @KafkaListener(topics = "payment-completed", groupId = "search-indexer-service-group")
    public void onPaymentCompleted(PaymentCompleted event) {
        upsert(event.getOrderId(), Map.of(
                "status", "PAID",
                "updatedAt", now()
        ));
        log.info("🔎 Indexed payment-completed for {}", event.getOrderId());
    }

    @KafkaListener(topics = "payment-failed", groupId = "search-indexer-service-group")
    public void onPaymentFailed(PaymentFailed event) {
        upsert(event.getOrderId(), Map.of(
                "status", "PAYMENT_FAILED",
                "reason", event.getReason(),
                "updatedAt", now()
        ));
        log.info("🔎 Indexed payment-failed for {}", event.getOrderId());
    }

    @KafkaListener(topics = "shipment-created", groupId = "search-indexer-service-group")
    public void onShipmentCreated(ShipmentCreated event) {
        upsert(event.getOrderId(), Map.of(
                "status", "SHIPPED",
                "shipmentId", event.getShipmentId(),
                "updatedAt", now()
        ));
        log.info("🔎 Indexed shipment-created for {}", event.getOrderId());
    }

    /**
     * A partial-document merge with {@code docAsUpsert(true)}: if
     * `orderId` doesn't exist yet in the index, THIS partial document
     * becomes the whole document; if it already exists, only the fields
     * present here are merged in, leaving every other field (written by
     * a DIFFERENT listener, possibly minutes earlier) untouched. This is
     * what makes six independent writers safely building ONE document
     * possible without any of them needing to read the current state
     * first — Elasticsearch does the merge server-side, atomically, per
     * document.
     */
    private void upsert(String orderId, Map<String, Object> fields) {
        Document document = Document.from(fields);
        UpdateQuery updateQuery = UpdateQuery.builder(orderId)
                .withDocument(document)
                .withDocAsUpsert(true)
                .build();
        elasticsearchOperations.update(updateQuery,
                elasticsearchOperations.getIndexCoordinatesFor(OrderDocument.class));
    }

    private Map<String, Object> toItemMap(OrderItem item) {
        return Map.of("productId", item.getProductId(), "quantity", item.getQuantity());
    }

    private long now() {
        return Instant.now().toEpochMilli();
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Dual-write problem, avoided the SAME way order-service's outbox
 *    pattern (Build Order Step 5) avoids it: this service writes to
 *    Elasticsearch from a KAFKA CONSUMER, never directly from whichever
 *    service produced the original event. order-service never talks to
 *    Elasticsearch at all — it only ever writes to Postgres and Kafka,
 *    and this consumer is what turns "Kafka has the fact" into
 *    "Elasticsearch has the fact," on its own schedule, asynchronously.
 * 2. Eventual consistency, made visible: this index can genuinely lag
 *    behind Postgres by however far this consumer group's lag currently
 *    is. A search result here is a SNAPSHOT as of `updatedAt`, not a
 *    live view — the same honesty `order-status`'s compacted topic
 *    already required accepting in Build Order Step 8.
 * 3. Partial updates with `docAsUpsert`, not read-modify-write: six
 *    listeners writing to the same document with NO coordination between
 *    them works safely because Elasticsearch merges each partial
 *    document server-side. A naive "GET the document, mutate a field in
 *    Java, PUT it back" approach would race two listeners for the same
 *    orderId arriving close together — one write would silently
 *    overwrite the other's fields.
 *
 * 🔧 TRY IT YOURSELF
 * Place an order, then immediately (before the saga finishes)
 * `curl localhost:9200/orders/_doc/<orderId>` — watch the document exist
 * with `status: CREATED` and no `shipmentId` yet. Poll the same URL every
 * second and watch `status` progress to RESERVED, then PAID, then
 * SHIPPED (with `shipmentId` finally appearing), each write a real
 * partial merge, not a full re-index.
 * ════════════════════════════════════════════════════════════════════════
 */
