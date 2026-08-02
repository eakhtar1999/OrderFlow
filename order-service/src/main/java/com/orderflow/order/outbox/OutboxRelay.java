package com.orderflow.order.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.avro.OrderCreatedEvent;
import com.orderflow.avro.OrderItem;
import com.orderflow.avro.OrderStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import static com.orderflow.order.config.KafkaTopicConfig.ORDER_CREATED_TOPIC;
import static com.orderflow.order.config.KafkaTopicConfig.ORDER_STATUS_TOPIC;

/**
 * The other half of the outbox pattern: a background poller that reads
 * rows {@link OutboxWriter} committed and republishes them to Kafka.
 *
 * This is the ONLY Kafka producer left in order-service — see
 * application.yml's {@code transaction-id-prefix} comment.
 * {@code OrderController} no longer talks to Kafka at all, directly or
 * indirectly, on the request thread. Everything here runs completely
 * decoupled from any HTTP request, on its own schedule
 * ({@code outbox.relay.poll-interval-ms}).
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH_SIZE = 20;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public OutboxRelay(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper,
                        KafkaTemplate<Object, Object> kafkaTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms}")
    public void publishPendingOutboxRows() {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                "SELECT id, order_id, payload FROM outbox ORDER BY id ASC LIMIT ?", BATCH_SIZE);

        for (Map<String, Object> row : rows) {
            long id = ((Number) row.get("id")).longValue();
            String orderId = (String) row.get("order_id");
            try {
                publishOneRow(id, orderId, row.get("payload").toString());
            } catch (Exception e) {
                // Deliberately NOT deleting the row on failure — it stays
                // in the outbox and gets retried next poll. A genuinely
                // malformed row would retry forever, every poll cycle:
                // the exact same "no limit on retries" gap Build Order
                // Step 1 called out on the CONSUMER side
                // (OrderEventListener's original TRY IT YOURSELF), now
                // showing up on the PRODUCER side instead, and just as
                // deliberately left open here. Step 4's retry topics +
                // DLT don't have an equivalent on this side of the
                // pipeline yet.
                log.error("Failed to publish outbox row id={} orderId={}: {}",
                        id, orderId, e.getMessage(), e);
            }
        }
    }

    private void publishOneRow(long id, String orderId, String payloadJson) throws Exception {
        OutboxOrderPayload payload = objectMapper.readValue(payloadJson, OutboxOrderPayload.class);

        List<OrderItem> avroItems = payload.items().stream()
                .map(item -> OrderItem.newBuilder()
                        .setProductId(item.productId())
                        .setQuantity(item.quantity())
                        .build())
                .toList();

        OrderCreatedEvent event = OrderCreatedEvent.newBuilder()
                .setOrderId(payload.orderId())
                .setCustomerId(payload.customerId())
                .setRegion(payload.region())
                .setItems(avroItems)
                .setTotalAmount(payload.totalAmount())
                .setCreatedAt(payload.createdAt())
                .setGiftMessage(payload.giftMessage())
                .build();

        OrderStatus status = OrderStatus.newBuilder()
                .setOrderId(payload.orderId())
                .setStatus("CREATED")
                .setUpdatedAt(payload.createdAt())
                .build();

        // Both sends succeed together, or neither is visible to any
        // consumer — a real Kafka transaction, spanning two DIFFERENT
        // topics. This is what Build Order Step 5's "Kafka transactions"
        // half actually demonstrates: NOT Postgres-to-Kafka atomicity
        // (nothing can span two different systems like that — that's
        // the whole reason the outbox table exists one layer up, in
        // OutboxWriter), but genuine Kafka-to-Kafka atomicity, once
        // you're inside the broker's own domain.
        //
        // TWO DIFFERENT KEYS ON PURPOSE — a real bug this class shipped
        // with, caught two Build Order steps later (Step 6) when
        // fraud-detection-service's per-customer velocity count came back
        // wrong for every order. The original version of this method
        // keyed BOTH sends by `orderId`, because that's what was sitting
        // right there in this method's parameter list. That silently
        // broke order-created's Step 1 partition-key promise (see
        // order-service's original OrderEventProducer, since deleted —
        // "same customerId -> same partition -> ordering guaranteed"),
        // since every orderId is unique: groupByKey() on a topic keyed by
        // orderId can never see more than one record per key, ever,
        // making "how many orders has this customer placed" structurally
        // unanswerable downstream. order-status genuinely DOES want
        // orderId as its key (compaction should keep the latest status
        // PER ORDER, not merge every order from one customer together)
        // — the bug was applying that same key to order-created too,
        // where it doesn't belong.
        kafkaTemplate.executeInTransaction(kt -> {
            kt.send(ORDER_CREATED_TOPIC, payload.customerId(), event);
            kt.send(ORDER_STATUS_TOPIC, orderId, status);
            return true;
        });

        // Only delete AFTER the Kafka transaction above committed. If
        // this process dies in the gap between that commit and this
        // DELETE, the next poll (on restart) re-reads this same row and
        // republishes it — a duplicate order-created AND order-status
        // pair, together (never one without the other, thanks to the
        // transaction above). That's the same at-least-once/duplicate-
        // redelivery story already documented on the consumer side in
        // OrderEventListener — just sourced from the relay instead of a
        // crashed consumer. Not a new class of bug, the same one, seen
        // from a different angle. See the root README's crash-resilience
        // walkthrough, where we triggered exactly this window on
        // purpose.
        jdbcTemplate.update("DELETE FROM outbox WHERE id = ?", id);

        log.info("📤 Relayed outbox row id={} -> order-created + order-status for orderId={}", id, orderId);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Kafka transactions solve Kafka-to-Kafka atomicity — multiple sends
 *    (here, to two different topics) becoming visible together or not at
 *    all. They do NOT, and structurally cannot, make a Postgres write and
 *    a Kafka write atomic with each other; that boundary is fixed by
 *    physics (two different systems, two different commit protocols),
 *    which is exactly why the outbox table in OutboxWriter exists one
 *    layer up — it's solving a DIFFERENT problem than this transaction
 *    is.
 * 2. This relay is a polling implementation of the outbox pattern — it
 *    checks Postgres every `poll-interval-ms`, meaning a message can sit
 *    published-but-not-yet-relayed for up to that long. Debezium CDC
 *    (see docker-compose.yml's commented-out block) is the alternative:
 *    tail Postgres's write-ahead log directly instead of polling,
 *    trading "one more Java class you own" for "one more piece of
 *    infrastructure you depend on." See the root README for the full
 *    comparison.
 * 3. A failure surfaces in a completely different place now than in
 *    Steps 1-4: an incompatible Avro schema change used to throw
 *    synchronously out of the HTTP request (a 500 to the client — see
 *    the old OrderEventProducer's Javadoc, since deleted). Now it fails
 *    silently in THIS scheduled method's log, on a background thread,
 *    with the client having already gotten a 202 minutes/hours earlier.
 *    Decoupling the write path from Kafka fixed the dual-write problem
 *    and simultaneously moved where you'd notice a totally different
 *    class of failure — worth sitting with, not just accepting as pure
 *    upside.
 * 4. A silent regression can live for multiple Build Order steps before
 *    anything notices — this method keyed order-created by the wrong
 *    field from the moment Step 5 was written, and NOTHING in Steps 5's
 *    own testing caught it, because nothing in Step 5 (or inventory-
 *    service) actually depends on which key order-created uses. It took
 *    Step 6 building something that genuinely NEEDS the documented
 *    partition-key contract (per-customer aggregation) to expose that
 *    the contract had quietly stopped holding. Tests and manual
 *    verification only catch what they specifically check — an
 *    unenforced design invariant (like "this topic is keyed by
 *    customerId") is exactly the kind of thing that erodes silently
 *    unless something downstream actually relies on it being true.
 *
 * 🔧 TRY IT YOURSELF
 * Set outbox.relay.poll-interval-ms to something huge (like 3600000 — an
 * hour) via an environment variable override, place an order, and query
 * `SELECT * FROM outbox;` — the row just sits there, visibly waiting,
 * for as long as you configured. Kafka UI's order-created topic won't
 * show the message until the relay actually runs. That visible gap IS
 * the durability guarantee working as designed, not a bug: the order was
 * never at risk of being lost, it was just waiting its turn.
 * ════════════════════════════════════════════════════════════════════════
 */
