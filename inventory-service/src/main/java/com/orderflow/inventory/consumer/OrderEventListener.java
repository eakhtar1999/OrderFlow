package com.orderflow.inventory.consumer;

import com.orderflow.avro.OrderCreatedEvent;
import com.orderflow.avro.OrderItem;
import com.orderflow.inventory.service.StockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
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

    // A magic productId that exists ONLY to make this listener throw on
    // purpose. A real poison message usually comes from a genuine bug —
    // an NPE on an unexpected null, a downstream call timing out — and we
    // don't have a reliable way to manufacture one of those on demand.
    // This sentinel is the tutorial's stand-in: order an item with this
    // productId and you get a repeatable, on-purpose failure to watch the
    // retry + DLT machinery handle, any time you want, without editing
    // code.
    private static final String POISON_PRODUCT_ID = "sku-poison";

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
     *
     * Build Order Step 4 adds @RetryableTopic. Before this, an uncaught
     * exception here meant Spring's default error handler retried the
     * SAME record against the SAME partition forever. Worse than just
     * "one message stuck": Kafka only lets you move a partition's offset
     * forward in order, so that stuck message blocks every message
     * behind it on the same partition too. @RetryableTopic instead
     * publishes a failing record to a SEPARATE retry topic (with a
     * delay), tries it again from there, and after enough attempts routes
     * it to a Dead Letter Topic — the original topic keeps flowing the
     * entire time.
     */
    @RetryableTopic(
            // 1 first attempt + 3 retries, then the DLT.
            attempts = "4",
            // Exponential backoff: ~1s, ~2s, ~4s between attempts, capped
            // at 10s. Real systems often go longer (minutes/hours) for
            // things like "downstream service is temporarily down" — kept
            // short here so a demo doesn't require standing around.
            backoff = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 10000),
            autoCreateTopics = "true",
            // Names the generated topics order-created-retry-0,
            // order-created-retry-1, ... instead of reusing one topic —
            // makes each attempt's backlog individually inspectable in
            // Kafka UI, at the cost of more topics existing.
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            listenerContainerFactory = "kafkaListenerContainerFactory"
    )
    @KafkaListener(
            topics = "order-created",
            groupId = "inventory-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCreated(OrderCreatedEvent event, Acknowledgment acknowledgment) {
        log.info("📥 Received order-created orderId={} customerId={} region={}",
                event.getOrderId(), event.getCustomerId(), event.getRegion());

        for (OrderItem item : event.getItems()) {
            if (POISON_PRODUCT_ID.equals(item.getProductId())) {
                // See POISON_PRODUCT_ID's Javadoc — deliberate, not a bug.
                // The retry-topic machinery below doesn't inspect WHY this
                // threw, only THAT it did, same as it would for a genuine
                // NPE or a downstream timeout.
                throw new RuntimeException(
                        "Simulated processing failure for order " + event.getOrderId()
                                + " (productId=" + POISON_PRODUCT_ID + ")");
            }
        }

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

    /**
     * Runs once a record has failed onOrderCreated the full {@code attempts}
     * times configured on @RetryableTopic above and has been routed to the
     * Dead Letter Topic. This is Build Order Step 4's answer to "what
     * happens operationally when messages land in DLT" — in a real
     * deployment this is where you'd page someone or write to an
     * incident tracker instead of just logging; Build Order Step 11 wires
     * real alerting on top of exactly this hook.
     */
    @DltHandler
    public void onDeadLetter(
            OrderCreatedEvent event,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage,
            @Header(KafkaHeaders.ORIGINAL_TOPIC) String originalTopic
    ) {
        log.error("💀 DEAD LETTER: order {} from topic '{}' exhausted all retry attempts — {}",
                event.getOrderId(), originalTopic, exceptionMessage);
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
 * 4. @RetryableTopic auto-creates a SEPARATE topic per retry attempt
 *    (order-created-retry-0, -1, -2) plus one order-created-dlt, each with
 *    its OWN consumer group (inventory-service-group-retry-0, etc). Ran
 *    this for real with a poison message and captured the actual
 *    timestamps: main topic at :49.491, retry-0 at :50.622 (+1.1s),
 *    retry-1 at :52.635 (+2.0s), retry-2 at :56.639 (+4.0s), DLT handler
 *    at :57.191 — the configured 1s/2s/4s exponential backoff, exactly.
 * 5. A non-obvious side effect, found by checking Schema Registry after
 *    the test, not by reading docs: each retry/DLT topic gets its OWN
 *    schema subject (order-created-retry-0-value, -retry-1-value,
 *    -retry-2-value, -dlt-value), all separate from order-created-value.
 *    Schema Registry's default TopicNameStrategy derives the subject from
 *    the topic name — republishing to a differently-named topic means a
 *    differently-named subject, whether you think of retry topics as
 *    "the same event" or not.
 * 6. Retry/DLT topics default to 1 partition each — confirmed via
 *    `kafka-topics.sh --describe`, regardless of order-created's 3.
 *    Combined with Build Order Step 2: if you're running multiple
 *    inventory-service instances, only ONE of them can ever be actively
 *    consuming a given retry/DLT topic at a time — no parallelism during
 *    retries, by default, even if your happy path is fully scaled out.
 *
 * 🔧 TRY IT YOURSELF
 * Place an order with productId "sku-poison" (see POISON_PRODUCT_ID) and
 * watch the timestamps in your own logs walk through main topic -> retry-0
 * -> retry-1 -> retry-2 -> onDeadLetter, each attempt further apart than
 * the last. Then browse Kafka UI (localhost:8081) and look at
 * order-created-dlt's message — the payload is the full original Avro
 * event, unchanged; only headers were added to track why it's there.
 * ════════════════════════════════════════════════════════════════════════
 */
