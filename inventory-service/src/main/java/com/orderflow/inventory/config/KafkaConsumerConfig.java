package com.orderflow.inventory.config;

import com.orderflow.avro.OrderCreatedEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Hand-builds the consumer factory instead of leaning entirely on
 * application.yml auto-configuration, specifically so we can set the
 * AckMode explicitly and put a comment right next to the decision that
 * matters most: how/when do we tell Kafka "I'm done with this record."
 *
 * Build Order Step 2 adds two things on top of Step 1's manual-ack
 * consumer: a per-instance client.id (so you can tell instances apart in
 * logs/Kafka UI once you run more than one) and a rebalance listener
 * (so partition hand-off is something you SEE happen, not just something
 * you're told happens).
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    // Generated exactly ONCE, here, in code — when this bug bit us the
    // first time we actually ran two instances, it wasn't this line, it
    // was `@Value("${spring.kafka.consumer.client-id}")` pointing at
    // application.yml's `inventory-instance-${random.uuid}`. That looked
    // fine, but ${random.uuid} is a placeholder re-evaluated independently
    // EVERY time something resolves that property key — so the rebalance
    // listener and the startup banner (a different class, its own
    // separate @Value lookup) each got their OWN random UUID, silently
    // defeating the "match the id between the two logs" idea this whole
    // feature exists for. See application.yml for the full story. The fix
    // is to compute the id once, in Java, and share this single field via
    // the bean below — there's only one place left that can generate a
    // second value.
    private final String clientId = "inventory-instance-" + java.util.UUID.randomUUID();

    /**
     * Exposes this instance's id as a bean so other classes (see
     * InventoryServiceApplication's startup banner) can inject the SAME
     * value instead of independently resolving their own.
     */
    @Bean
    public String inventoryInstanceClientId() {
        return clientId;
    }

    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        // NOT setting ConsumerConfig.GROUP_INSTANCE_ID_CONFIG here, on
        // purpose. Setting it would enable STATIC group membership: this
        // instance keeps its partition assignment across a restart (within
        // session.timeout.ms) instead of triggering a rebalance every time.
        // That's genuinely useful in production — a rolling restart
        // shouldn't cause a rebalance storm — but Step 2's whole point is
        // to WATCH rebalancing happen, so turning on the feature that
        // suppresses it would defeat the lesson. Try it yourself at the
        // bottom of this file once you've seen a dynamic rebalance at
        // least once.

        // Where to fetch writer schemas from, keyed by the schema ID
        // embedded in each message's first few bytes (see OrderEventProducer
        // for what the producer side of this same handshake looks like).
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // Without this, KafkaAvroDeserializer hands the listener a generic
        // GenericRecord — a schema-shaped bag of Object fields you'd
        // access by string key (record.get("orderId")), with no compiler
        // check that "orderId" is even a real field. Setting this true
        // gets us back our generated, typed OrderCreatedEvent class instead
        // — the direct replacement for Step 1-2's
        // JsonDeserializer.VALUE_DEFAULT_TYPE, which told Jackson the same
        // kind of thing for JSON.
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        // earliest: a brand-new consumer group (or one whose committed
        // offset has expired/vanished) starts from the OLDEST retained
        // message instead of only seeing NEW ones from "now". For a
        // tutorial where you'll restart this service constantly while
        // experimenting, "latest" would mean silently missing every order
        // placed while the app was down.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // We commit offsets OURSELVES (see the AckMode below), so auto-
        // commit — which fires on a timer regardless of whether YOUR
        // business logic actually finished — must be off. Otherwise you
        // could crash mid-processing, restart, and Kafka would believe
        // (incorrectly) that the record was already handled.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // MANUAL_IMMEDIATE: the listener method must call
        // acknowledgment.acknowledge() itself, and doing so commits that
        // offset right away (not batched up for later). This lets us
        // commit the offset ONLY after the stock-reservation decision has
        // actually been made — see OrderEventListener. Compare to
        // auto-commit, which would move the offset forward on a timer
        // even if reserveStock(...) hadn't run yet, or had thrown.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // This is the "watch it happen" hook for Step 2: every time the
        // group coordinator reshuffles which partitions this JVM owns
        // (because an instance joined, left, or crashed), one of the
        // callbacks below fires. Without this listener, rebalancing still
        // happens exactly the same — it's just invisible to you.
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener());

        return factory;
    }

    @Bean
    public ConsumerAwareRebalanceListener rebalanceListener() {
        return new ConsumerAwareRebalanceListener() {

            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                // Fires after a rebalance completes and this instance has
                // been handed a (possibly new) set of partitions. Compare
                // this set across two terminals running two instances —
                // together they'll always add up to exactly the topic's
                // partition count, never overlapping.
                log.info("🔀 [{}] partitions ASSIGNED -> {}", clientId, partitions);
            }

            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                // Fires just before this instance gives up partitions
                // (graceful case: e.g. a second instance joined the group).
                // "BeforeCommit" is Spring Kafka's hook to let any
                // in-flight offset get committed before the partition is
                // handed to someone else — cooperating with a rebalance
                // instead of racing it.
                log.info("🔀 [{}] partitions REVOKED (graceful) -> {}", clientId, partitions);
            }

            @Override
            public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                // A common assumption — ours, until we actually tested
                // this — is that a SURVIVING instance sees this callback
                // when a PEER crashes. It doesn't. We hard-killed (kill
                // -9) one of two running instances and the survivor logged
                // a perfectly ordinary REVOKED-then-ASSIGNED cycle, same
                // as when a new instance joins. onPartitionsLost instead
                // fires on a consumer's OWN client when THAT client
                // discovers it may have already been fenced out of the
                // group before it got a chance to revoke cleanly — e.g.
                // its own processing paused past max.poll.interval.ms (a
                // slow listener method, a debugger breakpoint, a long GC
                // pause) and the coordinator gave its partitions to
                // someone else in the meantime. It's a "you, personally,
                // were the slow one" signal, not a "your neighbor died"
                // notification.
                log.warn("🔀 [{}] partitions LOST (this instance was fenced out) -> {}", clientId, partitions);
            }
        };
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Manual offset commit vs auto-commit: manual means YOU decide the
 *    exact moment "processed" is recorded, tying it to your business logic
 *    finishing rather than a fixed timer. This is what makes at-least-once
 *    semantics deliberate instead of accidental.
 * 2. auto.offset.reset only matters for a consumer group with no
 *    committed offset yet (brand new group, or an expired one) — it does
 *    NOT mean "always start from the earliest message on every restart."
 *    Restarting with the SAME group-id resumes from the last committed
 *    offset regardless of this setting.
 * 3. Build Order Step 3 update: TRUSTED_PACKAGES and VALUE_DEFAULT_TYPE
 *    (Jackson-specific security/typing controls from Steps 1-2) are gone
 *    from this file — Avro deserialization has no equivalent attack
 *    surface to close, because messages carry a schema ID, never a class
 *    name, and SPECIFIC_AVRO_READER_CONFIG (below) is what gets us a
 *    typed class back instead of a generic bag of fields.
 * 4. Rebalancing callbacks describe what happened to YOUR OWN client, not
 *    your peers': ASSIGNED (you got partitions), REVOKED (you gave some
 *    up, gracefully, because group membership changed), and LOST (YOU,
 *    specifically, were fenced out before you could revoke cleanly —
 *    almost always because your own listener took too long between
 *    polls). We assumed, before testing it, that killing a PEER would
 *    make the survivor log LOST for the dead peer's old partitions.
 *    Verified by hand: it doesn't — the survivor just sees a normal
 *    REVOKED + ASSIGNED, identical to a peer joining. LOST is purely
 *    self-referential.
 * 5. Static group membership (group.instance.id) trades "watchable
 *    rebalancing" for "no rebalancing on a routine restart" — a real
 *    production concern (rolling deploys) that Step 2 deliberately leaves
 *    off so the concept it's teaching stays visible. See TRY IT YOURSELF.
 *
 * 🔧 TRY IT YOURSELF
 * 1. Change AckMode to AckMode.RECORD (auto-commit-like, offset advances
 *    right after the listener method returns, no manual call needed) and
 *    remove the Acknowledgment parameter from OrderEventListener. Throw a
 *    RuntimeException partway through the listener on purpose. Notice the
 *    message is now considered "processed" by Kafka even though your
 *    business logic never completed for it — that's the silent data-loss
 *    risk manual ack is designed to prevent.
 * 2. Follow the root README's Step 2 walkthrough: run two instances, place
 *    orders, watch ASSIGNED partitions split between them. Then `kill -9`
 *    one instance's process (not a graceful Ctrl-C). Watch the survivor
 *    log REVOKED (graceful) then ASSIGNED with all partitions back — NOT
 *    LOST. That surprised us too; see concept #4 above for why.
 * 2b. To see a REAL onPartitionsLost, temporarily set
 *    `max.poll.interval.ms` to something small (e.g. 10000) in
 *    consumerFactory()'s props, then put a `Thread.sleep(15000)` at the
 *    top of OrderEventListener.onOrderCreated. Place an order and wait —
 *    THIS instance will log LOST for its own partitions once the
 *    coordinator decides it's unresponsive and kicks it out mid-sleep.
 * 3. Once you've seen a rebalance happen, add
 *    `props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, clientId);` to
 *    consumerFactory() and restart ONE instance with a plain Ctrl-C/
 *    restart (not a crash). With static membership, that restart — as
 *    long as it's back within session.timeout.ms — causes NO rebalance at
 *    all: the group coordinator holds its partitions open, waiting for
 *    the same group.instance.id to reappear.
 * ════════════════════════════════════════════════════════════════════════
 */
