package com.orderflow.fraud.query;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static com.orderflow.fraud.topology.FraudDetectionTopology.ORDER_VELOCITY_STORE;

/**
 * "Interactive queries" — Kafka Streams' term for reading a topology's
 * OWN local state directly, over a plain method call, instead of
 * consuming a topic to find out what it currently thinks. Nothing here
 * touches Kafka at request time; {@code order-velocity-store} already
 * has the answer sitting in local RocksDB (or in-memory, depending on
 * store type), kept up to date continuously by
 * {@code FraudDetectionTopology}'s Branch B as records flow through it.
 *
 * <p><b>Known simplification, not yet tested:</b> this only works
 * correctly with ONE instance of this service running. In a real
 * deployment, Kafka Streams partitions state ACROSS instances the same
 * way consumer groups partition topics (Build Order Step 2) — a given
 * customerId's velocity count lives on whichever instance owns that
 * partition, and querying the WRONG instance returns nothing, not an
 * error. Kafka Streams provides {@code KafkaStreams.metadataForKey(...)}
 * specifically to let an instance discover which OTHER instance actually
 * owns a key and forward the request there — not implemented here, and
 * flagged rather than silently working only by accident of running a
 * single instance.
 */
@RestController
public class FraudQueryController {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Value("${fraud.velocity.window-minutes}")
    private long windowMinutes;

    public FraudQueryController(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    @GetMapping("/api/fraud/velocity/{customerId}")
    public Map<String, Object> currentVelocity(@PathVariable String customerId) {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
            // Real state, not a made-up placeholder: a Kafka Streams app
            // goes through REBALANCING before it's able to serve
            // interactive queries at all (same rebalancing mechanics as
            // Build Order Step 2's consumer groups) — querying too early
            // after startup genuinely has no answer yet.
            return Map.of("customerId", customerId, "status", "streams not running yet", "state",
                    streams == null ? "unknown" : streams.state().toString());
        }

        ReadOnlyWindowStore<String, Long> store = streams.store(
                StoreQueryParameters.fromNameAndType(ORDER_VELOCITY_STORE, QueryableStoreTypes.windowStore()));

        Instant now = Instant.now();
        Instant windowStart = now.minus(Duration.ofMinutes(windowMinutes));

        // fetch(key, from, to) returns every window this key had ANY
        // count in, across the requested range — for a single
        // fixed-size window whose duration matches windowMinutes, this
        // is usually one entry, but "usually one" isn't "always
        // exactly one" (window boundaries don't align with "now minus N
        // minutes" queried at an arbitrary instant), so we take the
        // MOST RECENT one rather than assume there's only ever one.
        long currentCount = 0;
        try (WindowStoreIterator<Long> iterator = store.fetch(customerId, windowStart, now)) {
            while (iterator.hasNext()) {
                currentCount = iterator.next().value;
            }
        }

        return Map.of(
                "customerId", customerId,
                "windowMinutes", windowMinutes,
                "currentWindowOrderCount", currentCount
        );
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Interactive queries read a topology's LOCAL state directly — no
 *    Kafka round trip at request time, which is why this can answer in
 *    microseconds instead of "consume a topic and hope you catch the
 *    latest record."
 * 2. That local-state property has a real cost: it only tells the truth
 *    for keys THIS instance's partitions actually own. Scaling this
 *    service (Build Order Step 2's lesson, applied to a Streams app
 *    instead of a plain consumer) would require query-forwarding via
 *    KafkaStreams.metadataForKey(...) to stay correct — not built here,
 *    explicitly flagged rather than pretended away.
 * 3. A Kafka Streams app's state (RUNNING, REBALANCING, etc.) is a real,
 *    queryable thing your own code should check before trusting a
 *    result — not just an internal implementation detail.
 *
 * 🔧 TRY IT YOURSELF
 * curl localhost:8082/api/fraud/velocity/cust-1
 * Place 4 orders for the same customerId in quick succession, then query
 * again — watch currentWindowOrderCount climb in real time, no restart,
 * no delay beyond the topology actually processing each record.
 * ════════════════════════════════════════════════════════════════════════
 */
