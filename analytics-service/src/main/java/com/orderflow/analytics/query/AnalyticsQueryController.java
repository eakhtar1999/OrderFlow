package com.orderflow.analytics.query;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
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
import java.util.LinkedHashMap;
import java.util.Map;

import static com.orderflow.analytics.topology.AnalyticsTopology.GLOBAL_KEY;
import static com.orderflow.analytics.topology.AnalyticsTopology.ORDERS_PER_MINUTE_STORE;
import static com.orderflow.analytics.topology.AnalyticsTopology.REVENUE_BY_REGION_STORE;

/**
 * Same interactive-queries shape as fraud-detection-service's
 * {@code FraudQueryController} — reads local topology state directly, no
 * Kafka round trip — with one addition:
 * {@link #allRegionsRevenue()} demonstrates {@code fetchAll}, which reads
 * EVERY key a windowed store currently holds within a time range, not
 * just one you already know. That's the only way to answer "give me all
 * regions" from a windowed store — there's no "list distinct keys"
 * operation otherwise.
 *
 * <p>Same known simplification as {@code FraudQueryController}: correct
 * only for a single instance of this service. See that class's Javadoc
 * for the full explanation — it applies here unchanged.
 */
@RestController
public class AnalyticsQueryController {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Value("${analytics.window-size-seconds}")
    private long windowSizeSeconds;

    public AnalyticsQueryController(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    @GetMapping("/api/analytics/orders-per-minute")
    public Map<String, Object> currentOrdersPerMinute() {
        KafkaStreams streams = requireRunningStreams();
        if (streams == null) {
            return Map.of("status", "streams not running yet");
        }

        ReadOnlyWindowStore<String, Long> store = streams.store(
                StoreQueryParameters.fromNameAndType(ORDERS_PER_MINUTE_STORE, QueryableStoreTypes.windowStore()));

        long count = latestValueInWindow(store, GLOBAL_KEY, 0L);

        return Map.of("windowSizeSeconds", windowSizeSeconds, "currentWindowOrderCount", count);
    }

    @GetMapping("/api/analytics/revenue-by-region/{region}")
    public Map<String, Object> revenueForRegion(@PathVariable String region) {
        KafkaStreams streams = requireRunningStreams();
        if (streams == null) {
            return Map.of("status", "streams not running yet");
        }

        ReadOnlyWindowStore<String, Double> store = streams.store(
                StoreQueryParameters.fromNameAndType(REVENUE_BY_REGION_STORE, QueryableStoreTypes.windowStore()));

        double total = latestValueInWindow(store, region, 0.0);

        return Map.of("region", region, "windowSizeSeconds", windowSizeSeconds, "currentWindowRevenue", total);
    }

    @GetMapping("/api/analytics/revenue-by-region")
    public Map<String, Double> allRegionsRevenue() {
        KafkaStreams streams = requireRunningStreams();
        if (streams == null) {
            return Map.of();
        }

        ReadOnlyWindowStore<String, Double> store = streams.store(
                StoreQueryParameters.fromNameAndType(REVENUE_BY_REGION_STORE, QueryableStoreTypes.windowStore()));

        Instant now = Instant.now();
        Instant windowStart = now.minus(Duration.ofSeconds(windowSizeSeconds));

        // fetchAll(from, to) — unlike fetch(key, from, to), this doesn't
        // take a key at all. It walks every key the store currently
        // holds whose window overlaps the given range. This is the ONLY
        // way to answer "what regions exist and what's each one's
        // current total" from a windowed store — there's no separate
        // "list distinct keys" API.
        Map<String, Double> latestPerRegion = new LinkedHashMap<>();
        try (KeyValueIterator<org.apache.kafka.streams.kstream.Windowed<String>, Double> iterator =
                     store.fetchAll(windowStart, now)) {
            while (iterator.hasNext()) {
                KeyValue<org.apache.kafka.streams.kstream.Windowed<String>, Double> entry = iterator.next();
                // Later entries (from more recent windows) overwrite
                // earlier ones for the same region — we want each
                // region's MOST RECENT window, same "take the last one
                // seen" logic FraudQueryController uses for a single key.
                latestPerRegion.put(entry.key.key(), entry.value);
            }
        }
        return latestPerRegion;
    }

    private KafkaStreams requireRunningStreams() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        return (streams != null && streams.state() == KafkaStreams.State.RUNNING) ? streams : null;
    }

    private <V> V latestValueInWindow(ReadOnlyWindowStore<String, V> store, String key, V defaultValue) {
        Instant now = Instant.now();
        Instant windowStart = now.minus(Duration.ofSeconds(windowSizeSeconds));
        V latest = defaultValue;
        try (WindowStoreIterator<V> iterator = store.fetch(key, windowStart, now)) {
            while (iterator.hasNext()) {
                latest = iterator.next().value;
            }
        }
        return latest;
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. fetch(key, from, to) answers "what's the value for a key I already
 *    know"; fetchAll(from, to) answers "what keys even exist right now" —
 *    genuinely different questions, and windowed stores only support
 *    discovering the second one by scanning a time range, not by asking
 *    for a key list directly.
 * 2. Global aggregates need a query-side counterpart to the topology's
 *    fake constant key: this controller has to know GLOBAL_KEY ("ALL")
 *    to look up the orders-per-minute count, the same way the topology
 *    had to invent that key to produce it in the first place.
 *
 * 🔧 TRY IT YOURSELF
 * curl localhost:8084/api/analytics/orders-per-minute
 * curl localhost:8084/api/analytics/revenue-by-region
 * Place a handful of orders across 2-3 different regions, then query
 * both again — watch the global count and the per-region breakdown both
 * update in real time, no delay beyond the topology actually processing
 * each record.
 * ════════════════════════════════════════════════════════════════════════
 */
