package com.orderflow.search.consumer;

import com.orderflow.avro.OrdersPerMinute;
import com.orderflow.avro.RevenueByRegion;
import com.orderflow.search.document.OrdersPerMinuteDocument;
import com.orderflow.search.document.RevenueByRegionDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * claude.md section 6's second Elasticsearch use case: feed
 * analytics-service's windowed aggregates in for Kibana to chart, kept
 * completely separate from {@link OrderDocumentIndexer}'s per-order
 * documents — different index, different shape, different question
 * ("how is the whole PLATFORM doing over time" vs. "what happened to
 * THIS order").
 *
 * Unlike {@code OrderDocumentIndexer}'s partial-update-and-merge
 * approach, these are full document SAVES — each window's metric is one
 * complete, self-contained record with no other writer ever touching the
 * same document, so there's nothing to merge.
 */
@Component
public class AnalyticsMetricsIndexer {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsMetricsIndexer.class);

    private final ElasticsearchOperations elasticsearchOperations;

    public AnalyticsMetricsIndexer(ElasticsearchOperations elasticsearchOperations) {
        this.elasticsearchOperations = elasticsearchOperations;
    }

    @KafkaListener(topics = "orders-per-minute", groupId = "search-indexer-service-group")
    public void onOrdersPerMinute(OrdersPerMinute event) {
        OrdersPerMinuteDocument document = new OrdersPerMinuteDocument(
                event.getWindowStart(), event.getWindowEnd(), event.getOrderCount());
        elasticsearchOperations.save(document);
        log.info("📊 Indexed orders-per-minute window={} count={}", event.getWindowStart(), event.getOrderCount());
    }

    @KafkaListener(topics = "revenue-by-region", groupId = "search-indexer-service-group")
    public void onRevenueByRegion(RevenueByRegion event) {
        RevenueByRegionDocument document = new RevenueByRegionDocument(
                event.getRegion(), event.getWindowStart(), event.getWindowEnd(), event.getTotalRevenue());
        elasticsearchOperations.save(document);
        log.info("📊 Indexed revenue-by-region region={} window={} revenue={}",
                event.getRegion(), event.getWindowStart(), event.getTotalRevenue());
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Not every Elasticsearch write is a partial update — when a document
 *    has exactly ONE writer and represents one complete, self-contained
 *    fact (a closed or still-filling window's metric), a plain save() IS
 *    the correct operation. Reaching for docAsUpsert/partial-merge
 *    everywhere "because that's what the other indexer does" would be
 *    solving a problem (concurrent writers to the same document) that
 *    doesn't exist here.
 * 2. Two Kafka Streams apps (fraud-detection-service, analytics-service)
 *    and one plain Kafka consumer (this class) all read from the SAME
 *    underlying `order-created` lineage, completely independently, each
 *    building its own view for its own purpose — consumer groups made
 *    this possible from Build Order Step 1 onward, and Step 10 is simply
 *    one more independent reader added to that same pattern.
 * ════════════════════════════════════════════════════════════════════════
 */
