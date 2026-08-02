package com.orderflow.search.search;

import com.orderflow.search.document.OrderDocument;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;

/**
 * The payoff of denormalizing in the first place: a query like "delayed
 * orders for customer X in region Y" (claude.md's own example) is one
 * filtered Elasticsearch query against ONE document type here — the same
 * question against the write side would mean a Postgres join across
 * order-service's tables PLUS cross-referencing Kafka Streams state
 * stores in fraud-detection-service/analytics-service, none of which
 * were built to answer "search," only to answer their own specific
 * question.
 *
 * Every parameter is optional and AND-ed together when present — a
 * faceted filter, not free-text search (see {@link OrderDocument}'s
 * Javadoc on why every field is `Keyword`, not `Text`).
 */
@RestController
public class SearchController {

    private final ElasticsearchOperations elasticsearchOperations;

    public SearchController(ElasticsearchOperations elasticsearchOperations) {
        this.elasticsearchOperations = elasticsearchOperations;
    }

    @GetMapping("/api/search/orders")
    public List<OrderDocument> search(
            @RequestParam Optional<String> customerId,
            @RequestParam Optional<String> region,
            @RequestParam Optional<String> status
    ) {
        Criteria criteria = new Criteria();
        // An empty Criteria() matches everything by default — each
        // .and(...) below narrows it further, only for the parameters
        // the caller actually supplied. Skipping all three returns every
        // order currently indexed (bounded by Elasticsearch's own
        // default page size), which is a deliberate "no filter = show
        // everything" default, not an oversight.
        if (customerId.isPresent()) {
            criteria = criteria.and(new Criteria("customerId").is(customerId.get()));
        }
        if (region.isPresent()) {
            criteria = criteria.and(new Criteria("region").is(region.get()));
        }
        if (status.isPresent()) {
            criteria = criteria.and(new Criteria("status").is(status.get()));
        }

        CriteriaQuery query = new CriteriaQuery(criteria);
        SearchHits<OrderDocument> hits = elasticsearchOperations.search(query, OrderDocument.class);

        return hits.getSearchHits().stream()
                .map(SearchHit::getContent)
                .toList();
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. CQRS, made literal: this is the READ side, `OrderDocumentIndexer` is
 *    the WRITE side, and they share nothing but the index name — this
 *    controller never touches Kafka or Postgres, `OrderDocumentIndexer`
 *    never handles an HTTP request. Splitting read and write models isn't
 *    an abstraction here, it's two files that could not be more
 *    literally separated.
 * 2. Criteria queries compose: each `.and(...)` narrows the same query
 *    object, so an empty filter set, a single filter, and all three
 *    filters combined are the SAME code path, not three special cases.
 * 3. What this endpoint does NOT tell the caller: how stale the results
 *    are. A production version of this would surface each document's
 *    `updatedAt` prominently in the response (already stored, just not
 *    highlighted) — Elasticsearch views can lag behind Postgres by
 *    however far this consumer group's lag currently is, and a caller
 *    has no way to know that from this response shape alone. Flagged
 *    here, not fixed — see the module README's "what's deliberately not
 *    here yet."
 *
 * 🔧 TRY IT YOURSELF
 * curl "localhost:8090/api/search/orders?region=us-east&status=SHIPPED"
 * — then place a fresh order in us-east and re-run the same query
 * immediately (before the saga finishes) and again a few seconds later,
 * watching the new order first be ABSENT from a status=SHIPPED filter,
 * then appear once shipment-created actually gets consumed. That gap IS
 * the eventual-consistency lag, made observable instead of theoretical.
 * ════════════════════════════════════════════════════════════════════════
 */
