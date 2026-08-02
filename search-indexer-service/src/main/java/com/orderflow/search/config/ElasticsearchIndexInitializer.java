package com.orderflow.search.config;

import com.orderflow.search.document.OrderDocument;
import com.orderflow.search.document.OrdersPerMinuteDocument;
import com.orderflow.search.document.RevenueByRegionDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.stereotype.Component;

/**
 * Found live, the hard way: without this class, NONE of the {@code @Field}
 * type annotations on {@link OrderDocument} etc. ever took effect.
 * Elasticsearch auto-creates an index with DYNAMIC mapping the first time
 * anything is written to a name that doesn't exist yet — and this
 * service's first-ever write was a partial {@code UpdateQuery} with
 * {@code docAsUpsert(true)} (see {@code OrderDocumentIndexer}), not a
 * call that goes through Spring Data Elasticsearch's own mapping-creation
 * path. The result, confirmed via {@code GET orders/_mapping}:
 * `customerId`/`region`/`status`/etc. all came back as {@code text} with
 * a bolted-on {@code .keyword} sub-field — Elasticsearch's own generic
 * default for an unmapped string — instead of the plain {@code keyword}
 * type the annotations asked for.
 *
 * The difference is real, not cosmetic: a {@code text} field is
 * TOKENIZED by the standard analyzer before being indexed, meaning exact
 * "does this field equal X" queries stop being reliably exact — hyphens
 * and other punctuation get split into separate tokens. This class runs
 * ONCE at startup, before any Kafka listener has a chance to write
 * anything, and creates each index with its mapping taken directly from
 * the `@Document`/`@Field` annotations — the mapping Spring Data
 * Elasticsearch would have created automatically had this been declared
 * as an {@code ElasticsearchRepository} instead of accessed through the
 * lower-level {@code ElasticsearchOperations} API this service uses for
 * its partial-update logic.
 */
@Component
public class ElasticsearchIndexInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchIndexInitializer.class);

    private final ElasticsearchOperations elasticsearchOperations;

    public ElasticsearchIndexInitializer(ElasticsearchOperations elasticsearchOperations) {
        this.elasticsearchOperations = elasticsearchOperations;
    }

    @Override
    public void run(ApplicationArguments args) {
        ensureIndexExists(OrderDocument.class);
        ensureIndexExists(OrdersPerMinuteDocument.class);
        ensureIndexExists(RevenueByRegionDocument.class);
    }

    private void ensureIndexExists(Class<?> documentClass) {
        IndexOperations indexOps = elasticsearchOperations.indexOps(documentClass);
        if (indexOps.exists()) {
            log.info("🗂️  Index for {} already exists, leaving its mapping alone", documentClass.getSimpleName());
            return;
        }
        indexOps.create();
        Document mapping = indexOps.createMapping(documentClass);
        indexOps.putMapping(mapping);
        log.info("🗂️  Created index for {} with mapping from @Field annotations", documentClass.getSimpleName());
    }
}
