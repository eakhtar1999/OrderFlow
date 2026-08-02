package com.orderflow.order.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * The entire fix for the dual-write problem lives in this one class.
 * {@code orders} and {@code outbox} are two INSERTs into the SAME
 * Postgres database, wrapped in ONE JDBC transaction — either both rows
 * exist, or neither does, guaranteed by ordinary ACID semantics. No
 * Kafka is involved anywhere in this class; that's the point. Compare to
 * Steps 1-4, where a crash between "we decided to publish" and "Kafka
 * actually got it" was a real, undefended gap (see the old
 * OrderEventProducer's TRY IT YOURSELF, since deleted along with the
 * class itself — this file is what replaced it).
 */
@Component
public class OutboxWriter {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public OutboxWriter(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * "transactionManager" is named explicitly here, not left to Spring's
     * default resolution — and that's not boilerplate, it's load-bearing.
     * Build Order Step 5 also configures a KafkaTransactionManager (see
     * application.yml's {@code transaction-id-prefix}), so this
     * application context now has TWO {@code PlatformTransactionManager}
     * beans. Leave {@code @Transactional} unqualified and Spring has no
     * way to know which one you meant — found by reading Spring's own
     * disambiguation rules before running this, not by hitting the
     * exception first, but it's exactly the kind of ambiguity this
     * project's earlier steps only ever found BY running things. Naming
     * this one explicitly is what keeps this method's transaction
     * scoped to Postgres, not accidentally bound to the Kafka one.
     */
    @Transactional("transactionManager")
    public void save(OutboxOrderPayload payload) {
        jdbcTemplate.update(
                "INSERT INTO orders (order_id, customer_id, region, total_amount, created_at) VALUES (?, ?, ?, ?, ?)",
                payload.orderId(),
                payload.customerId(),
                payload.region(),
                payload.totalAmount(),
                Timestamp.from(Instant.ofEpochMilli(payload.createdAt()))
        );

        jdbcTemplate.update(
                "INSERT INTO outbox (order_id, payload) VALUES (?, ?::jsonb)",
                payload.orderId(),
                writeAsJson(payload)
        );
        // If EITHER statement above throws, @Transactional rolls BOTH
        // back — there is no possible state where "orders" has a row
        // that "outbox" doesn't know about, or vice versa.
    }

    private String writeAsJson(OutboxOrderPayload payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            // A serialization failure here means OUR OWN record can't
            // become JSON — a bug in this class, not a downstream
            // problem. Fail loudly and let the transaction roll back
            // entirely (including the orders insert above) rather than
            // silently drop the outbox half.
            throw new IllegalStateException(
                    "Failed to serialize outbox payload for order " + payload.orderId(), e);
        }
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. The dual-write problem, solved the way it's actually solvable: you
 *    can't get atomicity across two DIFFERENT systems (Postgres and
 *    Kafka) directly — nothing spans both. What you CAN do is make the
 *    "write to Kafka" step provably happen LATER, unconditionally, by
 *    durably recording the intent to do so inside the SAME transaction as
 *    the real work. The atomicity boundary moves to where it's actually
 *    achievable (Postgres-to-Postgres), and the Kafka half becomes "will
 *    happen eventually, at-least-once" instead of "might silently never
 *    happen."
 * 2. Multiple PlatformTransactionManager beans in one Spring context is a
 *    real, easy-to-hit situation the moment you mix a JDBC datasource
 *    with a Kafka transactional producer — @Transactional's default bean
 *    resolution isn't magic; naming the manager explicitly is often
 *    necessary, not decorative.
 * 3. JdbcTemplate over JPA/Hibernate here isn't a style preference for
 *    this project specifically — it's because the outbox pattern's whole
 *    value proposition is "look, it's obviously just two INSERTs in one
 *    transaction." An ORM's entity/session lifecycle would make that
 *    fact harder to see, not easier, for something this simple.
 *
 * 🔧 TRY IT YOURSELF
 * Temporarily make writeAsJson() always throw (e.g. add
 * `if (true) throw new RuntimeException("boom");` at its top). Place an
 * order, then query `SELECT * FROM orders;` in Postgres — the orders row
 * is GONE too, even though its own INSERT never failed. That's
 * @Transactional's rollback covering the whole method, not just the
 * statement that actually threw.
 * ════════════════════════════════════════════════════════════════════════
 */
