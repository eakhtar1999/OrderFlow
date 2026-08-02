-- inventory-service's stock table. Shares order-service's Postgres
-- DATABASE (see application.yml — same trade-off order-saga-orchestrator
-- already made in Build Order Step 8: one Postgres container, no
-- per-service database, safe only because these tables have no foreign
-- keys pointing at each other) but owns this table completely.
--
-- Through Build Order Step 8, stock lived ONLY in an in-memory
-- ConcurrentHashMap (see the old StockService) — per-instance, resets on
-- restart, and genuinely a different value on every running instance.
-- Postgres is now the real source of truth; Redis (see StockService.java)
-- is a cache IN FRONT of this table, not a replacement for it.
CREATE TABLE IF NOT EXISTS stock (
    product_id VARCHAR(255) PRIMARY KEY,
    quantity   INT          NOT NULL
);

-- Same three fake products StockService seeded in memory since Step 1,
-- now seeded into Postgres instead. ON CONFLICT DO NOTHING makes this
-- safe to re-run on every startup (schema.sql always runs, see
-- application.yml's sql.init.mode) without resetting stock back to these
-- values every single restart — a real migration tool (Flyway/Liquibase,
-- deliberately not used in this tutorial, see order-service's README)
-- would express this as a one-time seed migration instead.
INSERT INTO stock (product_id, quantity) VALUES
    ('sku-42', 50),
    ('sku-7', 5),
    ('sku-99', 0)
ON CONFLICT (product_id) DO NOTHING;
