-- order-service's database schema. Spring Boot runs this automatically
-- on every startup (spring.sql.init.mode=always, see application.yml) —
-- the IF NOT EXISTS guards make that safe to repeat, no migration tool
-- needed for a two-table schema this simple.

-- The actual business record — source of truth for "does this order
-- exist and what does it say." Nothing outside this database reads it
-- yet; it's here because a real order-tracking read model (Build Order
-- Step 9) needs somewhere durable to eventually read FROM, and because
-- "the outbox pattern" only makes sense once there's a real row being
-- written in the same transaction as the outbox entry.
CREATE TABLE IF NOT EXISTS orders (
    order_id     VARCHAR(36)   PRIMARY KEY,
    customer_id  VARCHAR(255)  NOT NULL,
    region       VARCHAR(255)  NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    created_at   TIMESTAMPTZ   NOT NULL
);

-- The outbox: a durable staging area for "things that still need to be
-- published to Kafka." Deliberately dumb — it doesn't know or care what
-- an OrderCreatedEvent is, it just holds a JSON blob and an id to poll
-- by. OutboxRelay.java is the only thing that ever reads this table, and
-- it DELETES rows once they're successfully published — this table is a
-- queue, not a permanent log (Kafka is the permanent log).
CREATE TABLE IF NOT EXISTS outbox (
    id         BIGSERIAL    PRIMARY KEY,
    order_id   VARCHAR(36)  NOT NULL,
    payload    JSONB        NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
