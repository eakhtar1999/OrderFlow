-- order-saga-orchestrator's saga state table. Shares order-service's
-- Postgres DATABASE (see application.yml) but owns this table
-- completely — order-service never reads or writes it.
--
-- Unlike order-service's outbox pattern, this table is NOT written
-- inside one all-or-nothing transaction spanning the whole saga — each
-- status update below is its own independent statement, committed the
-- instant it runs, deliberately. Wrapping the ENTIRE saga (including the
-- synchronous HTTP calls to three other services) in one open database
-- transaction would hold a connection for the full duration of the saga
-- and would misrepresent what a saga actually IS: a sequence of
-- independent local transactions coordinated by application logic, not
-- one distributed ACID transaction. Postgres itself has no idea this
-- table's rows relate to a multi-step business process — that
-- coordination lives entirely in SagaOrchestrator.java.
CREATE TABLE IF NOT EXISTS saga (
    order_id     VARCHAR(36)   PRIMARY KEY,
    customer_id  VARCHAR(255)  NOT NULL,
    region       VARCHAR(255)  NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    status       VARCHAR(50)   NOT NULL,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);
