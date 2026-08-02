package com.orderflow.search.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.List;

/**
 * The denormalized order document — the whole point of Step 10. Postgres
 * (order-service) and the individual saga events (Build Order Step 8)
 * each know only their own slice of an order's story; this is the ONE
 * place all of it lands together, built up incrementally by
 * {@code OrderDocumentIndexer} as the saga's events arrive, not written
 * all at once by any single service.
 *
 * Every field except {@code orderId} is nullable at some point in an
 * order's life — a brand-new document (from order-created alone) has no
 * {@code status} transition history yet, and {@code reason}/
 * {@code shipmentId} only ever get set for orders that actually failed or
 * shipped. That's a direct, honest reflection of eventual consistency:
 * this document is never "complete" in the way a single Postgres row
 * is — it's a snapshot of whatever's been consumed so far.
 */
@Document(indexName = "orders")
public class OrderDocument {

    @Id
    private String orderId;

    @Field(type = FieldType.Keyword)
    private String customerId;

    @Field(type = FieldType.Keyword)
    private String region;

    @Field(type = FieldType.Nested)
    private List<Item> items;

    @Field(type = FieldType.Double)
    private Double totalAmount;

    // CREATED, RESERVED, INVENTORY_FAILED, PAID, PAYMENT_FAILED, SHIPPED —
    // same open string vocabulary order-service's compacted order-status
    // topic uses (Build Order Step 5's OrderStatus.avsc), for the same
    // reason: new statuses can appear without an Elasticsearch mapping
    // change, at the cost of no server-side typo protection.
    @Field(type = FieldType.Keyword)
    private String status;

    @Field(type = FieldType.Keyword)
    private String reason;

    @Field(type = FieldType.Keyword)
    private String shipmentId;

    @Field(type = FieldType.Date, format = org.springframework.data.elasticsearch.annotations.DateFormat.epoch_millis)
    private Long createdAt;

    @Field(type = FieldType.Date, format = org.springframework.data.elasticsearch.annotations.DateFormat.epoch_millis)
    private Long updatedAt;

    public OrderDocument() {
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public List<Item> getItems() {
        return items;
    }

    public void setItems(List<Item> items) {
        this.items = items;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getShipmentId() {
        return shipmentId;
    }

    public void setShipmentId(String shipmentId) {
        this.shipmentId = shipmentId;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public Long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public record Item(String productId, int quantity) {
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. A denormalized read model, made concrete: this ONE document answers
 *    "everything about order X" in a single GET, where the write side
 *    (Postgres + five different saga events) needed five separate
 *    services and two different databases to represent the same story.
 *    Denormalization trades write-side simplicity (each service owns one
 *    small fact) for read-side simplicity (one document, one query).
 * 2. Keyword vs. Text field types: every field here is `Keyword` (exact
 *    match, sortable, aggregatable, NOT tokenized) rather than `Text`
 *    (analyzed/tokenized for relevance-ranked full-text search) — because
 *    every field IS an exact-match facet (a customerId, a region, a
 *    status), not free-form prose. A product-description field, if this
 *    document ever grew one, would want `Text` instead.
 * 3. Nullable-by-design, not nullable-by-bug: `reason` and `shipmentId`
 *    are absent on most documents, always — Elasticsearch has no schema
 *    concept of "required," so a sparse document is completely normal,
 *    unlike a Postgres row where every column exists for every row
 *    whether populated or not.
 * ════════════════════════════════════════════════════════════════════════
 */
