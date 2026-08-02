package com.orderflow.search.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * One document per 1-minute window, mirroring analytics-service's
 * {@code OrdersPerMinute} Avro event field for field. {@code windowStart}
 * is the document ID — analytics-service re-emits an UPDATE to the same
 * window as more orders arrive within it (Build Order Step 7's verified
 * window-rollover behavior), so reusing the window's start time as the ID
 * makes each re-emission a full overwrite of the SAME document instead of
 * a growing pile of near-duplicate records for one window.
 */
@Document(indexName = "orders-per-minute")
public class OrdersPerMinuteDocument {

    @Id
    private String windowStart;

    @Field(type = FieldType.Date, format = DateFormat.epoch_millis)
    private Long windowStartMillis;

    @Field(type = FieldType.Date, format = DateFormat.epoch_millis)
    private Long windowEnd;

    @Field(type = FieldType.Long)
    private Long orderCount;

    public OrdersPerMinuteDocument() {
    }

    public OrdersPerMinuteDocument(long windowStart, long windowEnd, long orderCount) {
        this.windowStart = String.valueOf(windowStart);
        this.windowStartMillis = windowStart;
        this.windowEnd = windowEnd;
        this.orderCount = orderCount;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public Long getWindowStartMillis() {
        return windowStartMillis;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public Long getOrderCount() {
        return orderCount;
    }
}
