package com.orderflow.search.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * One document per (region, window) pair, mirroring analytics-service's
 * {@code RevenueByRegion} Avro event. The ID combines region + windowStart
 * — same "reuse the natural key so re-emitted updates overwrite instead
 * of duplicate" reasoning as {@link OrdersPerMinuteDocument}, just with
 * an extra dimension since revenue is tracked PER REGION, not globally.
 */
@Document(indexName = "revenue-by-region")
public class RevenueByRegionDocument {

    @Id
    private String id;

    @Field(type = FieldType.Keyword)
    private String region;

    @Field(type = FieldType.Date, format = DateFormat.epoch_millis)
    private Long windowStart;

    @Field(type = FieldType.Date, format = DateFormat.epoch_millis)
    private Long windowEnd;

    @Field(type = FieldType.Double)
    private Double totalRevenue;

    public RevenueByRegionDocument() {
    }

    public RevenueByRegionDocument(String region, long windowStart, long windowEnd, double totalRevenue) {
        this.id = region + ":" + windowStart;
        this.region = region;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.totalRevenue = totalRevenue;
    }

    public String getId() {
        return id;
    }

    public String getRegion() {
        return region;
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public Double getTotalRevenue() {
        return totalRevenue;
    }
}
