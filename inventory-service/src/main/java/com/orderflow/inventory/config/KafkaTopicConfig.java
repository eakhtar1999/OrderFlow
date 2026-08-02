package com.orderflow.inventory.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Build Order Step 8: inventory-service's first-ever declared topics.
 * Through Step 7 this service only ever consumed — order-created
 * directly, plus the retry/DLT topics @RetryableTopic auto-creates for
 * it (Build Order Step 4). Now it's a real domain-event PRODUCER too,
 * publishing the outcome of every reservation attempt, which is exactly
 * why it owns these topic declarations — same "the producer declares
 * it" convention order-service, fraud-detection-service, and
 * analytics-service all follow.
 */
@Configuration
public class KafkaTopicConfig {

    public static final String INVENTORY_RESERVED_TOPIC = "inventory-reserved";
    public static final String INVENTORY_FAILED_TOPIC = "inventory-failed";

    @Bean
    public NewTopic inventoryReservedTopic() {
        return TopicBuilder.name(INVENTORY_RESERVED_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryFailedTopic() {
        return TopicBuilder.name(INVENTORY_FAILED_TOPIC).partitions(3).replicas(1).build();
    }
}
