package com.orderflow.shipment.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String SHIPMENT_CREATED_TOPIC = "shipment-created";

    @Bean
    public NewTopic shipmentCreatedTopic() {
        return TopicBuilder.name(SHIPMENT_CREATED_TOPIC).partitions(3).replicas(1).build();
    }
}
