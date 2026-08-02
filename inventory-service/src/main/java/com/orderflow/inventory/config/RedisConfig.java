package com.orderflow.inventory.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * A single {@link StringRedisTemplate} bean, shared by all three unrelated
 * Redis use cases in this service (cache-aside, distributed lock,
 * idempotency dedupe store) — every value this service stores in Redis
 * (a stock count, a lock token, a dedupe marker) is plain text, so there's
 * no need for the JSON/object serialization a generic {@code RedisTemplate}
 * would otherwise require configuring.
 */
@Configuration
public class RedisConfig {

    @Bean
    public StringRedisTemplate redisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }
}
