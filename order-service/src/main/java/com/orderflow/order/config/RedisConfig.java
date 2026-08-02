package com.orderflow.order.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Build Order Step 9: order-service's only Redis use case is the token
 * bucket rate limiter (see rate/TokenBucketRateLimiter.java) — everything
 * it stores (a token count, a timestamp) is plain text, so
 * {@link StringRedisTemplate} needs no further configuration.
 */
@Configuration
public class RedisConfig {

    @Bean
    public StringRedisTemplate redisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }
}
