package com.orderflow.inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Entry point of inventory-service.
 *
 * Unlike order-service, this app has NO controllers, no embedded web
 * server doing anything meaningful (Spring Boot still starts a tiny actuator-
 * less context). Its entire job is: listen to Kafka, react. That's worth
 * sitting with — a huge share of real backend services are exactly this
 * shape, and it's easy to over-imagine "a service" as "a REST API" when
 * plenty of the most important ones aren't.
 */
@SpringBootApplication
public class InventoryServiceApplication {

    private static final Logger log = LoggerFactory.getLogger(InventoryServiceApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }

    /**
     * Build Order Step 2 quality-of-life: once you're running two or three
     * copies of this exact jar in separate terminals to watch scaling and
     * rebalancing, plain "Started InventoryServiceApplication" lines look
     * identical across all of them. This banner is the first thing that
     * distinguishes "which terminal is which instance" — match its
     * client-id against the one printed in each 🔀 rebalance log line.
     *
     * clientId is injected from KafkaConsumerConfig's
     * {@code inventoryInstanceClientId} bean — a plain Java field computed
     * once — rather than re-resolved from a property placeholder here.
     * See application.yml for exactly why that distinction matters: we
     * found out the hard way.
     */
    @Bean
    public CommandLineRunner instanceIdentityBanner(
            String inventoryInstanceClientId,
            @Value("${spring.kafka.consumer.group-id}") String groupId
    ) {
        String clientId = inventoryInstanceClientId;
        return args -> log.info("""

                ╔══════════════════════════════════════════════════════════
                ║ 🏷️  inventory-service instance identity
                ║   client.id = {}
                ║   group.id  = {}
                ║ Run more copies of this jar to scale out — watch
                ║ KafkaConsumerConfig's rebalance listener log which
                ║ partitions THIS client.id owns as instances join/leave.
                ╚══════════════════════════════════════════════════════════
                """, clientId, groupId);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Not every microservice has an inbound API. A pure event consumer is a
 *    completely normal, first-class shape for a service in an event-driven
 *    architecture.
 * 2. Run TWO instances of this app (same groupId, see application.yml) at
 *    once against a 3-partition topic and Kafka's consumer group protocol
 *    transparently splits the partitions between them — no code change,
 *    no coordination logic you had to write. That's what Build Order
 *    Step 2 ("scale inventory-service horizontally") is really showing
 *    off.
 *
 * 🔧 TRY IT YOURSELF
 * Start this app twice (two terminals, same jar). Kill one of them mid-
 * processing and watch the logs of the survivor — a rebalance happens
 * automatically and the surviving instance picks up the now-orphaned
 * partitions. No orders are lost; at worst some get reprocessed (that's
 * at-least-once, see OrderEventListener).
 * ════════════════════════════════════════════════════════════════════════
 */
