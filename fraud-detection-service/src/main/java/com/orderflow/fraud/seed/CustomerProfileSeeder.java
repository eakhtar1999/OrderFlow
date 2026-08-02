package com.orderflow.fraud.seed;

import com.orderflow.avro.CustomerProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

import static com.orderflow.fraud.config.KafkaTopicConfig.CUSTOMER_PROFILE_TOPIC;

/**
 * Publishes a handful of hardcoded {@link CustomerProfile} records so
 * {@code FraudDetectionTopology}'s KStream-KTable join has real reference
 * data to enrich against.
 *
 * Runs ONLY under the "seed" Spring profile
 * ({@code --spring.profiles.active=seed} or
 * {@code SPRING_PROFILES_ACTIVE=seed}) — never on a normal startup.
 * There's no customer-service in this tutorial to produce this topic for
 * real; this class is an explicit, labeled test fixture standing in for
 * one, not a feature this service actually owns. Run it once to seed the
 * (compacted) topic, then start this service normally from then on —
 * the data is still there, compaction keeps it, no need to re-seed on
 * every startup.
 */
@Component
@Profile("seed")
public class CustomerProfileSeeder implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(CustomerProfileSeeder.class);

    private final KafkaTemplate<String, CustomerProfile> kafkaTemplate;

    public CustomerProfileSeeder(KafkaTemplate<String, CustomerProfile> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void run(String... args) {
        seed("cust-blocklisted-1", "BLOCKLISTED");
        seed("cust-trusted-1", "TRUSTED");
        seed("cust-new-1", "NEW");
        log.info("🌱 Seeded 3 customer profiles onto {}", CUSTOMER_PROFILE_TOPIC);
    }

    private void seed(String customerId, String riskTier) {
        CustomerProfile profile = CustomerProfile.newBuilder()
                .setCustomerId(customerId)
                .setRiskTier(riskTier)
                .setUpdatedAt(Instant.now().toEpochMilli())
                .build();
        kafkaTemplate.send(CUSTOMER_PROFILE_TOPIC, customerId, profile);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. @Profile is how Spring keeps test/demo fixtures out of normal
 *    production startup without a separate module or a manual comment-
 *    out-before-deploy ritual — the bean simply doesn't exist in the
 *    context unless that profile is active.
 * 2. Seeding a COMPACTED topic is a one-time operation, not a per-startup
 *    one — the whole point of compaction (Build Order Step 5) is that
 *    the latest value per key persists indefinitely, so re-running this
 *    seeder later would just overwrite the same keys with fresh
 *    timestamps, not duplicate anything.
 *
 * 🔧 TRY IT YOURSELF
 * Run this once: `mvn spring-boot:run -Dspring-boot.run.profiles=seed`.
 * Then start the service normally (no profile) and confirm via Kafka UI
 * that customer-profile still has all 3 records — this class never ran
 * on that second startup, and the data didn't need it to.
 * ════════════════════════════════════════════════════════════════════════
 */
