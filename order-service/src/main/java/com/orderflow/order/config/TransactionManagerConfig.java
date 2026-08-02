package com.orderflow.order.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Explicitly registers the JDBC transaction manager under the exact name
 * {@code OutboxWriter} asks for via {@code @Transactional("transactionManager")}.
 *
 * Found by running this, not by reading docs first: without this class,
 * order-service fails on the very first request with
 * {@code NoSuchBeanDefinitionException: No bean named 'transactionManager'
 * available} — not an ambiguity error, a genuinely MISSING bean. The
 * reason is a real Spring Boot auto-configuration interaction, not a typo:
 * Boot's {@code DataSourceTransactionManagerAutoConfiguration} only
 * creates its bean when {@code @ConditionalOnMissingBean(TransactionManager.class)}
 * holds. Once {@code spring.kafka.producer.transaction-id-prefix} is set
 * (see application.yml), Boot ALSO auto-configures a
 * {@code KafkaTransactionManager} — and depending on auto-configuration
 * processing order, that alone can satisfy "a TransactionManager bean
 * already exists," silently skipping the JDBC one entirely. The two
 * auto-configurations don't coordinate on WHICH TransactionManager should
 * win; they just both react to "is there already one," and Kafka's can
 * get there first.
 *
 * Defining this bean explicitly sidesteps that race completely — this
 * bean exists unconditionally, not "unless something else showed up
 * first."
 */
@Configuration
public class TransactionManagerConfig {

    @Bean(name = "transactionManager")
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. @ConditionalOnMissingBean is a race between auto-configurations, not
 *    a guarantee about WHICH bean of a given type wins — two different
 *    auto-configurations can each satisfy "something implements
 *    TransactionManager" without either one being the specific
 *    implementation your code actually needs.
 * 2. Mixing a JDBC datasource with a Kafka transactional producer in the
 *    same Spring context is a common real-world combination (this is
 *    what a proper outbox relay needs) — and this exact ambiguity is a
 *    documented, known interaction, not something unique to this
 *    tutorial's setup.
 * 3. The fix is to stop relying on auto-configuration's implicit
 *    ordering and be explicit: name the bean you need, exactly, so
 *    there's no "missing" or "ambiguous" state possible.
 * ════════════════════════════════════════════════════════════════════════
 */
