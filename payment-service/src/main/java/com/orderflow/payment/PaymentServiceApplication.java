package com.orderflow.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point of payment-service — Build Order Step 8's second saga step.
 * Same "consumer group underneath a Kafka listener" shape as
 * inventory-service, plus a small REST surface
 * ({@code internal/PaymentController.java}) for order-saga-orchestrator.
 */
@SpringBootApplication
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
