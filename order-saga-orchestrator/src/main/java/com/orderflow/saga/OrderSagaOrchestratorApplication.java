package com.orderflow.saga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Entry point of order-saga-orchestrator — Build Order Step 8's orchestration-based saga, standing beside (not replacing) the choreography saga already running across order/inventory/payment/shipment-service. */
@SpringBootApplication
public class OrderSagaOrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderSagaOrchestratorApplication.class, args);
    }
}
