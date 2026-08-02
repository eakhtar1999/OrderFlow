package com.orderflow.saga.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** Same shape as {@link InventoryServiceClient} — see that class for the full explanation. No compensating call — see ShipmentCreator's Javadoc for why shipment never fails in this tutorial. */
@Component
public class ShipmentServiceClient {

    private static final Logger log = LoggerFactory.getLogger(ShipmentServiceClient.class);

    private final RestClient restClient;

    public ShipmentServiceClient(@Value("${services.shipment-service.base-url}") String baseUrl) {
        this.restClient = RestClient.create(baseUrl);
    }

    @CircuitBreaker(name = "shipment-service")
    @Retry(name = "shipment-service", fallbackMethod = "shipFallback")
    public ShipResult ship(String orderId, String customerId) {
        ShipApiResponse response = restClient.post()
                .uri("/internal/ship")
                .body(new ShipApiRequest(orderId, customerId))
                .retrieve()
                .body(ShipApiResponse.class);
        return new ShipResult(response.shipmentId());
    }

    @SuppressWarnings("unused")
    private ShipResult shipFallback(String orderId, String customerId, Throwable t) {
        log.error("🔌 shipment-service unreachable after retries (orderId={}): {}", orderId, t.toString());
        return new ShipResult(null);
    }

    public record ShipResult(String shipmentId) {
    }

    private record ShipApiRequest(String orderId, String customerId) {
    }

    private record ShipApiResponse(String shipmentId) {
    }
}
