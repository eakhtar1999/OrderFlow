package com.orderflow.saga.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** Same shape as {@link InventoryServiceClient} — see that class for the full explanation. */
@Component
public class PaymentServiceClient {

    private static final Logger log = LoggerFactory.getLogger(PaymentServiceClient.class);

    private final RestClient restClient;

    public PaymentServiceClient(@Value("${services.payment-service.base-url}") String baseUrl) {
        this.restClient = RestClient.create(baseUrl);
    }

    @CircuitBreaker(name = "payment-service")
    @Retry(name = "payment-service", fallbackMethod = "chargeFallback")
    public ChargeResult charge(String orderId, String customerId, double totalAmount) {
        ChargeApiResponse response = restClient.post()
                .uri("/internal/charge")
                .body(new ChargeApiRequest(orderId, customerId, totalAmount))
                .retrieve()
                .body(ChargeApiResponse.class);
        return new ChargeResult(response.approved(), response.message());
    }

    @SuppressWarnings("unused")
    private ChargeResult chargeFallback(String orderId, String customerId, double totalAmount, Throwable t) {
        log.error("🔌 payment-service unreachable after retries (orderId={}): {}", orderId, t.toString());
        return new ChargeResult(false, "payment-service unavailable: " + t.getMessage());
    }

    public record ChargeResult(boolean approved, String message) {
    }

    private record ChargeApiRequest(String orderId, String customerId, double totalAmount) {
    }

    private record ChargeApiResponse(boolean approved, String message) {
    }
}
