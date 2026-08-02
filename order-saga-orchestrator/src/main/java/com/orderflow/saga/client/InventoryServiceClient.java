package com.orderflow.saga.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;

/**
 * Orchestration's synchronous, resilient call into inventory-service —
 * the direct counterpart of choreography's OrderEventListener consuming
 * order-created. Where Kafka gives a choreographed consumer redelivery
 * and backpressure for free, a plain HTTP call gives you NOTHING for
 * free: no automatic retry, no protection against hammering a struggling
 * dependency. @Retry and @CircuitBreaker below exist specifically to
 * replace what Kafka's transport was quietly providing all along.
 */
@Component
public class InventoryServiceClient {

    private static final Logger log = LoggerFactory.getLogger(InventoryServiceClient.class);

    private final RestClient restClient;

    public InventoryServiceClient(@Value("${services.inventory-service.base-url}") String baseUrl) {
        this.restClient = RestClient.create(baseUrl);
    }

    /**
     * @Retry is the OUTER wrapper here, @CircuitBreaker the inner one —
     * verified by hand from a live stack trace
     * (Retry.executeCheckedSupplier calling into
     * CircuitBreakerAspect.circuitBreakerAroundAdvice), not assumed. That
     * ordering is WHY {@code fallbackMethod} lives on @Retry and NOT on
     * @CircuitBreaker: a fallback attached to the INNER annotation
     * converts every failure into a normal return value before it ever
     * reaches the OUTER one, which means whichever annotation is
     * innermost effectively swallows the exception — the outer one never
     * sees a failure to react to. We first wrote this with
     * fallbackMethod on @CircuitBreaker (matching a mental model of
     * "retry inner, breaker outer" that turned out to be backwards) and
     * watched retry never fire even once, confirmed by DEBUG logs showing
     * exactly ONE recorded call per request. Moving fallbackMethod here
     * fixed it: now each individual call gets up to 3 real attempts,
     * 300ms apart, and only once retry itself gives up does the fallback
     * run. @CircuitBreaker still wraps EACH of those 3 attempts
     * individually — once enough recent attempts across MANY requests
     * have failed, it trips OPEN, and retry.ignore-exceptions
     * (application.yml) tells Retry not to bother re-attempting a call
     * that's already failing fast for a reason no retry can fix.
     */
    @CircuitBreaker(name = "inventory-service")
    @Retry(name = "inventory-service", fallbackMethod = "reserveFallback")
    public ReserveResult reserve(String orderId, List<Item> items) {
        ReserveApiResponse response = restClient.post()
                .uri("/internal/reserve")
                .body(new ReserveApiRequest(orderId, items))
                .retrieve()
                .body(ReserveApiResponse.class);
        return new ReserveResult(response.reserved(), response.message());
    }

    /**
     * Only ever called after every retry attempt has THROWN — meaning
     * inventory-service was genuinely unreachable, not that it answered
     * "no." A business decline (a normal 200 response with
     * {@code reserved=false}) never reaches this method at all; it
     * returns straight out of {@link #reserve} like any other successful
     * call, because from Resilience4j's point of view, a 200 IS success,
     * regardless of what the body says.
     */
    @SuppressWarnings("unused")
    private ReserveResult reserveFallback(String orderId, List<Item> items, Throwable t) {
        log.error("🔌 inventory-service unreachable after retries (orderId={}): {}", orderId, t.toString());
        return new ReserveResult(false, "inventory-service unavailable: " + t.getMessage());
    }

    @CircuitBreaker(name = "inventory-service")
    @Retry(name = "inventory-service", fallbackMethod = "releaseFallback")
    public void release(String orderId, List<Item> items) {
        restClient.post()
                .uri("/internal/release")
                .body(new ReleaseApiRequest(orderId, items))
                .retrieve()
                .toBodilessEntity();
    }

    @SuppressWarnings("unused")
    private void releaseFallback(String orderId, List<Item> items, Throwable t) {
        // Best-effort compensation: if inventory-service is ALSO down
        // when we try to compensate, there's genuinely nothing more a
        // synchronous caller can do — logging loudly and moving on is
        // the honest response, not a retry loop that would just fail
        // the identical way. A real system would likely fall back to a
        // durable retry queue here; not built in this tutorial.
        log.error("🔌 COMPENSATION FAILED — could not reach inventory-service to release stock for order {}: {}",
                orderId, t.toString());
    }

    public record Item(String productId, int quantity) {
    }

    public record ReserveResult(boolean reserved, String message) {
    }

    private record ReserveApiRequest(String orderId, List<Item> items) {
    }

    private record ReserveApiResponse(boolean reserved, String message) {
    }

    private record ReleaseApiRequest(String orderId, List<Item> items) {
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. Resilience4j reacts to EXCEPTIONS, not to business outcomes encoded
 *    in a successful response body. A payment decline or an
 *    insufficient-stock "no" is a completely normal 200 OK as far as
 *    HTTP (and therefore Resilience4j) is concerned — only genuine
 *    unavailability (connection refused, timeout, 5xx) trips retry or
 *    the circuit breaker.
 * 2. @Retry and @CircuitBreaker stack, and BOTH the order AND where you
 *    put fallbackMethod matter — found live, the hard way. Resilience4j-
 *    spring6's default stacking makes @Retry the OUTER decorator and
 *    @CircuitBreaker the INNER one (verified from a real stack trace, not
 *    assumed). A fallbackMethod on the inner annotation converts failures
 *    into normal returns before the outer one ever sees them — the first
 *    version of this file had fallbackMethod on @CircuitBreaker and retry
 *    silently never fired, EVER, confirmed only by turning on DEBUG
 *    logging and counting exactly one recorded call per request instead
 *    of up to three. Fallback belongs on whichever annotation is
 *    OUTERMOST.
 * 3. retry.ignore-exceptions (application.yml) tells @Retry not to
 *    re-attempt a CallNotPermittedException — the exception
 *    @CircuitBreaker throws once it's OPEN. Without that, an open circuit
 *    would make retry dutifully wait 300ms and try again anyway, twice,
 *    against a breaker that's already decided not to let calls through —
 *    slow AND pointless. With it, an open circuit fails on the very first
 *    attempt, instantly.
 * 4. This is the resilience story Kafka gave choreography for free: at-
 *    least-once redelivery on crash, natural backpressure via consumer
 *    lag, no caller ever blocked waiting on a slow downstream. A
 *    synchronous orchestrator has none of that by default — @Retry and
 *    @CircuitBreaker are what you reach for to get comparable behavior
 *    over plain HTTP.
 * ════════════════════════════════════════════════════════════════════════
 */
