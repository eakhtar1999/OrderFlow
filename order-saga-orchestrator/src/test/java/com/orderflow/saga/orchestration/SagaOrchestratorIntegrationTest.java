package com.orderflow.saga.orchestration;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Build Order Step 17. Covers order-saga-orchestrator's real contract —
 * the orchestration saga's happy path, its compensation path, AND (the
 * part choreography's Step 14 tests have no equivalent for) the actual
 * Resilience4j retry and circuit-breaker behavior this module exists to
 * demonstrate. See order-service's Step 14 sibling test for why this
 * project's integration tests are structured per-service.
 *
 * WireMock, not Testcontainers, stands in for inventory-service/
 * payment-service/shipment-service — ONE server handles all three, since
 * each client hits a different path with no collisions. This is what
 * makes the retry/circuit-breaker tests possible at all: scripting "fail
 * twice, then succeed" or "fail forever" on demand is something a stub
 * server can do that a real service (or Testcontainers) genuinely can't
 * be asked to do the same way.
 *
 * Real Testcontainers Postgres backs the actual {@code saga} table — the
 * same "assert against the real database, not just the response" rule
 * Step 14's tests followed.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class SagaOrchestratorIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("orderflow")
            .withUsername("orderflow")
            .withPassword("orderflow");

    @RegisterExtension
    static WireMockExtension wireMock = WireMockExtension.newInstance()
            // http2PlainDisabled: found necessary live. Spring's
            // RestClient falls back to the JDK's own java.net.http.HttpClient
            // here (WireMock's shaded httpclient5/Jetty aren't visible
            // on the classpath for Spring Boot's usual auto-detection to
            // find), and that client's default cleartext h2c-upgrade
            // attempt against WireMock's Jetty-based server was met with
            // a real "RST_STREAM: Stream cancelled" — every single
            // stubbed call failing at the transport level before WireMock
            // ever got to apply a stub. Forcing WireMock to serve plain
            // HTTP/1.1 only sidesteps the negotiation entirely. Real
            // inventory-service/payment-service/shipment-service run on
            // Tomcat, which doesn't enable h2c by default either — this
            // is a WireMock/Jetty-specific interop quirk, not evidence of
            // a latent bug in the actual services these tests stand in
            // for.
            .options(wireMockConfig().dynamicPort().http2PlainDisabled(true))
            .build();

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        // All three point at the SAME WireMock instance — see this
        // class's own Javadoc for why that's safe.
        registry.add("services.inventory-service.base-url", wireMock::baseUrl);
        registry.add("services.payment-service.base-url", wireMock::baseUrl);
        registry.add("services.shipment-service.base-url", wireMock::baseUrl);
    }

    @LocalServerPort
    int port;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    CircuitBreakerRegistry circuitBreakerRegistry;

    private TestRestTemplate rest;

    @BeforeEach
    void setUp() {
        wireMock.resetAll();
        rest = new TestRestTemplate();
        // Each test starts from a known CLOSED breaker, regardless of
        // what a PREVIOUS test's failures did to it — Resilience4j's
        // registry is a Spring singleton, shared across every test
        // method in this class.
        circuitBreakerRegistry.circuitBreaker("inventory-service").reset();
    }

    @Test
    void happyPath_sagaCompletesAndReachesShipped() {
        stubReserve(200, true, "reserved");
        stubCharge(200, true, "approved");
        stubShip(200, "SHIP-TEST-1");

        SagaResult result = startSaga("cust-1", "us-east", "sku-42", 2);

        assertThat(result.status()).isEqualTo("SHIPPED");
        assertThat(result.message()).contains("SHIP-TEST-1");

        String finalStatus = jdbcTemplate.queryForObject(
                "SELECT status FROM saga WHERE order_id = ?", String.class, result.orderId());
        assertThat(finalStatus).isEqualTo("SHIPPED");

        wireMock.verify(1, postRequestedFor(urlEqualTo("/internal/reserve")));
        wireMock.verify(1, postRequestedFor(urlEqualTo("/internal/charge")));
        wireMock.verify(1, postRequestedFor(urlEqualTo("/internal/ship")));
        // No compensation on the happy path.
        wireMock.verify(0, postRequestedFor(urlEqualTo("/internal/release")));
    }

    @Test
    void paymentDeclined_compensatesByReleasingInventory() {
        stubReserve(200, true, "reserved");
        stubCharge(200, false, "exceeds decline threshold");
        stubRelease(200);

        SagaResult result = startSaga("cust-2", "us-east", "sku-42", 30);

        assertThat(result.status()).isEqualTo("FAILED");
        assertThat(result.message()).contains("Payment:");

        String finalStatus = jdbcTemplate.queryForObject(
                "SELECT status FROM saga WHERE order_id = ?", String.class, result.orderId());
        assertThat(finalStatus).isEqualTo("FAILED");

        // THE compensation call — direct proof SagaOrchestrator actually
        // invoked inventoryClient.release(...) for exactly this orderId,
        // not just that the saga ended in FAILED.
        wireMock.verify(1, postRequestedFor(urlEqualTo("/internal/release"))
                .withRequestBody(matchingJsonPath("$.orderId", equalTo(result.orderId()))));
        // Never reached shipment — payment failed first.
        wireMock.verify(0, postRequestedFor(urlEqualTo("/internal/ship")));
    }

    @Test
    void inventoryDeclined_failsImmediately_noCompensationNoPaymentAttempt() {
        stubReserve(200, false, "insufficient stock");

        SagaResult result = startSaga("cust-3", "us-east", "sku-99", 5);

        assertThat(result.status()).isEqualTo("FAILED");
        assertThat(result.message()).contains("Inventory:");

        // Nothing was ever reserved, so there's genuinely nothing to
        // compensate — a release() call here would be a real bug (it
        // would attempt to release stock that was never actually taken).
        wireMock.verify(0, postRequestedFor(urlEqualTo("/internal/release")));
        wireMock.verify(0, postRequestedFor(urlEqualTo("/internal/charge")));
        wireMock.verify(0, postRequestedFor(urlEqualTo("/internal/ship")));
    }

    @Test
    void transientInventoryFailure_recoversViaRetry_sagaStillSucceeds() {
        // A WireMock stateful Scenario: the 1st and 2nd calls to
        // /internal/reserve fail (a transient blip), the 3rd succeeds —
        // exactly inside application.yml's configured max-attempts=3.
        // This directly proves @Retry actually retries, not just that
        // it's configured to.
        String scenario = "reserve-transient-failure";
        wireMock.stubFor(post(urlEqualTo("/internal/reserve"))
                .inScenario(scenario)
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(serverError())
                .willSetStateTo("attempt-2"));
        wireMock.stubFor(post(urlEqualTo("/internal/reserve"))
                .inScenario(scenario)
                .whenScenarioStateIs("attempt-2")
                .willReturn(serverError())
                .willSetStateTo("attempt-3"));
        wireMock.stubFor(post(urlEqualTo("/internal/reserve"))
                .inScenario(scenario)
                .whenScenarioStateIs("attempt-3")
                .willReturn(okJson("{\"reserved\":true,\"message\":\"reserved on 3rd attempt\"}")));
        stubCharge(200, true, "approved");
        stubShip(200, "SHIP-TEST-2");

        SagaResult result = startSaga("cust-4", "us-east", "sku-42", 2);

        // The saga sees only the FINAL outcome — retry is invisible to
        // SagaOrchestrator.java entirely, exactly as designed: @Retry
        // wraps the client method transparently, so 2 failed attempts +
        // 1 successful one look identical to "it just worked" from the
        // orchestrator's point of view.
        assertThat(result.status()).isEqualTo("SHIPPED");
        wireMock.verify(3, postRequestedFor(urlEqualTo("/internal/reserve")));
    }

    @Test
    void sustainedInventoryFailure_tripsCircuitBreakerOpen_thenStopsCallingDownstream() {
        stubReserve(500, false, null);
        CircuitBreaker breaker = circuitBreakerRegistry.circuitBreaker("inventory-service");
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

        // Fire sagas until the breaker trips OPEN, bounded so a genuine
        // regression fails this test instead of hanging forever. Each
        // saga call means up to 3 failed HTTP attempts (max-attempts=3)
        // — comfortably enough to fill the 5-call sliding window
        // (application.yml: sliding-window-size=5) within 2-3 saga
        // calls.
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            startSaga("cust-5", "us-east", "sku-42", 1);
            return breaker.getState() == CircuitBreaker.State.OPEN;
        });

        int requestsBeforeAnotherAttempt = wireMock.getAllServeEvents().size();

        // One more saga call while the breaker is OPEN — @Retry's
        // ignore-exceptions (application.yml) means CallNotPermittedException
        // fails FAST, on the first attempt, with no real HTTP call at
        // all. If this assertion ever fails, it means the circuit
        // breaker stopped actually protecting the downstream call.
        SagaResult result = startSaga("cust-6", "us-east", "sku-42", 1);
        assertThat(result.status()).isEqualTo("FAILED");

        int requestsAfterAnotherAttempt = wireMock.getAllServeEvents().size();
        assertThat(requestsAfterAnotherAttempt).isEqualTo(requestsBeforeAnotherAttempt);
    }

    private SagaResult startSaga(String customerId, String region, String productId, int quantity) {
        Map<String, Object> body = Map.of(
                "customerId", customerId,
                "region", region,
                "items", List.of(Map.of("productId", productId, "quantity", quantity)));
        return rest.postForObject("http://localhost:" + port + "/api/saga/orders", body, SagaResult.class);
    }

    private void stubReserve(int status, boolean reserved, String message) {
        wireMock.stubFor(post(urlEqualTo("/internal/reserve"))
                .willReturn(status == 200
                        ? okJson("{\"reserved\":" + reserved + ",\"message\":\"" + message + "\"}")
                        : aResponse().withStatus(status)));
    }

    private void stubCharge(int status, boolean approved, String message) {
        wireMock.stubFor(post(urlEqualTo("/internal/charge"))
                .willReturn(status == 200
                        ? okJson("{\"approved\":" + approved + ",\"message\":\"" + message + "\"}")
                        : aResponse().withStatus(status)));
    }

    private void stubRelease(int status) {
        wireMock.stubFor(post(urlEqualTo("/internal/release"))
                .willReturn(aResponse().withStatus(status)));
    }

    private void stubShip(int status, String shipmentId) {
        wireMock.stubFor(post(urlEqualTo("/internal/ship"))
                .willReturn(status == 200
                        ? okJson("{\"shipmentId\":\"" + shipmentId + "\"}")
                        : aResponse().withStatus(status)));
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. WireMock scripts downstream behavior ON DEMAND — "fail twice then
 *    succeed," "fail forever," "decline this specific charge" — none of
 *    which a real inventory-service/payment-service/shipment-service
 *    (or a Testcontainers copy of one) can be told to do reliably. This
 *    is the right stand-in specifically BECAUSE this module's own
 *    interesting behavior (Retry, CircuitBreaker, compensation) only
 *    shows up under conditions a real dependency won't reproduce on
 *    command.
 * 2. Resilience4j reacts to EXCEPTIONS (real unavailability), never to a
 *    business "no" encoded in a 200 — inventoryDeclined_...
 *    (reserved=false, still HTTP 200) triggers zero retries and zero
 *    circuit-breaker involvement, structurally identical from
 *    Resilience4j's point of view to a successful call that happened to
 *    say no. Compare directly against
 *    sustainedInventoryFailure_..., which needs actual 500s to engage
 *    either mechanism at all.
 * 3. The circuit breaker test asserts against the ACTUAL
 *    CircuitBreakerRegistry bean's state (CLOSED -> OPEN), not against a
 *    hardcoded predicted request count — Resilience4j's internal
 *    accounting (does one retry attempt count once or does the whole
 *    method call count once?) is exactly the kind of detail worth
 *    verifying directly rather than assuming, the same lesson
 *    InventoryServiceClient's own Javadoc learned the hard way about
 *    @Retry/@CircuitBreaker's stacking order.
 * 4. Compensation is verified as a REAL HTTP call WireMock actually
 *    received, with the right orderId in the body — not inferred from
 *    the saga's own status. A bug where SagaOrchestrator decided to
 *    compensate but the client silently swallowed the call would still
 *    show FAILED in the response; only checking WireMock's own request
 *    log catches that class of bug.
 *
 * 🔧 TRY IT YOURSELF
 * Change sustainedInventoryFailure_...'s stub from a 500 to a 200 with
 * reserved=false (a business decline, not a failure) and rerun — watch
 * the `await().until(...)` loop never terminate (breaker never opens,
 * since Resilience4j never even sees an exception) and the test time out
 * instead of passing. That failure IS the point being taught: it's not
 * possible to trip this circuit breaker with a well-formed "no."
 * ════════════════════════════════════════════════════════════════════════
 */
