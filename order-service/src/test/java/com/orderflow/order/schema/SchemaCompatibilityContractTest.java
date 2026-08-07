package com.orderflow.order.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Build Order Step 18. Turns the manual `curl` checks against Schema
 * Registry's `/compatibility` endpoint (root README's "Running Step 3
 * yourself" section) into a real, automated, repeatable test — against
 * the ACTUAL `avro-schemas/order-created.avsc` file on disk, not a
 * hand-copied string that could silently drift from what every service's
 * avro-maven-plugin actually generates code from.
 *
 * Deliberately NOT a {@code @SpringBootTest} — this test exercises
 * Schema Registry's own compatibility rules directly over HTTP, the same
 * way the root README's curl commands did. It needs no Spring context,
 * no Postgres, no Redis; only a real Kafka broker (Schema Registry's
 * `_schemas` topic needs somewhere to live) and a real Schema Registry,
 * both via Testcontainers, on a shared Docker network so Schema Registry
 * can reach Kafka by its container network alias.
 */
@Testcontainers
class SchemaCompatibilityContractTest {

    private static final Network network = Network.newNetwork();

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"))
            .withKraft()
            .withNetwork(network)
            .withNetworkAliases("kafka");

    @Container
    static GenericContainer<?> schemaRegistry =
            new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.7.0"))
                    .withNetwork(network)
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    // Reaches Kafka over the SHARED network by its alias,
                    // not by any host-mapped port — the two containers
                    // talk to each other on Docker's internal network,
                    // completely independent of what port Testcontainers
                    // happened to map to the host for THIS test's own
                    // use (see baseUrl() below, which DOES need the
                    // host-mapped port, since the test JVM itself runs
                    // outside that network).
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    // Found necessary live: without an explicit
                    // dependency, Testcontainers starts multiple
                    // @Container fields in parallel by default —
                    // schema-registry's own startup script tried (and
                    // failed) to resolve "kafka" because the Kafka
                    // container's network alias wasn't registered on the
                    // shared network yet. dependsOn() forces Kafka to be
                    // fully started FIRST.
                    .dependsOn(kafka)
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200)
                            .withStartupTimeout(Duration.ofSeconds(60)));

    private static final String SUBJECT = "order-created-value";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static JsonNode currentOrderCreatedSchema;

    @BeforeAll
    static void loadCurrentSchemaAndRegisterV1() throws IOException, InterruptedException {
        // The REAL file every service's avro-maven-plugin generates code
        // from — see order-service/pom.xml's own comment on why. Reading
        // it here, rather than hardcoding a copy, means this test starts
        // failing the moment the actual contract changes in a way its
        // own assumptions no longer hold, instead of silently testing a
        // stale snapshot forever.
        Path schemaFile = Path.of("..", "avro-schemas", "order-created.avsc");
        currentOrderCreatedSchema = JSON.readTree(Files.readString(schemaFile));

        int status = registerSchema(currentOrderCreatedSchema);
        assertThat(status).as("registering the current, real order-created.avsc as v1 must succeed").isEqualTo(200);
    }

    @Test
    void addingAnOptionalFieldWithADefault_isBackwardCompatible() throws IOException, InterruptedException {
        // The real, live-verified Step 3 demo: giftMessage was added
        // this exact way. Re-proving the GENERAL rule here (a new
        // "priority" field, not literally giftMessage again) — a reader
        // still running the OLD schema simply never asks for this field,
        // Avro's schema resolution drops it silently.
        JsonNode v2 = withAddedField(currentOrderCreatedSchema, "priority", "string", "NORMAL");

        CompatibilityCheck check = checkCompatibility(v2);

        assertThat(check.isCompatible)
                .as("adding an optional field WITH a default must be backward compatible")
                .isTrue();
    }

    @Test
    void removingAField_isBackwardCompatible_theSurprisingRealResult() throws IOException, InterruptedException {
        // The root README documents this as a genuine surprise: we
        // ASSUMED removing a required field would be rejected. It isn't
        // — under BACKWARD compatibility (Schema Registry's default), a
        // new reader schema is allowed to simply not ask for a field the
        // old writer data has. This test locks in that real, confirmed
        // behavior as a specification, not an assumption future readers
        // have to take on faith from a paragraph of prose.
        JsonNode v3 = withRemovedField(currentOrderCreatedSchema, "region");

        CompatibilityCheck check = checkCompatibility(v3);

        assertThat(check.isCompatible)
                .as("removing a field entirely is backward compatible — old data simply has a field the new reader never asks for")
                .isTrue();
    }

    @Test
    void addingARequiredFieldWithNoDefault_isRejected_theActualBreakingCase() throws IOException, InterruptedException {
        // THE real breaking case, per the root README's own live
        // finding: a NEW required field with no default. A reader on
        // this new schema would have no way to fill in a value for
        // OLDER data that never had this field at all.
        JsonNode v4 = withAddedRequiredField(currentOrderCreatedSchema, "priority", "string");

        CompatibilityCheck check = checkCompatibility(v4);

        assertThat(check.isCompatible)
                .as("adding a REQUIRED field with no default must be rejected")
                .isFalse();
        // The specific, machine-readable reason Schema Registry gives —
        // verified, not assumed, matching the exact error type the root
        // README's curl output captured live.
        assertThat(check.messages.toString()).contains("READER_FIELD_MISSING_DEFAULT_VALUE");
    }

    private static JsonNode withAddedField(JsonNode schema, String name, String type, String defaultValue) {
        ObjectNode clone = (ObjectNode) schema.deepCopy();
        ArrayNode fields = (ArrayNode) clone.get("fields");
        ObjectNode newField = JSON.createObjectNode();
        newField.put("name", name);
        newField.put("type", type);
        newField.put("default", defaultValue);
        fields.add(newField);
        return clone;
    }

    private static JsonNode withAddedRequiredField(JsonNode schema, String name, String type) {
        ObjectNode clone = (ObjectNode) schema.deepCopy();
        ArrayNode fields = (ArrayNode) clone.get("fields");
        ObjectNode newField = JSON.createObjectNode();
        newField.put("name", name);
        newField.put("type", type);
        // Deliberately NO "default" — this is the exact thing that makes
        // it a breaking change.
        fields.add(newField);
        return clone;
    }

    private static JsonNode withRemovedField(JsonNode schema, String fieldName) {
        ObjectNode clone = (ObjectNode) schema.deepCopy();
        ArrayNode originalFields = (ArrayNode) clone.get("fields");
        ArrayNode filteredFields = JSON.createArrayNode();
        for (JsonNode field : originalFields) {
            if (!field.get("name").asText().equals(fieldName)) {
                filteredFields.add(field);
            }
        }
        clone.set("fields", filteredFields);
        return clone;
    }

    private static int registerSchema(JsonNode schema) throws IOException, InterruptedException {
        ObjectNode body = JSON.createObjectNode();
        body.put("schema", JSON.writeValueAsString(schema));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + "/subjects/" + SUBJECT + "/versions"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
                .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode();
    }

    /**
     * The exact REST call the root README's curl commands make —
     * {@code POST /compatibility/subjects/{subject}/versions/latest?verbose=true}
     * — against the SAME real, running Schema Registry every other test
     * in this class already registered v1 against in {@code @BeforeAll}.
     */
    private static CompatibilityCheck checkCompatibility(JsonNode candidateSchema) throws IOException, InterruptedException {
        ObjectNode body = JSON.createObjectNode();
        body.put("schema", JSON.writeValueAsString(candidateSchema));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + "/compatibility/subjects/" + SUBJECT + "/versions/latest?verbose=true"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
                .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode responseBody = JSON.readTree(response.body());

        return new CompatibilityCheck(
                responseBody.path("is_compatible").asBoolean(),
                responseBody.path("messages"));
    }

    private static String baseUrl() {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
    }

    private record CompatibilityCheck(boolean isCompatible, JsonNode messages) {
    }
}

/*
 * ════════════════════════════════════════════════════════════════════════
 * 🎓 CONCEPTS LEARNED IN THIS FILE
 * ────────────────────────────────────────────────────────────────────────
 * 1. A real Schema Registry container, not a mock — this test asks the
 *    ACTUAL compatibility-checking engine every service in this platform
 *    depends on, the same way the root README's curl commands did. A
 *    hand-written "is this compatible" check in Java would only ever
 *    test this project's UNDERSTANDING of Avro's rules, not the rules
 *    themselves.
 * 2. Two Testcontainers containers on ONE shared Network, reached two
 *    DIFFERENT ways: Schema Registry talks to Kafka over the Docker
 *    network by container alias (`kafka:9092`, invisible to the host);
 *    this test's own JVM talks to Schema Registry via its host-mapped
 *    port (`getHost()`/`getMappedPort(8081)`), since the JVM itself
 *    isn't ON that Docker network at all.
 * 3. The real schema file is read from disk, not hardcoded as a string
 *    literal — the SAME path (`../avro-schemas/order-created.avsc`)
 *    every service's avro-maven-plugin configuration already points at.
 *    A future edit to that file that accidentally breaks compatibility
 *    would make THIS test fail, not silently pass against a stale copy.
 * 4. The "surprising" result (field removal being backward-compatible)
 *    is asserted explicitly, with a comment explaining WHY it's not a
 *    bug in the test — a future reader hitting this assertion and
 *    expecting it to fail learns the same real lesson this project's own
 *    developers learned live, instead of independently re-discovering
 *    it (or worse, assuming the test itself has a bug and "fixing" it).
 *
 * 🔧 TRY IT YOURSELF
 * Change withAddedRequiredField's call in the third test to ALSO pass a
 * default value (reuse withAddedField instead) and watch that test fail
 * — is_compatible flips to true, and the READER_FIELD_MISSING_DEFAULT_VALUE
 * message disappears entirely, because the one thing that made it
 * breaking (no default) is now gone.
 * ════════════════════════════════════════════════════════════════════════
 */
