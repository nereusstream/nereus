/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.evidence;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

final class Phase9EvidenceAggregatorTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern SCENARIO_ID =
            Pattern.compile("KF-[A-Z0-9]+-[0-9]{3}");
    private static final Set<String> RUNNABLE_STATUSES =
            Set.of("IMPLEMENTED_NOT_RUN", "PASSED_CURRENT_SOURCE");
    private static final Set<String> KNOWN_SERVICES =
            Set.of("kraft", "oxia", "object-store", "bookkeeper");
    private static final Set<String> KNOWN_SOURCE_LOCKS =
            Set.of("nereus", "automq", "kafka");
    private static final Map<String, String> METHOD_SEGMENTS =
            Map.of(
                    "SRC", "Src",
                    "API", "Api",
                    "META", "Meta",
                    "APP", "App",
                    "FET", "Fet",
                    "TXN", "Txn",
                    "RET", "Ret",
                    "CMP", "Cmp",
                    "OPS", "Ops",
                    "SCL", "Scl");

    @TestFactory
    Stream<DynamicTest> scenarioEvidence() throws IOException {
        Evidence evidence = loadEvidence();
        return evidence.scenarios().stream()
                .filter(scenario -> !scenario.id().equals("KF-SCL-010"))
                .map(scenario ->
                        DynamicTest.dynamicTest(
                                scenario.id()
                                        + "|"
                                        + scenario.testClass()
                                        + "#"
                                        + scenario.testMethod(),
                                () -> assertScenarioEvidence(scenario, evidence)));
    }

    @Test
    @DisplayName("KF-SCL-010|Phase9EvidenceAggregatorTest#scenarioKfScl010")
    void scenarioKfScl010() throws Exception {
        Evidence evidence = loadEvidence();
        assertThat(evidence.scenarios())
                .extracting(Scenario::id)
                .hasSize(146)
                .doesNotHaveDuplicates();
        assertThat(evidence.scenarios())
                .allSatisfy(scenario -> {
                    assertThat(scenario.status())
                            .as("runnable status for " + scenario.id())
                            .isIn(RUNNABLE_STATUSES);
                    assertThat(scenario.testClass()).isNotBlank();
                    assertThat(scenario.testMethod())
                            .isEqualTo(canonicalMethod(scenario.id()));
                    assertThat(scenario.task()).isNotBlank();
                    assertThat(scenario.sourceLocks())
                            .allMatch(KNOWN_SOURCE_LOCKS::contains);
                    assertThat(scenario.requiredServices())
                            .allMatch(KNOWN_SERVICES::contains);
                });

        Set<String> ownerKeys = new HashSet<>();
        evidence.scenarios().forEach(scenario ->
                assertThat(ownerKeys.add(
                                scenario.testClass() + "#" + scenario.testMethod()))
                        .as("unique canonical owner for " + scenario.id())
                        .isTrue());

        assertThat(evidence.preEvidence().path("schemaVersion").asInt())
                .isEqualTo(1);
        assertThat(evidence.preEvidence().path("scenarioId").asText())
                .isEqualTo("KF-SCL-010");
        assertThat(evidence.preEvidence().path("rerunTasks").asBoolean())
                .isTrue();
        assertThat(evidence.preEvidence().path("scenarioCount").asInt())
                .isEqualTo(146);
        assertThat(evidence.preEvidence().path("product").path("branch").asText())
                .isEqualTo("main");
        assertThat(evidence.preEvidence().path("product").path("commit").asText())
                .matches("[0-9a-f]{40}");
        assertThat(evidence.preEvidence().path("product").path("clean").asBoolean())
                .isTrue();
        assertThat(evidence.preEvidence()
                        .path("autoMqReference")
                        .path("branch")
                        .asText())
                .isEqualTo("main");
        assertThat(evidence.preEvidence()
                        .path("autoMqReference")
                        .path("commit")
                        .asText())
                .isEqualTo("1c648d84819d5c3fef2af585f02149c397584870");
        assertThat(evidence.preEvidence()
                        .path("autoMqReference")
                        .path("version")
                        .asText())
                .isEqualTo("3.9.0-SNAPSHOT");
        assertThat(evidence.preEvidence().path("kafkaFork").path("branch").asText())
                .isEqualTo("nereus/future9-native-kafka-storage");
        assertThat(evidence.preEvidence().path("kafkaFork").path("commit").asText())
                .isEqualTo("76f62f3b83e882105219b6c7687dbde594a8b8a2");
        assertThat(evidence.preEvidence().path("kafkaFork").path("clean").asBoolean())
                .isTrue();
        assertThat(evidence.preEvidence().path("pulsarFork").path("branch").asText())
                .isEqualTo("5.0.0-M1-nereus");
        assertThat(evidence.preEvidence().path("pulsarFork").path("commit").asText())
                .isEqualTo("50fc70fe4620febcf0fd31d97ff7d2be447af3d4");
        assertThat(evidence.preEvidence().path("pulsarFork").path("clean").asBoolean())
                .isTrue();
        assertThat(evidence.preEvidence().path("junit").path("suites").asInt())
                .isGreaterThanOrEqualTo(30);
        assertThat(evidence.preEvidence().path("junit").path("tests").asInt())
                .isGreaterThanOrEqualTo(100);
        assertThat(evidence.preEvidence().path("junit").path("canonicalSha256").asText())
                .matches("[0-9a-f]{64}");

        Set<String> manifestIds = new HashSet<>();
        evidence.scenarios().forEach(scenario -> manifestIds.add(scenario.id()));
        Set<String> markdownIds = markdownIds(requiredPath("nereus.f9.matrix"));
        assertThat(markdownIds).containsExactlyInAnyOrderElementsOf(manifestIds);

        JsonNode artifacts = evidence.preEvidence().path("artifacts");
        assertThat(artifacts.isArray()).isTrue();
        assertThat(artifacts.size()).isEqualTo(4);
        for (JsonNode artifact : artifacts) {
            Path path = evidence.repository().resolve(artifact.path("path").asText());
            assertThat(path).exists().isRegularFile();
            assertThat(sha256(path))
                    .as("artifact hash for " + path)
                    .isEqualTo(artifact.path("sha256").asText());
        }
    }

    private static void assertScenarioEvidence(
            Scenario scenario,
            Evidence evidence
    ) {
        assertThat(scenario.status())
                .as("runnable status for " + scenario.id())
                .isIn(RUNNABLE_STATUSES);
        assertThat(evidence.passedTasks())
                .as("fresh owner task for " + scenario.id())
                .contains(scenario.task());
        assertThat(evidence.services())
                .as("required services for " + scenario.id())
                .containsAll(scenario.requiredServices());
        if (scenario.sourceLocks().contains("nereus")) {
            assertThat(evidence.preEvidence()
                            .path("product")
                            .path("commit")
                            .asText())
                    .matches("[0-9a-f]{40}");
            assertThat(evidence.preEvidence()
                            .path("product")
                            .path("clean")
                            .asBoolean())
                    .isTrue();
        }
        if (scenario.sourceLocks().contains("automq")) {
            assertThat(evidence.preEvidence()
                            .path("autoMqReference")
                            .path("commit")
                            .asText())
                    .isEqualTo("1c648d84819d5c3fef2af585f02149c397584870");
            assertThat(evidence.preEvidence()
                            .path("autoMqReference")
                            .path("version")
                            .asText())
                    .isEqualTo("3.9.0-SNAPSHOT");
        }
        if (scenario.sourceLocks().contains("kafka")) {
            assertThat(evidence.preEvidence()
                            .path("kafkaFork")
                            .path("commit")
                            .asText())
                    .isEqualTo("76f62f3b83e882105219b6c7687dbde594a8b8a2");
            assertThat(evidence.preEvidence()
                            .path("kafkaFork")
                            .path("clean")
                            .asBoolean())
                    .isTrue();
        }
        assertThat(scenario.testMethod())
                .isEqualTo(canonicalMethod(scenario.id()));
    }

    private static Evidence loadEvidence() throws IOException {
        Path repository = requiredPath("nereus.f9.repository");
        JsonNode manifest = JSON.readTree(requiredPath("nereus.f9.manifest").toFile());
        JsonNode preEvidence =
                JSON.readTree(requiredPath("nereus.f9.pre.evidence").toFile());
        List<Scenario> scenarios = new ArrayList<>();
        for (JsonNode node : manifest.path("scenarios")) {
            scenarios.add(
                    new Scenario(
                            node.path("id").asText(),
                            node.path("task").asText(),
                            node.path("testClass").asText(),
                            node.path("testMethod").asText(),
                            node.path("status").asText(),
                            textList(node.path("requiredServices")),
                            textList(node.path("sourceLocks"))));
        }
        Set<String> passedTasks = new HashSet<>(
                textList(preEvidence.path("passedTasks")));
        Set<String> services = new HashSet<>(
                textList(preEvidence.path("services")));
        return new Evidence(
                repository,
                List.copyOf(scenarios),
                preEvidence,
                Set.copyOf(passedTasks),
                Set.copyOf(services));
    }

    private static List<String> textList(JsonNode array) {
        assertThat(array.isArray()).isTrue();
        List<String> values = new ArrayList<>();
        array.forEach(value -> values.add(value.asText()));
        return List.copyOf(values);
    }

    private static String canonicalMethod(String id) {
        String[] segments = id.split("-");
        assertThat(segments).hasSize(3);
        String group = METHOD_SEGMENTS.get(segments[1]);
        assertThat(group).as("known F9 group for " + id).isNotNull();
        return "scenarioKf" + group + segments[2];
    }

    private static Set<String> markdownIds(Path matrix) throws IOException {
        Matcher matcher = SCENARIO_ID.matcher(Files.readString(matrix));
        Map<String, Boolean> ordered = new LinkedHashMap<>();
        while (matcher.find()) {
            ordered.put(matcher.group(), Boolean.TRUE);
        }
        return Set.copyOf(ordered.keySet());
    }

    private static Path requiredPath(String property) {
        String configured = System.getProperty(property);
        assertThat(configured).as(property).isNotBlank();
        Path path = Path.of(configured).toAbsolutePath().normalize();
        assertThat(path).as(property).exists();
        return path;
    }

    private static String sha256(Path path) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(Files.readAllBytes(path));
        StringBuilder encoded = new StringBuilder(64);
        for (byte value : digest) {
            encoded.append(String.format(Locale.ROOT, "%02x", value));
        }
        return encoded.toString();
    }

    private record Scenario(
            String id,
            String task,
            String testClass,
            String testMethod,
            String status,
            List<String> requiredServices,
            List<String> sourceLocks) {
    }

    private record Evidence(
            Path repository,
            List<Scenario> scenarios,
            JsonNode preEvidence,
            Set<String> passedTasks,
            Set<String> services) {
    }
}
