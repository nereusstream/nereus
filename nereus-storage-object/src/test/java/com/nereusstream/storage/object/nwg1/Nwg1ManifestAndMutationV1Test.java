/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class Nwg1ManifestAndMutationV1Test {
    private static final Set<String> TOP_LEVEL = Set.of(
            "artifact",
            "componentInventory",
            "expectedCounts",
            "externalCallProfiles",
            "externalFixtures",
            "mutationOperations",
            "mutations",
            "resignOperations",
            "vectors",
            "zstdFixtures");
    private static final List<String> PATHS =
            List.of("ROUTINE_RANGE_READ", "FULL_BODY_RECONCILIATION", "OPEN_RUN_RECOVERY");

    @Test
    void manifestIsCanonicalClosedJcsWithExactInventory() throws Exception {
        Map<String, Object> manifest = object(StrictJcsV1.parseCanonical(Files.readAllBytes(manifestPath())));
        assertThat(manifest.keySet()).containsExactlyInAnyOrderElementsOf(TOP_LEVEL);
        assertThat(manifest.get("artifact")).isEqualTo("NWG1_GOLDEN_MANIFEST_V1");
        assertThat(strings(manifest.get("componentInventory")))
                .containsExactlyElementsOf(Nwg1GoldenCorpusV1.COMPONENT_KINDS);
        assertThat(strings(manifest.get("mutationOperations")))
                .containsExactly(Arrays.stream(Nwg1MutationOperationV1.values())
                        .map(Enum::name)
                        .toArray(String[]::new));
        assertThat(strings(manifest.get("resignOperations")))
                .containsExactly(Arrays.stream(Nwg1ResignOperationV1.values())
                        .map(Enum::name)
                        .toArray(String[]::new));
        assertThat(list(manifest.get("vectors"))).hasSize(6).isEqualTo(Nwg1GoldenCorpusV1.positiveInputProjection());
        assertThat(list(manifest.get("externalFixtures"))).hasSize(2);
        assertThat(list(manifest.get("zstdFixtures"))).hasSize(2);
        List<Map<String, Object>> profiles = list(manifest.get("externalCallProfiles")).stream()
                .map(Nwg1ManifestAndMutationV1Test::object)
                .toList();
        assertThat(profiles).hasSize(2);
        assertExternalCallProfile(profiles.get(0), "NO_EXTERNAL_CALLS_AFTER_PRELOADED_CUT", 30, 0);
        assertExternalCallProfile(profiles.get(1), "AT_MOST_ONE_KMS_UNWRAP_CALL_AFTER_PRELOADED_CUT", 54, 1);
    }

    @Test
    void allEightyFourRecordsAreExplicitAndExecuteTwoHundredFortyPaths() throws Exception {
        Map<String, Object> manifest = object(StrictJcsV1.parseCanonical(Files.readAllBytes(manifestPath())));
        List<Object> records = list(manifest.get("mutations"));
        assertThat(records).hasSize(84);
        int paths = 0;
        Map<String, Integer> bases = new LinkedHashMap<>();
        Set<String> ids = new HashSet<>();
        Set<String> codes = new HashSet<>();
        Set<String> stages = new HashSet<>();
        Set<String> scopes = new HashSet<>();
        Set<String> roots = new HashSet<>();
        Set<String> keys = new HashSet<>();
        Map<String, Nwg1GoldenCorpusV1.Vector> vectors = new LinkedHashMap<>();
        for (Nwg1GoldenCorpusV1.Vector vector : Nwg1GoldenCorpusV1.vectors()) {
            vectors.put(vector.id(), vector);
        }
        int noCalls = 0;
        int unwrap = 0;
        for (Object value : records) {
            Map<String, Object> record = object(value);
            String id = string(record.get("mutationId"));
            assertThat(ids.add(id)).as("unique mutationId").isTrue();
            List<String> applicable = strings(record.get("applicablePaths"));
            assertThat(applicable)
                    .isEqualTo(PATHS.stream().filter(applicable::contains).toList());
            paths += applicable.size();
            String base = string(record.get("baseVectorId"));
            bases.merge(base, 1, Integer::sum);
            codes.add(string(record.get("expectedRejectionCode")));
            stages.add(string(record.get("expectedStage")));
            scopes.add(string(record.get("expectedIsolationScope")));
            assertThat(record.get("expectedPublication")).isEqualTo("NONE");
            assertThat(list(record.get("mutationOperations"))).isNotEmpty();
            assertCanonicalResignOperations(record);
            assertThat(record.get("verificationEntryCut")).isEqualTo("PRELOADED_VERIFIED_ROOT_AND_ACQUIRED_BYTES_V1");
            String profile = string(record.get("mutationClass"));
            if (profile.equals("NO_EXTERNAL_CALLS_AFTER_PRELOADED_CUT")) {
                noCalls++;
            } else if (profile.equals("AT_MOST_ONE_KMS_UNWRAP_CALL_AFTER_PRELOADED_CUT")) {
                unwrap++;
            } else {
                throw new AssertionError("unknown external-call profile " + profile);
            }
            byte[] mutationRoot = verifyRecipeAndRoot(record, manifest, roots);
            if (mutationRoot != null) {
                assertThat(strings(record.get("resignOperations")))
                        .contains(
                                "RECOMPUTE_HEADER_CRC",
                                "REENCRYPT_DIRECTORY",
                                "REENCRYPT_FRAME",
                                "RECOMPUTE_BODY_SHA_AND_LEAF");
            }
            Nwg1MutationRunnerV1.Spec spec = mutationSpec(record, mutationRoot);
            Map<String, Object> authoredActual = object(record.get("actualExternalCallsByPath"));
            assertThat(authoredActual.keySet()).containsExactlyInAnyOrderElementsOf(applicable);
            byte[] derivedObjectKey = null;
            for (String path : applicable) {
                Nwg1MutationRunnerV1.Execution execution =
                        Nwg1MutationRunnerV1.execute(vectors.get(base), spec, Nwg1VerificationPathV1.valueOf(path));
                if (derivedObjectKey == null) {
                    derivedObjectKey = execution.derivedObjectKeySha256();
                } else {
                    assertThat(execution.derivedObjectKeySha256())
                            .as("%s %s deterministic derived Object key", id, path)
                            .isEqualTo(derivedObjectKey);
                }
                assertThat(execution.changedComponents())
                        .isEqualTo(spec.operations().size());
                assertThat(execution.failure().rejection().name())
                        .as(
                                "%s %s rejection (%s)",
                                id, path, execution.failure().getMessage())
                        .isEqualTo(record.get("expectedRejectionCode"));
                assertThat(execution.failure().stage().name())
                        .as("%s %s stage", id, path)
                        .isEqualTo(record.get("expectedStage"));
                assertThat(execution.failure().scope().name())
                        .as("%s %s scope", id, path)
                        .isEqualTo(record.get("expectedIsolationScope"));
                assertThat(execution.publication()).isEqualTo(record.get("expectedPublication"));
                Map<String, Object> maxima = object(record.get("expectedMaximumExternalCallsByKind"));
                Map<String, Object> actual = object(authoredActual.get(path));
                assertThat(maxima.keySet())
                        .containsExactlyInAnyOrderElementsOf(Nwg1MutationRunnerV1.EXTERNAL_CALL_KINDS);
                assertThat(actual.keySet())
                        .containsExactlyInAnyOrderElementsOf(Nwg1MutationRunnerV1.EXTERNAL_CALL_KINDS);
                for (String callKind : Nwg1MutationRunnerV1.EXTERNAL_CALL_KINDS) {
                    int observed = execution.externalCalls().get(callKind);
                    assertThat(observed)
                            .as("%s %s %s actual", id, path, callKind)
                            .isEqualTo(number(actual.get(callKind)));
                    assertThat(observed)
                            .as("%s %s %s maximum", id, path, callKind)
                            .isLessThanOrEqualTo(number(maxima.get(callKind)));
                }
            }
            if (mutationRoot != null) {
                assertThat(keys.add(HexFormat.of().formatHex(derivedObjectKey)))
                        .as("unique deep derived Object key")
                        .isTrue();
            }
        }
        assertThat(paths).isEqualTo(240);
        assertThat(bases)
                .hasSize(6)
                .containsAllEntriesOf(Map.of(
                        "NWG1_KAFKA_MIN_ZERO_RECORD_NONE_V1", 60,
                        "NWG1_KAFKA_MULTI_BINDING_COMMIT_SET_NONE_V1", 16,
                        "NWG1_KAFKA_FIXED_ZSTD_V1", 4,
                        "NWG1_PULSAR_MIN_ZERO_BYTE_NONE_V1", 2,
                        "NWG1_PULSAR_MULTI_BINDING_ADJACENT_NONE_V1", 1,
                        "NWG1_PULSAR_FIXED_ZSTD_V1", 1));
        assertThat(codes)
                .containsExactlyInAnyOrder(
                        Arrays.stream(Nwg1RejectionV1.values()).map(Enum::name).toArray(String[]::new));
        assertThat(stages)
                .containsExactlyInAnyOrder(Arrays.stream(Nwg1ValidationStageV1.values())
                        .map(Enum::name)
                        .toArray(String[]::new));
        assertThat(scopes)
                .containsExactlyInAnyOrder(Arrays.stream(Nwg1IsolationScopeV1.values())
                        .map(Enum::name)
                        .toArray(String[]::new));
        assertThat(roots).hasSize(50);
        assertThat(keys).hasSize(50);
        assertThat(noCalls).isEqualTo(30);
        assertThat(unwrap).isEqualTo(54);
    }

    @Test
    void allTenMutationTokensHaveConcreteRunnerSemantics() {
        byte[] input = new byte[192];
        for (Nwg1MutationOperationV1 operation : Nwg1MutationOperationV1.values()) {
            int offset =
                    switch (operation) {
                        case SWAP_ROWS -> 0;
                        case DUPLICATE_ROW, REMOVE_ROW -> 48;
                        default -> 4;
                    };
            byte[] operand = operation == Nwg1MutationOperationV1.SWAP_ROWS ? new byte[] {0, 0, 0, 96} : new byte[] {1};
            assertThat(Nwg1MutationRunnerV1.apply(input, operation, offset, operand))
                    .isNotNull();
        }
    }

    private static byte[] verifyRecipeAndRoot(
            Map<String, Object> record, Map<String, Object> manifest, Set<String> roots) {
        Map<String, Object> recipe = new LinkedHashMap<>();
        for (String key : List.of(
                "applicablePaths",
                "baseVectorId",
                "mutationId",
                "mutationOperations",
                "neutralizedEarlierChecks",
                "resignOperations",
                "verificationEntryCut")) {
            recipe.put(key, record.get(key));
        }
        byte[] recipeSha = Nwg1CommitmentsV1.sha256(StrictJcsV1.encode(recipe).getBytes(StandardCharsets.UTF_8));
        assertThat(HexFormat.of().formatHex(recipeSha)).isEqualTo(record.get("mutationRecipeSha256"));
        if (record.containsKey("mutationRootSha256")) {
            String base = string(record.get("baseVectorId"));
            String fixtureId =
                    base.contains("KAFKA") ? "EXT_KAFKA_WALRUN_AUTHORITY_V1" : "EXT_PULSAR_WALRUN_AUTHORITY_V1";
            Map<String, Object> fixture = list(manifest.get("externalFixtures")).stream()
                    .map(Nwg1ManifestAndMutationV1Test::object)
                    .filter(item -> item.get("fixtureId").equals(fixtureId))
                    .findFirst()
                    .orElseThrow();
            byte[] root = Nwg1MutationRunnerV1.mutationRoot(
                    HexFormat.of().parseHex(string(fixture.get("walRunRootSha256Hex"))),
                    string(record.get("mutationId")),
                    recipeSha);
            String actual = HexFormat.of().formatHex(root);
            assertThat(actual).isEqualTo(record.get("mutationRootSha256"));
            assertThat(actual).doesNotMatch("0+");
            assertThat(roots.add(actual)).isTrue();
            return root;
        }
        return null;
    }

    private static Nwg1MutationRunnerV1.Spec mutationSpec(Map<String, Object> record, byte[] mutationRoot) {
        List<Nwg1MutationRunnerV1.Operation> operations = list(record.get("mutationOperations")).stream()
                .map(Nwg1ManifestAndMutationV1Test::object)
                .map(operation -> new Nwg1MutationRunnerV1.Operation(
                        string(operation.get("componentKind")),
                        number(operation.get("rowOrdinal")),
                        Nwg1MutationOperationV1.valueOf(string(operation.get("operation"))),
                        number(operation.get("offset")),
                        HexFormat.of().parseHex(string(operation.get("operandHex")))))
                .toList();
        EnumSet<Nwg1ResignOperationV1> resign = EnumSet.noneOf(Nwg1ResignOperationV1.class);
        for (String operation : strings(record.get("resignOperations"))) {
            resign.add(Nwg1ResignOperationV1.valueOf(operation));
        }
        return new Nwg1MutationRunnerV1.Spec(string(record.get("mutationId")), operations, resign, mutationRoot);
    }

    private static void assertCanonicalResignOperations(Map<String, Object> record) {
        List<String> authored = strings(record.get("resignOperations"));
        assertThat(new HashSet<>(authored)).hasSize(authored.size());
        List<String> canonical = Arrays.stream(Nwg1ResignOperationV1.values())
                .map(Enum::name)
                .filter(authored::contains)
                .toList();
        assertThat(authored).isEqualTo(canonical);
    }

    private static void assertExternalCallProfile(
            Map<String, Object> profile, String token, int count, int kmsUnwrapMaximum) {
        assertThat(profile.get("token")).isEqualTo(token);
        assertThat(number(profile.get("count"))).isEqualTo(count);
        Map<String, Object> maximum = object(profile.get("maximumCalls"));
        assertThat(maximum.keySet()).containsExactlyInAnyOrderElementsOf(Nwg1MutationRunnerV1.EXTERNAL_CALL_KINDS);
        for (String kind : Nwg1MutationRunnerV1.EXTERNAL_CALL_KINDS) {
            assertThat(number(maximum.get(kind))).isEqualTo(kind.equals("KMS_UNWRAP") ? kmsUnwrapMaximum : 0);
        }
    }

    private static int number(Object value) {
        return Math.toIntExact((Long) value);
    }

    private static Path manifestPath() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve("docs/v2/wire/nwg1-v1-golden-manifest.json");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("cannot locate NWG1 manifest");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Object value) {
        return (List<Object>) value;
    }

    private static List<String> strings(Object value) {
        return list(value).stream().map(Nwg1ManifestAndMutationV1Test::string).toList();
    }

    private static String string(Object value) {
        return (String) value;
    }
}
