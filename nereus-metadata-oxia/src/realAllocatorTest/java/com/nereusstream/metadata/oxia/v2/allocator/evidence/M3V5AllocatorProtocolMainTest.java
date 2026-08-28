/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPlanProfileV5;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV5;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M3V5AllocatorProtocolMainTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void nativeBaselineRowsPreserveV3V4IdentityAndUseIndependentV5Identity() {
        assertThat(M3V3NativeBaselineCanaryTest.rowSchema(false))
                .isEqualTo("NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_ROW_V3");
        assertThat(M3V3NativeBaselineCanaryTest.rowSchema(true))
                .isEqualTo("NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_ROW_V4");
        assertThat(M3V3NativeBaselineCanaryTest.rowSchema(5))
                .isEqualTo("NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_ROW_V5");
    }

    @Test
    void diagnosticSealerBindsTheIndependentTwentyFourTestNadv5InventoryAndRawManifest() throws Exception {
        SourceBinding source = source();
        Path junit = diagnosticJUnitDirectory();
        Path raw = diagnosticRawDirectory(source);
        Path output = temporaryDirectory.resolve("diagnostic.nadv5");

        M3V5AllocatorProtocolMain.main(arguments("seal-diagnostic", junit, output, source, raw));
        M3V5AllocatorProtocolMain.main(arguments("validate-diagnostic", output, junit, source, raw));

        var diagnostic = AllocatorCampaignPromotionGateV5.decodeDiagnostic(
                CanonicalBytes.copyOf(Files.readAllBytes(output)));
        assertThat(M3V5AllocatorProtocolMain.DIAGNOSTIC_SUITES).hasSize(10);
        assertThat(M3V5AllocatorProtocolMain.DIAGNOSTIC_TESTS).hasSize(24);
        assertThat(diagnostic.source()).isEqualTo(source);
        assertThat(diagnostic.rawManifestDigest())
                .isEqualTo(M3V5DiagnosticRawGate.validate(raw, source.nereusCommit()));
        assertThat(diagnostic.scenarios())
                .containsExactlyInAnyOrderElementsOf(java.util.EnumSet.allOf(
                        AllocatorCampaignPromotionGateV5.DiagnosticScenario.class));
        assertThatThrownBy(() -> AllocatorCampaignPromotionGateV3.decodeDiagnostic(
                        CanonicalBytes.copyOf(Files.readAllBytes(output))))
                .isInstanceOf(IllegalArgumentException.class);

        Path twentyFiveMillis = raw.resolve("v5-range1024-25ms-formal-sequence.json");
        String validTwentyFiveMillis = Files.readString(twentyFiveMillis);
        Files.writeString(twentyFiveMillis, validTwentyFiveMillis.replaceFirst("\\\"dropped\\\":0", "\"dropped\":1"));
        assertThatThrownBy(() -> M3V5AllocatorProtocolMain.main(
                        arguments("validate-diagnostic", output, junit, source, raw)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hard gate failed");
        Files.writeString(twentyFiveMillis, validTwentyFiveMillis);

        Path firstNative = raw.resolve("native-baseline-row-00.json");
        String validFirstNative = Files.readString(firstNative);
        Files.writeString(firstNative, validFirstNative.replace("\"latencyMillis\":1", "\"latencyMillis\":5"));
        assertThatThrownBy(() -> M3V5AllocatorProtocolMain.main(
                        arguments("validate-diagnostic", output, junit, source, raw)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Native diagnostic row hard gate failed");
        Files.writeString(firstNative, validFirstNative);

        Path strict = raw.resolve("strict-formal-sequence.json");
        String validStrict = Files.readString(strict);
        Files.writeString(
                strict,
                validStrict.replaceFirst(
                        "\"measuredDroppedBeforeAdmission\":0",
                        "\"measuredDroppedBeforeAdmission\":1"));
        assertThatThrownBy(() -> M3V5AllocatorProtocolMain.main(
                        arguments("validate-diagnostic", output, junit, source, raw)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("legacy hard gate failed");
        Files.writeString(strict, validStrict);

        Path v5Runner = junit.resolve("TEST-" + M3V5AsyncActorLaneRunnerTest.class.getName() + ".xml");
        Files.writeString(v5Runner, Files.readString(v5Runner).replaceFirst("/>", "><failure/></testcase>"));
        assertThatThrownBy(() -> M3V5AllocatorProtocolMain.main(arguments(
                        "seal-diagnostic",
                        junit,
                        temporaryDirectory.resolve("forged.nadv5"),
                        source,
                        raw)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("summary");
    }

    private Path diagnosticRawDirectory(SourceBinding source) throws Exception {
        Path directory = Files.createDirectory(temporaryDirectory.resolve("diagnostic-raw"));
        Files.writeString(
                directory.resolve("allocator-workflow-diagnostic.json"),
                diagnostic("NEREUS_V2_M3_ALLOCATOR_WORKFLOW_DIAGNOSTIC_V3", ""));
        Files.writeString(
                directory.resolve("native-baseline-canary-summary.json"),
                diagnostic(
                        "NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_CANARY_V5",
                        ",\"nativeExecutionProfileSha256\":\""
                                + com.nereusstream.domain.registry.allocator.AllocatorNativeExecutionProfileV5
                                        .executionProfileDigest()
                                        .toHex()
                                + "\",\"workloadScheduleSha256\":\""
                                + com.nereusstream.domain.registry.allocator.AllocatorNativeExecutionProfileV5
                                        .scheduleDigest()
                                        .toHex()
                                + "\",\"hiddenDispatchQueue\":0,\"runnerOutstandingMaximum\":512"
                                + ",\"managedLedgerOperationOutstandingMaximum\":16,\"rowCount\":10"));
        for (int ordinal = 0; ordinal < 10; ordinal++) {
            int[] populations = {
                10_000, 10_000, 10_000, 10_000, 10_000, 10_000, 100_000, 100_000, 100_000, 100_000
            };
            int[] latencies = {1, 5, 10, 25, 1, 25, 1, 5, 10, 25};
            int rate = ordinal == 4 || ordinal == 5 ? 500 : 200;
            long offered = Math.multiplyExact((long) rate, 30L);
            Files.writeString(
                    directory.resolve("native-baseline-row-%02d.json".formatted(ordinal)),
                    diagnostic(
                            "NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_ROW_V5",
                            ",\"activePopulation\":" + populations[ordinal]
                                    + ",\"latencyMillis\":" + latencies[ordinal]
                                    + ",\"offeredRate\":" + rate + ",\"offered\":" + offered
                                    + ",\"admitted\":" + offered + ",\"dropped\":0,\"completed\":" + offered
                                    + ",\"failed\":0,\"timedOut\":0,\"warmupFailed\":0"
                                    + ",\"warmupTimedOut\":0,\"queueDepthAtEnd\":0"
                                    + ",\"globalOutstandingAtEnd\":0,\"bindingBusyAtEnd\":0"
                                    + ",\"pendingPermitAtEnd\":0,\"managedLedgerOperationOutstandingAtEnd\":0"
                                    + ",\"hiddenNativeQueueDepth\":0,\"actorLanesStopped\":true"));
        }
        Files.writeString(
                directory.resolve("range16-formal-sequence.json"),
                diagnostic(
                        "NEREUS_V2_M3_ALLOCATOR_RANGE16_FORMAL_SEQUENCE_DIAGNOSTIC_V1",
                        ",\"sourceCommit\":\"" + source.nereusCommit() + "\""
                                + ",\"fixed1000\":" + legacyLosslessRow(1_000)));
        Files.writeString(
                directory.resolve("real-oxia-operation-diagnostic.json"),
                diagnostic("NEREUS_V2_M3_ALLOCATOR_OPERATION_DIAGNOSTIC_V3", ""));
        Files.writeString(
                directory.resolve("runner-only-diagnostic.json"),
                diagnostic("NEREUS_V2_M3_ALLOCATOR_RUNNER_DIAGNOSTIC_V3", ""));
        Files.writeString(
                directory.resolve("strict-formal-sequence.json"),
                diagnostic(
                        "NEREUS_V2_M3_ALLOCATOR_STRICT_FORMAL_SEQUENCE_DIAGNOSTIC_V1",
                        ",\"sourceCommit\":\"" + source.nereusCommit() + "\""
                                + ",\"fixed1000\":" + legacyLosslessRow(1_000)
                                + ",\"derived800\":" + legacyLosslessRow(800)));
        for (int latencyMillis : new int[] {10, 25}) {
            Files.writeString(
                    directory.resolve("v5-range1024-" + latencyMillis + "ms-formal-sequence.json"),
                    "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_RANGE_LATENCY_DIAGNOSTIC_V5\""
                            + ",\"diagnosticOnly\":true,\"authority\":false,\"selectionEligible\":false"
                            + ",\"sourceCommit\":\"" + source.nereusCommit() + "\""
                            + ",\"latencyMillis\":" + latencyMillis
                            + ",\"candidate\":\"RANGE_1024\",\"activePopulation\":10000"
                            + ",\"realOperationOutstandingMaximum\":16"
                            + ",\"fixed1000\":{\"offeredRate\":1000,\"offered\":30000,\"admitted\":30000"
                            + ",\"dropped\":0,\"completed\":30000,\"failed\":0,\"timedOut\":0"
                            + lifecycle() + "}"
                            + ",\"derived800\":{\"offeredRate\":800,\"offered\":24000,\"admitted\":24000"
                            + ",\"dropped\":0,\"completed\":24000,\"failed\":0,\"timedOut\":0"
                            + lifecycle() + "}}\n");
        }
        Files.writeString(
                directory.resolve("v5-terminal-admission-drain-diagnostic.json"),
                "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_TERMINAL_ADMISSION_DRAIN_DIAGNOSTIC_V5\""
                        + ",\"diagnosticOnly\":true,\"authority\":false,\"selectionEligible\":false"
                        + ",\"sourceCommit\":\"" + source.nereusCommit() + "\""
                        + ",\"offerHorizonSeconds\":40,\"terminalAdmissionDrainSeconds\":2"
                        + ",\"cleanupGraceSeconds\":5,\"fixed1000\":{\"offered\":30000"
                        + ",\"admitted\":30000,\"dropped\":0,\"completed\":30000"
                        + ",\"failed\":0,\"timedOut\":0" + lifecycle() + "}"
                        + ",\"derived800\":{\"offered\":24000,\"admitted\":24000"
                        + ",\"dropped\":0,\"completed\":24000,\"failed\":0,\"timedOut\":0"
                        + lifecycle() + "}}\n");
        return directory;
    }

    private static String diagnostic(String schema, String fields) {
        return "{\"schema\":\"" + schema
                + "\",\"diagnosticOnly\":true,\"authority\":false,\"selectionEligible\":false"
                + fields + "}\n";
    }

    private static String lifecycle() {
        return ",\"queueDepthAtEnd\":0,\"globalOutstandingAtEnd\":0,\"bindingBusyAtEnd\":0"
                + ",\"pendingPermitAtEnd\":0,\"actorLanesStopped\":true";
    }

    private static String legacyLosslessRow(int rate) {
        long warmup = Math.multiplyExact((long) rate, 10L);
        long measured = Math.multiplyExact((long) rate, 30L);
        return "{\"warmupOffered\":" + warmup + ",\"warmupCompleted\":" + warmup
                + ",\"warmupLoadRejectedAfterAdmission\":0"
                + ",\"warmupUnexpectedFailedAfterAdmission\":0,\"warmupTimedOutAfterAdmission\":0"
                + ",\"measuredOffered\":" + measured + ",\"measuredAdmitted\":" + measured
                + ",\"measuredDroppedBeforeAdmission\":0,\"measuredCompleted\":" + measured
                + ",\"measuredFailedAfterAdmission\":0,\"measuredTimedOutAfterAdmission\":0"
                + ",\"globalOutstandingMaximum\":16,\"actorLanesStoppedAtCleanupDeadline\":true}";
    }

    private Path diagnosticJUnitDirectory() throws Exception {
        Path directory = Files.createDirectory(temporaryDirectory.resolve("diagnostic-junit"));
        Map<String, List<String>> testsBySuite = M3V5AllocatorProtocolMain.DIAGNOSTIC_TESTS.stream()
                .map(identity -> identity.split("#", 2))
                .collect(Collectors.groupingBy(
                        values -> values[0],
                        Collectors.mapping(values -> values[1], Collectors.toList())));
        for (String suite : M3V5AllocatorProtocolMain.DIAGNOSTIC_SUITES.stream().sorted().toList()) {
            List<String> tests = testsBySuite.getOrDefault(suite, List.of()).stream()
                    .sorted(Comparator.naturalOrder())
                    .toList();
            StringBuilder xml = new StringBuilder("<testsuite name=\"")
                    .append(suite)
                    .append("\" tests=\"")
                    .append(tests.size())
                    .append("\" failures=\"0\" errors=\"0\" skipped=\"0\">");
            tests.forEach(test -> xml.append("<testcase classname=\"")
                    .append(suite)
                    .append("\" name=\"")
                    .append(test)
                    .append("\"/>"));
            xml.append("</testsuite>");
            Files.writeString(directory.resolve("TEST-" + suite + ".xml"), xml);
        }
        return directory;
    }

    private static String[] arguments(
            String command, Path input, Path output, SourceBinding source, Path diagnosticRaw) {
        List<String> args = new ArrayList<>();
        args.add(command);
        args.add(input.toString());
        args.add(output.toString());
        args.add(source.nereusCommit());
        args.add(source.oxiaImageDigest().toHex());
        args.add(source.dependencyLockDigest().toHex());
        args.add(source.executorDigest().toHex());
        args.add(source.workloadDigest().toHex());
        args.add(diagnosticRaw.toString());
        return args.toArray(String[]::new);
    }

    private static SourceBinding source() {
        return new SourceBinding(
                "d".repeat(40),
                digest("oxia"),
                digest("dependency"),
                digest("executor"),
                AllocatorCampaignPlanProfileV5.zeroDecisionPlanDigest());
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }
}
