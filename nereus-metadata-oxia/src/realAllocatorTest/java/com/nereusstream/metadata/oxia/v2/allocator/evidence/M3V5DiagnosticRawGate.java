/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorNativeExecutionProfileV5;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Canonical manifest and independent fail-closed checks over the complete V5 diagnostic raw inventory. */
final class M3V5DiagnosticRawGate {
    private static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_RANGE_LATENCY_DIAGNOSTIC_V5";
    private static final Pattern SOURCE = Pattern.compile("\\\"sourceCommit\\\":\\\"([0-9a-f]{40})\\\"");
    private static final Pattern LATENCY = Pattern.compile("\\\"latencyMillis\\\":([0-9]+)");
    private static final int[] NATIVE_POPULATIONS = {
        10_000, 10_000, 10_000, 10_000, 10_000, 10_000, 100_000, 100_000, 100_000, 100_000
    };
    private static final int[] NATIVE_LATENCIES = {1, 5, 10, 25, 1, 25, 1, 5, 10, 25};
    private static final int[] NATIVE_RATES = {200, 200, 200, 200, 500, 500, 200, 200, 200, 200};
    private static final List<String> EXPECTED_FILES = List.of(
            "allocator-workflow-diagnostic.json",
            "native-baseline-canary-summary.json",
            "native-baseline-row-00.json",
            "native-baseline-row-01.json",
            "native-baseline-row-02.json",
            "native-baseline-row-03.json",
            "native-baseline-row-04.json",
            "native-baseline-row-05.json",
            "native-baseline-row-06.json",
            "native-baseline-row-07.json",
            "native-baseline-row-08.json",
            "native-baseline-row-09.json",
            "range16-formal-sequence.json",
            "real-oxia-operation-diagnostic.json",
            "runner-only-diagnostic.json",
            "strict-formal-sequence.json",
            "v5-range1024-10ms-formal-sequence.json",
            "v5-range1024-25ms-formal-sequence.json",
            "v5-terminal-admission-drain-diagnostic.json");

    private M3V5DiagnosticRawGate() {}

    static Sha256Digest validate(Path directory, String sourceCommit) throws Exception {
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(directory)) {
            throw new IllegalArgumentException("allocator V5 diagnostic raw directory is absent or a link");
        }
        Set<String> actual = new TreeSet<>();
        try (var entries = Files.list(directory)) {
            entries.map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(".json"))
                    .forEach(actual::add);
        }
        if (!actual.equals(Set.copyOf(EXPECTED_FILES))) {
            throw new IllegalArgumentException("allocator V5 diagnostic raw JSON inventory differs");
        }
        StringBuilder manifest = new StringBuilder("NEREUS_V2_M3_ALLOCATOR_DIAGNOSTIC_RAW_MANIFEST_V5\n");
        for (String fileName : EXPECTED_FILES) {
            Path receipt = directory.resolve(fileName);
            byte[] bytes = M3V3AllocatorProtocolMain.readRegular(receipt, 16 * 1024 * 1024);
            String json = new String(bytes, StandardCharsets.UTF_8);
            requireLiteral(json, "\"diagnosticOnly\":true");
            requireLiteral(json, "\"authority\":false");
            requireLiteral(json, "\"selectionEligible\":false");
            validateReceipt(fileName, json, sourceCommit);
            manifest.append(fileName)
                    .append('\0')
                    .append(bytes.length)
                    .append('\0')
                    .append(Sha256Digest.hash(CanonicalBytes.copyOf(bytes)).toHex())
                    .append('\n');
        }
        return Sha256Digest.hash(CanonicalBytes.copyOf(manifest.toString().getBytes(StandardCharsets.UTF_8)));
    }

    private static void validateReceipt(String fileName, String json, String sourceCommit) {
        switch (fileName) {
            case "allocator-workflow-diagnostic.json" ->
                requireSchema(json, "NEREUS_V2_M3_ALLOCATOR_WORKFLOW_DIAGNOSTIC_V3");
            case "native-baseline-canary-summary.json" -> requireNativeSummary(json);
            case "range16-formal-sequence.json" -> {
                requireSchema(json, "NEREUS_V2_M3_ALLOCATOR_RANGE16_FORMAL_SEQUENCE_DIAGNOSTIC_V1");
                requireSource(json, sourceCommit);
                requireLegacyLosslessRow(json, "fixed1000", 1_000);
            }
            case "real-oxia-operation-diagnostic.json" ->
                requireSchema(json, "NEREUS_V2_M3_ALLOCATOR_OPERATION_DIAGNOSTIC_V3");
            case "runner-only-diagnostic.json" ->
                requireSchema(json, "NEREUS_V2_M3_ALLOCATOR_RUNNER_DIAGNOSTIC_V3");
            case "strict-formal-sequence.json" -> {
                requireSchema(json, "NEREUS_V2_M3_ALLOCATOR_STRICT_FORMAL_SEQUENCE_DIAGNOSTIC_V1");
                requireSource(json, sourceCommit);
                requireLegacyLosslessRow(json, "fixed1000", 1_000);
                requireLegacyLosslessRow(json, "derived800", 800);
            }
            case "v5-range1024-10ms-formal-sequence.json" ->
                requireRangeReceipt(json, sourceCommit, 10);
            case "v5-range1024-25ms-formal-sequence.json" ->
                requireRangeReceipt(json, sourceCommit, 25);
            case "v5-terminal-admission-drain-diagnostic.json" -> {
                requireSchema(json, "NEREUS_V2_M3_ALLOCATOR_TERMINAL_ADMISSION_DRAIN_DIAGNOSTIC_V5");
                requireSource(json, sourceCommit);
                requireLiteral(json, "\"offerHorizonSeconds\":40");
                requireLiteral(json, "\"terminalAdmissionDrainSeconds\":2");
                requireLiteral(json, "\"cleanupGraceSeconds\":5");
                requireLosslessRow(json, "fixed1000", 1_000, false);
                requireLosslessRow(json, "derived800", 800, false);
            }
            default -> {
                if (!fileName.matches("native-baseline-row-[0-9]{2}\\.json")) {
                    throw new IllegalArgumentException("allocator V5 diagnostic raw receipt is unexpected");
                }
                requireNativeRow(fileName, json);
            }
        }
    }

    private static void requireRangeReceipt(String json, String sourceCommit, int latencyMillis) {
        requireSchema(json, SCHEMA);
        requireSource(json, sourceCommit);
        requireLiteral(json, "\"candidate\":\"RANGE_1024\"");
        requireLiteral(json, "\"activePopulation\":10000");
        if (Integer.parseInt(match(LATENCY, json, "latency").group(1)) != latencyMillis) {
            throw new IllegalArgumentException("allocator V5 diagnostic raw latency differs");
        }
        if (longField(json, "realOperationOutstandingMaximum") <= 4) {
            throw new IllegalArgumentException("allocator V5 diagnostic raw operation concurrency proof differs");
        }
        requireLosslessRow(json, "fixed1000", 1_000, true);
        requireLosslessRow(json, "derived800", 800, true);
    }

    private static void requireNativeSummary(String json) {
        requireSchema(json, "NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_CANARY_V5");
        requireLiteral(json, "\"nativeExecutionProfileSha256\":\""
                + AllocatorNativeExecutionProfileV5.executionProfileDigest().toHex() + "\"");
        requireLiteral(json, "\"workloadScheduleSha256\":\""
                + AllocatorNativeExecutionProfileV5.scheduleDigest().toHex() + "\"");
        requireLiteral(json, "\"hiddenDispatchQueue\":0");
        requireLiteral(json, "\"rowCount\":10");
        if (longField(json, "runnerOutstandingMaximum") <= 4
                || longField(json, "managedLedgerOperationOutstandingMaximum") <= 4) {
            throw new IllegalArgumentException("allocator V5 Native diagnostic concurrency proof differs");
        }
    }

    private static void requireNativeRow(String fileName, String json) {
        requireSchema(json, "NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_ROW_V5");
        int ordinal = Integer.parseInt(fileName.substring("native-baseline-row-".length(), 22));
        long population = longField(json, "activePopulation");
        long latency = longField(json, "latencyMillis");
        long rate = longField(json, "offeredRate");
        long offered = longField(json, "offered");
        if (population != NATIVE_POPULATIONS[ordinal]
                || latency != NATIVE_LATENCIES[ordinal]
                || rate != NATIVE_RATES[ordinal]
                || offered != Math.multiplyExact(rate, 30L)
                || longField(json, "admitted") != offered
                || longField(json, "completed") != offered
                || longField(json, "dropped") != 0
                || longField(json, "failed") != 0
                || longField(json, "timedOut") != 0
                || longField(json, "warmupFailed") != 0
                || longField(json, "warmupTimedOut") != 0
                || longField(json, "queueDepthAtEnd") != 0
                || longField(json, "globalOutstandingAtEnd") != 0
                || longField(json, "bindingBusyAtEnd") != 0
                || longField(json, "pendingPermitAtEnd") != 0
                || longField(json, "managedLedgerOperationOutstandingAtEnd") != 0
                || longField(json, "hiddenNativeQueueDepth") != 0
                || !json.contains("\"actorLanesStopped\":true")) {
            throw new IllegalArgumentException("allocator V5 Native diagnostic row hard gate failed");
        }
    }

    private static void requireLosslessRow(
            String json, String name, int offeredRate, boolean includesOfferedRate) {
        Pattern row = Pattern.compile("\\\"" + name + "\\\":\\{"
                + (includesOfferedRate ? "\\\"offeredRate\\\":" + offeredRate + ',' : "")
                + "\\\"offered\\\":([0-9]+)"
                + ",\\\"admitted\\\":([0-9]+)"
                + ",\\\"dropped\\\":([0-9]+)"
                + ",\\\"completed\\\":([0-9]+)"
                + ",\\\"failed\\\":([0-9]+)"
                + ",\\\"timedOut\\\":([0-9]+)");
        Matcher matcher = match(row, json, name);
        long offered = Long.parseLong(matcher.group(1));
        long admitted = Long.parseLong(matcher.group(2));
        long dropped = Long.parseLong(matcher.group(3));
        long completed = Long.parseLong(matcher.group(4));
        long failed = Long.parseLong(matcher.group(5));
        long timedOut = Long.parseLong(matcher.group(6));
        if (offered != Math.multiplyExact((long) offeredRate, 30L)
                || admitted != offered
                || completed != offered
                || dropped != 0
                || failed != 0
                || timedOut != 0) {
            throw new IllegalArgumentException("allocator V5 diagnostic raw " + name + " hard gate failed");
        }
        String body = match(Pattern.compile("\\\"" + name + "\\\":\\{([^}]*)}"), json, name + " lifecycle")
                .group(1);
        for (String terminal : List.of(
                "\"queueDepthAtEnd\":0",
                "\"globalOutstandingAtEnd\":0",
                "\"bindingBusyAtEnd\":0",
                "\"pendingPermitAtEnd\":0",
                "\"actorLanesStopped\":true")) {
            requireLiteral(body, terminal);
        }
    }

    private static void requireLegacyLosslessRow(String json, String name, int offeredRate) {
        String body = match(Pattern.compile("\\\"" + name + "\\\":\\{([^}]*)}"), json, name + " legacy row")
                .group(1);
        long warmupOffered = longField(body, "warmupOffered");
        long warmupCompleted = longField(body, "warmupCompleted");
        long warmupLoadRejected = longField(body, "warmupLoadRejectedAfterAdmission");
        long warmupUnexpectedFailed = longField(body, "warmupUnexpectedFailedAfterAdmission");
        long warmupTimedOut = longField(body, "warmupTimedOutAfterAdmission");
        long measuredOffered = longField(body, "measuredOffered");
        if (warmupOffered != Math.multiplyExact((long) offeredRate, 10L)
                || warmupOffered
                        != warmupCompleted + warmupLoadRejected + warmupUnexpectedFailed + warmupTimedOut
                || warmupUnexpectedFailed != 0
                || warmupTimedOut != 0
                || measuredOffered != Math.multiplyExact((long) offeredRate, 30L)
                || longField(body, "measuredAdmitted") != measuredOffered
                || longField(body, "measuredDroppedBeforeAdmission") != 0
                || longField(body, "measuredCompleted") != measuredOffered
                || longField(body, "measuredFailedAfterAdmission") != 0
                || longField(body, "measuredTimedOutAfterAdmission") != 0
                || longField(body, "globalOutstandingMaximum") <= 4
                || !body.contains("\"actorLanesStoppedAtCleanupDeadline\":true")) {
            throw new IllegalArgumentException("allocator V5 diagnostic raw " + name + " legacy hard gate failed");
        }
    }

    private static void requireSchema(String json, String schema) {
        requireLiteral(json, "\"schema\":\"" + schema + "\"");
    }

    private static void requireSource(String json, String sourceCommit) {
        if (!match(SOURCE, json, "source commit").group(1).equals(sourceCommit)) {
            throw new IllegalArgumentException("allocator V5 diagnostic raw source commit differs");
        }
    }

    private static long longField(String json, String field) {
        return Long.parseLong(match(
                        Pattern.compile("\\\"" + field + "\\\":([0-9]+)"), json, field)
                .group(1));
    }

    private static void requireLiteral(String json, String literal) {
        if (!json.contains(literal)) {
            throw new IllegalArgumentException("allocator V5 diagnostic raw identity differs: " + literal);
        }
    }

    private static Matcher match(Pattern pattern, String json, String label) {
        Matcher matcher = pattern.matcher(json);
        if (!matcher.find()) {
            throw new IllegalArgumentException("allocator V5 diagnostic raw " + label + " is absent");
        }
        return matcher;
    }
}
