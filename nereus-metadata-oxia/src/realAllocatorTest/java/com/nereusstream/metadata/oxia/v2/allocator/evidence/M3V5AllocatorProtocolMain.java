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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV5;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignEvaluationSealV5;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPlanProfileV5;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV5;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV5.Decision;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV5.DecisionStatus;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV5.DiagnosticAttestation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV5.DiagnosticScenario;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV5.JUnitSummary;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignSelectionV5;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Offline NACP5/NAEV5/NADV5/NARS5 validation and sealing CLI. It never accesses Oxia. */
public final class M3V5AllocatorProtocolMain {
    private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-f]{64}");
    private static final String PACKAGE = "com.nereusstream.metadata.oxia.v2.allocator.evidence.";
    static final Set<String> DIAGNOSTIC_SUITES = union(
            M3V3AllocatorProtocolMain.DIAGNOSTIC_SUITES,
            Set.of(
                    PACKAGE + "M3V4AsyncActorLaneRunnerTest",
                    PACKAGE + "M3V5AsyncActorLaneRunnerTest",
                    PACKAGE + "M3V5TerminalAdmissionDrainDiagnosticTest",
                    PACKAGE + "M3V5RangeLatencyDiagnosticTest"));
    static final Set<String> DIAGNOSTIC_TESTS = union(
            M3V3AllocatorProtocolMain.DIAGNOSTIC_TESTS,
            Set.of(
                    identity("M3V4AsyncActorLaneRunnerTest", "admitsOnlyAlreadyOfferedWorkDuringTheTerminalDrain()"),
                    identity(
                            "M3V4AsyncActorLaneRunnerTest",
                            "dropsAnOnTimeRequestStillBlockedAtTheFinalAdmissionDeadline()"),
                    identity(
                            "M3V5AsyncActorLaneRunnerTest",
                            "reachesTheBoundedStormAdmissionCapWithoutChangingPerBindingSingleFlight()"),
                    identity(
                            "M3V5TerminalAdmissionDrainDiagnosticTest",
                            "exactRange16FixedAndDerivedRowsDrainEveryOnTimeOfferWithoutLoss()"),
                    identity(
                            "M3V5RangeLatencyDiagnosticTest",
                            "exactRange1024TenMillisSequenceAttributesOperationAndSchedulerCapacity()"),
                    identity(
                            "M3V5RangeLatencyDiagnosticTest",
                            "exactRange1024TwentyFiveMillisSequenceAttributesOperationAndSchedulerCapacity()")));

    private M3V5AllocatorProtocolMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("allocator V5 protocol command is required");
        }
        switch (args[0]) {
            case "validate-checkpoint" -> validateCheckpoint(args);
            case "seal-evaluation" -> sealEvaluation(args);
            case "seal-diagnostic" -> sealDiagnostic(args);
            case "validate-diagnostic" -> validateDiagnostic(args);
            case "promotion-check" -> promotionCheck(args);
            case "seal-selection" -> sealSelection(args);
            default -> throw new IllegalArgumentException("unknown allocator V5 protocol command: " + args[0]);
        }
    }

    private static void validateCheckpoint(String[] args) throws IOException {
        requireLength(args, 7);
        CanonicalBytes encoded = readBounded(Path.of(args[1]), AllocatorCampaignCheckpointV5.MAX_ENCODED_BYTES);
        AllocatorCampaignCheckpointV5 checkpoint = AllocatorCampaignCheckpointV5.decode(encoded);
        requireSource(checkpoint.source(), source(args, 2));
        System.out.printf(
                "allocator V5 checkpoint valid: status=%s sequence=%d executed=%d dispositions=%d campaign=%s%n",
                checkpoint.status(),
                checkpoint.checkpointSequence(),
                checkpoint.executionRecords().size(),
                checkpoint.dispositions().size(),
                checkpoint.campaignId().toHex());
    }

    private static void sealEvaluation(String[] args) throws IOException {
        requireLength(args, 8);
        CanonicalBytes checkpointBytes =
                readBounded(Path.of(args[1]), AllocatorCampaignCheckpointV5.MAX_ENCODED_BYTES);
        AllocatorCampaignCheckpointV5 checkpoint = AllocatorCampaignCheckpointV5.decode(checkpointBytes);
        requireSource(checkpoint.source(), source(args, 3));
        CanonicalBytes evaluation = AllocatorCampaignEvaluationSealV5.seal(checkpointBytes);
        M3V3AllocatorProtocolMain.writeCreateNew(Path.of(args[2]), evaluation.toByteArray());
        var decoded = AllocatorCampaignEvaluationSealV5.decode(evaluation);
        System.out.printf(
                "allocator V5 evaluation sealed: status=%s selectionEligible=%s checkpoint=%s%n",
                decoded.status(), decoded.selectionEligible(), decoded.checkpointDigest().toHex());
    }

    private static void sealDiagnostic(String[] args) throws Exception {
        requireLength(args, 9);
        SourceBinding source = source(args, 3);
        if (!source.workloadDigest().equals(AllocatorCampaignPlanProfileV5.zeroDecisionPlanDigest())) {
            throw new IllegalArgumentException(
                    "allocator V5 diagnostic workload identity differs from the formal plan");
        }
        M3V3AllocatorProtocolMain.DiagnosticSuite junit = readDiagnosticSuite(Path.of(args[1]));
        requireExactDiagnosticJUnit(junit);
        Sha256Digest rawManifest = M3V5DiagnosticRawGate.validate(Path.of(args[8]), source.nereusCommit());
        DiagnosticAttestation diagnostic = new DiagnosticAttestation(
                source,
                com.nereusstream.domain.registry.allocator.AllocatorNativeExecutionProfileV5
                        .executionProfileDigest(),
                AllocatorCampaignPlanProfileV5.zeroDecisionPlanDigest(),
                EnumSet.allOf(DiagnosticScenario.class),
                junit.manifestDigest(),
                rawManifest);
        CanonicalBytes encoded = AllocatorCampaignPromotionGateV5.encodeDiagnostic(diagnostic);
        AllocatorCampaignPromotionGateV5.decodeDiagnostic(encoded);
        M3V3AllocatorProtocolMain.writeCreateNew(Path.of(args[2]), encoded.toByteArray());
        System.out.printf(
                "allocator V5 diagnostic sealed: tests=%d failures=0 errors=0 skips=0 junitSha256=%s rawSha256=%s%n",
                junit.summary().tests(), diagnostic.receiptDigest().toHex(), rawManifest.toHex());
    }

    private static void validateDiagnostic(String[] args) throws Exception {
        requireLength(args, 9);
        CanonicalBytes encoded = readBounded(Path.of(args[1]), 4_096);
        DiagnosticAttestation diagnostic = AllocatorCampaignPromotionGateV5.decodeDiagnostic(encoded);
        M3V3AllocatorProtocolMain.DiagnosticSuite junit = readDiagnosticSuite(Path.of(args[2]));
        requireExactDiagnosticJUnit(junit);
        requireSource(diagnostic.source(), source(args, 3));
        Sha256Digest rawManifest =
                M3V5DiagnosticRawGate.validate(Path.of(args[8]), diagnostic.source().nereusCommit());
        if (!diagnostic.scenarios().equals(EnumSet.allOf(DiagnosticScenario.class))
                || !diagnostic.receiptDigest().equals(junit.manifestDigest())
                || !diagnostic.rawManifestDigest().equals(rawManifest)) {
            throw new IllegalArgumentException("allocator V5 diagnostic attestation differs from its JUnit suite");
        }
        System.out.printf(
                "allocator V5 diagnostic canonical: tests=%d failures=0 errors=0 skips=0 junitSha256=%s rawSha256=%s%n",
                junit.summary().tests(), diagnostic.receiptDigest().toHex(), rawManifest.toHex());
    }

    private static void promotionCheck(String[] args) throws Exception {
        requireLength(args, 14);
        CanonicalBytes evaluation = readBounded(Path.of(args[1]), 4_096);
        CanonicalBytes checkpoint = readBounded(Path.of(args[2]), AllocatorCampaignCheckpointV5.MAX_ENCODED_BYTES);
        CanonicalBytes diagnosticBytes = readBounded(Path.of(args[3]), 4_096);
        DiagnosticAttestation diagnostic = AllocatorCampaignPromotionGateV5.decodeDiagnostic(diagnosticBytes);
        M3V3AllocatorProtocolMain.DiagnosticSuite diagnosticJUnit = readDiagnosticSuite(Path.of(args[4]));
        requireExactDiagnosticJUnit(diagnosticJUnit);
        Sha256Digest rawManifest =
                M3V5DiagnosticRawGate.validate(Path.of(args[13]), diagnostic.source().nereusCommit());
        byte[] formalJUnitBytes = M3V3AllocatorProtocolMain.readRegular(Path.of(args[5]), 16 * 1024 * 1024);
        M3V3AllocatorProtocolMain.ParsedJUnit formalJUnit =
                M3V3AllocatorProtocolMain.parseJUnit(formalJUnitBytes);
        AllocatorCampaignCheckpointV5 decodedCheckpoint = AllocatorCampaignCheckpointV5.decode(checkpoint);
        Set<Sha256Digest> attachments = M3V3AllocatorProtocolMain.attachmentDigests(
                decodedCheckpoint.executionRecords(), Path.of(args[6]), "V5");
        Decision decision = AllocatorCampaignPromotionGateV5.evaluate(
                evaluation,
                checkpoint,
                source(args, 8),
                attachments,
                diagnostic,
                diagnosticJUnit.manifestDigest(),
                rawManifest,
                junitSummary(formalJUnit));
        if (decision.status() != DecisionStatus.PROMOTABLE
                && decision.status() != DecisionStatus.NON_PROMOTABLE_EVALUATION) {
            throw new IllegalStateException("allocator V5 promotion integrity gate rejected: " + decision.status());
        }
        String selected = decision.selectedCandidate().map(Enum::name).orElse("NONE");
        String json = "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_PROMOTION_DECISION_V5\","
                + "\"status\":\"" + decision.status()
                + "\",\"selectedCandidate\":\"" + selected
                + "\",\"checkpointSha256\":\"" + AllocatorCampaignCheckpointV5.digest(checkpoint).toHex()
                + "\",\"evaluationSha256\":\"" + AllocatorCampaignCheckpointV5.digest(evaluation).toHex()
                + "\",\"diagnosticSha256\":\"" + AllocatorCampaignCheckpointV5.digest(diagnosticBytes).toHex()
                + "\",\"diagnosticJUnitSha256\":\"" + diagnosticJUnit.manifestDigest().toHex()
                + "\",\"diagnosticRawManifestSha256\":\"" + rawManifest.toHex()
                + "\",\"formalJUnitSha256\":\""
                + Sha256Digest.hash(CanonicalBytes.copyOf(formalJUnitBytes)).toHex() + "\"}\n";
        M3V3AllocatorProtocolMain.writeCreateNew(Path.of(args[7]), json.getBytes(StandardCharsets.UTF_8));
        System.out.printf(
                "allocator V5 promotion checked: status=%s selectedCandidate=%s%n",
                decision.status(), selected);
    }

    private static void sealSelection(String[] args) throws Exception {
        requireLength(args, 14);
        CanonicalBytes evaluation = readBounded(Path.of(args[1]), 4_096);
        CanonicalBytes checkpoint = readBounded(Path.of(args[2]), AllocatorCampaignCheckpointV5.MAX_ENCODED_BYTES);
        CanonicalBytes diagnosticBytes = readBounded(Path.of(args[3]), 4_096);
        DiagnosticAttestation diagnostic = AllocatorCampaignPromotionGateV5.decodeDiagnostic(diagnosticBytes);
        M3V3AllocatorProtocolMain.DiagnosticSuite diagnosticJUnit = readDiagnosticSuite(Path.of(args[4]));
        requireExactDiagnosticJUnit(diagnosticJUnit);
        Sha256Digest rawManifest =
                M3V5DiagnosticRawGate.validate(Path.of(args[13]), diagnostic.source().nereusCommit());
        M3V3AllocatorProtocolMain.ParsedJUnit formalJUnit = M3V3AllocatorProtocolMain.parseJUnit(
                M3V3AllocatorProtocolMain.readRegular(Path.of(args[5]), 16 * 1024 * 1024));
        AllocatorCampaignCheckpointV5 decodedCheckpoint = AllocatorCampaignCheckpointV5.decode(checkpoint);
        CanonicalBytes selection = AllocatorCampaignSelectionV5.seal(
                evaluation,
                checkpoint,
                source(args, 8),
                M3V3AllocatorProtocolMain.attachmentDigests(
                        decodedCheckpoint.executionRecords(), Path.of(args[6]), "V5"),
                diagnostic,
                diagnosticJUnit.manifestDigest(),
                rawManifest,
                junitSummary(formalJUnit));
        AllocatorCampaignSelectionV5.decode(selection);
        M3V3AllocatorProtocolMain.writeCreateNew(Path.of(args[7]), selection.toByteArray());
        System.out.printf(
                "allocator V5 selection sealed: candidate=%s selectionSha256=%s%n",
                AllocatorCampaignSelectionV5.decode(selection).selectedCandidate(),
                Sha256Digest.hash(selection).toHex());
    }

    private static M3V3AllocatorProtocolMain.DiagnosticSuite readDiagnosticSuite(Path directory)
            throws Exception {
        return M3V3AllocatorProtocolMain.readDiagnosticSuite(directory, DIAGNOSTIC_SUITES, "V5");
    }

    private static JUnitSummary junitSummary(M3V3AllocatorProtocolMain.ParsedJUnit junit) {
        return new JUnitSummary(
                junit.summary().tests(),
                junit.summary().failures(),
                junit.summary().errors(),
                junit.summary().skips());
    }

    private static void requireExactDiagnosticJUnit(M3V3AllocatorProtocolMain.DiagnosticSuite junit) {
        if (junit.summary().failures() != 0
                || junit.summary().errors() != 0
                || junit.summary().skips() != 0
                || junit.summary().tests() != DIAGNOSTIC_TESTS.size()
                || !junit.suiteNames().equals(DIAGNOSTIC_SUITES)
                || !junit.testcaseIdentities().equals(DIAGNOSTIC_TESTS)) {
            throw new IllegalArgumentException("allocator V5 diagnostic JUnit inventory or result differs");
        }
    }

    private static SourceBinding source(String[] args, int offset) {
        return new SourceBinding(
                args[offset],
                digest(args[offset + 1]),
                digest(args[offset + 2]),
                digest(args[offset + 3]),
                digest(args[offset + 4]));
    }

    private static Sha256Digest digest(String hex) {
        if (!SHA256_HEX.matcher(hex).matches()) {
            throw new IllegalArgumentException("allocator V5 source digest is not lowercase SHA-256");
        }
        return Sha256Digest.copyOf(HexFormat.of().parseHex(hex));
    }

    private static void requireSource(SourceBinding actual, SourceBinding expected) {
        if (!actual.equals(expected)) {
            throw new IllegalArgumentException("allocator V5 exact source/executor binding differs");
        }
    }

    private static CanonicalBytes readBounded(Path path, int maximum) throws IOException {
        return CanonicalBytes.copyOf(M3V3AllocatorProtocolMain.readRegular(path, maximum));
    }

    private static void requireLength(String[] args, int expected) {
        if (args.length != expected) {
            throw new IllegalArgumentException(
                    "allocator V5 protocol argument count differs for " + args[0]);
        }
    }

    private static String identity(String className, String testName) {
        return PACKAGE + className + '#' + testName;
    }

    private static <T> Set<T> union(Set<T> left, Set<T> right) {
        return Stream.concat(left.stream(), right.stream())
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }
}
