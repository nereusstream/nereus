/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Exact-source V4 promotion gate; valid non-selection evaluations remain non-failing and non-promotable. */
public final class AllocatorCampaignPromotionGateV4 {
    public static final String DIAGNOSTIC_SCHEMA = "NEREUS_V2_M3_ALLOCATOR_DIAGNOSTIC_V4";
    private static final byte[] DIAGNOSTIC_MAGIC = {'N', 'A', 'D', 'V', '4', 0, 0, 0};
    private static final int DIAGNOSTIC_VERSION = 4;
    private static final int DIAGNOSTIC_LENGTH = 8 + 2 + 1 + 1 + 40 + 32 * 7;

    private AllocatorCampaignPromotionGateV4() {}

    public static Decision evaluate(
            CanonicalBytes evaluationBytes,
            CanonicalBytes checkpointBytes,
            SourceBinding currentSource,
            Set<Sha256Digest> verifiedAttachmentDigests,
            DiagnosticAttestation diagnostic,
            Sha256Digest verifiedDiagnosticReceiptDigest,
            JUnitSummary junit) {
        Objects.requireNonNull(currentSource, "currentSource");
        Objects.requireNonNull(verifiedAttachmentDigests, "verifiedAttachmentDigests");
        Objects.requireNonNull(diagnostic, "diagnostic");
        Objects.requireNonNull(verifiedDiagnosticReceiptDigest, "verifiedDiagnosticReceiptDigest");
        Objects.requireNonNull(junit, "junit");
        AllocatorCampaignEvaluationSealV4.SealedEvaluation evaluation =
                AllocatorCampaignEvaluationSealV4.decode(evaluationBytes);
        AllocatorCampaignCheckpointV4 checkpoint = AllocatorCampaignCheckpointV4.decode(checkpointBytes);
        if (checkpoint.status() != AllocatorCampaignCheckpointV3.Status.COMPLETED) {
            return Decision.rejected(DecisionStatus.CHECKPOINT_LINK_INVALID);
        }
        CanonicalBytes expectedEvaluation = AllocatorCampaignEvaluationSealV4.seal(checkpointBytes);
        if (!evaluationBytes.equals(expectedEvaluation)
                || !evaluation.checkpointDigest().equals(AllocatorCampaignCheckpointV4.digest(checkpointBytes))
                || !evaluation.campaignId().equals(checkpoint.campaignId())
                || !evaluation
                        .attachmentRootDigest()
                        .equals(AllocatorCampaignEvaluationSealV4.attachmentRoot(checkpoint))) {
            return Decision.rejected(DecisionStatus.CHECKPOINT_LINK_INVALID);
        }
        if (!evaluation.source().equals(currentSource)
                || !checkpoint.source().equals(currentSource)
                || !diagnostic.source().equals(currentSource)) {
            return Decision.rejected(DecisionStatus.SOURCE_MISMATCH);
        }
        if (!evaluation.executionProfileDigest().equals(diagnostic.executionProfileDigest())
                || !evaluation.planDigest().equals(diagnostic.planDigest())) {
            return Decision.rejected(DecisionStatus.PROFILE_MISMATCH);
        }
        if (!checkpoint.attachmentDigests().equals(Set.copyOf(verifiedAttachmentDigests))) {
            return Decision.rejected(DecisionStatus.ATTACHMENT_MISMATCH);
        }
        if (!diagnostic.scenarios().equals(EnumSet.allOf(DiagnosticScenario.class))
                || diagnostic.receiptDigest().isZero()
                || !diagnostic.receiptDigest().equals(verifiedDiagnosticReceiptDigest)) {
            return Decision.rejected(DecisionStatus.DIAGNOSTIC_INCOMPLETE);
        }
        if (junit.tests() <= 0 || junit.failures() != 0 || junit.errors() != 0 || junit.skips() != 0) {
            return Decision.rejected(DecisionStatus.JUNIT_INVALID);
        }
        if (!evaluation.selectionEligible()) {
            return Decision.rejected(DecisionStatus.NON_PROMOTABLE_EVALUATION);
        }
        return new Decision(DecisionStatus.PROMOTABLE, evaluation.selectedCandidate());
    }

    public static CanonicalBytes encodeDiagnostic(DiagnosticAttestation diagnostic) {
        Objects.requireNonNull(diagnostic, "diagnostic");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(DIAGNOSTIC_LENGTH);
            DataOutputStream output = new DataOutputStream(bytes);
            output.write(DIAGNOSTIC_MAGIC);
            output.writeShort(DIAGNOSTIC_VERSION);
            int scenarioMask = 0;
            for (DiagnosticScenario scenario : diagnostic.scenarios()) {
                scenarioMask |= 1 << scenario.ordinal();
            }
            output.writeByte(scenarioMask);
            output.writeByte(0);
            output.write(diagnostic.source().nereusCommit().getBytes(StandardCharsets.US_ASCII));
            writeDigest(output, diagnostic.source().oxiaImageDigest());
            writeDigest(output, diagnostic.source().dependencyLockDigest());
            writeDigest(output, diagnostic.source().executorDigest());
            writeDigest(output, diagnostic.source().workloadDigest());
            writeDigest(output, diagnostic.executionProfileDigest());
            writeDigest(output, diagnostic.planDigest());
            writeDigest(output, diagnostic.receiptDigest());
            output.flush();
            if (bytes.size() != DIAGNOSTIC_LENGTH) {
                throw new IllegalStateException("allocator V4 diagnostic encoder length differs");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("allocator V4 diagnostic memory encoding failed", failure);
        }
    }

    public static DiagnosticAttestation decodeDiagnostic(CanonicalBytes encoded) {
        Objects.requireNonNull(encoded, "encoded");
        byte[] bytes = encoded.toByteArray();
        if (bytes.length != DIAGNOSTIC_LENGTH) {
            throw new IllegalArgumentException("allocator V4 diagnostic length differs from fixed NADV4");
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
            if (!Arrays.equals(input.readNBytes(DIAGNOSTIC_MAGIC.length), DIAGNOSTIC_MAGIC)
                    || input.readUnsignedShort() != DIAGNOSTIC_VERSION) {
                throw new IllegalArgumentException("allocator V4 diagnostic magic or version differs");
            }
            int scenarioMask = input.readUnsignedByte();
            int allowedMask = (1 << DiagnosticScenario.values().length) - 1;
            if ((scenarioMask & ~allowedMask) != 0 || input.readUnsignedByte() != 0) {
                throw new IllegalArgumentException("allocator V4 diagnostic mask or reserved byte differs");
            }
            EnumSet<DiagnosticScenario> scenarios = EnumSet.noneOf(DiagnosticScenario.class);
            for (DiagnosticScenario scenario : DiagnosticScenario.values()) {
                if ((scenarioMask & (1 << scenario.ordinal())) != 0) {
                    scenarios.add(scenario);
                }
            }
            byte[] commit = input.readNBytes(40);
            if (commit.length != 40) {
                throw new EOFException("allocator V4 diagnostic commit is truncated");
            }
            DiagnosticAttestation diagnostic = new DiagnosticAttestation(
                    new SourceBinding(
                            new String(commit, StandardCharsets.US_ASCII),
                            readDigest(input),
                            readDigest(input),
                            readDigest(input),
                            readDigest(input)),
                    readDigest(input),
                    readDigest(input),
                    scenarios,
                    readDigest(input));
            if (input.available() != 0
                    || !Arrays.equals(bytes, encodeDiagnostic(diagnostic).toByteArray())) {
                throw new IllegalArgumentException("allocator V4 diagnostic is not canonical NADV4");
            }
            return diagnostic;
        } catch (EOFException failure) {
            throw new IllegalArgumentException("allocator V4 diagnostic is truncated", failure);
        } catch (IOException failure) {
            throw new IllegalArgumentException("allocator V4 diagnostic cannot be decoded", failure);
        }
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest digest) throws IOException {
        output.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] bytes = input.readNBytes(Sha256Digest.LENGTH);
        if (bytes.length != Sha256Digest.LENGTH) {
            throw new EOFException("allocator V4 diagnostic digest is truncated");
        }
        return Sha256Digest.copyOf(bytes);
    }

    public enum DiagnosticScenario {
        STRICT,
        INSTALLED_RANGE,
        RANGE_RENEWAL,
        CONFLICT_STORM,
        NATIVE_BASELINE,
        TERMINAL_ADMISSION_DRAIN
    }

    public enum DecisionStatus {
        PROMOTABLE,
        NON_PROMOTABLE_EVALUATION,
        CHECKPOINT_LINK_INVALID,
        SOURCE_MISMATCH,
        PROFILE_MISMATCH,
        ATTACHMENT_MISMATCH,
        DIAGNOSTIC_INCOMPLETE,
        JUNIT_INVALID
    }

    public record DiagnosticAttestation(
            SourceBinding source,
            Sha256Digest executionProfileDigest,
            Sha256Digest planDigest,
            Set<DiagnosticScenario> scenarios,
            Sha256Digest receiptDigest) {
        public DiagnosticAttestation {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(executionProfileDigest, "executionProfileDigest");
            Objects.requireNonNull(planDigest, "planDigest");
            scenarios = Set.copyOf(Objects.requireNonNull(scenarios, "scenarios"));
            Objects.requireNonNull(receiptDigest, "receiptDigest");
            if (!executionProfileDigest.equals(AllocatorNativeExecutionProfileV4.executionProfileDigest())
                    || !planDigest.equals(AllocatorCampaignPlanProfileV4.zeroDecisionPlanDigest())
                    || !source.workloadDigest().equals(planDigest)) {
                throw new IllegalArgumentException("allocator V4 diagnostic profile or plan identity differs");
            }
        }
    }

    public record JUnitSummary(long tests, long failures, long errors, long skips) {
        public JUnitSummary {
            if (tests < 0 || failures < 0 || errors < 0 || skips < 0) {
                throw new IllegalArgumentException("allocator V4 JUnit summary cannot be negative");
            }
        }
    }

    public record Decision(DecisionStatus status, Optional<Candidate> selectedCandidate) {
        public Decision {
            Objects.requireNonNull(status, "status");
            selectedCandidate = Objects.requireNonNull(selectedCandidate, "selectedCandidate");
            if ((status == DecisionStatus.PROMOTABLE) != selectedCandidate.isPresent()) {
                throw new IllegalArgumentException("allocator V4 promotion decision selection differs");
            }
        }

        private static Decision rejected(DecisionStatus status) {
            return new Decision(status, Optional.empty());
        }
    }
}
