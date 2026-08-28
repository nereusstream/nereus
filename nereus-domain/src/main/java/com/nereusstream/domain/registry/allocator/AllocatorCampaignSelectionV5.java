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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV5.Decision;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV5.DecisionStatus;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV5.DiagnosticAttestation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV5.JUnitSummary;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Strict canonical NARS5 created only from a successful V5 promotion decision. */
public final class AllocatorCampaignSelectionV5 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_SELECTION_V5";
    private static final byte[] MAGIC = {'N', 'A', 'R', 'S', '5', 0, 0, 0};
    private static final int WIRE_VERSION = 5;
    private static final int FIXED_LENGTH = 8 + 2 + 1 + 1 + 40 + 32 * 14;

    private AllocatorCampaignSelectionV5() {}

    public static CanonicalBytes seal(
            CanonicalBytes evaluationBytes,
            CanonicalBytes checkpointBytes,
            SourceBinding currentSource,
            Set<Sha256Digest> verifiedAttachmentDigests,
            DiagnosticAttestation diagnostic,
            Sha256Digest verifiedDiagnosticReceiptDigest,
            Sha256Digest verifiedRawManifestDigest,
            JUnitSummary junit) {
        Decision decision = AllocatorCampaignPromotionGateV5.evaluate(
                evaluationBytes,
                checkpointBytes,
                currentSource,
                verifiedAttachmentDigests,
                diagnostic,
                verifiedDiagnosticReceiptDigest,
                verifiedRawManifestDigest,
                junit);
        if (decision.status() != DecisionStatus.PROMOTABLE) {
            throw invalid("allocator V5 selection cannot seal a non-promotable decision");
        }
        AllocatorCampaignEvaluationSealV5.SealedEvaluation evaluation =
                AllocatorCampaignEvaluationSealV5.decode(evaluationBytes);
        return encode(new Selection(
                currentSource,
                evaluation.executionProfileDigest(),
                evaluation.planDigest(),
                decision.selectedCandidate().orElseThrow(),
                evaluation.campaignId(),
                evaluation.checkpointDigest(),
                Sha256Digest.hash(evaluationBytes),
                evaluation.attachmentRootDigest(),
                Sha256Digest.hash(AllocatorCampaignPromotionGateV5.encodeDiagnostic(diagnostic)),
                verifiedDiagnosticReceiptDigest,
                verifiedRawManifestDigest,
                junitDigest(junit)));
    }

    public static CanonicalBytes encode(Selection selection) {
        Objects.requireNonNull(selection, "selection");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(FIXED_LENGTH);
            DataOutputStream output = new DataOutputStream(bytes);
            output.write(MAGIC);
            output.writeShort(WIRE_VERSION);
            output.writeByte(selection.selectedCandidate().ordinal());
            output.writeByte(0);
            output.write(selection.source().nereusCommit().getBytes(StandardCharsets.US_ASCII));
            writeDigest(output, selection.source().oxiaImageDigest());
            writeDigest(output, selection.source().dependencyLockDigest());
            writeDigest(output, selection.source().executorDigest());
            writeDigest(output, selection.source().workloadDigest());
            writeDigest(output, selection.executionProfileDigest());
            writeDigest(output, selection.planDigest());
            writeDigest(output, selection.campaignId());
            writeDigest(output, selection.checkpointDigest());
            writeDigest(output, selection.evaluationDigest());
            writeDigest(output, selection.attachmentRootDigest());
            writeDigest(output, selection.diagnosticDigest());
            writeDigest(output, selection.diagnosticReceiptDigest());
            writeDigest(output, selection.diagnosticRawManifestDigest());
            writeDigest(output, selection.junitDigest());
            output.flush();
            if (bytes.size() != FIXED_LENGTH) {
                throw new IllegalStateException("allocator V5 fixed selection encoder length differs");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("allocator V5 selection memory encoding failed", failure);
        }
    }

    public static Selection decode(CanonicalBytes encoded) {
        Objects.requireNonNull(encoded, "encoded");
        byte[] bytes = encoded.toByteArray();
        if (bytes.length != FIXED_LENGTH) {
            throw invalid("allocator V5 selection length differs from fixed NARS5");
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
            if (!Arrays.equals(input.readNBytes(MAGIC.length), MAGIC) || input.readUnsignedShort() != WIRE_VERSION) {
                throw invalid("allocator V5 selection magic or version differs");
            }
            Candidate selected = enumValue(Candidate.values(), input.readUnsignedByte());
            if (input.readUnsignedByte() != 0) {
                throw invalid("allocator V5 selection reserved byte is nonzero");
            }
            byte[] commit = input.readNBytes(40);
            if (commit.length != 40) {
                throw new EOFException("allocator V5 selection commit is truncated");
            }
            SourceBinding source = new SourceBinding(
                    new String(commit, StandardCharsets.US_ASCII),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input));
            Selection selection = new Selection(
                    source,
                    readDigest(input),
                    readDigest(input),
                    selected,
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input));
            if (input.available() != 0
                    || !Arrays.equals(bytes, encode(selection).toByteArray())) {
                throw invalid("allocator V5 selection is not canonical NARS5");
            }
            return selection;
        } catch (EOFException failure) {
            throw invalid("allocator V5 selection is truncated", failure);
        } catch (IOException failure) {
            throw invalid("allocator V5 selection cannot be decoded", failure);
        }
    }

    private static Sha256Digest junitDigest(JUnitSummary junit) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.write("NEREUS-V2-M3-ALLOCATOR-JUNIT-SUMMARY-V5".getBytes(StandardCharsets.US_ASCII));
            output.writeLong(junit.tests());
            output.writeLong(junit.failures());
            output.writeLong(junit.errors());
            output.writeLong(junit.skips());
            output.flush();
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException failure) {
            throw new IllegalStateException("allocator V5 JUnit summary encoding failed", failure);
        }
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest digest) throws IOException {
        output.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] bytes = input.readNBytes(Sha256Digest.LENGTH);
        if (bytes.length != Sha256Digest.LENGTH) {
            throw new EOFException("allocator V5 selection digest is truncated");
        }
        return Sha256Digest.copyOf(bytes);
    }

    private static Candidate enumValue(Candidate[] values, int ordinal) {
        if (ordinal < 0 || ordinal >= values.length) {
            throw invalid("allocator V5 selected candidate ordinal differs");
        }
        return values[ordinal];
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }

    private static IllegalArgumentException invalid(String message, Throwable cause) {
        return new IllegalArgumentException(message, cause);
    }

    public record Selection(
            SourceBinding source,
            Sha256Digest executionProfileDigest,
            Sha256Digest planDigest,
            Candidate selectedCandidate,
            Sha256Digest campaignId,
            Sha256Digest checkpointDigest,
            Sha256Digest evaluationDigest,
            Sha256Digest attachmentRootDigest,
            Sha256Digest diagnosticDigest,
            Sha256Digest diagnosticReceiptDigest,
            Sha256Digest diagnosticRawManifestDigest,
            Sha256Digest junitDigest) {
        public Selection {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(executionProfileDigest, "executionProfileDigest");
            Objects.requireNonNull(planDigest, "planDigest");
            Objects.requireNonNull(selectedCandidate, "selectedCandidate");
            if (!executionProfileDigest.equals(AllocatorNativeExecutionProfileV5.executionProfileDigest())
                    || !planDigest.equals(AllocatorCampaignPlanProfileV5.zeroDecisionPlanDigest())
                    || !source.workloadDigest().equals(planDigest)
                    || selectedCandidate.nativePath()) {
                throw invalid("allocator V5 selection profile, plan, or candidate differs");
            }
            for (Sha256Digest digest : List.of(
                    campaignId,
                    checkpointDigest,
                    evaluationDigest,
                    attachmentRootDigest,
                    diagnosticDigest,
                    diagnosticReceiptDigest,
                    diagnosticRawManifestDigest,
                    junitDigest)) {
                if (Objects.requireNonNull(digest, "digest").isZero()) {
                    throw invalid("allocator V5 selection digest is zero");
                }
            }
        }
    }
}
