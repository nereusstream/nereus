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
import java.util.Objects;
import java.util.Optional;

/** Canonical NAEV5 evaluation derived only from one complete, validator-reproved NACP5 checkpoint. */
public final class AllocatorCampaignEvaluationSealV5 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_EVALUATION_V5";
    private static final byte[] MAGIC = {'N', 'A', 'E', 'V', '5', 0, 0, 0};
    private static final int WIRE_VERSION = 5;
    private static final int FIXED_LENGTH = 8 + 2 + 1 + 1 + 4 + 4 + 40 + 32 * 9;

    private AllocatorCampaignEvaluationSealV5() {}

    public static CanonicalBytes seal(CanonicalBytes checkpointBytes) {
        AllocatorCampaignCheckpointV5 checkpoint = AllocatorCampaignCheckpointV5.decode(checkpointBytes);
        if (checkpoint.status() != AllocatorCampaignCheckpointV3.Status.COMPLETED) {
            throw invalid("allocator V5 interrupted or infrastructure-failed campaign cannot produce an evaluation");
        }
        AllocatorCampaignEvaluationV3 evaluation = AllocatorCampaignSelectorV3.evaluate(checkpoint.campaign());
        return encode(new SealedEvaluation(
                checkpoint.source(),
                checkpoint.executionProfileDigest(),
                checkpoint.planDigest(),
                checkpoint.campaignId(),
                AllocatorCampaignCheckpointV5.digest(checkpointBytes),
                attachmentRoot(checkpoint),
                evaluation.status(),
                evaluation.selectedCandidate(),
                evaluation.executedPerformanceCells(),
                evaluation.dispositionCells()));
    }

    public static CanonicalBytes encode(SealedEvaluation evaluation) {
        Objects.requireNonNull(evaluation, "evaluation");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(FIXED_LENGTH);
            DataOutputStream output = new DataOutputStream(bytes);
            output.write(MAGIC);
            output.writeShort(WIRE_VERSION);
            output.writeByte(evaluation.status().ordinal());
            output.writeByte(
                    evaluation.selectedCandidate().map(Candidate::ordinal).orElse(255));
            output.writeInt(evaluation.executedPerformanceCells());
            output.writeInt(evaluation.dispositionCells());
            output.write(evaluation.source().nereusCommit().getBytes(StandardCharsets.US_ASCII));
            writeDigest(output, evaluation.source().oxiaImageDigest());
            writeDigest(output, evaluation.source().dependencyLockDigest());
            writeDigest(output, evaluation.source().executorDigest());
            writeDigest(output, evaluation.source().workloadDigest());
            writeDigest(output, evaluation.executionProfileDigest());
            writeDigest(output, evaluation.planDigest());
            writeDigest(output, evaluation.campaignId());
            writeDigest(output, evaluation.checkpointDigest());
            writeDigest(output, evaluation.attachmentRootDigest());
            output.flush();
            if (bytes.size() != FIXED_LENGTH) {
                throw new IllegalStateException("allocator V5 fixed evaluation encoder length differs");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("allocator V5 evaluation memory encoding failed", failure);
        }
    }

    public static SealedEvaluation decode(CanonicalBytes encoded) {
        Objects.requireNonNull(encoded, "encoded");
        byte[] bytes = encoded.toByteArray();
        if (bytes.length != FIXED_LENGTH) {
            throw invalid("allocator V5 evaluation length differs from fixed NAEV5");
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
            if (!Arrays.equals(input.readNBytes(MAGIC.length), MAGIC) || input.readUnsignedShort() != WIRE_VERSION) {
                throw invalid("allocator V5 evaluation magic or version differs");
            }
            AllocatorCampaignEvaluationV3.Status status = enumValue(
                    AllocatorCampaignEvaluationV3.Status.values(), input.readUnsignedByte(), "evaluation status");
            int selectedOrdinal = input.readUnsignedByte();
            Optional<Candidate> selected = selectedOrdinal == 255
                    ? Optional.empty()
                    : Optional.of(enumValue(Candidate.values(), selectedOrdinal, "selected candidate"));
            int executed = input.readInt();
            int dispositions = input.readInt();
            byte[] commit = input.readNBytes(40);
            if (commit.length != 40) {
                throw new EOFException("allocator V5 evaluation commit is truncated");
            }
            SourceBinding source = new SourceBinding(
                    new String(commit, StandardCharsets.US_ASCII),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input));
            SealedEvaluation evaluation = new SealedEvaluation(
                    source,
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    status,
                    selected,
                    executed,
                    dispositions);
            if (input.available() != 0
                    || !Arrays.equals(bytes, encode(evaluation).toByteArray())) {
                throw invalid("allocator V5 evaluation is not canonical NAEV5");
            }
            return evaluation;
        } catch (EOFException failure) {
            throw invalid("allocator V5 evaluation is truncated", failure);
        } catch (IOException failure) {
            throw invalid("allocator V5 evaluation cannot be decoded", failure);
        }
    }

    static Sha256Digest attachmentRoot(AllocatorCampaignCheckpointV5 checkpoint) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.write("NEREUS-V2-M3-ALLOCATOR-ATTACHMENT-ROOT-V5".getBytes(StandardCharsets.US_ASCII));
            for (AllocatorCampaignCheckpointV3.ExecutionRecord record : checkpoint.executionRecords()) {
                writeDigest(output, record.attachmentDigest());
            }
            output.flush();
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException failure) {
            throw new IllegalStateException("allocator V5 attachment root encoding failed", failure);
        }
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest digest) throws IOException {
        output.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] bytes = input.readNBytes(Sha256Digest.LENGTH);
        if (bytes.length != Sha256Digest.LENGTH) {
            throw new EOFException("allocator V5 evaluation digest is truncated");
        }
        return Sha256Digest.copyOf(bytes);
    }

    private static <T> T enumValue(T[] values, int ordinal, String label) {
        if (ordinal < 0 || ordinal >= values.length) {
            throw invalid("allocator V5 " + label + " ordinal differs");
        }
        return values[ordinal];
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }

    private static IllegalArgumentException invalid(String message, Throwable cause) {
        return new IllegalArgumentException(message, cause);
    }

    public record SealedEvaluation(
            SourceBinding source,
            Sha256Digest executionProfileDigest,
            Sha256Digest planDigest,
            Sha256Digest campaignId,
            Sha256Digest checkpointDigest,
            Sha256Digest attachmentRootDigest,
            AllocatorCampaignEvaluationV3.Status status,
            Optional<Candidate> selectedCandidate,
            int executedPerformanceCells,
            int dispositionCells) {
        public SealedEvaluation {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(executionProfileDigest, "executionProfileDigest");
            Objects.requireNonNull(planDigest, "planDigest");
            Objects.requireNonNull(campaignId, "campaignId");
            Objects.requireNonNull(checkpointDigest, "checkpointDigest");
            Objects.requireNonNull(attachmentRootDigest, "attachmentRootDigest");
            Objects.requireNonNull(status, "status");
            selectedCandidate = Objects.requireNonNull(selectedCandidate, "selectedCandidate");
            boolean eligible = status == AllocatorCampaignEvaluationV3.Status.STRICT_SELECTED
                    || status == AllocatorCampaignEvaluationV3.Status.RANGE_SELECTED;
            if (!executionProfileDigest.equals(AllocatorNativeExecutionProfileV5.executionProfileDigest())
                    || !planDigest.equals(AllocatorCampaignPlanProfileV5.zeroDecisionPlanDigest())
                    || !source.workloadDigest().equals(planDigest)
                    || campaignId.isZero()
                    || checkpointDigest.isZero()
                    || attachmentRootDigest.isZero()
                    || executedPerformanceCells < 0
                    || dispositionCells < 0
                    || executedPerformanceCells + dispositionCells != AllocatorCampaignV3.LOGICAL_PERFORMANCE_CELLS
                    || eligible != selectedCandidate.isPresent()
                    || selectedCandidate.filter(Candidate::nativePath).isPresent()
                    || (status == AllocatorCampaignEvaluationV3.Status.STRICT_SELECTED
                            && !selectedCandidate.equals(Optional.of(Candidate.STRICT)))
                    || (status == AllocatorCampaignEvaluationV3.Status.RANGE_SELECTED
                            && selectedCandidate.filter(Candidate::range).isEmpty())) {
                throw invalid("allocator V5 sealed evaluation accounting, profile, or selection differs");
            }
        }

        public boolean selectionEligible() {
            return selectedCandidate.isPresent();
        }
    }
}
