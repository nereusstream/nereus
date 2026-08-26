/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Candidate;
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

/** Canonical NAEV2 evaluation derived only from one complete, validator-reproved NACP2 checkpoint. */
public final class AllocatorCampaignEvaluationSealV2 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_EVALUATION_V2";
    private static final byte[] MAGIC = {'N', 'A', 'E', 'V', '2', 0, 0, 0};
    private static final int WIRE_VERSION = 2;
    private static final int FIXED_LENGTH = 8 + 2 + 1 + 1 + 4 + 4 + 40 + 32 * 7;

    private AllocatorCampaignEvaluationSealV2() {}

    public static CanonicalBytes seal(CanonicalBytes checkpointBytes) {
        AllocatorCampaignCheckpointV2 checkpoint = AllocatorCampaignCheckpointV2.decode(checkpointBytes);
        if (checkpoint.status() != AllocatorCampaignCheckpointV2.Status.COMPLETED) {
            throw invalid("allocator V2 interrupted or infrastructure-failed campaign cannot produce an evaluation");
        }
        AllocatorCampaignEvaluationV2 evaluation = AllocatorCampaignSelectorV2.evaluate(checkpoint.campaign());
        SealedEvaluation sealed = new SealedEvaluation(
                checkpoint.source(),
                checkpoint.campaignId(),
                AllocatorCampaignCheckpointV2.digest(checkpointBytes),
                attachmentRoot(checkpoint),
                evaluation.status(),
                evaluation.selectedCandidate(),
                evaluation.executedPerformanceCells(),
                evaluation.dispositionCells());
        return encode(sealed);
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
            writeDigest(output, evaluation.campaignId());
            writeDigest(output, evaluation.checkpointDigest());
            writeDigest(output, evaluation.attachmentRootDigest());
            output.flush();
            if (bytes.size() != FIXED_LENGTH) {
                throw new IllegalStateException("allocator V2 fixed evaluation encoder length differs");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("allocator V2 evaluation memory encoding failed", failure);
        }
    }

    public static SealedEvaluation decode(CanonicalBytes encoded) {
        Objects.requireNonNull(encoded, "encoded");
        byte[] bytes = encoded.toByteArray();
        if (bytes.length != FIXED_LENGTH) {
            throw invalid("allocator V2 evaluation length differs from fixed NAEV2");
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
            if (!Arrays.equals(input.readNBytes(MAGIC.length), MAGIC) || input.readUnsignedShort() != WIRE_VERSION) {
                throw invalid("allocator V2 evaluation magic or version differs");
            }
            AllocatorCampaignEvaluationV2.Status status = enumValue(
                    AllocatorCampaignEvaluationV2.Status.values(), input.readUnsignedByte(), "evaluation status");
            int selectedOrdinal = input.readUnsignedByte();
            Optional<Candidate> selected = selectedOrdinal == 255
                    ? Optional.empty()
                    : Optional.of(enumValue(Candidate.values(), selectedOrdinal, "selected candidate"));
            int executed = input.readInt();
            int dispositions = input.readInt();
            byte[] commitBytes = input.readNBytes(40);
            if (commitBytes.length != 40) {
                throw new EOFException("allocator V2 evaluation commit is truncated");
            }
            SourceBinding source = new SourceBinding(
                    new String(commitBytes, StandardCharsets.US_ASCII),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input));
            SealedEvaluation evaluation = new SealedEvaluation(
                    source,
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    status,
                    selected,
                    executed,
                    dispositions);
            if (input.available() != 0
                    || !Arrays.equals(bytes, encode(evaluation).toByteArray())) {
                throw invalid("allocator V2 evaluation is not canonical NAEV2");
            }
            return evaluation;
        } catch (EOFException failure) {
            throw invalid("allocator V2 evaluation is truncated", failure);
        } catch (IOException failure) {
            throw invalid("allocator V2 evaluation cannot be decoded", failure);
        }
    }

    static Sha256Digest attachmentRoot(AllocatorCampaignCheckpointV2 checkpoint) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.write("NEREUS-V2-M3-ALLOCATOR-ATTACHMENT-ROOT-V2".getBytes(StandardCharsets.US_ASCII));
            for (AllocatorCampaignCheckpointV2.ExecutionRecord record : checkpoint.executionRecords()) {
                writeDigest(output, record.attachmentDigest());
            }
            output.flush();
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException failure) {
            throw new IllegalStateException("allocator V2 attachment root encoding failed", failure);
        }
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest digest) throws IOException {
        output.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] bytes = input.readNBytes(Sha256Digest.LENGTH);
        if (bytes.length != Sha256Digest.LENGTH) {
            throw new EOFException("allocator V2 evaluation digest is truncated");
        }
        return Sha256Digest.copyOf(bytes);
    }

    private static <T> T enumValue(T[] values, int ordinal, String label) {
        if (ordinal < 0 || ordinal >= values.length) {
            throw invalid("allocator V2 " + label + " ordinal differs");
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
            Sha256Digest campaignId,
            Sha256Digest checkpointDigest,
            Sha256Digest attachmentRootDigest,
            AllocatorCampaignEvaluationV2.Status status,
            Optional<Candidate> selectedCandidate,
            int executedPerformanceCells,
            int dispositionCells) {
        public SealedEvaluation {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(campaignId, "campaignId");
            Objects.requireNonNull(checkpointDigest, "checkpointDigest");
            Objects.requireNonNull(attachmentRootDigest, "attachmentRootDigest");
            Objects.requireNonNull(status, "status");
            selectedCandidate = Objects.requireNonNull(selectedCandidate, "selectedCandidate");
            boolean eligible = status == AllocatorCampaignEvaluationV2.Status.STRICT_SELECTED
                    || status == AllocatorCampaignEvaluationV2.Status.RANGE_SELECTED;
            if (campaignId.isZero()
                    || checkpointDigest.isZero()
                    || attachmentRootDigest.isZero()
                    || executedPerformanceCells < 0
                    || dispositionCells < 0
                    || executedPerformanceCells > AllocatorCampaignV2.LOGICAL_PERFORMANCE_CELLS
                    || dispositionCells > AllocatorCampaignV2.LOGICAL_PERFORMANCE_CELLS
                    || executedPerformanceCells + dispositionCells != AllocatorCampaignV2.LOGICAL_PERFORMANCE_CELLS
                    || eligible != selectedCandidate.isPresent()
                    || selectedCandidate.filter(Candidate::nativePath).isPresent()
                    || (status == AllocatorCampaignEvaluationV2.Status.STRICT_SELECTED
                            && !selectedCandidate.equals(Optional.of(Candidate.STRICT)))
                    || (status == AllocatorCampaignEvaluationV2.Status.RANGE_SELECTED
                            && selectedCandidate.filter(Candidate::range).isEmpty())) {
                throw invalid("allocator V2 sealed evaluation accounting or selection differs");
            }
        }

        public boolean selectionEligible() {
            return selectedCandidate.isPresent();
        }
    }
}
