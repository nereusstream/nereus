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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Campaign;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Disposition;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Plan;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Strict canonical NACP3 checkpoint and resume authority for the ADR-0104 adaptive campaign. */
public final class AllocatorCampaignCheckpointV3 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_CHECKPOINT_V3";
    public static final int MAX_ENCODED_BYTES = 2 * 1024 * 1024;
    private static final byte[] MAGIC = {'N', 'A', 'C', 'P', '3', 0, 0, 0};
    private static final int WIRE_VERSION = 3;
    private static final int MAX_OBSERVATIONS = AllocatorCampaignV3.LOGICAL_PERFORMANCE_CELLS + 40;
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");
    private static final Sha256Digest ZERO_DIGEST = Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]);

    private final Status status;
    private final long checkpointSequence;
    private final SourceBinding source;
    private final Sha256Digest campaignId;
    private final Sha256Digest predecessorCheckpointDigest;
    private final RemainingBudgets remainingBudgets;
    private final List<ExecutionRecord> executionRecords;
    private final List<Disposition> dispositions;

    private AllocatorCampaignCheckpointV3(
            Status status,
            long checkpointSequence,
            SourceBinding source,
            Sha256Digest campaignId,
            Sha256Digest predecessorCheckpointDigest,
            RemainingBudgets remainingBudgets,
            List<ExecutionRecord> executionRecords,
            List<Disposition> dispositions) {
        this.status = Objects.requireNonNull(status, "status");
        if (checkpointSequence < 0) {
            throw invalid("allocator V3 checkpoint sequence cannot be negative");
        }
        this.checkpointSequence = checkpointSequence;
        this.source = Objects.requireNonNull(source, "source");
        this.campaignId = Objects.requireNonNull(campaignId, "campaignId");
        this.predecessorCheckpointDigest =
                Objects.requireNonNull(predecessorCheckpointDigest, "predecessorCheckpointDigest");
        this.remainingBudgets = Objects.requireNonNull(remainingBudgets, "remainingBudgets");
        this.executionRecords = List.copyOf(Objects.requireNonNull(executionRecords, "executionRecords"));
        this.dispositions = List.copyOf(Objects.requireNonNull(dispositions, "dispositions"));
        if (!campaignId.equals(campaignId(source))) {
            throw invalid("allocator V3 campaign identity differs from its frozen source/executor tuple");
        }
        if ((checkpointSequence == 0) != predecessorCheckpointDigest.isZero()) {
            throw invalid("allocator V3 checkpoint predecessor lineage differs from its sequence");
        }
        Set<Sha256Digest> attachmentDigests = new HashSet<>();
        if (this.executionRecords.size() > MAX_OBSERVATIONS
                || this.executionRecords.stream()
                        .anyMatch(record -> !attachmentDigests.add(record.attachmentDigest()))) {
            throw invalid("allocator V3 execution attachment inventory is oversized or aliased");
        }
        Plan plan = AllocatorCampaignValidatorV3.validate(campaign());
        if (status == Status.COMPLETED && !plan.completed()) {
            throw invalid("allocator V3 completed checkpoint has a required action");
        }
        if (status == Status.RUNNING && plan.completed()) {
            throw invalid("allocator V3 running checkpoint is already complete");
        }
    }

    public static AllocatorCampaignCheckpointV3 initial(
            SourceBinding source,
            RemainingBudgets remainingBudgets,
            List<ExecutionRecord> executionRecords,
            List<Disposition> dispositions,
            Status status) {
        return new AllocatorCampaignCheckpointV3(
                status, 0, source, campaignId(source), ZERO_DIGEST, remainingBudgets, executionRecords, dispositions);
    }

    /**
     * Revalidates the complete predecessor and returns the next checkpoint only when source identity and ordered
     * execution prefix are exact. An infrastructure-failed campaign cannot be resumed as a formal campaign.
     */
    public static AllocatorCampaignCheckpointV3 resume(
            CanonicalBytes predecessorBytes,
            SourceBinding currentSource,
            RemainingBudgets remainingBudgets,
            List<ExecutionRecord> executionRecords,
            List<Disposition> dispositions,
            Status nextStatus) {
        AllocatorCampaignCheckpointV3 predecessor = decode(predecessorBytes);
        if (predecessor.status != Status.RUNNING && predecessor.status != Status.INTERRUPTED) {
            throw invalid("allocator V3 checkpoint status cannot resume");
        }
        if (!predecessor.source.equals(currentSource)) {
            throw invalid("allocator V3 resume source/executor tuple differs");
        }
        if (nextStatus != Status.RUNNING
                && nextStatus != Status.COMPLETED
                && nextStatus != Status.INTERRUPTED
                && nextStatus != Status.INFRASTRUCTURE_FAILED) {
            throw invalid("allocator V3 resume target status differs");
        }
        List<ExecutionRecord> exactRecords = List.copyOf(executionRecords);
        if (exactRecords.size() < predecessor.executionRecords.size()
                || !exactRecords.subList(0, predecessor.executionRecords.size()).equals(predecessor.executionRecords)
                || !remainingBudgets.doesNotExceed(predecessor.remainingBudgets)) {
            throw invalid("allocator V3 resume changed its executed prefix or increased a phase budget");
        }
        return new AllocatorCampaignCheckpointV3(
                nextStatus,
                Math.addExact(predecessor.checkpointSequence, 1),
                currentSource,
                predecessor.campaignId,
                digest(predecessorBytes),
                remainingBudgets,
                exactRecords,
                dispositions);
    }

    public static CanonicalBytes encode(AllocatorCampaignCheckpointV3 checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.write(MAGIC);
            output.writeShort(WIRE_VERSION);
            output.writeByte(checkpoint.status.ordinal());
            output.writeByte(0);
            output.writeLong(checkpoint.checkpointSequence);
            writeCommit(output, checkpoint.source.nereusCommit());
            writeDigest(output, checkpoint.source.oxiaImageDigest());
            writeDigest(output, checkpoint.source.dependencyLockDigest());
            writeDigest(output, checkpoint.source.executorDigest());
            writeDigest(output, checkpoint.source.workloadDigest());
            writeDigest(output, checkpoint.campaignId);
            writeDigest(output, checkpoint.predecessorCheckpointDigest);
            checkpoint.remainingBudgets.write(output);
            List<AllocatorCampaignV3.Cell> logicalCells = AllocatorCampaignV3.logicalCells();
            output.writeInt(logicalCells.size());
            for (AllocatorCampaignV3.Cell cell : logicalCells) {
                output.writeInt(cell.contextId());
            }
            output.writeInt(checkpoint.executionRecords.size());
            for (ExecutionRecord record : checkpoint.executionRecords) {
                writeExecutionRecord(output, record);
            }
            output.writeInt(checkpoint.dispositions.size());
            for (Disposition disposition : checkpoint.dispositions) {
                output.writeInt(disposition.cell().contextId());
                output.writeByte(disposition.kind().ordinal());
                output.writeShort(disposition.dependencyContextIds().size());
                for (int dependency : disposition.dependencyContextIds()) {
                    output.writeInt(dependency);
                }
            }
            output.flush();
            if (bytes.size() > MAX_ENCODED_BYTES) {
                throw invalid("allocator V3 checkpoint exceeds its parser cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("allocator V3 checkpoint memory encoding failed", failure);
        }
    }

    public static AllocatorCampaignCheckpointV3 decode(CanonicalBytes encoded) {
        Objects.requireNonNull(encoded, "encoded");
        byte[] bytes = encoded.toByteArray();
        if (bytes.length == 0 || bytes.length > MAX_ENCODED_BYTES) {
            throw invalid("allocator V3 checkpoint length is outside its parser cap");
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
            if (!Arrays.equals(input.readNBytes(MAGIC.length), MAGIC) || input.readUnsignedShort() != WIRE_VERSION) {
                throw invalid("allocator V3 checkpoint magic or version differs");
            }
            Status status = enumValue(Status.values(), input.readUnsignedByte(), "checkpoint status");
            if (input.readUnsignedByte() != 0) {
                throw invalid("allocator V3 checkpoint reserved byte is nonzero");
            }
            long sequence = input.readLong();
            SourceBinding source = new SourceBinding(
                    readCommit(input), readDigest(input), readDigest(input), readDigest(input), readDigest(input));
            Sha256Digest campaignId = readDigest(input);
            Sha256Digest predecessor = readDigest(input);
            RemainingBudgets budgets = RemainingBudgets.read(input);
            Map<Integer, AllocatorCampaignV3.Cell> cells = logicalInventory(input);
            int observationCount = boundedCount(input.readInt(), MAX_OBSERVATIONS, "execution record");
            List<ExecutionRecord> records = new ArrayList<>(observationCount);
            for (int index = 0; index < observationCount; index++) {
                records.add(readExecutionRecord(input, cells));
            }
            int dispositionCount =
                    boundedCount(input.readInt(), AllocatorCampaignV3.LOGICAL_PERFORMANCE_CELLS, "disposition");
            List<Disposition> dispositions = new ArrayList<>(dispositionCount);
            for (int index = 0; index < dispositionCount; index++) {
                AllocatorCampaignV3.Cell cell = requireCell(cells, input.readInt());
                AllocatorCampaignV3.DispositionKind kind = enumValue(
                        AllocatorCampaignV3.DispositionKind.values(), input.readUnsignedByte(), "disposition kind");
                int dependencyCount =
                        boundedCount(input.readUnsignedShort(), MAX_OBSERVATIONS, "disposition dependency");
                List<Integer> dependencies = new ArrayList<>(dependencyCount);
                for (int dependency = 0; dependency < dependencyCount; dependency++) {
                    dependencies.add(input.readInt());
                }
                dispositions.add(new Disposition(cell, kind, dependencies));
            }
            if (input.available() != 0) {
                throw invalid("allocator V3 checkpoint has trailing bytes");
            }
            AllocatorCampaignCheckpointV3 checkpoint = new AllocatorCampaignCheckpointV3(
                    status, sequence, source, campaignId, predecessor, budgets, records, dispositions);
            if (!Arrays.equals(bytes, encode(checkpoint).toByteArray())) {
                throw invalid("allocator V3 checkpoint is not canonical NACP3");
            }
            return checkpoint;
        } catch (EOFException failure) {
            throw invalid("allocator V3 checkpoint is truncated", failure);
        } catch (IOException failure) {
            throw invalid("allocator V3 checkpoint cannot be decoded", failure);
        }
    }

    public Status status() {
        return status;
    }

    public long checkpointSequence() {
        return checkpointSequence;
    }

    public SourceBinding source() {
        return source;
    }

    public Sha256Digest campaignId() {
        return campaignId;
    }

    public Sha256Digest predecessorCheckpointDigest() {
        return predecessorCheckpointDigest;
    }

    public RemainingBudgets remainingBudgets() {
        return remainingBudgets;
    }

    public List<ExecutionRecord> executionRecords() {
        return executionRecords;
    }

    public List<Disposition> dispositions() {
        return dispositions;
    }

    public Campaign campaign() {
        return new Campaign(
                executionRecords.stream().map(ExecutionRecord::observation).toList(), dispositions);
    }

    public Set<Sha256Digest> attachmentDigests() {
        return Set.copyOf(
                executionRecords.stream().map(ExecutionRecord::attachmentDigest).toList());
    }

    public static Sha256Digest digest(CanonicalBytes value) {
        return Sha256Digest.hash(Objects.requireNonNull(value, "value"));
    }

    private static Sha256Digest campaignId(SourceBinding source) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.write("NEREUS-V2-M3-ALLOCATOR-CAMPAIGN-ID-V3".getBytes(StandardCharsets.US_ASCII));
            writeCommit(output, source.nereusCommit());
            writeDigest(output, source.oxiaImageDigest());
            writeDigest(output, source.dependencyLockDigest());
            writeDigest(output, source.executorDigest());
            writeDigest(output, source.workloadDigest());
            output.writeInt(AllocatorCampaignV3.PLANNER_VERSION);
            for (AllocatorCampaignV3.Cell cell : AllocatorCampaignV3.logicalCells()) {
                output.writeInt(cell.contextId());
            }
            output.flush();
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException failure) {
            throw new IllegalStateException("allocator V3 campaign identity encoding failed", failure);
        }
    }

    private static void writeExecutionRecord(DataOutputStream output, ExecutionRecord record) throws IOException {
        Observation observation = record.observation();
        if (observation instanceof AllocatorCampaignV3.IntervalEvidence interval) {
            output.writeByte(1);
            output.writeInt(interval.cell().contextId());
            output.writeInt(interval.offeredRate());
            long[] values = {
                interval.offered(),
                interval.admitted(),
                interval.overloadDroppedBeforeAdmission(),
                interval.completed(),
                interval.failedAfterAdmission(),
                interval.timedOutAfterAdmission(),
                interval.terminal(),
                interval.failedAssertions(),
                interval.unexpectedErrors(),
                interval.skipped(),
                interval.duplicateLedgerIds(),
                interval.reusedLedgerIds(),
                interval.rolloverP99Micros(),
                interval.oxiaOperationP99Micros(),
                interval.queueAgeP99Micros(),
                interval.queueDepthMaximum(),
                interval.starvationMaximumMicros(),
                interval.appendStallP99Micros(),
                interval.backlogAtEnd(),
                interval.inFlightAtEnd(),
                interval.waiterCountAtEnd()
            };
            for (long value : values) {
                output.writeLong(value);
            }
        } else if (observation instanceof AllocatorCampaignV3.FaultEvidence fault) {
            output.writeByte(2);
            output.writeByte(fault.row().candidate().ordinal());
            output.writeInt(fault.row().activeManagedLedgers());
            output.writeInt(fault.row().metadataLatencyP99Millis());
            int cuts = 0;
            for (AllocatorFaultCutV1 cut : fault.cuts()) {
                cuts |= 1 << cut.ordinal();
            }
            output.writeInt(cuts);
            long[] values = {
                fault.failed(),
                fault.timedOut(),
                fault.unexpectedErrors(),
                fault.failedAssertions(),
                fault.skipped(),
                fault.duplicateLedgerIds(),
                fault.reusedLedgerIds(),
                fault.permanentOrphans(),
                fault.staleCandidateBurnMaximum(),
                fault.massTakeoverRecoveryMicros()
            };
            for (long value : values) {
                output.writeLong(value);
            }
        } else {
            throw invalid("allocator V3 checkpoint observation kind differs");
        }
        writeDigest(output, record.attachmentDigest());
    }

    private static ExecutionRecord readExecutionRecord(
            DataInputStream input, Map<Integer, AllocatorCampaignV3.Cell> cells) throws IOException {
        int kind = input.readUnsignedByte();
        Observation observation;
        if (kind == 1) {
            AllocatorCampaignV3.Cell cell = requireCell(cells, input.readInt());
            int offeredRate = input.readInt();
            long[] values = readLongs(input, 21);
            observation = new AllocatorCampaignV3.IntervalEvidence(
                    cell,
                    offeredRate,
                    values[0],
                    values[1],
                    values[2],
                    values[3],
                    values[4],
                    values[5],
                    values[6],
                    values[7],
                    values[8],
                    values[9],
                    values[10],
                    values[11],
                    values[12],
                    values[13],
                    values[14],
                    values[15],
                    values[16],
                    values[17],
                    values[18],
                    values[19],
                    values[20]);
        } else if (kind == 2) {
            AllocatorCampaignV3.Candidate candidate =
                    enumValue(AllocatorCampaignV3.Candidate.values(), input.readUnsignedByte(), "fault candidate");
            AllocatorCampaignV3.Row row = new AllocatorCampaignV3.Row(candidate, input.readInt(), input.readInt());
            int cutMask = input.readInt();
            EnumSet<AllocatorFaultCutV1> cuts = EnumSet.noneOf(AllocatorFaultCutV1.class);
            int allowedMask = 0;
            for (AllocatorFaultCutV1 cut : AllocatorFaultCutV1.values()) {
                int bit = 1 << cut.ordinal();
                allowedMask |= bit;
                if ((cutMask & bit) != 0) {
                    cuts.add(cut);
                }
            }
            if ((cutMask & ~allowedMask) != 0) {
                throw invalid("allocator V3 fault cut mask has unknown bits");
            }
            long[] values = readLongs(input, 10);
            observation = new AllocatorCampaignV3.FaultEvidence(
                    row, cuts, values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7],
                    values[8], values[9]);
        } else {
            throw invalid("allocator V3 checkpoint observation tag differs");
        }
        return new ExecutionRecord(observation, readDigest(input));
    }

    private static Map<Integer, AllocatorCampaignV3.Cell> logicalInventory(DataInputStream input) throws IOException {
        int count = input.readInt();
        List<AllocatorCampaignV3.Cell> logical = AllocatorCampaignV3.logicalCells();
        if (count != logical.size()) {
            throw invalid("allocator V3 checkpoint logical inventory count differs");
        }
        Map<Integer, AllocatorCampaignV3.Cell> cells = new HashMap<>();
        for (AllocatorCampaignV3.Cell expected : logical) {
            int contextId = input.readInt();
            if (contextId != expected.contextId() || cells.put(contextId, expected) != null) {
                throw invalid("allocator V3 checkpoint logical inventory order differs");
            }
        }
        return Map.copyOf(cells);
    }

    private static AllocatorCampaignV3.Cell requireCell(Map<Integer, AllocatorCampaignV3.Cell> cells, int contextId) {
        AllocatorCampaignV3.Cell cell = cells.get(contextId);
        if (cell == null) {
            throw invalid("allocator V3 checkpoint references an unknown logical cell");
        }
        return cell;
    }

    private static long[] readLongs(DataInputStream input, int count) throws IOException {
        long[] values = new long[count];
        for (int index = 0; index < count; index++) {
            values[index] = input.readLong();
        }
        return values;
    }

    private static void writeCommit(DataOutputStream output, String commit) throws IOException {
        output.write(commit.getBytes(StandardCharsets.US_ASCII));
    }

    private static String readCommit(DataInputStream input) throws IOException {
        byte[] bytes = input.readNBytes(40);
        if (bytes.length != 40) {
            throw new EOFException("allocator V3 commit is truncated");
        }
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest digest) throws IOException {
        output.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] bytes = input.readNBytes(Sha256Digest.LENGTH);
        if (bytes.length != Sha256Digest.LENGTH) {
            throw new EOFException("allocator V3 digest is truncated");
        }
        return Sha256Digest.copyOf(bytes);
    }

    private static int boundedCount(int value, int maximum, String label) {
        if (value < 0 || value > maximum) {
            throw invalid("allocator V3 " + label + " count is outside its cap");
        }
        return value;
    }

    private static <T> T enumValue(T[] values, int ordinal, String label) {
        if (ordinal < 0 || ordinal >= values.length) {
            throw invalid("allocator V3 " + label + " ordinal differs");
        }
        return values[ordinal];
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }

    private static IllegalArgumentException invalid(String message, Throwable cause) {
        return new IllegalArgumentException(message, cause);
    }

    public enum Status {
        RUNNING,
        COMPLETED,
        INTERRUPTED,
        INFRASTRUCTURE_FAILED
    }

    public record SourceBinding(
            String nereusCommit,
            Sha256Digest oxiaImageDigest,
            Sha256Digest dependencyLockDigest,
            Sha256Digest executorDigest,
            Sha256Digest workloadDigest) {
        public SourceBinding {
            Objects.requireNonNull(nereusCommit, "nereusCommit");
            Objects.requireNonNull(oxiaImageDigest, "oxiaImageDigest");
            Objects.requireNonNull(dependencyLockDigest, "dependencyLockDigest");
            Objects.requireNonNull(executorDigest, "executorDigest");
            Objects.requireNonNull(workloadDigest, "workloadDigest");
            if (!COMMIT.matcher(nereusCommit).matches()
                    || oxiaImageDigest.isZero()
                    || dependencyLockDigest.isZero()
                    || executorDigest.isZero()
                    || workloadDigest.isZero()) {
                throw invalid("allocator V3 source/executor binding is not exact and nonzero");
            }
        }
    }

    public record RemainingBudgets(
            long setupSeconds,
            long populationSeconds,
            long faultSeconds,
            long scaleSeconds,
            long intervalSeconds,
            long cleanupSeconds,
            long checkpointAndSealSeconds) {
        public RemainingBudgets {
            if (setupSeconds < 0
                    || populationSeconds < 0
                    || faultSeconds < 0
                    || scaleSeconds < 0
                    || intervalSeconds < 0
                    || cleanupSeconds < 0
                    || checkpointAndSealSeconds < 0
                    || setupSeconds > 900
                    || populationSeconds > 5_400
                    || faultSeconds > 7_200
                    || scaleSeconds > 5_400
                    || intervalSeconds > 13_120
                    || cleanupSeconds > 1_640
                    || checkpointAndSealSeconds > 600) {
                throw invalid("allocator V3 remaining phase budget is outside its frozen bound");
            }
        }

        private boolean doesNotExceed(RemainingBudgets predecessor) {
            return setupSeconds <= predecessor.setupSeconds
                    && populationSeconds <= predecessor.populationSeconds
                    && faultSeconds <= predecessor.faultSeconds
                    && scaleSeconds <= predecessor.scaleSeconds
                    && intervalSeconds <= predecessor.intervalSeconds
                    && cleanupSeconds <= predecessor.cleanupSeconds
                    && checkpointAndSealSeconds <= predecessor.checkpointAndSealSeconds;
        }

        private void write(DataOutputStream output) throws IOException {
            output.writeLong(setupSeconds);
            output.writeLong(populationSeconds);
            output.writeLong(faultSeconds);
            output.writeLong(scaleSeconds);
            output.writeLong(intervalSeconds);
            output.writeLong(cleanupSeconds);
            output.writeLong(checkpointAndSealSeconds);
        }

        private static RemainingBudgets read(DataInputStream input) throws IOException {
            return new RemainingBudgets(
                    input.readLong(),
                    input.readLong(),
                    input.readLong(),
                    input.readLong(),
                    input.readLong(),
                    input.readLong(),
                    input.readLong());
        }
    }

    public record ExecutionRecord(Observation observation, Sha256Digest attachmentDigest) {
        public ExecutionRecord {
            Objects.requireNonNull(observation, "observation");
            Objects.requireNonNull(attachmentDigest, "attachmentDigest");
            if (attachmentDigest.isZero()) {
                throw invalid("allocator V3 executed observation attachment digest is zero");
            }
        }
    }
}
