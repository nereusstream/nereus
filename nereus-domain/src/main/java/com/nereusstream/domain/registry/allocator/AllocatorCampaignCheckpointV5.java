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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.ExecutionRecord;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Campaign;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Disposition;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Strict NACP5 authority. The nested NACP3 is an implementation-private logical-algebra payload; V5 lineage,
 * campaign identity, plan/profile identity, and the 42-second interval budget are independently canonicalized here.
 */
public final class AllocatorCampaignCheckpointV5 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_CHECKPOINT_V5";
    public static final int MAX_ENCODED_BYTES = AllocatorCampaignCheckpointV3.MAX_ENCODED_BYTES + 512;
    private static final byte[] MAGIC = {'N', 'A', 'C', 'P', '5', 0, 0, 0};
    private static final int WIRE_VERSION = 5;
    private static final Sha256Digest ZERO_DIGEST = Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]);
    private static final long V3_INTERVAL_BUDGET = 13_120;
    private static final long V3_INTERVAL_CHARGE = 40;
    private static final long V5_INTERVAL_BUDGET = 13_776;
    private static final long V5_INTERVAL_CHARGE = 42;

    private final CanonicalBytes logicalCheckpointBytes;
    private final AllocatorCampaignCheckpointV3 logicalCheckpoint;
    private final Sha256Digest executionProfileDigest;
    private final Sha256Digest planDigest;
    private final Sha256Digest campaignId;
    private final Sha256Digest predecessorCheckpointDigest;
    private final RemainingBudgets remainingBudgets;

    private AllocatorCampaignCheckpointV5(
            CanonicalBytes logicalCheckpointBytes,
            Sha256Digest executionProfileDigest,
            Sha256Digest planDigest,
            Sha256Digest campaignId,
            Sha256Digest predecessorCheckpointDigest,
            RemainingBudgets remainingBudgets) {
        this.logicalCheckpointBytes = Objects.requireNonNull(logicalCheckpointBytes, "logicalCheckpointBytes");
        this.logicalCheckpoint = AllocatorCampaignCheckpointV3.decode(logicalCheckpointBytes);
        this.executionProfileDigest = Objects.requireNonNull(executionProfileDigest, "executionProfileDigest");
        this.planDigest = Objects.requireNonNull(planDigest, "planDigest");
        this.campaignId = Objects.requireNonNull(campaignId, "campaignId");
        this.predecessorCheckpointDigest =
                Objects.requireNonNull(predecessorCheckpointDigest, "predecessorCheckpointDigest");
        this.remainingBudgets = Objects.requireNonNull(remainingBudgets, "remainingBudgets");
        if (!executionProfileDigest.equals(AllocatorNativeExecutionProfileV5.executionProfileDigest())
                || !planDigest.equals(AllocatorCampaignPlanProfileV5.zeroDecisionPlanDigest())
                || !logicalCheckpoint.source().workloadDigest().equals(planDigest)) {
            throw invalid("allocator V5 checkpoint profile or plan identity differs");
        }
        if (!campaignId.equals(campaignId(logicalCheckpoint.source(), logicalCheckpoint.campaignId()))) {
            throw invalid("allocator V5 campaign identity differs from its source/profile tuple");
        }
        if ((logicalCheckpoint.checkpointSequence() == 0) != predecessorCheckpointDigest.isZero()) {
            throw invalid("allocator V5 checkpoint predecessor lineage differs from its sequence");
        }
        validateLogicalBudgetAccounting(logicalCheckpoint);
        if (!remainingBudgets.equals(deriveBudgets(logicalCheckpoint))) {
            throw invalid("allocator V5 checkpoint budget does not match its logical action prefix");
        }
    }

    public static AllocatorCampaignCheckpointV5 initial(CanonicalBytes logicalCheckpointBytes) {
        AllocatorCampaignCheckpointV3 logical = AllocatorCampaignCheckpointV3.decode(logicalCheckpointBytes);
        if (logical.checkpointSequence() != 0) {
            throw invalid("allocator V5 initial checkpoint sequence differs");
        }
        return create(logicalCheckpointBytes, ZERO_DIGEST);
    }

    public static AllocatorCampaignCheckpointV5 resume(
            CanonicalBytes predecessorBytes, CanonicalBytes logicalCheckpointBytes) {
        AllocatorCampaignCheckpointV5 predecessor = decode(predecessorBytes);
        AllocatorCampaignCheckpointV3 logical = AllocatorCampaignCheckpointV3.decode(logicalCheckpointBytes);
        if (logical.checkpointSequence() != Math.addExact(predecessor.checkpointSequence(), 1)
                || !logical.source().equals(predecessor.source())
                || !logical.predecessorCheckpointDigest()
                        .equals(AllocatorCampaignCheckpointV3.digest(predecessor.logicalCheckpointBytes))) {
            throw invalid("allocator V5 resume changed its logical source, sequence, or nested lineage");
        }
        return create(logicalCheckpointBytes, digest(predecessorBytes));
    }

    private static AllocatorCampaignCheckpointV5 create(
            CanonicalBytes logicalCheckpointBytes, Sha256Digest predecessorDigest) {
        AllocatorCampaignCheckpointV3 logical = AllocatorCampaignCheckpointV3.decode(logicalCheckpointBytes);
        return new AllocatorCampaignCheckpointV5(
                logicalCheckpointBytes,
                AllocatorNativeExecutionProfileV5.executionProfileDigest(),
                AllocatorCampaignPlanProfileV5.zeroDecisionPlanDigest(),
                campaignId(logical.source(), logical.campaignId()),
                predecessorDigest,
                deriveBudgets(logical));
    }

    public static CanonicalBytes encode(AllocatorCampaignCheckpointV5 checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.write(MAGIC);
            output.writeShort(WIRE_VERSION);
            output.writeByte(checkpoint.status().ordinal());
            output.writeByte(0);
            output.writeLong(checkpoint.checkpointSequence());
            writeDigest(output, checkpoint.executionProfileDigest);
            writeDigest(output, checkpoint.planDigest);
            writeDigest(output, checkpoint.campaignId);
            writeDigest(output, checkpoint.predecessorCheckpointDigest);
            checkpoint.remainingBudgets.write(output);
            byte[] logicalBytes = checkpoint.logicalCheckpointBytes.toByteArray();
            output.writeInt(logicalBytes.length);
            output.write(logicalBytes);
            output.flush();
            if (bytes.size() > MAX_ENCODED_BYTES) {
                throw invalid("allocator V5 checkpoint exceeds its parser cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("allocator V5 checkpoint memory encoding failed", failure);
        }
    }

    public static AllocatorCampaignCheckpointV5 decode(CanonicalBytes encoded) {
        Objects.requireNonNull(encoded, "encoded");
        byte[] bytes = encoded.toByteArray();
        if (bytes.length == 0 || bytes.length > MAX_ENCODED_BYTES) {
            throw invalid("allocator V5 checkpoint length is outside its parser cap");
        }
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
            if (!Arrays.equals(input.readNBytes(MAGIC.length), MAGIC) || input.readUnsignedShort() != WIRE_VERSION) {
                throw invalid("allocator V5 checkpoint magic or version differs");
            }
            int statusOrdinal = input.readUnsignedByte();
            if (statusOrdinal >= Status.values().length || input.readUnsignedByte() != 0) {
                throw invalid("allocator V5 checkpoint status or reserved byte differs");
            }
            long sequence = input.readLong();
            Sha256Digest executionProfile = readDigest(input);
            Sha256Digest plan = readDigest(input);
            Sha256Digest campaign = readDigest(input);
            Sha256Digest predecessor = readDigest(input);
            RemainingBudgets budgets = RemainingBudgets.read(input);
            int logicalLength = input.readInt();
            if (logicalLength <= 0 || logicalLength > AllocatorCampaignCheckpointV3.MAX_ENCODED_BYTES) {
                throw invalid("allocator V5 nested logical checkpoint length differs");
            }
            byte[] logical = input.readNBytes(logicalLength);
            if (logical.length != logicalLength || input.available() != 0) {
                throw invalid("allocator V5 checkpoint is truncated or has trailing bytes");
            }
            AllocatorCampaignCheckpointV5 checkpoint = new AllocatorCampaignCheckpointV5(
                    CanonicalBytes.copyOf(logical), executionProfile, plan, campaign, predecessor, budgets);
            if (checkpoint.status().ordinal() != statusOrdinal
                    || checkpoint.checkpointSequence() != sequence
                    || !Arrays.equals(bytes, encode(checkpoint).toByteArray())) {
                throw invalid("allocator V5 checkpoint is not canonical NACP5");
            }
            return checkpoint;
        } catch (EOFException failure) {
            throw invalid("allocator V5 checkpoint is truncated", failure);
        } catch (IOException failure) {
            throw invalid("allocator V5 checkpoint cannot be decoded", failure);
        }
    }

    public Status status() {
        return logicalCheckpoint.status();
    }

    public long checkpointSequence() {
        return logicalCheckpoint.checkpointSequence();
    }

    public SourceBinding source() {
        return logicalCheckpoint.source();
    }

    public Sha256Digest executionProfileDigest() {
        return executionProfileDigest;
    }

    public Sha256Digest planDigest() {
        return planDigest;
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

    public CanonicalBytes logicalCheckpointBytes() {
        return logicalCheckpointBytes;
    }

    public Campaign campaign() {
        return logicalCheckpoint.campaign();
    }

    public List<ExecutionRecord> executionRecords() {
        return logicalCheckpoint.executionRecords();
    }

    public List<Disposition> dispositions() {
        return logicalCheckpoint.dispositions();
    }

    public Set<Sha256Digest> attachmentDigests() {
        return logicalCheckpoint.attachmentDigests();
    }

    public static Sha256Digest digest(CanonicalBytes value) {
        return Sha256Digest.hash(Objects.requireNonNull(value, "value"));
    }

    private static RemainingBudgets deriveBudgets(AllocatorCampaignCheckpointV3 logical) {
        AllocatorCampaignCheckpointV3.RemainingBudgets inner = logical.remainingBudgets();
        long consumedV3 = Math.subtractExact(V3_INTERVAL_BUDGET, inner.intervalSeconds());
        if (consumedV3 < 0 || consumedV3 % V3_INTERVAL_CHARGE != 0) {
            throw invalid("allocator V5 nested interval budget does not map to complete physical actions");
        }
        long intervalActions = consumedV3 / V3_INTERVAL_CHARGE;
        long remainingV5 =
                Math.subtractExact(V5_INTERVAL_BUDGET, Math.multiplyExact(intervalActions, V5_INTERVAL_CHARGE));
        return new RemainingBudgets(
                inner.setupSeconds(),
                inner.populationSeconds(),
                inner.faultSeconds(),
                inner.scaleSeconds(),
                remainingV5,
                inner.cleanupSeconds(),
                inner.checkpointAndSealSeconds());
    }

    private static void validateLogicalBudgetAccounting(AllocatorCampaignCheckpointV3 logical) {
        long intervalCount = logical.executionRecords().stream()
                .map(ExecutionRecord::observation)
                .filter(AllocatorCampaignV3.IntervalEvidence.class::isInstance)
                .count();
        long faultRows = logical.executionRecords().stream()
                .map(ExecutionRecord::observation)
                .filter(AllocatorCampaignV3.FaultEvidence.class::isInstance)
                .count();
        Set<AllocatorCampaignV3.Candidate> populatedTenThousand = new HashSet<>();
        Set<AllocatorCampaignV3.Candidate> populatedHundredThousand = new HashSet<>();
        logical.executionRecords().stream().map(ExecutionRecord::observation).forEach(observation -> {
            AllocatorCampaignV3.Row row = observation instanceof AllocatorCampaignV3.IntervalEvidence interval
                    ? interval.cell().row()
                    : ((AllocatorCampaignV3.FaultEvidence) observation).row();
            if (row.activeManagedLedgers() == 10_000) {
                populatedTenThousand.add(row.candidate());
            } else if (row.activeManagedLedgers() == 100_000) {
                populatedHundredThousand.add(row.candidate());
            }
        });
        AllocatorCampaignCheckpointV3.RemainingBudgets expected = new AllocatorCampaignCheckpointV3.RemainingBudgets(
                logical.executionRecords().isEmpty() ? 900 : 0,
                Math.subtractExact(5_400, Math.multiplyExact(900L, populatedTenThousand.size())),
                Math.subtractExact(7_200, Math.multiplyExact(180L, faultRows)),
                Math.subtractExact(5_400, Math.multiplyExact(900L, populatedHundredThousand.size())),
                Math.subtractExact(V3_INTERVAL_BUDGET, Math.multiplyExact(V3_INTERVAL_CHARGE, intervalCount)),
                Math.subtractExact(1_640, Math.multiplyExact(5L, intervalCount)),
                600);
        AllocatorCampaignCheckpointV3.RemainingBudgets actual = logical.remainingBudgets();
        boolean sequenceCoversRecords =
                logical.checkpointSequence() >= logical.executionRecords().size();
        if ((logical.status() == Status.RUNNING || logical.status() == Status.COMPLETED)
                && (!actual.equals(expected) || !sequenceCoversRecords)) {
            throw invalid("allocator V5 logical checkpoint budget or sequence differs from executed actions");
        }
        if ((logical.status() == Status.INTERRUPTED || logical.status() == Status.INFRASTRUCTURE_FAILED)
                && (!doesNotExceed(actual, expected) || !sequenceCoversRecords)) {
            throw invalid("allocator V5 terminal logical checkpoint budget or sequence differs");
        }
    }

    private static boolean doesNotExceed(
            AllocatorCampaignCheckpointV3.RemainingBudgets actual,
            AllocatorCampaignCheckpointV3.RemainingBudgets expected) {
        return actual.setupSeconds() <= expected.setupSeconds()
                && actual.populationSeconds() <= expected.populationSeconds()
                && actual.faultSeconds() <= expected.faultSeconds()
                && actual.scaleSeconds() <= expected.scaleSeconds()
                && actual.intervalSeconds() <= expected.intervalSeconds()
                && actual.cleanupSeconds() <= expected.cleanupSeconds()
                && actual.checkpointAndSealSeconds() <= expected.checkpointAndSealSeconds();
    }

    private static Sha256Digest campaignId(SourceBinding source, Sha256Digest logicalCampaignId) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.write("NEREUS-V2-M3-ALLOCATOR-CAMPAIGN-ID-V5".getBytes(StandardCharsets.US_ASCII));
            output.write(source.nereusCommit().getBytes(StandardCharsets.US_ASCII));
            writeDigest(output, source.oxiaImageDigest());
            writeDigest(output, source.dependencyLockDigest());
            writeDigest(output, source.executorDigest());
            writeDigest(output, source.workloadDigest());
            writeDigest(output, AllocatorNativeExecutionProfileV5.executionProfileDigest());
            writeDigest(output, AllocatorCampaignPlanProfileV5.zeroDecisionPlanDigest());
            writeDigest(output, logicalCampaignId);
            output.flush();
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException failure) {
            throw new IllegalStateException("allocator V5 campaign identity encoding failed", failure);
        }
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest digest) throws IOException {
        output.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] bytes = input.readNBytes(Sha256Digest.LENGTH);
        if (bytes.length != Sha256Digest.LENGTH) {
            throw new EOFException("allocator V5 checkpoint digest is truncated");
        }
        return Sha256Digest.copyOf(bytes);
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }

    private static IllegalArgumentException invalid(String message, Throwable cause) {
        return new IllegalArgumentException(message, cause);
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
                    || intervalSeconds > V5_INTERVAL_BUDGET
                    || cleanupSeconds > 1_640
                    || checkpointAndSealSeconds > 600) {
                throw invalid("allocator V5 remaining phase budget is outside its frozen bound");
            }
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
}
