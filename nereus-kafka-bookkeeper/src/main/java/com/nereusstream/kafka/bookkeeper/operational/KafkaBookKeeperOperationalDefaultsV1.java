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

package com.nereusstream.kafka.bookkeeper.operational;

import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ConstantsV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendCapacityBudgetV1;
import com.nereusstream.kafka.bookkeeper.replication.KafkaReplicaEligibilityBoundsV1;
import com.nereusstream.kafka.bookkeeper.replication.KafkaReplicaJournalBoundsV1;
import java.util.Objects;

/** K9-selected operational maxima. Lower authorities may only replace them with a component-wise lower profile. */
public record KafkaBookKeeperOperationalDefaultsV1(
        CheckpointBudget checkpoint,
        IndexBudget index,
        ActiveTailBudget activeTail,
        RecoveryBudget recovery,
        PipelineBudget pipeline,
        RolloverBudget rollover,
        HandleBudget handles,
        ReplicaBudget replica,
        WaiterBudget waiters,
        CursorBudget cursor) {
    public static final String SELECTION_ID = "KAFKA_BOOKKEEPER_M2_K9_2026_08_14_V1";

    public KafkaBookKeeperOperationalDefaultsV1 {
        Objects.requireNonNull(checkpoint, "checkpoint");
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(activeTail, "activeTail");
        Objects.requireNonNull(recovery, "recovery");
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(rollover, "rollover");
        Objects.requireNonNull(handles, "handles");
        Objects.requireNonNull(replica, "replica");
        Objects.requireNonNull(waiters, "waiters");
        Objects.requireNonNull(cursor, "cursor");
        if (index.maximumLocators() > Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT) {
            throw new IllegalArgumentException("index budget enlarges the NBKE2 v1 locator cap");
        }
        if (checkpoint.maximumRecordBatches() > activeTail.maximumLocators()
                || checkpoint.maximumEncodedBytes() > activeTail.maximumEncodedBytes()) {
            throw new IllegalArgumentException("active tail cannot cover one checkpoint interval");
        }
        if (recovery.maximumEntries() < activeTail.maximumLocators()
                || recovery.maximumEncodedBytes() < activeTail.maximumEncodedBytes()
                || recovery.maximumElapsedNanos() < activeTail.maximumAgeNanos()) {
            throw new IllegalArgumentException("recovery envelope cannot cover the selected active tail");
        }
        if (!atLeast(pipeline.global(), pipeline.partition())) {
            throw new IllegalArgumentException("global pipeline budget cannot be lower than one partition budget");
        }
        if (handles.maximumConcurrentOpens() > handles.maximumCachedLedgers()) {
            throw new IllegalArgumentException("concurrent ledger opens cannot exceed the handle cache");
        }
        if (replica.journal().maximumRecords() < replica.maximumApplyLagOffsets()
                || replica.journal().maximumEncodedBytes() < replica.maximumUnappliedBytes()) {
            throw new IllegalArgumentException("observation journal cannot cover the selected apply lag");
        }
        if (waiters.maximumCellWaiters() < waiters.maximumPartitionWaiters()) {
            throw new IllegalArgumentException("Cell waiter budget cannot be lower than one partition budget");
        }
        if (cursor.maximumCoalescedEntries() > recovery.maximumEntries()
                || cursor.maximumCoalescedBytes() > recovery.maximumEncodedBytes()) {
            throw new IllegalArgumentException("cursor coalescing cannot exceed the recovery I/O envelope");
        }
    }

    public static KafkaBookKeeperOperationalDefaultsV1 evidenceSelected() {
        return new KafkaBookKeeperOperationalDefaultsV1(
                new CheckpointBudget(1_024, 1_048_576),
                new IndexBudget(32_768, 1_024),
                new ActiveTailBudget(4_096, 4_194_304, 5_000_000_000L),
                new RecoveryBudget(4_096, 8_388_608, 30_000_000_000L),
                new PipelineBudget(
                        new CapacityBudget(16, 64, 67_108_864), new CapacityBudget(1_024, 4_096, 268_435_456)),
                new RolloverBudget(268_435_456, 262_144, 900_000_000_000L),
                new HandleBudget(1_024, 64),
                new ReplicaBudget(4_096, 33_554_432, 5_000_000_000L, new JournalBudget(8_192, 67_108_864)),
                new WaiterBudget(1_024, 65_536),
                new CursorBudget(64, 4_194_304));
    }

    public KafkaBookKeeperOperationalDefaultsV1 loweredBy(KafkaBookKeeperOperationalDefaultsV1 requested) {
        Objects.requireNonNull(requested, "requested");
        if (!atMost(requested.checkpoint, checkpoint)
                || !atMost(requested.index, index)
                || !atMost(requested.activeTail, activeTail)
                || !atMost(requested.recovery, recovery)
                || !atMost(requested.pipeline.partition, pipeline.partition)
                || !atMost(requested.pipeline.global, pipeline.global)
                || !atMost(requested.rollover, rollover)
                || !atMost(requested.handles, handles)
                || !atMost(requested.replica, replica)
                || !atMost(requested.replica.journal, replica.journal)
                || !atMost(requested.waiters, waiters)
                || !atMost(requested.cursor, cursor)) {
            throw new IllegalArgumentException("lower authority cannot enlarge a K9-selected operational bound");
        }
        return requested;
    }

    public KafkaBookKeeperRecoveryEnvelopeV1 recoveryEnvelope() {
        return new KafkaBookKeeperRecoveryEnvelopeV1(
                recovery.maximumEntries, recovery.maximumEncodedBytes, recovery.maximumElapsedNanos);
    }

    public KafkaAppendCapacityBudgetV1 partitionPipelineBudget() {
        return pipeline.partition.toCapacityBudget();
    }

    public KafkaAppendCapacityBudgetV1 globalPipelineBudget() {
        return pipeline.global.toCapacityBudget();
    }

    public KafkaReplicaEligibilityBoundsV1 replicaEligibilityBounds() {
        return new KafkaReplicaEligibilityBoundsV1(
                replica.maximumApplyLagOffsets, replica.maximumUnappliedBytes, replica.maximumUnappliedNanos);
    }

    public KafkaReplicaJournalBoundsV1 replicaJournalBounds() {
        return new KafkaReplicaJournalBoundsV1(
                Math.toIntExact(replica.journal.maximumRecords), replica.journal.maximumEncodedBytes);
    }

    public String toCanonicalJson() {
        return """
                {
                  "activeTail": {
                    "maximumAgeNanos": %d,
                    "maximumEncodedBytes": %d,
                    "maximumLocators": %d
                  },
                  "checkpoint": {
                    "maximumEncodedBytes": %d,
                    "maximumRecordBatches": %d
                  },
                  "cursor": {
                    "maximumCoalescedBytes": %d,
                    "maximumCoalescedEntries": %d
                  },
                  "handles": {
                    "maximumCachedLedgers": %d,
                    "maximumConcurrentOpens": %d
                  },
                  "index": {
                    "maximumEncodedBytes": %d,
                    "maximumLocators": %d
                  },
                  "pipeline": {
                    "global": {
                      "maximumBytes": %d,
                      "maximumEntries": %d,
                      "maximumGroups": %d
                    },
                    "partition": {
                      "maximumBytes": %d,
                      "maximumEntries": %d,
                      "maximumGroups": %d
                    }
                  },
                  "recovery": {
                    "maximumElapsedNanos": %d,
                    "maximumEncodedBytes": %d,
                    "maximumEntries": %d
                  },
                  "replica": {
                    "journal": {
                      "maximumEncodedBytes": %d,
                      "maximumRecords": %d
                    },
                    "maximumApplyLagOffsets": %d,
                    "maximumUnappliedBytes": %d,
                    "maximumUnappliedNanos": %d
                  },
                  "rollover": {
                    "maximumAgeNanos": %d,
                    "maximumEncodedBytes": %d,
                    "maximumEntries": %d
                  },
                  "schema": "NEREUS_V2_M2_KAFKA_K9_SELECTED_DEFAULTS_V1",
                  "selectionId": "%s",
                  "waiters": {
                    "maximumCellWaiters": %d,
                    "maximumPartitionWaiters": %d
                  }
                }
                """.formatted(
                activeTail.maximumAgeNanos,
                activeTail.maximumEncodedBytes,
                activeTail.maximumLocators,
                checkpoint.maximumEncodedBytes,
                checkpoint.maximumRecordBatches,
                cursor.maximumCoalescedBytes,
                cursor.maximumCoalescedEntries,
                handles.maximumCachedLedgers,
                handles.maximumConcurrentOpens,
                index.maximumEncodedBytes,
                index.maximumLocators,
                pipeline.global.maximumBytes,
                pipeline.global.maximumEntries,
                pipeline.global.maximumGroups,
                pipeline.partition.maximumBytes,
                pipeline.partition.maximumEntries,
                pipeline.partition.maximumGroups,
                recovery.maximumElapsedNanos,
                recovery.maximumEncodedBytes,
                recovery.maximumEntries,
                replica.journal.maximumEncodedBytes,
                replica.journal.maximumRecords,
                replica.maximumApplyLagOffsets,
                replica.maximumUnappliedBytes,
                replica.maximumUnappliedNanos,
                rollover.maximumAgeNanos,
                rollover.maximumEncodedBytes,
                rollover.maximumEntries,
                SELECTION_ID,
                waiters.maximumCellWaiters,
                waiters.maximumPartitionWaiters).strip();
    }

    private static boolean atLeast(CapacityBudget larger, CapacityBudget smaller) {
        return larger.maximumGroups >= smaller.maximumGroups
                && larger.maximumEntries >= smaller.maximumEntries
                && larger.maximumBytes >= smaller.maximumBytes;
    }

    private static boolean atMost(CheckpointBudget lower, CheckpointBudget upper) {
        return lower.maximumRecordBatches <= upper.maximumRecordBatches
                && lower.maximumEncodedBytes <= upper.maximumEncodedBytes;
    }

    private static boolean atMost(IndexBudget lower, IndexBudget upper) {
        return lower.maximumEncodedBytes <= upper.maximumEncodedBytes && lower.maximumLocators <= upper.maximumLocators;
    }

    private static boolean atMost(ActiveTailBudget lower, ActiveTailBudget upper) {
        return lower.maximumLocators <= upper.maximumLocators
                && lower.maximumEncodedBytes <= upper.maximumEncodedBytes
                && lower.maximumAgeNanos <= upper.maximumAgeNanos;
    }

    private static boolean atMost(RecoveryBudget lower, RecoveryBudget upper) {
        return lower.maximumEntries <= upper.maximumEntries
                && lower.maximumEncodedBytes <= upper.maximumEncodedBytes
                && lower.maximumElapsedNanos <= upper.maximumElapsedNanos;
    }

    private static boolean atMost(CapacityBudget lower, CapacityBudget upper) {
        return lower.maximumGroups <= upper.maximumGroups
                && lower.maximumEntries <= upper.maximumEntries
                && lower.maximumBytes <= upper.maximumBytes;
    }

    private static boolean atMost(RolloverBudget lower, RolloverBudget upper) {
        return lower.maximumEncodedBytes <= upper.maximumEncodedBytes
                && lower.maximumEntries <= upper.maximumEntries
                && lower.maximumAgeNanos <= upper.maximumAgeNanos;
    }

    private static boolean atMost(HandleBudget lower, HandleBudget upper) {
        return lower.maximumCachedLedgers <= upper.maximumCachedLedgers
                && lower.maximumConcurrentOpens <= upper.maximumConcurrentOpens;
    }

    private static boolean atMost(ReplicaBudget lower, ReplicaBudget upper) {
        return lower.maximumApplyLagOffsets <= upper.maximumApplyLagOffsets
                && lower.maximumUnappliedBytes <= upper.maximumUnappliedBytes
                && lower.maximumUnappliedNanos <= upper.maximumUnappliedNanos;
    }

    private static boolean atMost(JournalBudget lower, JournalBudget upper) {
        return lower.maximumRecords <= upper.maximumRecords && lower.maximumEncodedBytes <= upper.maximumEncodedBytes;
    }

    private static boolean atMost(WaiterBudget lower, WaiterBudget upper) {
        return lower.maximumPartitionWaiters <= upper.maximumPartitionWaiters
                && lower.maximumCellWaiters <= upper.maximumCellWaiters;
    }

    private static boolean atMost(CursorBudget lower, CursorBudget upper) {
        return lower.maximumCoalescedEntries <= upper.maximumCoalescedEntries
                && lower.maximumCoalescedBytes <= upper.maximumCoalescedBytes;
    }

    public record CheckpointBudget(long maximumRecordBatches, long maximumEncodedBytes) {
        public CheckpointBudget {
            requirePositive(maximumRecordBatches, maximumEncodedBytes);
        }
    }

    public record IndexBudget(long maximumEncodedBytes, long maximumLocators) {
        public IndexBudget {
            requirePositive(maximumEncodedBytes, maximumLocators);
        }
    }

    public record ActiveTailBudget(long maximumLocators, long maximumEncodedBytes, long maximumAgeNanos) {
        public ActiveTailBudget {
            requirePositive(maximumLocators, maximumEncodedBytes, maximumAgeNanos);
        }
    }

    public record RecoveryBudget(long maximumEntries, long maximumEncodedBytes, long maximumElapsedNanos) {
        public RecoveryBudget {
            requirePositive(maximumEntries, maximumEncodedBytes, maximumElapsedNanos);
        }
    }

    public record CapacityBudget(long maximumGroups, long maximumEntries, long maximumBytes) {
        public CapacityBudget {
            requirePositive(maximumGroups, maximumEntries, maximumBytes);
        }

        private KafkaAppendCapacityBudgetV1 toCapacityBudget() {
            return new KafkaAppendCapacityBudgetV1(maximumGroups, maximumEntries, maximumBytes);
        }
    }

    public record PipelineBudget(CapacityBudget partition, CapacityBudget global) {
        public PipelineBudget {
            Objects.requireNonNull(partition, "partition");
            Objects.requireNonNull(global, "global");
        }
    }

    public record RolloverBudget(long maximumEncodedBytes, long maximumEntries, long maximumAgeNanos) {
        public RolloverBudget {
            requirePositive(maximumEncodedBytes, maximumEntries, maximumAgeNanos);
        }
    }

    public record HandleBudget(long maximumCachedLedgers, long maximumConcurrentOpens) {
        public HandleBudget {
            requirePositive(maximumCachedLedgers, maximumConcurrentOpens);
        }
    }

    public record JournalBudget(long maximumRecords, long maximumEncodedBytes) {
        public JournalBudget {
            requirePositive(maximumRecords, maximumEncodedBytes);
        }
    }

    public record ReplicaBudget(
            long maximumApplyLagOffsets,
            long maximumUnappliedBytes,
            long maximumUnappliedNanos,
            JournalBudget journal) {
        public ReplicaBudget {
            requirePositive(maximumApplyLagOffsets, maximumUnappliedBytes, maximumUnappliedNanos);
            Objects.requireNonNull(journal, "journal");
        }
    }

    public record WaiterBudget(long maximumPartitionWaiters, long maximumCellWaiters) {
        public WaiterBudget {
            requirePositive(maximumPartitionWaiters, maximumCellWaiters);
        }
    }

    public record CursorBudget(long maximumCoalescedEntries, long maximumCoalescedBytes) {
        public CursorBudget {
            requirePositive(maximumCoalescedEntries, maximumCoalescedBytes);
        }
    }

    private static void requirePositive(long... values) {
        for (long value : values) {
            if (value <= 0) {
                throw new IllegalArgumentException("operational maxima must be positive");
            }
        }
    }
}
