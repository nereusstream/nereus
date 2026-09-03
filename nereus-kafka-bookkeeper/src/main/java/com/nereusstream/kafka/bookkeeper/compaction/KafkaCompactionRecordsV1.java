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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.record.CompressionType;

/** Closed, bounded records for deterministic M5-B Kafka semantic compaction. */
public final class KafkaCompactionRecordsV1 {
    public static final int MAX_BATCHES = 65_536;
    public static final int MAX_RECORDS = 1_048_576;
    public static final int MAX_KEYS = 1_048_576;
    public static final int MAX_HEADERS = 1_024;
    public static final int MAX_KEY_BYTES = 1_048_576;
    public static final int MAX_VALUE_BYTES = 32 * 1024 * 1024;
    public static final int MAX_BATCH_BYTES = 64 * 1024 * 1024;

    private KafkaCompactionRecordsV1() {}

    public enum CleanupPolicy {
        COMPACT,
        COMPACT_DELETE
    }

    public enum TransactionOutcome {
        OPEN,
        COMMITTED,
        ABORTED
    }

    public enum ControlKind {
        NONE,
        COMMIT,
        ABORT
    }

    public enum Disposition {
        KEEP_KEY_LATEST,
        KEEP_NULL_KEY,
        KEEP_TOMBSTONE_WITHIN_RETENTION,
        KEEP_TRANSACTION_OR_CONTROL,
        DROP_SUPERSEDED_VALUE,
        DROP_EXPIRED_TOMBSTONE,
        RETAIN_UNKNOWN
    }

    public record Caps(
            long maximumDirtyBytes,
            int maximumInputBatches,
            int maximumRecords,
            int maximumDistinctKeys,
            long maximumKeyBytes,
            long maximumOutputBytes,
            long maximumIndexBytes,
            int maximumTransactions,
            int maximumTombstones) {
        public Caps {
            if (maximumDirtyBytes <= 0
                    || maximumInputBatches <= 0
                    || maximumInputBatches > MAX_BATCHES
                    || maximumRecords <= 0
                    || maximumRecords > MAX_RECORDS
                    || maximumDistinctKeys <= 0
                    || maximumDistinctKeys > MAX_KEYS
                    || maximumKeyBytes <= 0
                    || maximumOutputBytes <= 0
                    || maximumIndexBytes <= 0
                    || maximumTransactions <= 0
                    || maximumTombstones <= 0) {
                throw new IllegalArgumentException("M5-B caps are outside their closed positive domain");
            }
        }
    }

    public record Policy(
            int kafkaFeatureLevel,
            byte messageMagic,
            CleanupPolicy cleanupPolicy,
            int minimumCleanableRatioPartsPerMillion,
            long deleteRetentionMs,
            long capturedNowMs,
            long policyGeneration,
            String rewritePolicyVersion,
            String nativeKafkaOracleVersion,
            CompressionType outputCompression,
            Caps caps) {
        public Policy {
            Objects.requireNonNull(cleanupPolicy, "cleanupPolicy");
            requireText(rewritePolicyVersion, "rewritePolicyVersion");
            requireText(nativeKafkaOracleVersion, "nativeKafkaOracleVersion");
            Objects.requireNonNull(outputCompression, "outputCompression");
            Objects.requireNonNull(caps, "caps");
            if (kafkaFeatureLevel < 0
                    || messageMagic != 2
                    || minimumCleanableRatioPartsPerMillion < 0
                    || minimumCleanableRatioPartsPerMillion > 1_000_000
                    || deleteRetentionMs < 0
                    || capturedNowMs < 0
                    || policyGeneration <= 0) {
                throw new IllegalArgumentException("M5-B policy is outside the accepted Kafka domain");
            }
        }
    }

    public record Frontiers(
            long logStartOffset,
            long durableOffset,
            long logEndOffset,
            long highWatermark,
            long lastStableOffset,
            long candidateStartOffset,
            long candidateEndOffsetExclusive) {
        public Frontiers {
            if (logStartOffset < 0
                    || candidateStartOffset < logStartOffset
                    || candidateEndOffsetExclusive <= candidateStartOffset
                    || durableOffset < candidateEndOffsetExclusive
                    || logEndOffset < durableOffset
                    || highWatermark < candidateEndOffsetExclusive
                    || highWatermark > logEndOffset
                    || lastStableOffset < candidateEndOffsetExclusive
                    || lastStableOffset > highWatermark) {
                throw new IllegalArgumentException("M5-B candidate crosses an unsafe captured frontier");
            }
        }
    }

    public record ProtocolRoots(
            Sha256Digest committedProducerStateSha256,
            Sha256Digest speculativeQueueSha256,
            Sha256Digest transactionIndexSha256,
            Sha256Digest abortedTransactionIndexSha256,
            Sha256Digest leaderEpochIndexSha256,
            Sha256Digest timestampIndexSha256,
            Sha256Digest recoveryCheckpointSha256,
            Sha256Digest activeTailSha256,
            Sha256Digest completeKeyDomainSha256) {
        public ProtocolRoots {
            requireDigest(committedProducerStateSha256, "committedProducerStateSha256");
            requireDigest(speculativeQueueSha256, "speculativeQueueSha256");
            requireDigest(transactionIndexSha256, "transactionIndexSha256");
            requireDigest(abortedTransactionIndexSha256, "abortedTransactionIndexSha256");
            requireDigest(leaderEpochIndexSha256, "leaderEpochIndexSha256");
            requireDigest(timestampIndexSha256, "timestampIndexSha256");
            requireDigest(recoveryCheckpointSha256, "recoveryCheckpointSha256");
            requireDigest(activeTailSha256, "activeTailSha256");
            requireDigest(completeKeyDomainSha256, "completeKeyDomainSha256");
        }
    }

    public record LatestKeyProof(
            CanonicalBytes key,
            long latestEligibleOffset,
            boolean completeDomain,
            boolean latestCommitted,
            boolean olderValueMayReappear,
            long tombstoneDeadlineMs) {
        public LatestKeyProof {
            key = copyBounded(key, MAX_KEY_BYTES, "key");
            if (key.isEmpty() || latestEligibleOffset < 0 || tombstoneDeadlineMs < 0) {
                throw new IllegalArgumentException("M5-B key proof is outside its domain");
            }
        }
    }

    public record TransactionRange(
            long producerId,
            long firstOffset,
            long endOffsetExclusive,
            TransactionOutcome outcome,
            int coordinatorEpoch) {
        public TransactionRange {
            Objects.requireNonNull(outcome, "outcome");
            if (producerId < 0 || firstOffset < 0 || endOffsetExclusive <= firstOffset || coordinatorEpoch < 0) {
                throw new IllegalArgumentException("M5-B transaction range is outside its domain");
            }
        }

        public boolean touches(long start, long end) {
            return firstOffset < end && endOffsetExclusive > start;
        }
    }

    public record LeaderEpochRange(int leaderEpoch, long startOffset, long endOffsetExclusive) {
        public LeaderEpochRange {
            if (leaderEpoch < 0 || startOffset < 0 || endOffsetExclusive <= startOffset) {
                throw new IllegalArgumentException("M5-B leader-epoch range is outside its domain");
            }
        }
    }

    public record HeaderValue(String key, Optional<CanonicalBytes> value) {
        public HeaderValue {
            requireText(key, "header key");
            value = Objects.requireNonNull(value, "value")
                    .map(item -> copyBounded(item, MAX_VALUE_BYTES, "header value"));
        }
    }

    public record RecordValue(
            long offset,
            int sequence,
            long timestamp,
            Optional<CanonicalBytes> key,
            Optional<CanonicalBytes> value,
            List<HeaderValue> headers) {
        public RecordValue {
            key = Objects.requireNonNull(key, "key").map(item -> copyBounded(item, MAX_KEY_BYTES, "record key"));
            value = Objects.requireNonNull(value, "value")
                    .map(item -> copyBounded(item, MAX_VALUE_BYTES, "record value"));
            headers = List.copyOf(Objects.requireNonNull(headers, "headers"));
            if (offset < 0 || sequence < -1 || timestamp < -1 || headers.size() > MAX_HEADERS) {
                throw new IllegalArgumentException("M5-B record is outside its bounded domain");
            }
        }

        public boolean tombstone() {
            return value.isEmpty();
        }
    }

    public record InputBatch(Sha256Digest sourceIdentitySha256, int sourceBatchOrdinal, CanonicalBytes canonicalBody) {
        public InputBatch {
            requireDigest(sourceIdentitySha256, "sourceIdentitySha256");
            canonicalBody = copyBounded(canonicalBody, MAX_BATCH_BYTES, "input batch");
            if (sourceBatchOrdinal < 0 || canonicalBody.isEmpty()) {
                throw new IllegalArgumentException("M5-B input batch locator/body is invalid");
            }
        }
    }

    public record ParsedBatch(
            CanonicalBytes canonicalBody,
            Sha256Digest bodySha256,
            long baseOffset,
            long lastOffset,
            int partitionLeaderEpoch,
            byte magic,
            CompressionType compressionType,
            org.apache.kafka.common.record.TimestampType timestampType,
            long maxTimestamp,
            long producerId,
            short producerEpoch,
            int baseSequence,
            boolean transactional,
            ControlKind controlKind,
            List<RecordValue> records) {
        public ParsedBatch {
            canonicalBody = copyBounded(canonicalBody, MAX_BATCH_BYTES, "RecordBatch body");
            requireDigest(bodySha256, "bodySha256");
            Objects.requireNonNull(compressionType, "compressionType");
            Objects.requireNonNull(timestampType, "timestampType");
            Objects.requireNonNull(controlKind, "controlKind");
            records = List.copyOf(Objects.requireNonNull(records, "records"));
            if (canonicalBody.isEmpty()
                    || !Sha256Digest.hash(canonicalBody).equals(bodySha256)
                    || baseOffset < 0
                    || lastOffset < baseOffset
                    || partitionLeaderEpoch < 0
                    || magic != 2
                    || maxTimestamp < -1
                    || records.size() > MAX_RECORDS) {
                throw new IllegalArgumentException("M5-B parsed batch is outside its canonical domain");
            }
            long previous = -1;
            for (RecordValue record : records) {
                if (record.offset() < baseOffset || record.offset() > lastOffset || record.offset() <= previous) {
                    throw new IllegalArgumentException("M5-B batch records are not strictly offset ordered");
                }
                previous = record.offset();
            }
            if (controlKind != ControlKind.NONE && (!transactional || records.size() != 1)) {
                throw new IllegalArgumentException("M5-B control marker shape is invalid");
            }
        }

        public long endOffsetExclusive() {
            return Math.addExact(lastOffset, 1);
        }
    }

    public record CompactionPlan(
            MaterializationSourceCut sourceCut,
            Policy policy,
            Frontiers frontiers,
            ProtocolRoots protocolRoots,
            List<InputBatch> inputBatches,
            List<LatestKeyProof> keyProofs,
            List<TransactionRange> transactions,
            List<LeaderEpochRange> leaderEpochs,
            List<Long> undecidableOffsets,
            List<Long> recoveryRequiredOffsets) {
        public CompactionPlan {
            Objects.requireNonNull(sourceCut, "sourceCut");
            Objects.requireNonNull(policy, "policy");
            Objects.requireNonNull(frontiers, "frontiers");
            Objects.requireNonNull(protocolRoots, "protocolRoots");
            inputBatches = List.copyOf(Objects.requireNonNull(inputBatches, "inputBatches"));
            keyProofs = List.copyOf(Objects.requireNonNull(keyProofs, "keyProofs"));
            transactions = List.copyOf(Objects.requireNonNull(transactions, "transactions"));
            leaderEpochs = List.copyOf(Objects.requireNonNull(leaderEpochs, "leaderEpochs"));
            undecidableOffsets = List.copyOf(Objects.requireNonNull(undecidableOffsets, "undecidableOffsets"));
            recoveryRequiredOffsets =
                    List.copyOf(Objects.requireNonNull(recoveryRequiredOffsets, "recoveryRequiredOffsets"));
            if (sourceCut.coverage().domain() != PositionDomain.KAFKA_OFFSET
                    || sourceCut.coverage().inclusiveStart() != frontiers.candidateStartOffset()
                    || sourceCut.coverage().exclusiveEnd() != frontiers.candidateEndOffsetExclusive()
                    || sourceCut.durableFrontier() != frontiers.durableOffset()
                    || sourceCut.logEndFrontier() != frontiers.logEndOffset()
                    || sourceCut.highWatermark() != frontiers.highWatermark()
                    || sourceCut.lastStableFrontier() != frontiers.lastStableOffset()) {
                throw new IllegalArgumentException("M5-B frontiers differ from the M5-A source cut");
            }
            if (inputBatches.isEmpty()
                    || inputBatches.size() > Math.min(policy.caps().maximumInputBatches(), MAX_BATCHES)) {
                throw new IllegalArgumentException("M5-B input batch count exceeds its cap");
            }
            List<Sha256Digest> sourceIdentities = sourceCut.sources().stream()
                    .map(value -> value.sourceIdentitySha256())
                    .toList();
            if (inputBatches.stream().anyMatch(value -> !sourceIdentities.contains(value.sourceIdentitySha256()))
                    || inputBatches.stream()
                                    .map(value ->
                                            value.sourceIdentitySha256().toHex() + ":" + value.sourceBatchOrdinal())
                                    .distinct()
                                    .count()
                            != inputBatches.size()) {
                throw new IllegalArgumentException("M5-B input batch locator is duplicate or outside the source cut");
            }
            requireCanonical(
                    keyProofs, Comparator.comparing(value -> value.key().toHex()), "key proofs");
            requireCanonical(
                    transactions,
                    Comparator.comparingLong(TransactionRange::firstOffset)
                            .thenComparingLong(TransactionRange::producerId),
                    "transactions");
            requireCanonical(leaderEpochs, Comparator.comparingInt(LeaderEpochRange::leaderEpoch), "leader epochs");
            requireSortedUniqueLongs(undecidableOffsets, "undecidable offsets");
            requireSortedUniqueLongs(recoveryRequiredOffsets, "recovery-required offsets");
            if (keyProofs.size() > policy.caps().maximumDistinctKeys()
                    || transactions.size() > policy.caps().maximumTransactions()) {
                throw new IllegalArgumentException("M5-B plan exceeds its key/transaction cap");
            }
            for (TransactionRange transaction : transactions) {
                if (transaction.outcome() == TransactionOutcome.OPEN
                        && transaction.touches(
                                frontiers.candidateStartOffset(), frontiers.candidateEndOffsetExclusive())) {
                    throw new IllegalArgumentException("M5-B candidate touches an open transaction");
                }
            }
        }
    }

    public record DispositionRow(
            long offset, Sha256Digest inputBatchSha256, Disposition disposition, Sha256Digest recordIdentitySha256) {
        public DispositionRow {
            if (offset < 0) {
                throw new IllegalArgumentException("M5-B disposition offset is negative");
            }
            requireDigest(inputBatchSha256, "inputBatchSha256");
            Objects.requireNonNull(disposition, "disposition");
            requireDigest(recordIdentitySha256, "recordIdentitySha256");
        }

        public boolean retained() {
            return switch (disposition) {
                case KEEP_KEY_LATEST,
                        KEEP_NULL_KEY,
                        KEEP_TOMBSTONE_WITHIN_RETENTION,
                        KEEP_TRANSACTION_OR_CONTROL,
                        RETAIN_UNKNOWN -> true;
                case DROP_SUPERSEDED_VALUE, DROP_EXPIRED_TOMBSTONE -> false;
            };
        }
    }

    public record Gap(long inclusiveStart, long exclusiveEnd) {
        public Gap {
            if (inclusiveStart < 0 || exclusiveEnd <= inclusiveStart) {
                throw new IllegalArgumentException("M5-B gap is empty or reversed");
            }
        }

        public boolean contains(long offset) {
            return inclusiveStart <= offset && offset < exclusiveEnd;
        }
    }

    private static CanonicalBytes copyBounded(CanonicalBytes value, int maximum, String label) {
        Objects.requireNonNull(value, label);
        if (value.length() > maximum) {
            throw new IllegalArgumentException(label + " exceeds its M5-B cap");
        }
        return CanonicalBytes.copyOf(value.toByteArray());
    }

    private static <T> void requireCanonical(List<T> values, Comparator<T> comparator, String label) {
        if (!values.equals(values.stream().sorted(comparator).toList())
                || values.stream().distinct().count() != values.size()) {
            throw new IllegalArgumentException("M5-B " + label + " are not sorted unique");
        }
    }

    private static void requireSortedUniqueLongs(List<Long> values, String label) {
        requireCanonical(values, Long::compare, label);
        if (values.stream().anyMatch(value -> value == null || value < 0)) {
            throw new IllegalArgumentException("M5-B " + label + " contain an invalid value");
        }
    }

    static void requireDigest(Sha256Digest value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isZero()) {
            throw new IllegalArgumentException(label + " is the zero digest");
        }
    }

    private static void requireText(String value, String label) {
        if (value == null || value.isBlank() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(label + " is empty or contains NUL");
        }
    }
}
