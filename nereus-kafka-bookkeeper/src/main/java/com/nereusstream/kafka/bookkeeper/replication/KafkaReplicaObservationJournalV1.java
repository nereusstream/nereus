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

package com.nereusstream.kafka.bookkeeper.replication;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Bounded predecessor-chained journal kernel over an M6-owned sync-capable storage port. */
public final class KafkaReplicaObservationJournalV1 {
    private final KafkaReplicaObservationJournalStorageV1 storage;
    private final KafkaReplicaJournalBoundsV1 bounds;
    private KafkaPartitionFenceV1 expectedFence;
    private long appliedBaseOffset;
    private KafkaReplicaJournalHealthV1 health;
    private final List<KafkaReplicaObservationRecordV1> records = new ArrayList<>();
    private long durableThroughOffset;
    private Sha256Digest predecessorDigest = Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]);
    private long encodedBytes;
    private long lastObservedAtNanos;
    private boolean recovered;

    public KafkaReplicaObservationJournalV1(
            KafkaReplicaObservationJournalStorageV1 storage, KafkaReplicaJournalBoundsV1 bounds) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.bounds = Objects.requireNonNull(bounds, "bounds");
    }

    public synchronized KafkaReplicaObservationJournalSnapshotV1 recover(
            KafkaPartitionFenceV1 expectedFence, long appliedBaseOffset) {
        Objects.requireNonNull(expectedFence, "expectedFence");
        if (recovered || appliedBaseOffset < 0) {
            throw new IllegalStateException("journal recovery is one-shot and requires a non-negative Applied base");
        }
        recovered = true;
        this.expectedFence = expectedFence;
        this.appliedBaseOffset = appliedBaseOffset;
        this.durableThroughOffset = appliedBaseOffset;
        KafkaReplicaJournalStorageSnapshotV1 stored = storage.readBounded(bounds);
        health = stored.health();
        long chainEnd = -1;
        long lastStateVersion = -1;
        for (CanonicalBytes raw : stored.records()) {
            if (records.size() >= bounds.maximumRecords()
                    || raw.length() > KafkaReplicaObservationRecordCodecV1.FORMAT_MAX_RECORD_BYTES
                    || encodedBytes > bounds.maximumEncodedBytes() - raw.length()) {
                health = KafkaReplicaJournalHealthV1.OVER_BOUND;
                break;
            }
            KafkaReplicaObservationRecordV1 record;
            try {
                record = KafkaReplicaObservationRecordCodecV1.decode(raw);
            } catch (RuntimeException invalidRecord) {
                health = KafkaReplicaJournalHealthV1.CORRUPT;
                break;
            }
            boolean offsetContinuous = chainEnd < 0
                    || record.descriptor().startOffset() == chainEnd
                    || chainEnd < appliedBaseOffset && record.descriptor().startOffset() == appliedBaseOffset;
            if (record.ordinal() != records.size()
                    || !record.predecessorRecordDigest().equals(predecessorDigest)
                    || !record.descriptor().fence().equals(expectedFence)
                    || !offsetContinuous
                    || record.descriptor().validatedStateVersion() <= lastStateVersion
                    || record.observedAtNanos() < lastObservedAtNanos
                    || appliedBaseOffset > record.descriptor().startOffset()
                            && appliedBaseOffset < record.descriptor().endOffsetExclusive()) {
                health = KafkaReplicaJournalHealthV1.CORRUPT;
                break;
            }
            if (records.isEmpty() && record.descriptor().startOffset() > appliedBaseOffset) {
                health = KafkaReplicaJournalHealthV1.CORRUPT;
                break;
            }
            records.add(record);
            encodedBytes = Math.addExact(encodedBytes, raw.length());
            predecessorDigest = Sha256Digest.hash(raw);
            chainEnd = record.descriptor().endOffsetExclusive();
            lastStateVersion = record.descriptor().validatedStateVersion();
            lastObservedAtNanos = record.observedAtNanos();
            durableThroughOffset = Math.max(appliedBaseOffset, chainEnd);
        }
        return snapshot();
    }

    public synchronized KafkaReplicaObservationRecordV1 append(
            KafkaReplicaCommitDescriptorV1 descriptor, long observedAtNanos) {
        requireRecovered();
        Objects.requireNonNull(descriptor, "descriptor");
        if (health != KafkaReplicaJournalHealthV1.HEALTHY
                || !descriptor.fence().equals(expectedFence)
                || descriptor.startOffset() != durableThroughOffset
                || observedAtNanos < lastObservedAtNanos) {
            throw new IllegalStateException("journal cannot append across health, fence, or offset discontinuity");
        }
        KafkaReplicaObservationRecordV1 record =
                new KafkaReplicaObservationRecordV1(records.size(), predecessorDigest, observedAtNanos, descriptor);
        CanonicalBytes encoded = KafkaReplicaObservationRecordCodecV1.encode(record);
        if (records.size() >= bounds.maximumRecords()
                || encodedBytes > bounds.maximumEncodedBytes() - encoded.length()) {
            throw new IllegalStateException("journal append would exceed its hard record/byte bound");
        }
        KafkaReplicaJournalAppendProofV1 proof;
        try {
            proof = Objects.requireNonNull(
                    storage.appendAndSync(record.ordinal(), encoded), "null journal append proof");
        } catch (RuntimeException appendFailure) {
            health = KafkaReplicaJournalHealthV1.INDETERMINATE;
            throw appendFailure;
        }
        Sha256Digest digest = Sha256Digest.hash(encoded);
        if (proof.ordinal() != record.ordinal()
                || proof.encodedBytes() != encoded.length()
                || !proof.recordDigest().equals(digest)) {
            health = KafkaReplicaJournalHealthV1.INDETERMINATE;
            throw new IllegalStateException("journal sync proof differs from the exact KRO1 record");
        }
        records.add(record);
        encodedBytes = Math.addExact(encodedBytes, encoded.length());
        predecessorDigest = digest;
        durableThroughOffset = descriptor.endOffsetExclusive();
        lastObservedAtNanos = observedAtNanos;
        return record;
    }

    public synchronized KafkaReplicaObservationJournalSnapshotV1 snapshot() {
        requireRecovered();
        return new KafkaReplicaObservationJournalSnapshotV1(
                health, records, durableThroughOffset, records.size(), predecessorDigest, encodedBytes);
    }

    private void requireRecovered() {
        if (!recovered) {
            throw new IllegalStateException("journal must recover before use");
        }
    }
}
