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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import org.junit.jupiter.api.Test;

class KafkaReplicaObservationJournalV1Test {
    private static final KafkaReplicaJournalBoundsV1 LARGE_BOUNDS = new KafkaReplicaJournalBoundsV1(100, 1_000_000);

    @Test
    void appendsOnlyAfterExactSyncProofAndRecoversTheSameContiguousPrefix() {
        KafkaReplicationTestFixtures.FakeJournalStorage storage = new KafkaReplicationTestFixtures.FakeJournalStorage();
        KafkaReplicaObservationJournalV1 journal = new KafkaReplicaObservationJournalV1(storage, LARGE_BOUNDS);
        journal.recover(KafkaReplicationTestFixtures.fence(), 100);
        journal.append(KafkaReplicationTestFixtures.descriptor(100, 101, 2), 10);
        journal.append(KafkaReplicationTestFixtures.descriptor(101, 103, 3), 20);

        KafkaReplicaObservationJournalSnapshotV1 live = journal.snapshot();
        KafkaReplicaObservationJournalSnapshotV1 recovered = new KafkaReplicaObservationJournalV1(storage, LARGE_BOUNDS)
                .recover(KafkaReplicationTestFixtures.fence(), 100);

        assertThat(live.health()).isEqualTo(KafkaReplicaJournalHealthV1.HEALTHY);
        assertThat(live.durableThroughOffset()).isEqualTo(103);
        assertThat(recovered).isEqualTo(live);
        assertThat(storage.appendCalls).isEqualTo(2);
    }

    @Test
    void substitutedSyncProofCannotAdvanceTheInMemoryJournalCut() {
        KafkaReplicationTestFixtures.FakeJournalStorage storage = new KafkaReplicationTestFixtures.FakeJournalStorage();
        storage.proofOverride = new KafkaReplicaJournalAppendProofV1(0, 1, KafkaReplicationTestFixtures.digest(99));
        KafkaReplicaObservationJournalV1 journal = new KafkaReplicaObservationJournalV1(storage, LARGE_BOUNDS);
        journal.recover(KafkaReplicationTestFixtures.fence(), 100);

        assertThatThrownBy(() -> journal.append(KafkaReplicationTestFixtures.descriptor(100, 101, 2), 10))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sync proof");
        assertThat(journal.snapshot().durableThroughOffset()).isEqualTo(100);
        assertThat(journal.snapshot().records()).isEmpty();
        assertThat(journal.snapshot().health()).isEqualTo(KafkaReplicaJournalHealthV1.INDETERMINATE);
        assertThat(journal.snapshot().acceptsAppend()).isFalse();
        assertThat(storage.records).hasSize(1);
        KafkaReplicaObservationJournalSnapshotV1 reread = new KafkaReplicaObservationJournalV1(storage, LARGE_BOUNDS)
                .recover(KafkaReplicationTestFixtures.fence(), 100);
        assertThat(reread.health()).isEqualTo(KafkaReplicaJournalHealthV1.HEALTHY);
        assertThat(reread.durableThroughOffset()).isEqualTo(101);

        KafkaReplicationTestFixtures.FakeJournalStorage exceptional =
                new KafkaReplicationTestFixtures.FakeJournalStorage();
        exceptional.appendFailure = new IllegalStateException("response lost after local sync");
        KafkaReplicaObservationJournalV1 exceptionalJournal =
                new KafkaReplicaObservationJournalV1(exceptional, LARGE_BOUNDS);
        exceptionalJournal.recover(KafkaReplicationTestFixtures.fence(), 100);
        assertThatThrownBy(() -> exceptionalJournal.append(KafkaReplicationTestFixtures.descriptor(100, 101, 2), 10))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("response lost");
        assertThat(exceptionalJournal.snapshot().health()).isEqualTo(KafkaReplicaJournalHealthV1.INDETERMINATE);
        assertThat(new KafkaReplicaObservationJournalV1(exceptional, LARGE_BOUNDS)
                        .recover(KafkaReplicationTestFixtures.fence(), 100)
                        .durableThroughOffset())
                .isEqualTo(101);
    }

    @Test
    void corruptTailRollsBackToTheHighestContiguousSurvivingRecord() {
        KafkaReplicationTestFixtures.FakeJournalStorage storage = chain(2);
        byte[] corrupt = storage.records.get(1).toByteArray();
        corrupt[20] ^= 1;
        storage.records.set(1, CanonicalBytes.copyOf(corrupt));

        KafkaReplicaObservationJournalSnapshotV1 recovered = new KafkaReplicaObservationJournalV1(storage, LARGE_BOUNDS)
                .recover(KafkaReplicationTestFixtures.fence(), 100);

        assertThat(recovered.health()).isEqualTo(KafkaReplicaJournalHealthV1.CORRUPT);
        assertThat(recovered.records()).hasSize(1);
        assertThat(recovered.durableThroughOffset()).isEqualTo(101);
        assertThat(recovered.acceptsAppend()).isFalse();
    }

    @Test
    void storageReportedTruncationPreservesPrefixButBlocksFurtherAppend() {
        KafkaReplicationTestFixtures.FakeJournalStorage storage = chain(1);
        storage.health = KafkaReplicaJournalHealthV1.TRUNCATED;
        KafkaReplicaObservationJournalV1 journal = new KafkaReplicaObservationJournalV1(storage, LARGE_BOUNDS);

        KafkaReplicaObservationJournalSnapshotV1 recovered = journal.recover(KafkaReplicationTestFixtures.fence(), 100);

        assertThat(recovered.health()).isEqualTo(KafkaReplicaJournalHealthV1.TRUNCATED);
        assertThat(recovered.durableThroughOffset()).isEqualTo(101);
        assertThatThrownBy(() -> journal.append(KafkaReplicationTestFixtures.descriptor(101, 102, 3), 20))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void ordinalPredecessorFenceAndOffsetSubstitutionEachStopThePrefix() {
        KafkaReplicationTestFixtures.FakeJournalStorage badPredecessor = chain(1);
        KafkaReplicaObservationRecordV1 replacement = new KafkaReplicaObservationRecordV1(
                1, KafkaReplicationTestFixtures.digest(98), 20, KafkaReplicationTestFixtures.descriptor(101, 102, 3));
        badPredecessor.records.add(KafkaReplicaObservationRecordCodecV1.encode(replacement));
        KafkaReplicaObservationJournalSnapshotV1 predecessorResult = new KafkaReplicaObservationJournalV1(
                        badPredecessor, LARGE_BOUNDS)
                .recover(KafkaReplicationTestFixtures.fence(), 100);

        KafkaReplicationTestFixtures.FakeJournalStorage gap = chain(1);
        KafkaReplicaObservationRecordV1 gapRecord = new KafkaReplicaObservationRecordV1(
                1, Sha256Digest.hash(gap.records.get(0)), 20, KafkaReplicationTestFixtures.descriptor(102, 103, 3));
        gap.records.add(KafkaReplicaObservationRecordCodecV1.encode(gapRecord));
        KafkaReplicaObservationJournalSnapshotV1 gapResult = new KafkaReplicaObservationJournalV1(gap, LARGE_BOUNDS)
                .recover(KafkaReplicationTestFixtures.fence(), 100);

        KafkaReplicationTestFixtures.FakeJournalStorage timeRegression = chain(2);
        KafkaReplicaObservationRecordV1 olderTime = new KafkaReplicaObservationRecordV1(
                2,
                Sha256Digest.hash(timeRegression.records.get(1)),
                5,
                KafkaReplicationTestFixtures.descriptor(102, 103, 4));
        timeRegression.records.add(KafkaReplicaObservationRecordCodecV1.encode(olderTime));
        KafkaReplicaObservationJournalSnapshotV1 timeResult = new KafkaReplicaObservationJournalV1(
                        timeRegression, LARGE_BOUNDS)
                .recover(KafkaReplicationTestFixtures.fence(), 100);

        assertThat(predecessorResult.health()).isEqualTo(KafkaReplicaJournalHealthV1.CORRUPT);
        assertThat(predecessorResult.records()).hasSize(1);
        assertThat(gapResult.health()).isEqualTo(KafkaReplicaJournalHealthV1.CORRUPT);
        assertThat(gapResult.records()).hasSize(1);
        assertThat(timeResult.health()).isEqualTo(KafkaReplicaJournalHealthV1.CORRUPT);
        assertThat(timeResult.records()).hasSize(2);
    }

    @Test
    void hardRecordAndEncodedByteBoundsFailBeforeAnotherAppend() {
        KafkaReplicationTestFixtures.FakeJournalStorage recordStorage =
                new KafkaReplicationTestFixtures.FakeJournalStorage();
        KafkaReplicaObservationJournalV1 recordBound =
                new KafkaReplicaObservationJournalV1(recordStorage, new KafkaReplicaJournalBoundsV1(1, 1_000_000));
        recordBound.recover(KafkaReplicationTestFixtures.fence(), 100);
        recordBound.append(KafkaReplicationTestFixtures.descriptor(100, 101, 2), 10);

        assertThatThrownBy(() -> recordBound.append(KafkaReplicationTestFixtures.descriptor(101, 102, 3), 20))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("hard record/byte bound");

        KafkaReplicationTestFixtures.FakeJournalStorage byteStorage =
                new KafkaReplicationTestFixtures.FakeJournalStorage();
        KafkaReplicaObservationJournalV1 byteBound =
                new KafkaReplicaObservationJournalV1(byteStorage, new KafkaReplicaJournalBoundsV1(10, 1));
        byteBound.recover(KafkaReplicationTestFixtures.fence(), 100);
        assertThatThrownBy(() -> byteBound.append(KafkaReplicationTestFixtures.descriptor(100, 101, 2), 10))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void appliedBaseMayCompactAProvenOldPrefixWithoutBreakingTheDigestChain() {
        KafkaReplicationTestFixtures.FakeJournalStorage storage = chain(2);
        KafkaReplicaObservationJournalV1 journal = new KafkaReplicaObservationJournalV1(storage, LARGE_BOUNDS);
        KafkaReplicaObservationJournalSnapshotV1 recovered = journal.recover(KafkaReplicationTestFixtures.fence(), 105);

        journal.append(KafkaReplicationTestFixtures.descriptor(105, 106, 4), 30);
        KafkaReplicaObservationJournalSnapshotV1 restarted = new KafkaReplicaObservationJournalV1(storage, LARGE_BOUNDS)
                .recover(KafkaReplicationTestFixtures.fence(), 105);

        assertThat(recovered.durableThroughOffset()).isEqualTo(105);
        assertThat(restarted.health()).isEqualTo(KafkaReplicaJournalHealthV1.HEALTHY);
        assertThat(restarted.durableThroughOffset()).isEqualTo(106);
        assertThat(restarted.records()).hasSize(3);
    }

    private static KafkaReplicationTestFixtures.FakeJournalStorage chain(int count) {
        KafkaReplicationTestFixtures.FakeJournalStorage storage = new KafkaReplicationTestFixtures.FakeJournalStorage();
        Sha256Digest predecessor = Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]);
        for (int index = 0; index < count; index++) {
            KafkaReplicaObservationRecordV1 record = new KafkaReplicaObservationRecordV1(
                    index,
                    predecessor,
                    10L + index,
                    KafkaReplicationTestFixtures.descriptor(100L + index, 101L + index, 2L + index));
            CanonicalBytes encoded = KafkaReplicaObservationRecordCodecV1.encode(record);
            storage.records.add(encoded);
            predecessor = Sha256Digest.hash(encoded);
        }
        return storage;
    }
}
