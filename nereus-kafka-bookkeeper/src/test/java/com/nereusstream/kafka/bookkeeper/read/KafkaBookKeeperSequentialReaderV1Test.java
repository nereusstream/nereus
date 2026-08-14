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

package com.nereusstream.kafka.bookkeeper.read;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.commit.KafkaActiveTailStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBookKeeperActiveTailLocatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBookKeeperDataLocatorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2AppendGroupDescriptorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperSequentialReaderV1Test {
    @Test
    void byteBudgetStopsBeforeTheNextCompleteBatchAndReturnsADisposableCursor() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(session);
        KafkaBookKeeperTargetedReaderV1 reader = new KafkaBookKeeperTargetedReaderV1(session, 2);

        KafkaBookKeeperReadResultV1 result = reader.readSequential(
                        snapshot,
                        new KafkaBookKeeperSequentialReadRequestV1(
                                100, KafkaReadIsolationV1.REPLICA, 61, Optional.empty()))
                .toCompletableFuture()
                .join();

        assertThat(result.batches())
                .singleElement()
                .extracting(KafkaBookKeeperReadBatchV1::entryId)
                .isEqualTo(1L);
        assertThat(result.nextCursor()).get().satisfies(cursor -> {
            assertThat(cursor.nextKafkaOffset()).isEqualTo(101);
            assertThat(cursor.nextEntryId()).isEqualTo(2);
            assertThat(cursor.indexBlockIdentity()).isEqualTo(3);
        });
        assertThat(session.readEntryIds).containsExactly(3L, 1L);
    }

    @Test
    void exactCursorIsAcceptedOnlyAgainstTheFreshCoherentSnapshot() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(session);
        KafkaBookKeeperTargetedReaderV1 reader = new KafkaBookKeeperTargetedReaderV1(session, 2);
        KafkaBookKeeperReadCursorV1 cursor = reader.readSequential(
                        snapshot,
                        new KafkaBookKeeperSequentialReadRequestV1(
                                100, KafkaReadIsolationV1.REPLICA, 61, Optional.empty()))
                .toCompletableFuture()
                .join()
                .nextCursor()
                .orElseThrow();

        KafkaBookKeeperReadResultV1 continued = reader.readSequential(
                        snapshot,
                        new KafkaBookKeeperSequentialReadRequestV1(
                                101, KafkaReadIsolationV1.REPLICA, 61, Optional.of(cursor)))
                .toCompletableFuture()
                .join();

        assertThat(continued.suppliedCursorAccepted()).isTrue();
        assertThat(continued.batches())
                .singleElement()
                .extracting(KafkaBookKeeperReadBatchV1::entryId)
                .isEqualTo(2L);
    }

    @Test
    void staleCursorIsDiscardedAndThePackedLookupIsReplanned() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(session);
        KafkaBookKeeperTargetedReaderV1 reader = new KafkaBookKeeperTargetedReaderV1(session, 2);
        KafkaBookKeeperReadCursorV1 stale = new KafkaBookKeeperReadCursorV1(
                KafkaReadTestFixtures.binding(),
                snapshot.root().fence(),
                KafkaReadTestFixtures.SOURCE_GENERATION,
                3,
                1,
                1,
                100,
                snapshot.root().stateVersion());

        KafkaBookKeeperReadResultV1 result = reader.readSequential(
                        snapshot,
                        new KafkaBookKeeperSequentialReadRequestV1(
                                100, KafkaReadIsolationV1.REPLICA, 61, Optional.of(stale)))
                .toCompletableFuture()
                .join();

        assertThat(result.suppliedCursorAccepted()).isFalse();
        assertThat(result.batches())
                .singleElement()
                .extracting(KafkaBookKeeperReadBatchV1::entryId)
                .isEqualTo(1L);
        assertThat(session.readEntryIds).containsExactly(3L, 1L);
    }

    @Test
    void ownerFenceChangeDiscardsCursorEvenWhenRunAndOffsetStillMatch() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(session);
        KafkaPartitionFenceV1 fence = snapshot.root().fence();
        KafkaBookKeeperReadCursorV1 stale = new KafkaBookKeeperReadCursorV1(
                KafkaReadTestFixtures.binding(),
                new KafkaPartitionFenceV1(
                        fence.bindingId(),
                        fence.topicIncarnation(),
                        fence.partitionId(),
                        fence.bindingGeneration(),
                        fence.storageEpochId(),
                        fence.ownerEpoch() + 1,
                        fence.kafkaLeaderEpoch()),
                KafkaReadTestFixtures.SOURCE_GENERATION,
                3,
                0,
                1,
                100,
                snapshot.root().stateVersion());

        KafkaBookKeeperReadResultV1 result = new KafkaBookKeeperTargetedReaderV1(session, 2)
                .readSequential(
                        snapshot,
                        new KafkaBookKeeperSequentialReadRequestV1(
                                100, KafkaReadIsolationV1.REPLICA, 61, Optional.of(stale)))
                .toCompletableFuture()
                .join();

        assertThat(result.suppliedCursorAccepted()).isFalse();
        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperReadOutcomeV1.FOUND);
    }

    @Test
    void firstOversizedBatchIsReturnedWholeAndNeverSplit() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(session);

        KafkaBookKeeperReadResultV1 result = new KafkaBookKeeperTargetedReaderV1(session, 2)
                .readSequential(
                        snapshot,
                        new KafkaBookKeeperSequentialReadRequestV1(
                                100, KafkaReadIsolationV1.REPLICA, 1, Optional.empty()))
                .toCompletableFuture()
                .join();

        assertThat(result.batches())
                .singleElement()
                .extracting(batch -> batch.rawAssignedRecordBatch().length())
                .isEqualTo(61);
        assertThat(result.nextCursor()).isPresent();
    }

    @Test
    void sequentialReadCrossesIndexAndCompactionGapsUnderOneByteBudget() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(session);

        KafkaBookKeeperReadResultV1 result = new KafkaBookKeeperTargetedReaderV1(session, 2)
                .readSequential(
                        snapshot,
                        new KafkaBookKeeperSequentialReadRequestV1(
                                100, KafkaReadIsolationV1.REPLICA, 244, Optional.empty()))
                .toCompletableFuture()
                .join();

        assertThat(result.batches())
                .extracting(KafkaBookKeeperReadBatchV1::startOffset)
                .containsExactly(100L, 101L, 105L, 106L);
        assertThat(result.nextCursor()).isEmpty();
        assertThat(session.readEntryIds).containsExactly(3L, 1L, 2L, 6L, 4L, 5L);
    }

    @Test
    void readCommittedReturnsAbortedMetadataWithoutStorageFilteringData() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(
                session,
                KafkaReadTestFixtures.root(9, 100, 107, 107, 107, 107, 107),
                KafkaReadTestFixtures.abortedTransactionState());

        KafkaBookKeeperReadResultV1 result = new KafkaBookKeeperTargetedReaderV1(session, 2)
                .readSequential(
                        snapshot,
                        new KafkaBookKeeperSequentialReadRequestV1(
                                100, KafkaReadIsolationV1.READ_COMMITTED, 122, Optional.empty()))
                .toCompletableFuture()
                .join();

        assertThat(result.batches())
                .extracting(KafkaBookKeeperReadBatchV1::startOffset)
                .containsExactly(100L, 101L);
        assertThat(result.abortedTransactions()).singleElement().satisfies(aborted -> {
            assertThat(aborted.producerId()).isEqualTo(71);
            assertThat(aborted.firstOffset()).isEqualTo(100);
        });
    }

    @Test
    void refusesALocatorWhoseCompleteBatchCrossesTheCapturedUpperBound() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        CanonicalBytes raw = KafkaReadTestFixtures.raw(100, 2);
        Nbke2DataV1 data = new Nbke2DataV1(
                KafkaReadTestFixtures.binding(),
                100,
                2,
                0,
                1,
                new Id128(0, 101),
                new Id128(0, 201),
                Optional.of(new Nbke2AppendGroupDescriptorV1(100, 103, 1, 1, Sha256Digest.hash(raw))),
                raw);
        session.entries.put(1L, CanonicalBytes.copyOf(Nbke2CodecV1.encode(KafkaReadTestFixtures.LEDGER_ID, 1, data)));
        KafkaBookKeeperActiveTailLocatorV1 group = new KafkaBookKeeperActiveTailLocatorV1(
                100,
                103,
                KafkaReadTestFixtures.binding(),
                KafkaReadTestFixtures.handle(),
                1,
                1,
                1,
                300,
                Sha256Digest.hash(raw),
                new Id128(0, 101),
                new Id128(0, 201),
                List.of(new KafkaBookKeeperDataLocatorV1(100, 103, 1, 0, raw.length())));
        KafkaPackedBatchLocatorIndexV1 index =
                KafkaPackedBatchLocatorIndexV1.fromActiveTail(new KafkaActiveTailStateV1(100, 103, List.of(group)));
        KafkaBookKeeperReadRunV1 run = new KafkaBookKeeperReadRunV1(
                KafkaReadTestFixtures.binding(),
                KafkaReadTestFixtures.handle(),
                100,
                103,
                KafkaReadTestFixtures.SOURCE_GENERATION,
                Optional.of(index),
                Optional.empty());
        KafkaBookKeeperReadSnapshotV1 snapshot = new KafkaBookKeeperReadSnapshotV1(
                KafkaReadTestFixtures.root(3, 100, 103, 103, 103, 103, 102),
                new KafkaBookKeeperRunTableV1(List.of(run)),
                com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1.empty());

        KafkaBookKeeperReadResultV1 result = new KafkaBookKeeperTargetedReaderV1(session, 1)
                .readSequential(
                        snapshot,
                        new KafkaBookKeeperSequentialReadRequestV1(
                                100, KafkaReadIsolationV1.READ_COMMITTED, 1000, Optional.empty()))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperReadOutcomeV1.END_OF_SNAPSHOT);
        assertThat(session.readEntryIds).isEmpty();
    }
}
