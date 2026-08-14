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
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperTargetedReaderV1Test {
    @Test
    void sealedRandomReadLoadsOneIndexBlockThenOneTargetDataEntry() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(session);
        KafkaBookKeeperTargetedReaderV1 reader = new KafkaBookKeeperTargetedReaderV1(session, 2);

        KafkaBookKeeperReadResultV1 first = reader.readOne(snapshot, 100, KafkaReadIsolationV1.REPLICA)
                .toCompletableFuture()
                .join();
        KafkaBookKeeperReadResultV1 second = reader.readOne(snapshot, 101, KafkaReadIsolationV1.REPLICA)
                .toCompletableFuture()
                .join();

        assertThat(first.outcome()).isEqualTo(KafkaBookKeeperReadOutcomeV1.FOUND);
        assertThat(first.batches())
                .singleElement()
                .extracting(KafkaBookKeeperReadBatchV1::entryId)
                .isEqualTo(1L);
        assertThat(second.batches())
                .singleElement()
                .extracting(KafkaBookKeeperReadBatchV1::entryId)
                .isEqualTo(2L);
        assertThat(session.readEntryIds).containsExactly(3L, 1L, 2L);
        assertThat(reader.cachedIndexBlockCount()).isEqualTo(1);
    }

    @Test
    void compactionGapSelectsTheFirstSuccessorWithoutReadingAnOldBlockOrWholeRun() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(session);
        KafkaBookKeeperTargetedReaderV1 reader = new KafkaBookKeeperTargetedReaderV1(session, 2);

        KafkaBookKeeperReadResultV1 result = reader.readOne(snapshot, 103, KafkaReadIsolationV1.REPLICA)
                .toCompletableFuture()
                .join();

        assertThat(result.batches())
                .singleElement()
                .extracting(KafkaBookKeeperReadBatchV1::startOffset, KafkaBookKeeperReadBatchV1::entryId)
                .containsExactly(105L, 4L);
        assertThat(session.readEntryIds).containsExactly(6L, 4L);
    }

    @Test
    void activeTailReadTargetsDataDirectlyWithoutAnIndexEntryRead() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.activeSnapshot(session);
        KafkaBookKeeperTargetedReaderV1 reader = new KafkaBookKeeperTargetedReaderV1(session, 1);

        KafkaBookKeeperReadResultV1 result = reader.readOne(snapshot, 101, KafkaReadIsolationV1.REPLICA)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperReadOutcomeV1.FOUND);
        assertThat(session.readEntryIds).containsExactly(2L);
        assertThat(reader.cachedIndexBlockCount()).isZero();
    }

    @Test
    void refusesDurableAsAReadBoundForReplicaAndConsumerIsolation() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(
                session,
                KafkaReadTestFixtures.root(9, 100, 110, 110, 107, 102, 101),
                com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1.empty());
        KafkaBookKeeperTargetedReaderV1 reader = new KafkaBookKeeperTargetedReaderV1(session, 2);

        assertThat(reader.readOne(snapshot, 107, KafkaReadIsolationV1.REPLICA)
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(KafkaBookKeeperReadOutcomeV1.END_OF_SNAPSHOT);
        assertThat(reader.readOne(snapshot, 102, KafkaReadIsolationV1.READ_UNCOMMITTED)
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(KafkaBookKeeperReadOutcomeV1.END_OF_SNAPSHOT);
        assertThat(reader.readOne(snapshot, 101, KafkaReadIsolationV1.READ_COMMITTED)
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(KafkaBookKeeperReadOutcomeV1.END_OF_SNAPSHOT);
        assertThat(reader.readOne(snapshot, 108, KafkaReadIsolationV1.REPLICA)
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(KafkaBookKeeperReadOutcomeV1.OFFSET_OUT_OF_RANGE);
        assertThat(session.readEntryIds).isEmpty();
    }

    @Test
    void distinguishesClosedProviderOutcomesAtTheIndexReadBoundary() {
        List<RunLedgerReadOutcomeV1> providerOutcomes = List.of(
                RunLedgerReadOutcomeV1.DEFINITIVELY_ABSENT,
                RunLedgerReadOutcomeV1.FENCED,
                RunLedgerReadOutcomeV1.PROVIDER_FAILURE);
        List<KafkaBookKeeperReadOutcomeV1> expected = List.of(
                KafkaBookKeeperReadOutcomeV1.DEFINITIVELY_ABSENT,
                KafkaBookKeeperReadOutcomeV1.FENCED,
                KafkaBookKeeperReadOutcomeV1.PROVIDER_FAILURE);

        for (int index = 0; index < providerOutcomes.size(); index++) {
            KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
            KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(session);
            session.readOverrides.put(3L, RunLedgerReadResultV1.withoutEntry(providerOutcomes.get(index)));

            KafkaBookKeeperReadResultV1 result = new KafkaBookKeeperTargetedReaderV1(session, 1)
                    .readOne(snapshot, 100, KafkaReadIsolationV1.REPLICA)
                    .toCompletableFuture()
                    .join();

            assertThat(result.outcome()).isEqualTo(expected.get(index));
            assertThat(result.batches()).isEmpty();
        }
    }

    @Test
    void rejectsEntryLocalNbke2CorruptionWithoutReadingAnotherEntry() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(session);
        byte[] corrupt = session.entries.get(1L).toByteArray();
        corrupt[corrupt.length - 1] ^= 1;
        session.entries.put(1L, CanonicalBytes.copyOf(corrupt));

        KafkaBookKeeperReadResultV1 result = new KafkaBookKeeperTargetedReaderV1(session, 1)
                .readOne(snapshot, 100, KafkaReadIsolationV1.REPLICA)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperReadOutcomeV1.CORRUPT);
        assertThat(session.readEntryIds).containsExactly(3L, 1L);
    }

    @Test
    void rejectsRawKafkaCrcCorruptionEvenWhenTheOuterNbke2FrameIsValid() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(session);
        byte[] corrupt = KafkaReadTestFixtures.raw(100, 0).toByteArray();
        corrupt[corrupt.length - 1] ^= 1;
        session.entries.put(
                1L,
                CanonicalBytes.copyOf(Nbke2CodecV1.encode(
                        KafkaReadTestFixtures.LEDGER_ID,
                        1,
                        KafkaReadTestFixtures.data(1, 100, CanonicalBytes.copyOf(corrupt)))));

        KafkaBookKeeperReadResultV1 result = new KafkaBookKeeperTargetedReaderV1(session, 1)
                .readOne(snapshot, 100, KafkaReadIsolationV1.REPLICA)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperReadOutcomeV1.CORRUPT);
        assertThat(result.detail()).contains("Kafka");
    }

    @Test
    void boundedLruEvictsOnlyDisposableValidatedIndexAcceleration() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperReadSnapshotV1 snapshot = KafkaReadTestFixtures.sealedSnapshot(session);
        KafkaBookKeeperTargetedReaderV1 reader = new KafkaBookKeeperTargetedReaderV1(session, 1);

        reader.readOne(snapshot, 100, KafkaReadIsolationV1.REPLICA)
                .toCompletableFuture()
                .join();
        reader.readOne(snapshot, 105, KafkaReadIsolationV1.REPLICA)
                .toCompletableFuture()
                .join();
        reader.readOne(snapshot, 101, KafkaReadIsolationV1.REPLICA)
                .toCompletableFuture()
                .join();

        assertThat(session.readEntryIds).containsExactly(3L, 1L, 6L, 4L, 3L, 2L);
        assertThat(reader.cachedIndexBlockCount()).isEqualTo(1);
    }
}
