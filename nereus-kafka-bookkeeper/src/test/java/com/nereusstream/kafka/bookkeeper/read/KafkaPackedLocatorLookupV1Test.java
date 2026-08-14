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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.kafka.bookkeeper.commit.KafkaActiveTailStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2BatchLocatorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2IndexDirectoryEntryV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RangeIndexBlockV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class KafkaPackedLocatorLookupV1Test {
    @Test
    void acceptsFloorOnlyWhenOneCompleteBatchCoversTheRequestedOffset() {
        Nbke2RangeIndexBlockV1 block = new Nbke2RangeIndexBlockV1(
                KafkaReadTestFixtures.binding(),
                100,
                1,
                105,
                1,
                2,
                -1,
                4,
                List.of(new Nbke2BatchLocatorV1(0, 3, 0, 0, 0, 61, 0), new Nbke2BatchLocatorV1(3, 2, 1, 1, 0, 61, 0)));
        KafkaPackedBatchLocatorIndexV1 index = KafkaPackedBatchLocatorIndexV1.fromRangeIndexBlock(3, block);

        assertThat(index.floorOrSuccessor(102))
                .get()
                .extracting(KafkaPackedBatchLocatorV1::entryId)
                .isEqualTo(1L);
        assertThat(index.floorOrSuccessor(103))
                .get()
                .extracting(KafkaPackedBatchLocatorV1::entryId)
                .isEqualTo(2L);
        assertThat(index.floorOrSuccessor(105)).isEmpty();
    }

    @Test
    void directorySkipsACompactionGapToTheFirstSuccessorBlock() {
        KafkaPackedIndexDirectoryV1 directory = new KafkaPackedIndexDirectoryV1(
                List.of(new Nbke2IndexDirectoryEntryV1(3, 100, 102), new Nbke2IndexDirectoryEntryV1(6, 105, 107)));

        assertThat(directory.floorOrSuccessor(101))
                .get()
                .extracting(KafkaIndexBlockPointerV1::indexBlockEntryId)
                .isEqualTo(3L);
        assertThat(directory.floorOrSuccessor(103))
                .get()
                .extracting(KafkaIndexBlockPointerV1::indexBlockEntryId)
                .isEqualTo(6L);
        assertThat(directory.floorOrSuccessor(107)).isEmpty();
    }

    @Test
    void directoryResolvesExactCursorIdentityAndOrdinalSuccessor() {
        KafkaPackedIndexDirectoryV1 directory = new KafkaPackedIndexDirectoryV1(
                List.of(new Nbke2IndexDirectoryEntryV1(3, 100, 102), new Nbke2IndexDirectoryEntryV1(6, 105, 107)));

        assertThat(directory.findByEntryId(6))
                .get()
                .extracting(KafkaIndexBlockPointerV1::ordinal)
                .isEqualTo(1);
        assertThat(directory.findByEntryId(5)).isEmpty();
        assertThat(directory.successor(0))
                .get()
                .extracting(KafkaIndexBlockPointerV1::indexBlockEntryId)
                .isEqualTo(6L);
    }

    @Test
    void activeTailFlattensPerRecordBatchMembersIntoPrimitiveRows() {
        KafkaActiveTailStateV1 tail = new KafkaActiveTailStateV1(
                100,
                102,
                List.of(KafkaReadTestFixtures.activeGroup(1, 100), KafkaReadTestFixtures.activeGroup(2, 101)));
        KafkaPackedBatchLocatorIndexV1 index = KafkaPackedBatchLocatorIndexV1.fromActiveTail(tail);

        assertThat(index.size()).isEqualTo(2);
        assertThat(index.at(1))
                .extracting(
                        KafkaPackedBatchLocatorV1::sourceKind,
                        KafkaPackedBatchLocatorV1::indexIdentity,
                        KafkaPackedBatchLocatorV1::entryId,
                        KafkaPackedBatchLocatorV1::startOffset)
                .containsExactly(KafkaLocatorSourceKindV1.ACTIVE_TAIL, -1L, 2L, 101L);
    }

    @Test
    void runTableSkipsAWholeCompactedRunGap() {
        KafkaPackedBatchLocatorIndexV1 firstIndex = KafkaPackedBatchLocatorIndexV1.fromActiveTail(
                new KafkaActiveTailStateV1(100, 101, List.of(KafkaReadTestFixtures.activeGroup(1, 100))));
        KafkaPackedBatchLocatorIndexV1 secondIndex = KafkaPackedBatchLocatorIndexV1.fromActiveTail(
                new KafkaActiveTailStateV1(105, 106, List.of(KafkaReadTestFixtures.activeGroup(4, 105))));
        KafkaBookKeeperReadRunV1 first = new KafkaBookKeeperReadRunV1(
                KafkaReadTestFixtures.binding(),
                KafkaReadTestFixtures.handle(),
                100,
                101,
                KafkaReadTestFixtures.SOURCE_GENERATION,
                Optional.of(firstIndex),
                Optional.empty());
        KafkaBookKeeperReadRunV1 second = new KafkaBookKeeperReadRunV1(
                KafkaRunTestFixtures.binding(7, 11, 5),
                new com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1(
                        KafkaReadTestFixtures.binding().providerScopeId(),
                        KafkaRunTestFixtures.binding(7, 11, 5).runId(),
                        new com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity(48),
                        KafkaRunTestFixtures.digest(30)),
                105,
                106,
                KafkaReadTestFixtures.SOURCE_GENERATION,
                Optional.of(secondIndex),
                Optional.empty());

        assertThat(new KafkaBookKeeperRunTableV1(List.of(first, second)).floorOrSuccessor(103))
                .contains(second);
    }

    @Test
    void rejectsDirectoryAndPackedIndexRegression() {
        assertThatThrownBy(() -> new KafkaPackedIndexDirectoryV1(List.of(
                        new Nbke2IndexDirectoryEntryV1(6, 105, 107), new Nbke2IndexDirectoryEntryV1(3, 108, 109))))
                .isInstanceOf(IllegalArgumentException.class);
        KafkaPackedBatchLocatorIndexV1 index =
                KafkaPackedBatchLocatorIndexV1.fromRangeIndexBlock(3, KafkaReadTestFixtures.block(100, 1, 3, -1, 4));
        assertThatThrownBy(() -> index.at(2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void snapshotRejectsAnActiveRunFromAnotherPublishedGeneration() {
        assertThat(new KafkaBookKeeperRunTableV1(List.of()).floorOrSuccessor(0)).isEmpty();
        KafkaBookKeeperReadRunV1 active = new KafkaBookKeeperReadRunV1(
                KafkaReadTestFixtures.binding(),
                KafkaReadTestFixtures.handle(),
                100,
                101,
                KafkaReadTestFixtures.SOURCE_GENERATION + 1,
                Optional.of(KafkaPackedBatchLocatorIndexV1.fromActiveTail(
                        new KafkaActiveTailStateV1(100, 101, List.of(KafkaReadTestFixtures.activeGroup(1, 100))))),
                Optional.empty());

        assertThatThrownBy(() -> new KafkaBookKeeperReadSnapshotV1(
                        KafkaReadTestFixtures.root(1, 100, 101, 101, 101, 101, 101),
                        new KafkaBookKeeperRunTableV1(List.of(active)),
                        com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
