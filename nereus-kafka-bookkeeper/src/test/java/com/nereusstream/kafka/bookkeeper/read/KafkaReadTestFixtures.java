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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.commit.KafkaActiveTailStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBookKeeperActiveTailLocatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBookKeeperDataLocatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2AppendGroupDescriptorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2BatchLocatorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2IndexDirectoryEntryV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RangeIndexBlockV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFrontiersV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionStateReferenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionStateReferencesV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.zip.CRC32C;

final class KafkaReadTestFixtures {
    static final long LEDGER_ID = 47;
    static final long SOURCE_GENERATION = 4;
    static final int RAW_BYTES = 61;

    private KafkaReadTestFixtures() {}

    static Nbke2RunBindingV1 binding() {
        return KafkaRunTestFixtures.binding(6, 11, 5);
    }

    static RunLedgerHandleV1 handle() {
        return new RunLedgerHandleV1(
                binding().providerScopeId(),
                binding().runId(),
                new BookKeeperLedgerIdentity(LEDGER_ID),
                KafkaRunTestFixtures.digest(20));
    }

    static KafkaPartitionFenceV1 fence() {
        return new KafkaPartitionFenceV1(
                binding().bindingId(),
                binding().topicIncarnation(),
                binding().partitionId(),
                13,
                binding().storageEpochId(),
                binding().creatorOwnerEpoch(),
                binding().kafkaLeaderEpoch());
    }

    static KafkaPartitionProtocolStateV1 root(
            long stateVersion,
            long trimStart,
            long allocated,
            long durable,
            long readable,
            long highWatermark,
            long lastStableOffset) {
        return new KafkaPartitionProtocolStateV1(
                fence(),
                stateVersion,
                new KafkaPartitionFrontiersV1(trimStart, allocated, durable, readable, highWatermark, lastStableOffset),
                references());
    }

    static KafkaPartitionStateReferencesV1 references() {
        return new KafkaPartitionStateReferencesV1(
                reference(1, 21),
                reference(SOURCE_GENERATION, 22),
                reference(2, 23),
                reference(2, 24),
                reference(2, 25),
                reference(2, 26),
                reference(2, 27),
                reference(2, 28),
                reference(2, 29));
    }

    static KafkaPartitionStateReferenceV1 reference(long generation, int digestByte) {
        return new KafkaPartitionStateReferenceV1(generation, KafkaRunTestFixtures.digest(digestByte));
    }

    static KafkaBookKeeperReadSnapshotV1 sealedSnapshot(KafkaRunTestFixtures.FakeSession session) {
        return sealedSnapshot(session, root(9, 100, 107, 107, 107, 107, 107), KafkaTransactionStateV1.empty());
    }

    static KafkaBookKeeperReadSnapshotV1 sealedSnapshot(
            KafkaRunTestFixtures.FakeSession session,
            KafkaPartitionProtocolStateV1 root,
            KafkaTransactionStateV1 transactions) {
        installData(session, 1, 100);
        installData(session, 2, 101);
        installData(session, 4, 105);
        installData(session, 5, 106);
        Nbke2RangeIndexBlockV1 first = block(100, 1, 3, -1, 4);
        Nbke2RangeIndexBlockV1 second = block(105, 4, 6, 3, 7);
        session.entries.put(3L, CanonicalBytes.copyOf(Nbke2CodecV1.encode(LEDGER_ID, 3, first)));
        session.entries.put(6L, CanonicalBytes.copyOf(Nbke2CodecV1.encode(LEDGER_ID, 6, second)));
        KafkaPackedIndexDirectoryV1 directory = new KafkaPackedIndexDirectoryV1(
                List.of(new Nbke2IndexDirectoryEntryV1(3, 100, 102), new Nbke2IndexDirectoryEntryV1(6, 105, 107)));
        KafkaBookKeeperReadRunV1 run = new KafkaBookKeeperReadRunV1(
                binding(), handle(), 100, 107, SOURCE_GENERATION, Optional.empty(), Optional.of(directory));
        return new KafkaBookKeeperReadSnapshotV1(root, new KafkaBookKeeperRunTableV1(List.of(run)), transactions);
    }

    static KafkaBookKeeperReadSnapshotV1 activeSnapshot(KafkaRunTestFixtures.FakeSession session) {
        installData(session, 1, 100);
        installData(session, 2, 101);
        KafkaActiveTailStateV1 tail =
                new KafkaActiveTailStateV1(100, 102, List.of(activeGroup(1, 100), activeGroup(2, 101)));
        KafkaPackedBatchLocatorIndexV1 index = KafkaPackedBatchLocatorIndexV1.fromActiveTail(tail);
        KafkaBookKeeperReadRunV1 run = new KafkaBookKeeperReadRunV1(
                binding(), handle(), 100, 102, SOURCE_GENERATION, Optional.of(index), Optional.empty());
        return new KafkaBookKeeperReadSnapshotV1(
                root(3, 100, 102, 102, 102, 102, 102),
                new KafkaBookKeeperRunTableV1(List.of(run)),
                KafkaTransactionStateV1.empty());
    }

    static KafkaBookKeeperActiveTailLocatorV1 activeGroup(long entryId, long offset) {
        CanonicalBytes raw = raw(offset, 0);
        return new KafkaBookKeeperActiveTailLocatorV1(
                offset,
                offset + 1,
                binding(),
                handle(),
                entryId,
                entryId,
                1,
                300,
                Sha256Digest.hash(raw),
                new Id128(0, entryId + 100),
                new Id128(0, entryId + 200),
                List.of(new KafkaBookKeeperDataLocatorV1(offset, offset + 1, entryId, 0, raw.length())));
    }

    static Nbke2RangeIndexBlockV1 block(
            long anchorOffset,
            long anchorEntryId,
            long blockEntryId,
            long predecessorBlockEntryId,
            long successorDataEntryId) {
        return new Nbke2RangeIndexBlockV1(
                binding(),
                anchorOffset,
                anchorEntryId,
                anchorOffset + 2,
                anchorEntryId,
                anchorEntryId + 1,
                predecessorBlockEntryId,
                successorDataEntryId,
                List.of(
                        new Nbke2BatchLocatorV1(0, 1, 0, 0, 0, RAW_BYTES, 0),
                        new Nbke2BatchLocatorV1(1, 1, 1, 1, 0, RAW_BYTES, 0)));
    }

    static void installData(KafkaRunTestFixtures.FakeSession session, long entryId, long offset) {
        Nbke2DataV1 data = data(entryId, offset, raw(offset, 0));
        session.entries.put(entryId, CanonicalBytes.copyOf(Nbke2CodecV1.encode(LEDGER_ID, entryId, data)));
    }

    static Nbke2DataV1 data(long entryId, long offset, CanonicalBytes raw) {
        return new Nbke2DataV1(
                binding(),
                offset,
                0,
                0,
                1,
                new Id128(0, entryId + 100),
                new Id128(0, entryId + 200),
                Optional.of(
                        new Nbke2AppendGroupDescriptorV1(offset, offset + 1, entryId, entryId, Sha256Digest.hash(raw))),
                raw);
    }

    static CanonicalBytes raw(long baseOffset, int lastOffsetDelta) {
        byte[] raw = new byte[RAW_BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(0, baseOffset);
        buffer.putInt(Long.BYTES, raw.length - 12);
        buffer.putInt(12, binding().kafkaLeaderEpoch());
        buffer.put(16, (byte) 2);
        buffer.putInt(23, lastOffsetDelta);
        buffer.putLong(43, -1L);
        buffer.putShort(51, (short) -1);
        buffer.putInt(53, -1);
        CRC32C crc = new CRC32C();
        crc.update(raw, 21, raw.length - 21);
        buffer.putInt(17, (int) crc.getValue());
        return CanonicalBytes.copyOf(raw);
    }

    static KafkaTransactionStateV1 abortedTransactionState() {
        return new KafkaTransactionStateV1(
                new TreeMap<>(), List.of(new KafkaTransactionStateV1.CompletedTransactionV1(71, 100, 102, true, 3)));
    }
}
