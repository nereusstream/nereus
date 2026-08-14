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

package com.nereusstream.kafka.bookkeeper.recovery;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolBatchDeltaV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2AppendGroupDescriptorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunHeaderV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32C;

final class KafkaRecoveryTestFixtures {
    static final long LEDGER_ID = 47;

    private KafkaRecoveryTestFixtures() {}

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

    static KafkaPartitionFenceV1 recoveredFence() {
        return new KafkaPartitionFenceV1(
                binding().bindingId(),
                binding().topicIncarnation(),
                binding().partitionId(),
                13,
                binding().storageEpochId(),
                12,
                6);
    }

    static void installHeader(KafkaRunTestFixtures.FakeSession session) {
        Nbke2RunHeaderV1 header = new Nbke2RunHeaderV1(binding(), 100, 1, handle().configurationDigest());
        session.entries.put(0L, CanonicalBytes.copyOf(Nbke2CodecV1.encode(LEDGER_ID, 0, header)));
    }

    static long installGroup(
            KafkaRunTestFixtures.FakeSession session, long firstEntryId, long startOffset, int... logicalOffsetCounts) {
        List<CanonicalBytes> rawBatches = new ArrayList<>();
        long nextOffset = startOffset;
        for (int logicalOffsetCount : logicalOffsetCounts) {
            rawBatches.add(raw(nextOffset, logicalOffsetCount - 1));
            nextOffset += logicalOffsetCount;
        }
        ByteArrayOutputStream aggregate = new ByteArrayOutputStream();
        rawBatches.forEach(raw -> aggregate.writeBytes(raw.toByteArray()));
        Sha256Digest digest = Sha256Digest.hash(CanonicalBytes.copyOf(aggregate.toByteArray()));
        Id128 groupId = new Id128(0, firstEntryId + 100);
        Id128 attemptId = new Id128(0, firstEntryId + 200);
        long offset = startOffset;
        for (int index = 0; index < rawBatches.size(); index++) {
            int count = logicalOffsetCounts[index];
            long entryId = firstEntryId + index;
            Optional<Nbke2AppendGroupDescriptorV1> descriptor = index == rawBatches.size() - 1
                    ? Optional.of(new Nbke2AppendGroupDescriptorV1(
                            startOffset, nextOffset, firstEntryId, firstEntryId + rawBatches.size() - 1, digest))
                    : Optional.empty();
            Nbke2DataV1 data = new Nbke2DataV1(
                    binding(),
                    offset,
                    count - 1,
                    index,
                    rawBatches.size(),
                    groupId,
                    attemptId,
                    descriptor,
                    rawBatches.get(index));
            session.entries.put(entryId, CanonicalBytes.copyOf(Nbke2CodecV1.encode(LEDGER_ID, entryId, data)));
            offset += count;
        }
        return nextOffset;
    }

    static void installPartialTwoMemberGroup(
            KafkaRunTestFixtures.FakeSession session, long firstEntryId, long startOffset) {
        CanonicalBytes raw = raw(startOffset, 0);
        Nbke2DataV1 first = new Nbke2DataV1(
                binding(),
                startOffset,
                0,
                0,
                2,
                new Id128(0, firstEntryId + 100),
                new Id128(0, firstEntryId + 200),
                Optional.empty(),
                raw);
        session.entries.put(firstEntryId, CanonicalBytes.copyOf(Nbke2CodecV1.encode(LEDGER_ID, firstEntryId, first)));
    }

    static KafkaProtocolCheckpointStateV1 checkpointState(long coveredThrough) {
        KafkaLeaderEpochIndexV1 leaderEpochs = coveredThrough == 100
                ? KafkaLeaderEpochIndexV1.empty()
                : KafkaLeaderEpochIndexV1.empty().observe(5, 100);
        return new KafkaProtocolCheckpointStateV1(
                new KafkaRecoveryCheckpointVectorV1(
                        binding(), coveredThrough, coveredThrough, coveredThrough, coveredThrough),
                KafkaCommittedProducerStateV1.empty(),
                KafkaTransactionStateV1.empty(),
                leaderEpochs);
    }

    static void installCheckpoint(
            KafkaRunTestFixtures.FakeSession session, long entryId, KafkaProtocolCheckpointStateV1 state) {
        session.entries.put(entryId, CanonicalBytes.copyOf(Nbke2CodecV1.encode(LEDGER_ID, entryId, state.toNbke2())));
    }

    static KafkaBookKeeperRecoveryRequestV1 request(
            long observed, long applied, long adoptable, OptionalLong checkpointHint) {
        return request(
                observed,
                applied,
                adoptable,
                checkpointHint,
                new KafkaBookKeeperRecoveryEnvelopeV1(100, 1_000_000, 1_000_000));
    }

    static KafkaBookKeeperRecoveryRequestV1 request(
            long observed,
            long applied,
            long adoptable,
            OptionalLong checkpointHint,
            KafkaBookKeeperRecoveryEnvelopeV1 envelope) {
        return new KafkaBookKeeperRecoveryRequestV1(
                binding(),
                handle(),
                100,
                checkpointHint,
                envelope,
                new KafkaElectionRecoveryBoundaryV1(KafkaElectionKindV1.ISR_ELECTION, observed, applied, adoptable),
                recoveredFence());
    }

    static KafkaBookKeeperTakeoverRecoveryV1 engine(KafkaRunTestFixtures.FakeSession session) {
        AtomicLong clock = new AtomicLong();
        KafkaRecoveryBatchProtocolAdapterV1 adapter =
                batch -> KafkaProtocolBatchDeltaV1.nonIdempotent((long) batch.lastOffsetDelta() + 1L);
        return new KafkaBookKeeperTakeoverRecoveryV1(session, adapter, () -> clock.getAndAdd(100));
    }

    private static CanonicalBytes raw(long baseOffset, int lastOffsetDelta) {
        byte[] raw = new byte[61];
        ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(0, baseOffset);
        buffer.putInt(8, raw.length - 12);
        buffer.putInt(12, 5);
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
}
