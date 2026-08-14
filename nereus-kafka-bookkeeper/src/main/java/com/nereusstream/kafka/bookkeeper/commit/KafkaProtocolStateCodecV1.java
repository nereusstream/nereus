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

package com.nereusstream.kafka.bookkeeper.commit;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Deterministic owner-local component encoding used only for K1 immutable content references. */
final class KafkaProtocolStateCodecV1 {
    private KafkaProtocolStateCodecV1() {}

    static CanonicalBytes activeTail(KafkaActiveTailStateV1 state) {
        return encode(out -> {
            tag(out, "K5-ACTIVE-TAIL-V1");
            out.writeLong(state.startOffset());
            out.writeLong(state.endOffsetExclusive());
            out.writeInt(state.locators().size());
            for (KafkaBookKeeperActiveTailLocatorV1 locator : state.locators()) {
                out.writeLong(locator.startOffset());
                out.writeLong(locator.endOffsetExclusive());
                runBinding(out, locator.runBinding());
                handle(out, locator.handle());
                out.writeLong(locator.firstDataEntryId());
                out.writeLong(locator.lastDataEntryId());
                out.writeInt(locator.memberCount());
                out.writeLong(locator.encodedDataBytes());
                digest(out, locator.aggregateAssignedPayloadSha256());
                out.writeLong(locator.appendGroupId().highBits());
                out.writeLong(locator.appendGroupId().lowBits());
                out.writeLong(locator.storageAttemptId().highBits());
                out.writeLong(locator.storageAttemptId().lowBits());
                out.writeInt(locator.members().size());
                for (KafkaBookKeeperDataLocatorV1 member : locator.members()) {
                    out.writeLong(member.startOffset());
                    out.writeLong(member.endOffsetExclusive());
                    out.writeLong(member.entryId());
                    out.writeInt(member.memberOrdinal());
                    out.writeLong(member.rawAssignedRecordBatchBytes());
                }
            }
        });
    }

    static CanonicalBytes producers(KafkaCommittedProducerStateV1 state) {
        return encode(out -> {
            tag(out, "K5-PRODUCER-V1");
            out.writeInt(state.producers().size());
            for (KafkaProducerSessionStateV1 producer : state.producers().values()) {
                out.writeLong(producer.producerId());
                out.writeShort(producer.producerEpoch());
                out.writeInt(producer.lastSequence());
                out.writeLong(producer.lastOffset());
                out.writeInt(producer.recentBatches().size());
                for (KafkaProducerBatchResultV1 batch : producer.recentBatches()) {
                    identity(out, batch.identity());
                    out.writeLong(batch.startOffset());
                    out.writeLong(batch.endOffsetExclusive());
                }
            }
        });
    }

    static CanonicalBytes speculative(KafkaSpeculativeQueueV1 queue) {
        return encode(out -> {
            tag(out, "K5-SPECULATIVE-V1");
            out.writeInt(queue.commits().size());
            for (KafkaSpeculativeCommitV1 commit : queue.commits()) {
                out.writeLong(commit.startOffset());
                out.writeLong(commit.endOffsetExclusive());
                fence(out, commit.expectedFence());
                out.writeInt(commit.batches().size());
                for (KafkaAssignedProtocolBatchV1 batch : commit.batches()) {
                    out.writeLong(batch.startOffset());
                    out.writeLong(batch.endOffsetExclusive());
                    KafkaProtocolBatchDeltaV1 delta = batch.delta();
                    out.writeLong(delta.logicalOffsetCount());
                    out.writeBoolean(delta.duplicateIdentity().isPresent());
                    if (delta.duplicateIdentity().isPresent()) {
                        identity(out, delta.duplicateIdentity().orElseThrow());
                    }
                    out.writeByte(delta.transactionKind().ordinal());
                    out.writeLong(delta.transactionalProducerId());
                    out.writeInt(delta.coordinatorEpoch());
                }
            }
        });
    }

    static CanonicalBytes transactions(KafkaTransactionStateV1 state) {
        return encode(out -> {
            tag(out, "K5-TRANSACTION-V1");
            out.writeInt(state.ongoingTransactions().size());
            for (KafkaTransactionStateV1.OngoingTransactionV1 transaction :
                    state.ongoingTransactions().values()) {
                out.writeLong(transaction.producerId());
                out.writeLong(transaction.firstOffset());
            }
            out.writeInt(state.completedTransactions().size());
            for (KafkaTransactionStateV1.CompletedTransactionV1 transaction : state.completedTransactions()) {
                out.writeLong(transaction.producerId());
                out.writeLong(transaction.firstOffset());
                out.writeLong(transaction.markerEndOffsetExclusive());
                out.writeBoolean(transaction.aborted());
                out.writeInt(transaction.coordinatorEpoch());
            }
        });
    }

    static CanonicalBytes leaderEpochs(KafkaLeaderEpochIndexV1 index) {
        return encode(out -> {
            tag(out, "K5-LEADER-EPOCH-V1");
            out.writeInt(index.startOffsets().size());
            for (var entry : index.startOffsets().entrySet()) {
                out.writeInt(entry.getKey());
                out.writeLong(entry.getValue());
            }
        });
    }

    static CanonicalBytes label(String value) {
        return encode(out -> tag(out, value));
    }

    private static void identity(DataOutputStream out, KafkaBatchDuplicateIdentityV1 identity) throws IOException {
        out.writeLong(identity.producerId());
        out.writeShort(identity.producerEpoch());
        out.writeInt(identity.baseSequence());
        out.writeInt(identity.lastSequence());
    }

    private static void fence(DataOutputStream out, KafkaPartitionFenceV1 fence) throws IOException {
        digest(out, fence.bindingId().digest());
        out.writeLong(fence.topicIncarnation().topicId().value().highBits());
        out.writeLong(fence.topicIncarnation().topicId().value().lowBits());
        tag(out, fence.topicIncarnation().topicName().value());
        out.writeInt(fence.partitionId());
        out.writeLong(fence.bindingGeneration());
        digest(out, fence.storageEpochId().digest());
        out.writeLong(fence.ownerEpoch());
        out.writeInt(fence.kafkaLeaderEpoch());
    }

    private static void handle(DataOutputStream out, RunLedgerHandleV1 handle) throws IOException {
        digest(out, handle.providerScopeId().digest());
        out.writeLong(handle.runId().value().highBits());
        out.writeLong(handle.runId().value().lowBits());
        out.writeLong(handle.ledgerIdentity().ledgerId());
        digest(out, handle.configurationDigest());
    }

    private static void runBinding(
            DataOutputStream out, com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1 binding)
            throws IOException {
        digest(out, binding.bindingId().digest());
        out.writeLong(binding.topicIncarnation().topicId().value().highBits());
        out.writeLong(binding.topicIncarnation().topicId().value().lowBits());
        tag(out, binding.topicIncarnation().topicName().value());
        out.writeInt(binding.partitionId());
        digest(out, binding.storageEpochId().digest());
        out.writeLong(binding.creatorOwnerEpoch());
        out.writeInt(binding.kafkaLeaderEpoch());
        digest(out, binding.providerScopeId().digest());
        out.writeLong(binding.runId().value().highBits());
        out.writeLong(binding.runId().value().lowBits());
    }

    private static void digest(DataOutputStream out, Sha256Digest digest) throws IOException {
        out.write(digest.bytes().toByteArray());
    }

    private static void tag(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static CanonicalBytes encode(Writer writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                writer.write(out);
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory protocol state encoding failed", failure);
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream out) throws IOException;
    }
}
