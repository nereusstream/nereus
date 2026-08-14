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

package com.nereusstream.kafka.bookkeeper.adapter;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperDataAdmissionTicketV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2AppendGroupDescriptorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Cross-checks one assigned append group and adapts its unchanged bytes to NBKE2 DATA frames. */
public final class KafkaAssignedRecordBatchGroupAdapterV1 {
    private KafkaAssignedRecordBatchGroupAdapterV1() {}

    public static KafkaNbke2AssignedAppendGroupV1 adapt(
            KafkaPartitionFenceV1 fence,
            Nbke2RunBindingV1 runBinding,
            long firstDataEntryId,
            Id128 appendGroupId,
            Id128 storageAttemptId,
            List<KafkaNativeAssignedRecordBatchV1> assignedBatches,
            List<KafkaBookKeeperDataAdmissionTicketV1> admissionTickets) {
        Objects.requireNonNull(fence, "fence");
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(appendGroupId, "appendGroupId");
        Objects.requireNonNull(storageAttemptId, "storageAttemptId");
        assignedBatches = List.copyOf(Objects.requireNonNull(assignedBatches, "assignedBatches"));
        admissionTickets = List.copyOf(Objects.requireNonNull(admissionTickets, "admissionTickets"));
        if (appendGroupId.isZero() || storageAttemptId.isZero()) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.APPEND_GROUP_MISMATCH,
                    "append-group and storage-attempt IDs must be non-zero");
        }
        if (assignedBatches.isEmpty() || assignedBatches.size() != admissionTickets.size()) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.APPEND_GROUP_MISMATCH,
                    "assigned batches and pre-allocation tickets must be non-empty and one-to-one");
        }
        validateRunFence(fence, runBinding);

        int memberCount = assignedBatches.size();
        long nextOffset = assignedBatches.get(0).baseOffset();
        MessageDigest aggregate = sha256();
        for (int index = 0; index < memberCount; index++) {
            KafkaNativeAssignedRecordBatchV1 batch = assignedBatches.get(index);
            KafkaBookKeeperDataAdmissionTicketV1 ticket = admissionTickets.get(index);
            if (batch.partitionLeaderEpoch() != runBinding.kafkaLeaderEpoch()) {
                throw reject(
                        KafkaAssignedRecordBatchRejectionV1.RUN_FENCE_MISMATCH,
                        "assigned batch leader epoch differs from the run fence");
            }
            if (batch.baseOffset() != nextOffset) {
                throw reject(
                        KafkaAssignedRecordBatchRejectionV1.APPEND_GROUP_MISMATCH,
                        "assigned append-group offset coverage is not contiguous");
            }
            if (!ticket.providerScopeId().equals(runBinding.providerScopeId())
                    || ticket.rawRecordBatchBytes()
                            != batch.rawAssignedRecordBatch().length()
                    || ticket.memberOrdinal() != index
                    || ticket.memberCount() != memberCount
                    || ticket.terminalMember() != (index == memberCount - 1)) {
                throw reject(
                        KafkaAssignedRecordBatchRejectionV1.APPEND_GROUP_MISMATCH,
                        "pre-allocation ticket does not bind the exact assigned member");
            }
            aggregate.update(batch.rawAssignedRecordBatch().toByteArray());
            nextOffset = batch.endOffsetExclusive();
        }

        long lastDataEntryId;
        try {
            lastDataEntryId = Math.addExact(firstDataEntryId, (long) memberCount - 1L);
        } catch (ArithmeticException failure) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.APPEND_GROUP_MISMATCH,
                    "assigned append-group entry range overflows",
                    failure);
        }
        Nbke2AppendGroupDescriptorV1 descriptor = new Nbke2AppendGroupDescriptorV1(
                assignedBatches.get(0).baseOffset(),
                nextOffset,
                firstDataEntryId,
                lastDataEntryId,
                Sha256Digest.copyOf(aggregate.digest()));

        List<Nbke2DataV1> frames = new ArrayList<>(memberCount);
        for (int index = 0; index < memberCount; index++) {
            KafkaNativeAssignedRecordBatchV1 batch = assignedBatches.get(index);
            frames.add(new Nbke2DataV1(
                    runBinding,
                    batch.baseOffset(),
                    batch.lastOffsetDelta(),
                    index,
                    memberCount,
                    appendGroupId,
                    storageAttemptId,
                    index == memberCount - 1 ? Optional.of(descriptor) : Optional.empty(),
                    batch.rawAssignedRecordBatch()));
        }
        return new KafkaNbke2AssignedAppendGroupV1(firstDataEntryId, frames);
    }

    private static void validateRunFence(KafkaPartitionFenceV1 fence, Nbke2RunBindingV1 runBinding) {
        if (!fence.bindingId().equals(runBinding.bindingId())
                || !fence.topicIncarnation().equals(runBinding.topicIncarnation())
                || fence.partitionId() != runBinding.partitionId()
                || !fence.storageEpochId().equals(runBinding.storageEpochId())
                || fence.ownerEpoch() != runBinding.creatorOwnerEpoch()
                || fence.kafkaLeaderEpoch() != runBinding.kafkaLeaderEpoch()) {
            throw reject(
                    KafkaAssignedRecordBatchRejectionV1.RUN_FENCE_MISMATCH,
                    "partition publication fence differs from the run binding");
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JDK has no SHA-256 provider", failure);
        }
    }

    private static KafkaAssignedRecordBatchException reject(
            KafkaAssignedRecordBatchRejectionV1 rejection, String message) {
        return new KafkaAssignedRecordBatchException(rejection, message);
    }

    private static KafkaAssignedRecordBatchException reject(
            KafkaAssignedRecordBatchRejectionV1 rejection, String message, Throwable cause) {
        return new KafkaAssignedRecordBatchException(rejection, message, cause);
    }
}
