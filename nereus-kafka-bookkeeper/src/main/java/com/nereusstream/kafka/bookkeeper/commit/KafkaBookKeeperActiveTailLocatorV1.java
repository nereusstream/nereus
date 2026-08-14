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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedDurableCommitV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.util.List;
import java.util.Objects;

/** Owner-local exact locator for one coherently published BookKeeper append group. */
public record KafkaBookKeeperActiveTailLocatorV1(
        long startOffset,
        long endOffsetExclusive,
        Nbke2RunBindingV1 runBinding,
        RunLedgerHandleV1 handle,
        long firstDataEntryId,
        long lastDataEntryId,
        int memberCount,
        long encodedDataBytes,
        Sha256Digest aggregateAssignedPayloadSha256,
        Id128 appendGroupId,
        Id128 storageAttemptId,
        List<KafkaBookKeeperDataLocatorV1> members) {
    public KafkaBookKeeperActiveTailLocatorV1 {
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(aggregateAssignedPayloadSha256, "aggregateAssignedPayloadSha256");
        Objects.requireNonNull(appendGroupId, "appendGroupId");
        Objects.requireNonNull(storageAttemptId, "storageAttemptId");
        members = List.copyOf(Objects.requireNonNull(members, "members"));
        if (startOffset < 0
                || endOffsetExclusive <= startOffset
                || firstDataEntryId <= 0
                || lastDataEntryId < firstDataEntryId
                || memberCount <= 0
                || encodedDataBytes <= 0
                || aggregateAssignedPayloadSha256.isZero()
                || appendGroupId.isZero()
                || storageAttemptId.isZero()
                || members.size() != memberCount
                || !runBinding.providerScopeId().equals(handle.providerScopeId())
                || !runBinding.runId().equals(handle.runId())
                || Math.addExact(firstDataEntryId, (long) memberCount - 1L) != lastDataEntryId) {
            throw new IllegalArgumentException("active-tail locator is outside its logical/physical domain");
        }
        long nextOffset = startOffset;
        for (int index = 0; index < members.size(); index++) {
            KafkaBookKeeperDataLocatorV1 member = members.get(index);
            if (member.startOffset() != nextOffset
                    || member.entryId() != Math.addExact(firstDataEntryId, index)
                    || member.memberOrdinal() != index) {
                throw new IllegalArgumentException("active-tail DATA locators are not contiguous and ordinal");
            }
            nextOffset = member.endOffsetExclusive();
        }
        if (nextOffset != endOffsetExclusive) {
            throw new IllegalArgumentException("active-tail DATA locators do not cover the append group");
        }
    }

    public static KafkaBookKeeperActiveTailLocatorV1 from(KafkaOrderedDurableCommitV1 commit) {
        Objects.requireNonNull(commit, "commit");
        List<KafkaBookKeeperDataLocatorV1> members = commit.members().stream()
                .map(member -> new KafkaBookKeeperDataLocatorV1(
                        member.startOffset(),
                        member.endOffsetExclusive(),
                        member.entryId(),
                        member.memberOrdinal(),
                        member.rawAssignedRecordBatchBytes()))
                .toList();
        return new KafkaBookKeeperActiveTailLocatorV1(
                commit.startOffset(),
                commit.endOffsetExclusive(),
                commit.runBinding(),
                commit.handle(),
                commit.firstDataEntryId(),
                commit.lastDataEntryId(),
                commit.memberCount(),
                commit.encodedDataBytes(),
                commit.aggregateAssignedPayloadSha256(),
                commit.appendGroupId(),
                commit.storageAttemptId(),
                members);
    }
}
