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

package com.nereusstream.kafka.bookkeeper.pipeline;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.util.Objects;

/** Exact K4 quorum-validated logical/physical append fact delivered to the ordered K5 publication seam. */
public record KafkaOrderedDurableCommitV1(
        long startOffset,
        long endOffsetExclusive,
        RunLedgerHandleV1 handle,
        long firstDataEntryId,
        long lastDataEntryId,
        int memberCount,
        long encodedDataBytes,
        Sha256Digest aggregateAssignedPayloadSha256) {
    public KafkaOrderedDurableCommitV1 {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(aggregateAssignedPayloadSha256, "aggregateAssignedPayloadSha256");
        if (startOffset < 0
                || endOffsetExclusive <= startOffset
                || firstDataEntryId <= 0
                || lastDataEntryId < firstDataEntryId
                || memberCount <= 0
                || encodedDataBytes <= 0
                || aggregateAssignedPayloadSha256.isZero()
                || Math.addExact(firstDataEntryId, (long) memberCount - 1L) != lastDataEntryId) {
            throw new IllegalArgumentException("ordered durable commit is outside its logical/physical domain");
        }
    }
}
