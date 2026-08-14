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

package com.nereusstream.kafka.bookkeeper.nbke2;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Terminal DATA descriptor for the complete append group. */
public record Nbke2AppendGroupDescriptorV1(
        long groupStartOffset,
        long groupEndOffsetExclusive,
        long firstDataEntryId,
        long lastDataEntryId,
        Sha256Digest aggregateAssignedPayloadSha256) {
    public Nbke2AppendGroupDescriptorV1 {
        Objects.requireNonNull(aggregateAssignedPayloadSha256, "aggregateAssignedPayloadSha256");
        if (groupStartOffset < 0
                || groupEndOffsetExclusive <= groupStartOffset
                || firstDataEntryId < 0
                || lastDataEntryId < firstDataEntryId
                || aggregateAssignedPayloadSha256.isZero()) {
            throw new IllegalArgumentException("append-group descriptor is outside the NBKE2 v1 domain");
        }
    }
}
