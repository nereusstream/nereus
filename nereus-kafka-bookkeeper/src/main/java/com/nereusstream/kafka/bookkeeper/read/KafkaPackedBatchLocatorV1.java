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

/** Ephemeral lookup result materialized from one primitive packed locator row. */
public record KafkaPackedBatchLocatorV1(
        KafkaLocatorSourceKindV1 sourceKind,
        long indexIdentity,
        int ordinal,
        long startOffset,
        long endOffsetExclusive,
        long entryId,
        long appendGroupOrdinal,
        long rawPayloadBytes,
        long physicalChecksumGeneration) {
    public KafkaPackedBatchLocatorV1 {
        if (sourceKind == null
                || indexIdentity < -1
                || ordinal < 0
                || startOffset < 0
                || endOffsetExclusive <= startOffset
                || entryId <= 0
                || appendGroupOrdinal < 0
                || rawPayloadBytes <= 0
                || physicalChecksumGeneration < 0) {
            throw new IllegalArgumentException("packed batch locator is outside its domain");
        }
        if ((sourceKind == KafkaLocatorSourceKindV1.ACTIVE_TAIL) != (indexIdentity == -1)) {
            throw new IllegalArgumentException("active and persisted locator identities are inconsistent");
        }
    }

    public boolean covers(long requestedOffset) {
        return requestedOffset >= startOffset && requestedOffset < endOffsetExclusive;
    }
}
