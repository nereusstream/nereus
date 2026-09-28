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

package com.nereusstream.storage.api.kafka;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;
import java.util.OptionalLong;

/** Fixed native fenced ledger cut for a crash-terminated run; its Kafka end is carried by the sealed root. */
public record KafkaRunRecoveryCutV1(
        Sha256Digest closedOwnerSha256, long recoveredLastAddConfirmed, OptionalLong inertFromEntryId) {
    public KafkaRunRecoveryCutV1 {
        Objects.requireNonNull(closedOwnerSha256, "closedOwnerSha256");
        Objects.requireNonNull(inertFromEntryId, "inertFromEntryId");
        if (closedOwnerSha256.isZero()
                || recoveredLastAddConfirmed < 0
                || inertFromEntryId.isPresent()
                        && (inertFromEntryId.getAsLong() <= 0
                                || inertFromEntryId.getAsLong() > recoveredLastAddConfirmed)) {
            throw new IllegalArgumentException("recovered run cut is outside its exact native domain");
        }
    }
}
