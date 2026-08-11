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

package com.nereusstream.metadata.spi.model;

import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.aggregate.TopicBindingV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** One exact versioned aggregate snapshot and its same-instance projections. */
public record VersionedAggregateSnapshot(
        TopicBindingAggregateV1 aggregate,
        CanonicalBytes canonicalStoredBytes,
        Sha256Digest canonicalStoredDigest,
        MetadataVersion metadataVersion) {
    public VersionedAggregateSnapshot {
        Objects.requireNonNull(aggregate, "aggregate");
        Objects.requireNonNull(metadataVersion, "metadataVersion");
        ExactStoredValue.requireMatchingDigest(canonicalStoredBytes, canonicalStoredDigest);
    }

    public TopicBindingV1 binding() {
        return aggregate.binding();
    }

    public InitialStorageEpochV1 initialEpoch() {
        return aggregate.initialEpoch();
    }
}

final class ExactStoredValue {
    private ExactStoredValue() {}

    static void requireMatchingDigest(CanonicalBytes bytes, Sha256Digest digest) {
        Objects.requireNonNull(bytes, "canonicalStoredBytes");
        Objects.requireNonNull(digest, "canonicalStoredDigest");
        if (!Sha256Digest.hash(bytes).equals(digest)) {
            throw new IllegalArgumentException("canonical stored bytes do not match their SHA-256 digest");
        }
    }
}
