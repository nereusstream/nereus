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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Physical-only checkpoint row. It deliberately contains no Root repetition or binding/protocol frontier. */
public record ProviderResolvedExtentRowV1(
        WalLaneId laneId,
        long laneSequence,
        int directoryPrefixEnd,
        long bodyLength,
        Sha256Digest objectSha256,
        ProviderVersionProof providerProof) {
    public ProviderResolvedExtentRowV1 {
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(objectSha256, "objectSha256");
        Objects.requireNonNull(providerProof, "providerProof");
        if (laneSequence < 0 || directoryPrefixEnd <= 0 || bodyLength <= 0) {
            throw new IllegalArgumentException("extent sequence, prefix, and body length must be positive/bounded");
        }
        if (directoryPrefixEnd > ObjectProviderRootConfiguration.FORMAT_MAX_PREFIX_BYTES
                || directoryPrefixEnd > bodyLength) {
            throw new IllegalArgumentException("directory prefix lies outside the Object body");
        }
        if (objectSha256.isZero()) {
            throw new IllegalArgumentException("Object SHA-256 must be non-zero");
        }
    }
}
