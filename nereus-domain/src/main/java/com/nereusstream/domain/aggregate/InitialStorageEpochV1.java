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

package com.nereusstream.domain.aggregate;

import com.nereusstream.domain.identity.StorageEpochId;
import java.util.Objects;

/** The ordinal-zero Storage Epoch projection of a logical aggregate. */
public record InitialStorageEpochV1(
        StorageEpochId storageEpochId,
        long epochOrdinal,
        StorageProfileV1 storageProfile,
        ProfileOriginV1 profileOrigin,
        PolicyCatalogDigest policyCatalogDigest,
        FrameEncodingPolicyValueV1 frameEncodingPolicy) {
    public InitialStorageEpochV1 {
        Objects.requireNonNull(storageEpochId, "storageEpochId");
        Objects.requireNonNull(storageProfile, "storageProfile");
        Objects.requireNonNull(profileOrigin, "profileOrigin");
        Objects.requireNonNull(policyCatalogDigest, "policyCatalogDigest");
        Objects.requireNonNull(frameEncodingPolicy, "frameEncodingPolicy");
    }
}
