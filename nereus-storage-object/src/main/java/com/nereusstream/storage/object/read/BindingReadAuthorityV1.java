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

package com.nereusstream.storage.object.read;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import java.util.Objects;

/** Exact immutable Binding/selector/owner/protocol authority validated around hazard publication. */
public record BindingReadAuthorityV1(
        TopicBindingId bindingId,
        Sha256Digest topicIncarnationIdentity,
        StorageEpochId storageEpochId,
        BindingReadProtocolV1 protocol,
        Sha256Digest selectedViewSha256,
        long ownerEpoch,
        long readAdmissionEpoch,
        boolean admitting,
        long capabilityGeneration,
        Sha256Digest capabilityEvidenceSha256,
        BindingReadPublicationCellV1 publicationCell) {
    public BindingReadAuthorityV1 {
        Objects.requireNonNull(bindingId, "bindingId");
        Objects.requireNonNull(topicIncarnationIdentity, "topicIncarnationIdentity");
        Objects.requireNonNull(storageEpochId, "storageEpochId");
        Objects.requireNonNull(protocol, "protocol");
        Objects.requireNonNull(selectedViewSha256, "selectedViewSha256");
        Objects.requireNonNull(capabilityEvidenceSha256, "capabilityEvidenceSha256");
        Objects.requireNonNull(publicationCell, "publicationCell");
        if (bindingId.digest().isZero()
                || topicIncarnationIdentity.isZero()
                || storageEpochId.digest().isZero()
                || selectedViewSha256.isZero()
                || ownerEpoch <= 0
                || readAdmissionEpoch <= 0
                || capabilityGeneration <= 0
                || capabilityEvidenceSha256.isZero()) {
            throw new IllegalArgumentException("read authority identity is outside its exact domain");
        }
    }

    public long sourceGeneration() {
        return publicationCell.sourceGeneration();
    }
}
