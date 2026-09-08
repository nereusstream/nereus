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

package com.nereusstream.storage.object.gc;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import java.util.Objects;

/** Bounded last-refresh evidence. Prior history remains bound by the exact predecessor hash. */
public record DeleteDispatchRefreshV2(
        long predecessorAuthorityRevision,
        Sha256Digest predecessorAuthoritySha256,
        long predecessorDispatchEpoch,
        DeleteObservationContextV2 previous,
        DeleteObservationContextV2 current,
        ExternalIdentityObservationV1 externalObservation) {
    public DeleteDispatchRefreshV2 {
        M5TargetDeleteAuthorityRecordsV1.requirePositive(predecessorAuthorityRevision, "predecessorAuthorityRevision");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(predecessorAuthoritySha256, "predecessorAuthoritySha256");
        M5TargetDeleteAuthorityRecordsV1.requirePositive(predecessorDispatchEpoch, "predecessorDispatchEpoch");
        Objects.requireNonNull(previous, "previous");
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(externalObservation, "externalObservation");
        boolean changedOwner = !previous.coordinatorOwner().equals(current.coordinatorOwner());
        if (current.observationEpoch() != Math.addExact(previous.observationEpoch(), 1)
                || changedOwner != current.predecessorOwnerFenced().isPresent()) {
            throw new IllegalArgumentException("dispatch refresh requires the next observation and exact owner fence");
        }
    }

    public Sha256Digest sha256() {
        return M5TargetDeleteAuthorityCodecV1.dispatchRefreshSha256(this);
    }
}
