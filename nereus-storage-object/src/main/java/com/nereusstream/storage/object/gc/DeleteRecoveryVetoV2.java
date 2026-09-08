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
import java.util.Objects;

/** One bounded, conservative recovery rejection; diagnostics never establish native eligibility or ownership. */
public record DeleteRecoveryVetoV2(
        Reason reason,
        long rejectedObservationEpoch,
        Sha256Digest rejectedContextSha256,
        Sha256Digest rejectedAuthoritySha256) {
    public enum Reason {
        PREDECESSOR_OWNER_AUTHORITY_REJECTED,
        CURRENT_OBSERVATION_AUTHORITY_REJECTED,
        ELIGIBILITY_FACTS_REJECTED
    }

    public DeleteRecoveryVetoV2 {
        Objects.requireNonNull(reason, "reason");
        M5TargetDeleteAuthorityRecordsV1.requirePositive(rejectedObservationEpoch, "rejectedObservationEpoch");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(rejectedContextSha256, "rejectedContextSha256");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(rejectedAuthoritySha256, "rejectedAuthoritySha256");
    }
}
