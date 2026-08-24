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

/** Unique post-plan-seal lane sequence reservation. */
public record LaneSequenceReservation(
        WalLaneId laneId, long laneSequence, Sha256Digest canonicalPlanSha256, long maximumCanonicalBodyBytes) {
    public LaneSequenceReservation {
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(canonicalPlanSha256, "canonicalPlanSha256");
        if (laneSequence < 0 || maximumCanonicalBodyBytes <= 0 || canonicalPlanSha256.isZero()) {
            throw new IllegalArgumentException("lane sequence reservation is invalid");
        }
    }
}
