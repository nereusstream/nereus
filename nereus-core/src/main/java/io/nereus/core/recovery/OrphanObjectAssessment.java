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

package io.nereus.core.recovery;

import io.nereus.api.ObjectId;
import java.util.Objects;

/** Result of inspecting one operationally supplied object id against metadata truth. */
public record OrphanObjectAssessment(
        ObjectId objectId,
        OrphanObjectStatus status,
        int manifestSliceCount,
        int reachableSliceCount,
        long orphanExpiresAtMillis) {
    public OrphanObjectAssessment {
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(status, "status");
        if (manifestSliceCount < 0 || reachableSliceCount < 0
                || reachableSliceCount > manifestSliceCount || orphanExpiresAtMillis < 0) {
            throw new IllegalArgumentException("orphan assessment counts and expiry must be valid");
        }
        if (status == OrphanObjectStatus.MISSING_MANIFEST
                && (manifestSliceCount != 0 || reachableSliceCount != 0 || orphanExpiresAtMillis != 0)) {
            throw new IllegalArgumentException("missing-manifest assessment cannot carry manifest state");
        }
        switch (status) {
            case MISSING_MANIFEST -> {
                // Validated above.
            }
            case UNREFERENCED_MANIFEST -> {
                if (manifestSliceCount == 0 || reachableSliceCount != 0) {
                    throw new IllegalArgumentException("unreferenced manifest must have no reachable slices");
                }
            }
            case PARTIALLY_REFERENCED -> {
                if (reachableSliceCount == 0 || reachableSliceCount == manifestSliceCount) {
                    throw new IllegalArgumentException("partially referenced manifest counts are inconsistent");
                }
            }
            case FULLY_REFERENCED -> {
                if (manifestSliceCount == 0 || reachableSliceCount != manifestSliceCount) {
                    throw new IllegalArgumentException("fully referenced manifest counts are inconsistent");
                }
            }
        }
    }

    /** Phase 1 assessments are diagnostic only and never prove safe deletion. */
    public boolean deletionAllowed() {
        return false;
    }
}
