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

package io.nereus.metadata.oxia;

import io.nereus.api.StreamId;
import java.util.Objects;

/** Opaque continuation for a bounded backward walk of one stream's committed append chain. */
public record DerivedIndexRepairCursor(
        StreamId streamId,
        long targetOffset,
        String observedHeadCommitId,
        long observedCommitVersion,
        String nextCommitId,
        long nextOffsetEnd,
        long nextCumulativeSize,
        long nextCommitVersion) {
    public DerivedIndexRepairCursor {
        Objects.requireNonNull(streamId, "streamId");
        observedHeadCommitId = requireNonBlank(observedHeadCommitId, "observedHeadCommitId");
        nextCommitId = requireNonBlank(nextCommitId, "nextCommitId");
        if (targetOffset < 0 || observedCommitVersion <= 0
                || nextOffsetEnd <= targetOffset || nextCumulativeSize < 0 || nextCommitVersion <= 0
                || nextCommitVersion >= observedCommitVersion) {
            throw new IllegalArgumentException(
                    "repair cursor offsets and versions must describe an earlier positive commit");
        }
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }
}
