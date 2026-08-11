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

import java.util.Objects;
import java.util.Optional;

/** A closed create result whose exact outcomes alone carry an authority snapshot. */
public record CreateMutationResult<T>(CreateMutationOutcome outcome, Optional<T> exactSnapshot) {
    public CreateMutationResult {
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(exactSnapshot, "exactSnapshot");
        boolean exactOutcome =
                outcome == CreateMutationOutcome.CREATED || outcome == CreateMutationOutcome.EXISTING_EXACT;
        if (exactOutcome != exactSnapshot.isPresent()) {
            throw new IllegalArgumentException("only exact create outcomes carry a snapshot");
        }
    }

    public static <T> CreateMutationResult<T> created(T snapshot) {
        return new CreateMutationResult<>(CreateMutationOutcome.CREATED, Optional.of(snapshot));
    }

    public static <T> CreateMutationResult<T> existingExact(T snapshot) {
        return new CreateMutationResult<>(CreateMutationOutcome.EXISTING_EXACT, Optional.of(snapshot));
    }

    public static <T> CreateMutationResult<T> definitiveConflict() {
        return new CreateMutationResult<>(CreateMutationOutcome.DEFINITIVE_CONFLICT, Optional.empty());
    }

    public static <T> CreateMutationResult<T> indeterminate() {
        return new CreateMutationResult<>(CreateMutationOutcome.INDETERMINATE, Optional.empty());
    }
}
