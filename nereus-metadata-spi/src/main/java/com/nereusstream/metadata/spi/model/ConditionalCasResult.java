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

/** A closed CAS result whose exact outcomes alone carry an authority snapshot. */
public record ConditionalCasResult<T>(ConditionalCasOutcome outcome, Optional<T> exactSnapshot) {
    public ConditionalCasResult {
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(exactSnapshot, "exactSnapshot");
        boolean exactOutcome = outcome == ConditionalCasOutcome.APPLIED_EXACT
                || outcome == ConditionalCasOutcome.PREDECESSOR_UNCHANGED;
        if (exactOutcome != exactSnapshot.isPresent()) {
            throw new IllegalArgumentException("only exact CAS outcomes carry a snapshot");
        }
    }

    public static <T> ConditionalCasResult<T> appliedExact(T snapshot) {
        return new ConditionalCasResult<>(ConditionalCasOutcome.APPLIED_EXACT, Optional.of(snapshot));
    }

    public static <T> ConditionalCasResult<T> predecessorUnchanged(T snapshot) {
        return new ConditionalCasResult<>(ConditionalCasOutcome.PREDECESSOR_UNCHANGED, Optional.of(snapshot));
    }

    public static <T> ConditionalCasResult<T> definitiveConflict() {
        return new ConditionalCasResult<>(ConditionalCasOutcome.DEFINITIVE_CONFLICT, Optional.empty());
    }

    public static <T> ConditionalCasResult<T> indeterminate() {
        return new ConditionalCasResult<>(ConditionalCasOutcome.INDETERMINATE, Optional.empty());
    }
}
