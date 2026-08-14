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

package com.nereusstream.storage.api.bookkeeper;

import java.util.Objects;
import java.util.Optional;

/** A mutation result carrying proof only when the exact mutation is established. */
public record ProviderMutationResultV1<T>(ProviderMutationOutcomeV1 outcome, Optional<T> exactProof) {
    public ProviderMutationResultV1 {
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(exactProof, "exactProof");
        if ((outcome == ProviderMutationOutcomeV1.APPLIED_EXACT) != exactProof.isPresent()) {
            throw new IllegalArgumentException("only APPLIED_EXACT carries an exact proof");
        }
    }

    public static <T> ProviderMutationResultV1<T> appliedExact(T proof) {
        return new ProviderMutationResultV1<>(ProviderMutationOutcomeV1.APPLIED_EXACT, Optional.of(proof));
    }

    public static <T> ProviderMutationResultV1<T> definitelyNotApplied() {
        return new ProviderMutationResultV1<>(ProviderMutationOutcomeV1.DEFINITIVELY_NOT_APPLIED, Optional.empty());
    }

    public static <T> ProviderMutationResultV1<T> outcomeUnknown() {
        return new ProviderMutationResultV1<>(ProviderMutationOutcomeV1.OUTCOME_UNKNOWN, Optional.empty());
    }

    public static <T> ProviderMutationResultV1<T> fencedOrConflict() {
        return new ProviderMutationResultV1<>(ProviderMutationOutcomeV1.FENCED_OR_CONFLICT, Optional.empty());
    }
}
