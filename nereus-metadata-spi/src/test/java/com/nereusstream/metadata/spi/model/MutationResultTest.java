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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MutationResultTest {
    @Test
    void createOutcomeTableHasExactlyFourMembersAndLegalShapes() {
        assertThat(CreateMutationOutcome.values())
                .containsExactly(
                        CreateMutationOutcome.CREATED,
                        CreateMutationOutcome.EXISTING_EXACT,
                        CreateMutationOutcome.DEFINITIVE_CONFLICT,
                        CreateMutationOutcome.INDETERMINATE);
        assertThat(CreateMutationResult.created("created").exactSnapshot()).contains("created");
        assertThat(CreateMutationResult.existingExact("existing").exactSnapshot())
                .contains("existing");
        assertThat(CreateMutationResult.definitiveConflict().exactSnapshot()).isEmpty();
        assertThat(CreateMutationResult.indeterminate().exactSnapshot()).isEmpty();

        assertThatThrownBy(() -> new CreateMutationResult<>(CreateMutationOutcome.CREATED, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() ->
                        new CreateMutationResult<>(CreateMutationOutcome.DEFINITIVE_CONFLICT, Optional.of("forbidden")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void casOutcomeTableHasExactlyFourMembersAndLegalShapes() {
        assertThat(ConditionalCasOutcome.values())
                .containsExactly(
                        ConditionalCasOutcome.APPLIED_EXACT,
                        ConditionalCasOutcome.PREDECESSOR_UNCHANGED,
                        ConditionalCasOutcome.DEFINITIVE_CONFLICT,
                        ConditionalCasOutcome.INDETERMINATE);
        assertThat(ConditionalCasResult.appliedExact("applied").exactSnapshot()).contains("applied");
        assertThat(ConditionalCasResult.predecessorUnchanged("old").exactSnapshot())
                .contains("old");
        assertThat(ConditionalCasResult.definitiveConflict().exactSnapshot()).isEmpty();
        assertThat(ConditionalCasResult.indeterminate().exactSnapshot()).isEmpty();

        assertThatThrownBy(
                        () -> new ConditionalCasResult<>(ConditionalCasOutcome.PREDECESSOR_UNCHANGED, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () -> new ConditionalCasResult<>(ConditionalCasOutcome.INDETERMINATE, Optional.of("forbidden")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
