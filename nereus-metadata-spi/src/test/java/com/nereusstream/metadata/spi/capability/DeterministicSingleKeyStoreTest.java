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

package com.nereusstream.metadata.spi.capability;

import static com.nereusstream.metadata.spi.SpiTestFixtures.metadataVersion;
import static com.nereusstream.metadata.spi.SpiTestFixtures.selectorValue;
import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.metadata.spi.model.ConditionalCasOutcome;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationOutcome;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.model.VersionedSelectorSnapshot;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class DeterministicSingleKeyStoreTest {
    @Test
    void exercisesEveryClosedCreateAndCasResolutionCut() {
        DeterministicSelectorStore fake = new DeterministicSelectorStore();
        PulsarTopicGenerationSelectorValueV1 reserved =
                selectorValue(PulsarTopicGenerationSelectorStateV1.RESERVED, "reserved");
        PulsarTopicGenerationSelectorValueV1 conflicting =
                selectorValue(PulsarTopicGenerationSelectorStateV1.RESERVED, "conflicting");
        PulsarTopicGenerationSelectorValueV1 active =
                selectorValue(PulsarTopicGenerationSelectorStateV1.ACTIVE, "active");

        CreateMutationResult<VersionedSelectorSnapshot> created = join(fake.createSelector(reserved));
        assertThat(created.outcome()).isEqualTo(CreateMutationOutcome.CREATED);
        VersionedSelectorSnapshot predecessor = created.exactSnapshot().orElseThrow();

        assertThat(join(fake.createSelector(reserved)).outcome()).isEqualTo(CreateMutationOutcome.EXISTING_EXACT);
        assertThat(join(fake.createSelector(conflicting)).outcome())
                .isEqualTo(CreateMutationOutcome.DEFINITIVE_CONFLICT);

        DeterministicSelectorStore responseLossCreate = new DeterministicSelectorStore();
        responseLossCreate.nextCut = Cut.RESPONSE_LOSS;
        assertThat(join(responseLossCreate.createSelector(reserved)).outcome())
                .isEqualTo(CreateMutationOutcome.EXISTING_EXACT);

        fake.nextCut = Cut.PREDECESSOR_UNCHANGED;
        ConditionalCasResult<VersionedSelectorSnapshot> unchanged =
                join(fake.compareAndSetSelector(predecessor, active));
        assertThat(unchanged.outcome()).isEqualTo(ConditionalCasOutcome.PREDECESSOR_UNCHANGED);
        assertThat(unchanged.exactSnapshot()).contains(predecessor);

        fake.nextCut = Cut.RESPONSE_LOSS;
        ConditionalCasResult<VersionedSelectorSnapshot> applied = join(fake.compareAndSetSelector(predecessor, active));
        assertThat(applied.outcome()).isEqualTo(ConditionalCasOutcome.APPLIED_EXACT);
        VersionedSelectorSnapshot activeSnapshot = applied.exactSnapshot().orElseThrow();
        assertThat(activeSnapshot.value()).isEqualTo(active);

        assertThat(join(fake.compareAndSetSelector(predecessor, active)).outcome())
                .isEqualTo(ConditionalCasOutcome.APPLIED_EXACT);
        assertThat(join(fake.compareAndSetSelector(predecessor, conflicting)).outcome())
                .isEqualTo(ConditionalCasOutcome.DEFINITIVE_CONFLICT);

        fake.nextCut = Cut.INDETERMINATE;
        assertThat(join(fake.compareAndSetSelector(activeSnapshot, conflicting)).outcome())
                .isEqualTo(ConditionalCasOutcome.INDETERMINATE);
        fake.nextCut = Cut.INDETERMINATE;
        assertThat(join(fake.createSelector(active)).outcome()).isEqualTo(CreateMutationOutcome.INDETERMINATE);

        assertThat(join(fake.readSelector(reserved.persistenceName()))).contains(activeSnapshot);
        assertThat(join(fake.readSelector(PulsarPersistenceName.fromString("persistent://tenant/ns/other"))))
                .isEmpty();
    }

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }

    private enum Cut {
        NONE,
        RESPONSE_LOSS,
        PREDECESSOR_UNCHANGED,
        INDETERMINATE
    }

    private static final class DeterministicSelectorStore implements PulsarTopicGenerationSelectorStore {
        private VersionedSelectorSnapshot stored;
        private long version;
        private Cut nextCut = Cut.NONE;

        @Override
        public CompletionStage<Optional<VersionedSelectorSnapshot>> readSelector(
                PulsarPersistenceName persistenceName) {
            Optional<VersionedSelectorSnapshot> result = Optional.ofNullable(stored)
                    .filter(snapshot -> snapshot.value().persistenceName().equals(persistenceName));
            return CompletableFuture.completedFuture(result);
        }

        @Override
        public CompletionStage<CreateMutationResult<VersionedSelectorSnapshot>> createSelector(
                PulsarTopicGenerationSelectorValueV1 candidate) {
            Cut cut = consumeCut();
            if (cut == Cut.INDETERMINATE) {
                return CompletableFuture.completedFuture(CreateMutationResult.indeterminate());
            }
            if (stored == null) {
                stored = new VersionedSelectorSnapshot(candidate, metadataVersion(++version));
                CreateMutationResult<VersionedSelectorSnapshot> result = cut == Cut.RESPONSE_LOSS
                        ? CreateMutationResult.existingExact(stored)
                        : CreateMutationResult.created(stored);
                return CompletableFuture.completedFuture(result);
            }
            CreateMutationResult<VersionedSelectorSnapshot> result =
                    stored.value().equals(candidate)
                            ? CreateMutationResult.existingExact(stored)
                            : CreateMutationResult.definitiveConflict();
            return CompletableFuture.completedFuture(result);
        }

        @Override
        public CompletionStage<ConditionalCasResult<VersionedSelectorSnapshot>> compareAndSetSelector(
                VersionedSelectorSnapshot exactPredecessor, PulsarTopicGenerationSelectorValueV1 candidate) {
            Cut cut = consumeCut();
            if (cut == Cut.INDETERMINATE) {
                return CompletableFuture.completedFuture(ConditionalCasResult.indeterminate());
            }
            if (stored != null && stored.equals(exactPredecessor)) {
                if (cut == Cut.PREDECESSOR_UNCHANGED) {
                    return CompletableFuture.completedFuture(ConditionalCasResult.predecessorUnchanged(stored));
                }
                stored = new VersionedSelectorSnapshot(candidate, metadataVersion(++version));
                return CompletableFuture.completedFuture(ConditionalCasResult.appliedExact(stored));
            }
            if (stored != null && stored.value().equals(candidate)) {
                return CompletableFuture.completedFuture(ConditionalCasResult.appliedExact(stored));
            }
            return CompletableFuture.completedFuture(ConditionalCasResult.definitiveConflict());
        }

        private Cut consumeCut() {
            Cut cut = nextCut;
            nextCut = Cut.NONE;
            return cut;
        }
    }
}
