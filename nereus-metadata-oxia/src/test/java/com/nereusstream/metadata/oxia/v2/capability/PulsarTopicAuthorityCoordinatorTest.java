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

package com.nereusstream.metadata.oxia.v2.capability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.codec.Nps1SelectorAuthorityCodec;
import com.nereusstream.metadata.oxia.v2.codec.OxiaV2CodecSet;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient.MutationMode;
import com.nereusstream.metadata.oxia.v2.testing.O2TestValues;
import com.nereusstream.metadata.spi.capability.PulsarTopicGenerationSelectorStore;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.model.VersionedSelectorSnapshot;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PulsarTopicAuthorityCoordinatorTest {
    private DeterministicOxiaConditionalClient client;
    private OxiaV2AuthorityKeys keys;
    private PulsarTopicAuthorityCoordinator coordinator;

    @BeforeEach
    void setUp() {
        client = new DeterministicOxiaConditionalClient();
        keys = new OxiaV2AuthorityKeys("/nereus/test");
        OxiaV2CodecSet codecs = OxiaV2CodecSet.productionP1();
        ConditionalMutationEngine engine = new ConditionalMutationEngine(client, new MutationFailureClassifier());
        coordinator = new PulsarTopicAuthorityCoordinator(
                new OxiaTopicBindingAggregatePublisher(() -> {}, keys, codecs.aggregate(), engine),
                new OxiaTopicBindingAggregateReader(() -> {}, keys, codecs.aggregate(), client),
                new OxiaPulsarTopicGenerationSelectorStore(() -> {}, keys, codecs.selector(), client, engine));
    }

    @Test
    void activationPublishesAggregateBeforeSelectingItAndIsIdempotent() {
        var candidate = O2TestValues.productionAggregateCandidate();

        var first = coordinator.activate(candidate).toCompletableFuture().join();
        var retry = coordinator.activate(candidate).toCompletableFuture().join();

        assertThat(first.selector().value().state()).isEqualTo(PulsarTopicGenerationSelectorStateV1.ACTIVE);
        assertThat(first.aggregate().aggregate()).isEqualTo(candidate.aggregate());
        assertThat(retry).isEqualTo(first);
        assertThat(client.createCount()).isEqualTo(3);
        assertThat(client.casCount()).isEqualTo(1);
    }

    @Test
    void responseLossAfterFirstSelectorCreateConvergesExactly() {
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);

        var authority = coordinator
                .activate(O2TestValues.productionAggregateCandidate())
                .toCompletableFuture()
                .join();

        assertThat(authority.selector().value().state()).isEqualTo(PulsarTopicGenerationSelectorStateV1.ACTIVE);
        assertThat(client.createCount()).isEqualTo(2);
    }

    @Test
    void concurrentExactCreatorAcceptsWinnerAlreadyAdvancedToActive() {
        var candidate = O2TestValues.productionAggregateCandidate();
        var incarnation = O2TestValues.incarnation(1);
        var active = Nps1SelectorAuthorityCodec.createValue(
                incarnation.persistenceName(),
                incarnation.bindingGeneration(),
                PulsarTopicGenerationSelectorStateV1.ACTIVE,
                candidate.aggregate().binding().bindingId(),
                candidate.canonicalStoredDigest());
        var activeSnapshot = new VersionedSelectorSnapshot(active, MetadataVersionMapper.fromOxia(2));
        var selector = new ConflictThenActiveSelectorStore(activeSnapshot);
        OxiaV2CodecSet codecs = OxiaV2CodecSet.productionP1();
        ConditionalMutationEngine engine = new ConditionalMutationEngine(client, new MutationFailureClassifier());
        var raceCoordinator = new PulsarTopicAuthorityCoordinator(
                new OxiaTopicBindingAggregatePublisher(() -> {}, keys, codecs.aggregate(), engine),
                new OxiaTopicBindingAggregateReader(() -> {}, keys, codecs.aggregate(), client),
                selector);

        var authority =
                raceCoordinator.activate(candidate).toCompletableFuture().join();

        assertThat(authority.selector()).isEqualTo(activeSnapshot);
        assertThat(selector.readCount).isEqualTo(2);
    }

    @Test
    void deletionAndRecreationUsePermanentGenerationFence() {
        var first = coordinator
                .activate(O2TestValues.productionAggregateCandidate(1))
                .toCompletableFuture()
                .join();
        var deleting = coordinator
                .beginDeletion(first.selector().value().persistenceName())
                .toCompletableFuture()
                .join();
        var deleted =
                coordinator.completeDeletion(deleting).toCompletableFuture().join();

        var recreated = coordinator
                .activate(O2TestValues.productionAggregateCandidate(2))
                .toCompletableFuture()
                .join();

        assertThat(deleted.value().state()).isEqualTo(PulsarTopicGenerationSelectorStateV1.DELETED);
        assertThat(recreated.selector().value().generation().value()).isEqualTo(2);
        assertThat(recreated.selector().value().state()).isEqualTo(PulsarTopicGenerationSelectorStateV1.ACTIVE);
        assertThat(recreated.aggregate().aggregate().binding().incarnationIdentity())
                .isEqualTo(O2TestValues.incarnation(2));
    }

    @Test
    void conflictingReservedSelectorFailsClosedWithoutPublishingAggregate() {
        var candidate = O2TestValues.productionAggregateCandidate();
        var codec = new Nps1SelectorAuthorityCodec();
        var conflict = Nps1SelectorAuthorityCodec.createValue(
                O2TestValues.incarnation(1).persistenceName(),
                O2TestValues.incarnation(1).bindingGeneration(),
                PulsarTopicGenerationSelectorStateV1.RESERVED,
                candidate.aggregate().binding().bindingId(),
                Sha256Digest.hash(O2TestValues.bytes("different aggregate")));
        client.seed(keys.selectorKey(conflict.persistenceName()), codec.encode(conflict), 7);

        assertThatThrownBy(() ->
                        coordinator.activate(candidate).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(PulsarTopicAuthorityException.class)
                .rootCause()
                .extracting(error -> ((PulsarTopicAuthorityException) error).kind())
                .isEqualTo(PulsarTopicAuthorityException.Kind.INVALID_STATE);
        assertThat(client.stored(
                        keys.aggregateKey(candidate.aggregate().binding().incarnationIdentity())))
                .isEmpty();
    }

    @Test
    void deletionRequiresExactActiveAndDeletingStates() {
        assertThatThrownBy(() -> coordinator
                        .beginDeletion(O2TestValues.incarnation(1).persistenceName())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(PulsarTopicAuthorityException.class);
    }

    private static final class ConflictThenActiveSelectorStore implements PulsarTopicGenerationSelectorStore {
        private final VersionedSelectorSnapshot active;
        private int readCount;

        private ConflictThenActiveSelectorStore(VersionedSelectorSnapshot active) {
            this.active = active;
        }

        @Override
        public CompletionStage<Optional<VersionedSelectorSnapshot>> readSelector(
                com.nereusstream.domain.protocol.PulsarPersistenceName persistenceName) {
            readCount++;
            return CompletableFuture.completedFuture(readCount == 1 ? Optional.empty() : Optional.of(active));
        }

        @Override
        public CompletionStage<CreateMutationResult<VersionedSelectorSnapshot>> createSelector(
                PulsarTopicGenerationSelectorValueV1 candidate) {
            return CompletableFuture.completedFuture(CreateMutationResult.definitiveConflict());
        }

        @Override
        public CompletionStage<ConditionalCasResult<VersionedSelectorSnapshot>> compareAndSetSelector(
                VersionedSelectorSnapshot exactPredecessor, PulsarTopicGenerationSelectorValueV1 candidate) {
            return CompletableFuture.failedFuture(new AssertionError("exact ACTIVE successor must not be mutated"));
        }
    }
}
