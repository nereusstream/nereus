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

import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.metadata.oxia.v2.codec.Nps1SelectorAuthorityCodec;
import com.nereusstream.metadata.spi.capability.PulsarTopicGenerationSelectorStore;
import com.nereusstream.metadata.spi.capability.TopicBindingAggregatePublisher;
import com.nereusstream.metadata.spi.capability.TopicBindingAggregateReader;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import com.nereusstream.metadata.spi.model.ConditionalCasOutcome;
import com.nereusstream.metadata.spi.model.CreateMutationOutcome;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot;
import com.nereusstream.metadata.spi.model.VersionedSelectorSnapshot;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** P1 control-plane coordinator over the existing narrow single-key capabilities. */
public final class PulsarTopicAuthorityCoordinator {
    private final TopicBindingAggregatePublisher aggregatePublisher;
    private final TopicBindingAggregateReader aggregateReader;
    private final PulsarTopicGenerationSelectorStore selectorStore;

    public PulsarTopicAuthorityCoordinator(
            TopicBindingAggregatePublisher aggregatePublisher,
            TopicBindingAggregateReader aggregateReader,
            PulsarTopicGenerationSelectorStore selectorStore) {
        this.aggregatePublisher = Objects.requireNonNull(aggregatePublisher, "aggregatePublisher");
        this.aggregateReader = Objects.requireNonNull(aggregateReader, "aggregateReader");
        this.selectorStore = Objects.requireNonNull(selectorStore, "selectorStore");
    }

    /** Reserves, publishes, verifies, and activates one exact Pulsar incarnation. */
    public CompletionStage<PulsarActiveTopicAuthority> activate(AggregatePublicationCandidate aggregateCandidate) {
        return AdapterFutures.localValidation(() -> {
            Objects.requireNonNull(aggregateCandidate, "aggregateCandidate");
            PulsarTopicIncarnationIdentity incarnation = requirePulsar(aggregateCandidate);
            PulsarTopicGenerationSelectorValueV1 reserved =
                    selectorValue(aggregateCandidate, incarnation, PulsarTopicGenerationSelectorStateV1.RESERVED);
            return ensureReserved(reserved).thenCompose(reservation -> ensureAggregate(aggregateCandidate)
                    .thenCompose(aggregate -> ensureActive(reservation, aggregate)));
        });
    }

    /** Establishes the pre-native-delete fence. */
    public CompletionStage<VersionedSelectorSnapshot> beginDeletion(PulsarPersistenceName persistenceName) {
        return AdapterFutures.localValidation(() -> selectorStore
                .readSelector(Objects.requireNonNull(persistenceName, "persistenceName"))
                .thenCompose(current -> {
                    VersionedSelectorSnapshot active =
                            requireState(current, PulsarTopicGenerationSelectorStateV1.ACTIVE, "begin deletion");
                    return applyCas(
                            active,
                            withState(active.value(), PulsarTopicGenerationSelectorStateV1.DELETING),
                            "begin deletion");
                }));
    }

    /** Records the post-native-delete fact. */
    public CompletionStage<VersionedSelectorSnapshot> completeDeletion(VersionedSelectorSnapshot exactDeleting) {
        return AdapterFutures.localValidation(() -> {
            Objects.requireNonNull(exactDeleting, "exactDeleting");
            if (exactDeleting.value().state() != PulsarTopicGenerationSelectorStateV1.DELETING) {
                throw failure(PulsarTopicAuthorityException.Kind.INVALID_STATE, "complete deletion requires DELETING");
            }
            return applyCas(
                    exactDeleting,
                    withState(exactDeleting.value(), PulsarTopicGenerationSelectorStateV1.DELETED),
                    "complete deletion");
        });
    }

    private CompletionStage<VersionedSelectorSnapshot> ensureReserved(PulsarTopicGenerationSelectorValueV1 reserved) {
        return selectorStore.readSelector(reserved.persistenceName()).thenCompose(current -> {
            if (current.isEmpty()) {
                return selectorStore.createSelector(reserved).thenCompose(result -> switch (result.outcome()) {
                    case CREATED, EXISTING_EXACT ->
                        completed(result.exactSnapshot().orElseThrow());
                    case DEFINITIVE_CONFLICT ->
                        failed(
                                PulsarTopicAuthorityException.Kind.DEFINITIVE_CONFLICT,
                                "selector first-create conflict");
                    case INDETERMINATE ->
                        failed(
                                PulsarTopicAuthorityException.Kind.INDETERMINATE,
                                "selector first-create outcome is indeterminate");
                });
            }

            VersionedSelectorSnapshot snapshot = current.orElseThrow();
            if (snapshot.value().equals(reserved)) {
                return completed(snapshot);
            }
            if (snapshot.value().state() == PulsarTopicGenerationSelectorStateV1.ACTIVE
                    && sameSelectedAggregate(snapshot.value(), reserved)) {
                return completed(snapshot);
            }
            if (snapshot.value().state() == PulsarTopicGenerationSelectorStateV1.DELETED
                    && reserved.generation().value()
                            == Math.addExact(snapshot.value().generation().value(), 1)) {
                return applyCas(snapshot, reserved, "selector recreation");
            }
            return failed(
                    PulsarTopicAuthorityException.Kind.INVALID_STATE,
                    "selector cannot reserve requested incarnation from "
                            + snapshot.value().state());
        });
    }

    private CompletionStage<VersionedAggregateSnapshot> ensureAggregate(
            AggregatePublicationCandidate aggregateCandidate) {
        return aggregatePublisher.publishIfAbsent(aggregateCandidate).thenCompose(result -> {
            if (result.outcome() == CreateMutationOutcome.CREATED
                    || result.outcome() == CreateMutationOutcome.EXISTING_EXACT) {
                VersionedAggregateSnapshot snapshot = result.exactSnapshot().orElseThrow();
                requireExactAggregate(snapshot, aggregateCandidate);
                return completed(snapshot);
            }
            if (result.outcome() == CreateMutationOutcome.DEFINITIVE_CONFLICT) {
                return failed(
                        PulsarTopicAuthorityException.Kind.DEFINITIVE_CONFLICT, "immutable aggregate create conflict");
            }
            return failed(
                    PulsarTopicAuthorityException.Kind.INDETERMINATE, "immutable aggregate create is indeterminate");
        });
    }

    private CompletionStage<PulsarActiveTopicAuthority> ensureActive(
            VersionedSelectorSnapshot reservation, VersionedAggregateSnapshot aggregate) {
        if (reservation.value().state() == PulsarTopicGenerationSelectorStateV1.ACTIVE) {
            return verifySelectedAggregate(reservation, aggregate);
        }
        if (reservation.value().state() != PulsarTopicGenerationSelectorStateV1.RESERVED) {
            return failed(PulsarTopicAuthorityException.Kind.INVALID_STATE, "selector is not RESERVED or ACTIVE");
        }
        PulsarTopicGenerationSelectorValueV1 active =
                withState(reservation.value(), PulsarTopicGenerationSelectorStateV1.ACTIVE);
        return applyCas(reservation, active, "selector activation")
                .thenCompose(selected -> verifySelectedAggregate(selected, aggregate));
    }

    private CompletionStage<PulsarActiveTopicAuthority> verifySelectedAggregate(
            VersionedSelectorSnapshot selector, VersionedAggregateSnapshot expectedAggregate) {
        PulsarTopicIncarnationIdentity incarnation = (PulsarTopicIncarnationIdentity)
                expectedAggregate.aggregate().binding().incarnationIdentity();
        return aggregateReader.readAggregate(incarnation).thenCompose(read -> {
            if (read.isEmpty()) {
                return failed(
                        PulsarTopicAuthorityException.Kind.MISSING_AGGREGATE, "ACTIVE selector aggregate is missing");
            }
            VersionedAggregateSnapshot actual = read.orElseThrow();
            if (!actual.equals(expectedAggregate)) {
                return failed(
                        PulsarTopicAuthorityException.Kind.DEFINITIVE_CONFLICT,
                        "ACTIVE selector aggregate differs from the exact published snapshot");
            }
            return completed(new PulsarActiveTopicAuthority(selector, actual));
        });
    }

    private CompletionStage<VersionedSelectorSnapshot> applyCas(
            VersionedSelectorSnapshot predecessor, PulsarTopicGenerationSelectorValueV1 candidate, String operation) {
        return selectorStore.compareAndSetSelector(predecessor, candidate).thenCompose(result -> {
            if (result.outcome() == ConditionalCasOutcome.APPLIED_EXACT) {
                return completed(result.exactSnapshot().orElseThrow());
            }
            if (result.outcome() == ConditionalCasOutcome.PREDECESSOR_UNCHANGED) {
                return failed(
                        PulsarTopicAuthorityException.Kind.INDETERMINATE,
                        operation + " left the exact predecessor unchanged");
            }
            if (result.outcome() == ConditionalCasOutcome.DEFINITIVE_CONFLICT) {
                return failed(PulsarTopicAuthorityException.Kind.DEFINITIVE_CONFLICT, operation + " conflicted");
            }
            return failed(PulsarTopicAuthorityException.Kind.INDETERMINATE, operation + " is indeterminate");
        });
    }

    private static PulsarTopicIncarnationIdentity requirePulsar(AggregatePublicationCandidate candidate) {
        if (!(candidate.aggregate().binding().incarnationIdentity() instanceof PulsarTopicIncarnationIdentity pulsar)) {
            throw new IllegalArgumentException("P1 coordinator accepts only Pulsar aggregate candidates");
        }
        return pulsar;
    }

    private static PulsarTopicGenerationSelectorValueV1 selectorValue(
            AggregatePublicationCandidate aggregateCandidate,
            PulsarTopicIncarnationIdentity incarnation,
            PulsarTopicGenerationSelectorStateV1 state) {
        return Nps1SelectorAuthorityCodec.createValue(
                incarnation.persistenceName(),
                incarnation.bindingGeneration(),
                state,
                aggregateCandidate.aggregate().binding().bindingId(),
                aggregateCandidate.canonicalStoredDigest());
    }

    private static PulsarTopicGenerationSelectorValueV1 withState(
            PulsarTopicGenerationSelectorValueV1 value, PulsarTopicGenerationSelectorStateV1 state) {
        return Nps1SelectorAuthorityCodec.createValue(
                value.persistenceName(),
                value.generation(),
                state,
                value.aggregateBindingId(),
                value.aggregateCanonicalStoredDigest());
    }

    private static boolean sameSelectedAggregate(
            PulsarTopicGenerationSelectorValueV1 left, PulsarTopicGenerationSelectorValueV1 right) {
        return left.persistenceName().equals(right.persistenceName())
                && left.generation().equals(right.generation())
                && left.aggregateBindingId().equals(right.aggregateBindingId())
                && left.aggregateCanonicalStoredDigest().equals(right.aggregateCanonicalStoredDigest());
    }

    private static VersionedSelectorSnapshot requireState(
            Optional<VersionedSelectorSnapshot> current, PulsarTopicGenerationSelectorStateV1 state, String operation) {
        if (current.isEmpty() || current.orElseThrow().value().state() != state) {
            throw failure(
                    PulsarTopicAuthorityException.Kind.INVALID_STATE, operation + " requires selector state " + state);
        }
        return current.orElseThrow();
    }

    private static void requireExactAggregate(
            VersionedAggregateSnapshot snapshot, AggregatePublicationCandidate candidate) {
        if (!snapshot.aggregate().equals(candidate.aggregate())
                || !snapshot.canonicalStoredBytes().equals(candidate.canonicalStoredBytes())
                || !snapshot.canonicalStoredDigest().equals(candidate.canonicalStoredDigest())) {
            throw failure(
                    PulsarTopicAuthorityException.Kind.DEFINITIVE_CONFLICT,
                    "aggregate result is not exact to the publication candidate");
        }
    }

    private static PulsarTopicAuthorityException failure(PulsarTopicAuthorityException.Kind kind, String message) {
        return new PulsarTopicAuthorityException(kind, message);
    }

    private static <T> CompletionStage<T> completed(T value) {
        return CompletableFuture.completedFuture(value);
    }

    private static <T> CompletionStage<T> failed(PulsarTopicAuthorityException.Kind kind, String message) {
        return CompletableFuture.failedFuture(failure(kind, message));
    }
}
