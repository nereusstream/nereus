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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1.RegisteredReferenceWriterV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Dispatch boundary for every proof-bound external mutation.
 *
 * <p>The external mutation is never invoked until its durable ticket is visible. A response-unknown,
 * failed authoritative reread, mismatched reconciliation root, or failed ticket clear leaves the
 * ticket durable and therefore keeps retirement fenced out.
 */
public final class M5ReferenceMutationGuardV1 {
    public enum ExternalMutationOutcomeV1 {
        APPLIED_EXACT,
        EXISTING_EXACT,
        DEFINITIVELY_NOT_APPLIED,
        RESPONSE_UNKNOWN,
        PARTIAL_OR_CONFLICT
    }

    public enum GuardOutcomeV1 {
        MUTATION_APPLIED_AND_TICKET_CLEARED,
        MUTATION_NOT_APPLIED_AND_TICKET_CLEARED,
        RETAINED_BEFORE_EXTERNAL_MUTATION,
        RETAINED_AMBIGUOUS_EXTERNAL_MUTATION,
        RETAINED_TICKET_STATE_UNKNOWN
    }

    public record ExternalMutationResultV1(
            ExternalMutationOutcomeV1 outcome, Sha256Digest authoritativeKeySetRootSha256) {
        public ExternalMutationResultV1 {
            Objects.requireNonNull(outcome, "outcome");
            M5RetentionRecordsV1.requireDigest(authoritativeKeySetRootSha256, "authoritativeKeySetRootSha256");
        }
    }

    @FunctionalInterface
    public interface ExternalMutationV1 {
        CompletionStage<ExternalMutationResultV1> dispatch();
    }

    public record GuardedMutationRequestV1(
            RegisteredReferenceWriterV1 writer,
            ReferenceTargetKindV1 targetKind,
            Sha256Digest targetIdentitySha256,
            Sha256Digest operationIdSha256,
            Sha256Digest externalAuthorityRootSha256) {
        public GuardedMutationRequestV1 {
            Objects.requireNonNull(writer, "writer");
            Objects.requireNonNull(targetKind, "targetKind");
            M5RetentionRecordsV1.requireDigest(targetIdentitySha256, "targetIdentitySha256");
            M5RetentionRecordsV1.requireDigest(operationIdSha256, "operationIdSha256");
            M5RetentionRecordsV1.requireDigest(externalAuthorityRootSha256, "externalAuthorityRootSha256");
            if (targetKind != ReferenceTargetKindV1.RETIREMENT_BATCH
                    && targetKind != ReferenceTargetKindV1.PULSAR_AGGREGATE) {
                throw new IllegalArgumentException("guarded mutation target lacks a retirement authority cell");
            }
        }
    }

    public record GuardResultV1(GuardOutcomeV1 outcome, boolean externalMutationDispatched) {
        public GuardResultV1 {
            Objects.requireNonNull(outcome, "outcome");
            if (!externalMutationDispatched
                    && outcome != GuardOutcomeV1.RETAINED_BEFORE_EXTERNAL_MUTATION
                    && outcome != GuardOutcomeV1.RETAINED_TICKET_STATE_UNKNOWN) {
                throw new IllegalArgumentException("guard result outcome contradicts external dispatch state");
            }
        }
    }

    private enum TicketOutcomeV1 {
        APPLIED_EXACT,
        EXISTING_EXACT,
        RETAIN,
        UNKNOWN_OR_CONFLICT
    }

    private interface TicketAuthorityV1 {
        CompletionStage<Optional<VersionedValue>> read();

        CompletionStage<TicketOutcomeV1> acquire(VersionedValue exact, ReferenceMutationTicketV1 ticket);

        CompletionStage<TicketOutcomeV1> clear(VersionedValue exact, ReferenceMutationTicketV1 ticket);

        boolean containsTicket(VersionedValue exact, ReferenceMutationTicketV1 ticket);

        boolean enrollmentMatches(VersionedValue exact, M5ClosedWriterRegistryV1 registry);
    }

    private final M5ClosedWriterRegistryV1 registry;
    private final TicketAuthorityV1 authority;
    private final ReferenceTargetKindV1 authorityTargetKind;
    private final Sha256Digest authorityTargetIdentitySha256;

    private M5ReferenceMutationGuardV1(
            M5ClosedWriterRegistryV1 registry,
            TicketAuthorityV1 authority,
            ReferenceTargetKindV1 authorityTargetKind,
            Sha256Digest authorityTargetIdentitySha256) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.authorityTargetKind = Objects.requireNonNull(authorityTargetKind, "authorityTargetKind");
        M5RetentionRecordsV1.requireDigest(authorityTargetIdentitySha256, "authorityTargetIdentitySha256");
        this.authorityTargetIdentitySha256 = authorityTargetIdentitySha256;
    }

    public static M5ReferenceMutationGuardV1 forBindingBatch(
            M5ClosedWriterRegistryV1 registry,
            M5BindingRetirementCoordinatorV1 coordinator,
            String authorityKey,
            Sha256Digest batchIdSha256) {
        Objects.requireNonNull(coordinator, "coordinator");
        Objects.requireNonNull(authorityKey, "authorityKey");
        return new M5ReferenceMutationGuardV1(
                registry,
                new TicketAuthorityV1() {
                    @Override
                    public CompletionStage<Optional<VersionedValue>> read() {
                        return coordinator.read(authorityKey);
                    }

                    @Override
                    public CompletionStage<TicketOutcomeV1> acquire(
                            VersionedValue exact, ReferenceMutationTicketV1 ticket) {
                        return coordinator
                                .acquireTicket(
                                        new M5BindingRetirementCoordinatorV1.TicketRequest(authorityKey, exact, ticket))
                                .thenApply(M5ReferenceMutationGuardV1::normalize);
                    }

                    @Override
                    public CompletionStage<TicketOutcomeV1> clear(
                            VersionedValue exact, ReferenceMutationTicketV1 ticket) {
                        return coordinator
                                .clearTicket(
                                        new M5BindingRetirementCoordinatorV1.TicketRequest(authorityKey, exact, ticket))
                                .thenApply(M5ReferenceMutationGuardV1::normalize);
                    }

                    @Override
                    public boolean containsTicket(VersionedValue exact, ReferenceMutationTicketV1 ticket) {
                        BindingRetirementAuthorityV1 value =
                                M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
                        return value.referenceMutationTickets().contains(ticket);
                    }

                    @Override
                    public boolean enrollmentMatches(VersionedValue exact, M5ClosedWriterRegistryV1 exactRegistry) {
                        BindingRetirementAuthorityV1 value =
                                M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
                        return value.writerEnrollment().equals(Optional.of(exactRegistry.enrollment()));
                    }
                },
                ReferenceTargetKindV1.RETIREMENT_BATCH,
                batchIdSha256);
    }

    public static M5ReferenceMutationGuardV1 forPulsarAggregate(
            M5ClosedWriterRegistryV1 registry,
            M5PulsarAggregateRetirementCoordinatorV1 coordinator,
            String authorityKey,
            Sha256Digest originalAggregateSha256) {
        Objects.requireNonNull(coordinator, "coordinator");
        Objects.requireNonNull(authorityKey, "authorityKey");
        return new M5ReferenceMutationGuardV1(
                registry,
                new TicketAuthorityV1() {
                    @Override
                    public CompletionStage<Optional<VersionedValue>> read() {
                        return coordinator.read(authorityKey);
                    }

                    @Override
                    public CompletionStage<TicketOutcomeV1> acquire(
                            VersionedValue exact, ReferenceMutationTicketV1 ticket) {
                        return coordinator
                                .acquireTicket(new M5PulsarAggregateRetirementCoordinatorV1.TicketRequest(
                                        authorityKey, exact, ticket))
                                .thenApply(M5ReferenceMutationGuardV1::normalize);
                    }

                    @Override
                    public CompletionStage<TicketOutcomeV1> clear(
                            VersionedValue exact, ReferenceMutationTicketV1 ticket) {
                        return coordinator
                                .clearTicket(new M5PulsarAggregateRetirementCoordinatorV1.TicketRequest(
                                        authorityKey, exact, ticket))
                                .thenApply(M5ReferenceMutationGuardV1::normalize);
                    }

                    @Override
                    public boolean containsTicket(VersionedValue exact, ReferenceMutationTicketV1 ticket) {
                        PulsarAggregateRetirementAuthorityV1 value =
                                M5PulsarAggregateAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
                        return value.referenceMutationTickets().contains(ticket);
                    }

                    @Override
                    public boolean enrollmentMatches(VersionedValue exact, M5ClosedWriterRegistryV1 exactRegistry) {
                        PulsarAggregateRetirementAuthorityV1 value =
                                M5PulsarAggregateAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
                        return value.writerEnrollment().equals(Optional.of(exactRegistry.enrollment()));
                    }
                },
                ReferenceTargetKindV1.PULSAR_AGGREGATE,
                originalAggregateSha256);
    }

    public CompletionStage<GuardResultV1> execute(
            GuardedMutationRequestV1 request, ExternalMutationV1 externalMutation) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(externalMutation, "externalMutation");
        registry.requireRegistered(request.writer());
        if (request.targetKind() != authorityTargetKind
                || !request.targetIdentitySha256().equals(authorityTargetIdentitySha256)) {
            throw new IllegalArgumentException("guarded mutation target differs from its authority cell");
        }
        ReferenceMutationTicketV1 ticket = new ReferenceMutationTicketV1(
                request.targetKind(),
                request.targetIdentitySha256(),
                request.writer().referenceKind(),
                request.writer().capability(),
                request.operationIdSha256(),
                request.externalAuthorityRootSha256());
        return authority.read().thenCompose(current -> {
            if (current.isEmpty() || !authority.enrollmentMatches(current.orElseThrow(), registry)) {
                return completed(GuardOutcomeV1.RETAINED_BEFORE_EXTERNAL_MUTATION, false);
            }
            return authority.acquire(current.orElseThrow(), ticket).thenCompose(acquired -> {
                if (acquired != TicketOutcomeV1.APPLIED_EXACT && acquired != TicketOutcomeV1.EXISTING_EXACT) {
                    return completed(
                            acquired == TicketOutcomeV1.RETAIN
                                    ? GuardOutcomeV1.RETAINED_BEFORE_EXTERNAL_MUTATION
                                    : GuardOutcomeV1.RETAINED_TICKET_STATE_UNKNOWN,
                            false);
                }
                return authority.read().thenCompose(ticketed -> {
                    if (ticketed.isEmpty() || !authority.containsTicket(ticketed.orElseThrow(), ticket)) {
                        return completed(GuardOutcomeV1.RETAINED_TICKET_STATE_UNKNOWN, false);
                    }
                    return dispatch(externalMutation).thenCompose(external -> {
                        if (external.isEmpty()) {
                            return completed(GuardOutcomeV1.RETAINED_AMBIGUOUS_EXTERNAL_MUTATION, true);
                        }
                        ExternalMutationResultV1 result = external.orElseThrow();
                        boolean terminal = result.outcome() == ExternalMutationOutcomeV1.APPLIED_EXACT
                                || result.outcome() == ExternalMutationOutcomeV1.EXISTING_EXACT
                                || result.outcome() == ExternalMutationOutcomeV1.DEFINITIVELY_NOT_APPLIED;
                        if (!terminal
                                || !result.authoritativeKeySetRootSha256()
                                        .equals(request.externalAuthorityRootSha256())) {
                            return completed(GuardOutcomeV1.RETAINED_AMBIGUOUS_EXTERNAL_MUTATION, true);
                        }
                        return clearVisibleTicket(ticket, result.outcome());
                    });
                });
            });
        });
    }

    private CompletionStage<GuardResultV1> clearVisibleTicket(
            ReferenceMutationTicketV1 ticket, ExternalMutationOutcomeV1 externalOutcome) {
        return authority.read().thenCompose(current -> {
            if (current.isEmpty() || !authority.containsTicket(current.orElseThrow(), ticket)) {
                return completed(GuardOutcomeV1.RETAINED_TICKET_STATE_UNKNOWN, true);
            }
            return authority.clear(current.orElseThrow(), ticket).thenApply(cleared -> {
                if (cleared != TicketOutcomeV1.APPLIED_EXACT && cleared != TicketOutcomeV1.EXISTING_EXACT) {
                    return new GuardResultV1(GuardOutcomeV1.RETAINED_TICKET_STATE_UNKNOWN, true);
                }
                return new GuardResultV1(
                        externalOutcome == ExternalMutationOutcomeV1.DEFINITIVELY_NOT_APPLIED
                                ? GuardOutcomeV1.MUTATION_NOT_APPLIED_AND_TICKET_CLEARED
                                : GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED,
                        true);
            });
        });
    }

    private static CompletionStage<Optional<ExternalMutationResultV1>> dispatch(ExternalMutationV1 externalMutation) {
        try {
            CompletionStage<ExternalMutationResultV1> stage =
                    Objects.requireNonNull(externalMutation.dispatch(), "external mutation stage");
            return stage.handle((result, failure) -> failure == null ? Optional.ofNullable(result) : Optional.empty());
        } catch (RuntimeException failure) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
    }

    private static TicketOutcomeV1 normalize(M5BindingRetirementCoordinatorV1.Outcome outcome) {
        return switch (outcome) {
            case APPLIED_EXACT -> TicketOutcomeV1.APPLIED_EXACT;
            case EXISTING_EXACT -> TicketOutcomeV1.EXISTING_EXACT;
            case RETAIN -> TicketOutcomeV1.RETAIN;
            case DEFINITIVELY_NOT_APPLIED, CONFLICT, QUARANTINED, RESPONSE_UNKNOWN ->
                TicketOutcomeV1.UNKNOWN_OR_CONFLICT;
        };
    }

    private static TicketOutcomeV1 normalize(M5PulsarAggregateRetirementCoordinatorV1.Outcome outcome) {
        return switch (outcome) {
            case APPLIED_EXACT -> TicketOutcomeV1.APPLIED_EXACT;
            case EXISTING_EXACT -> TicketOutcomeV1.EXISTING_EXACT;
            case RETAIN -> TicketOutcomeV1.RETAIN;
            case DEFINITIVELY_NOT_APPLIED, CONFLICT, QUARANTINED, RESPONSE_UNKNOWN ->
                TicketOutcomeV1.UNKNOWN_OR_CONFLICT;
        };
    }

    private static CompletionStage<GuardResultV1> completed(GuardOutcomeV1 outcome, boolean dispatched) {
        return CompletableFuture.completedFuture(new GuardResultV1(outcome, dispatched));
    }
}
