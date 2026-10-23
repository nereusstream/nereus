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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceWriterEnrollmentV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateScanFenceV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.PhysicalCleanupSummaryV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredTopicIncarnationTombstoneV1;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Single-key ticket, fence, and final tombstone transitions for one Pulsar aggregate authority. */
public final class M5PulsarAggregateRetirementCoordinatorV1 {
    public enum Outcome {
        APPLIED_EXACT,
        EXISTING_EXACT,
        DEFINITIVELY_NOT_APPLIED,
        CONFLICT,
        QUARANTINED,
        RETAIN,
        RESPONSE_UNKNOWN
    }

    public record NativeOnlyCreationRequest(
            String authorityKey,
            CanonicalBytes canonicalAggregate,
            ReferenceWriterEnrollmentV1 enrollment,
            M5PulsarAggregateAuthorityRecordsV1.PulsarRetentionPolicyV1 policy,
            Optional<M5PulsarAuditDomainV1> initialAudits) {
        public NativeOnlyCreationRequest(
                String authorityKey,
                CanonicalBytes canonicalAggregate,
                ReferenceWriterEnrollmentV1 enrollment,
                M5PulsarAggregateAuthorityRecordsV1.PulsarRetentionPolicyV1 policy) {
            this(authorityKey, canonicalAggregate, enrollment, policy, Optional.empty());
        }

        public NativeOnlyCreationRequest(
                String authorityKey, CanonicalBytes canonicalAggregate, ReferenceWriterEnrollmentV1 enrollment) {
            this(
                    authorityKey,
                    canonicalAggregate,
                    enrollment,
                    M5PulsarAggregateAuthorityRecordsV1.PulsarRetentionPolicyV1.NATIVE_PULSAR_ONLY_V1);
        }

        public NativeOnlyCreationRequest {
            authorityKey = requireKey(authorityKey);
            Objects.requireNonNull(canonicalAggregate, "canonicalAggregate");
            Objects.requireNonNull(enrollment, "enrollment");
            Objects.requireNonNull(policy, "policy");
            initialAudits = Objects.requireNonNull(initialAudits, "initialAudits");
        }
    }

    public record MigrationRequest(
            String authorityKey, VersionedValue exactLegacyAggregate, CapabilityBinding capability) {
        public MigrationRequest {
            authorityKey = requireKey(authorityKey);
            Objects.requireNonNull(exactLegacyAggregate, "exactLegacyAggregate");
            Objects.requireNonNull(capability, "capability");
            requireExactKey(authorityKey, exactLegacyAggregate);
        }
    }

    public record TicketRequest(String authorityKey, VersionedValue exactAuthority, ReferenceMutationTicketV1 ticket) {
        public TicketRequest {
            authorityKey = requireKey(authorityKey);
            Objects.requireNonNull(exactAuthority, "exactAuthority");
            Objects.requireNonNull(ticket, "ticket");
            requireExactKey(authorityKey, exactAuthority);
        }
    }

    public record EnrollmentRequest(
            String authorityKey, VersionedValue exactOpenAuthority, ReferenceWriterEnrollmentV1 enrollment) {
        public EnrollmentRequest {
            authorityKey = requireKey(authorityKey);
            Objects.requireNonNull(exactOpenAuthority, "exactOpenAuthority");
            Objects.requireNonNull(enrollment, "enrollment");
            requireExactKey(authorityKey, exactOpenAuthority);
        }
    }

    public record FenceRequest(
            String authorityKey,
            VersionedValue exactOpenAuthority,
            String selectorKey,
            VersionedValue exactDeletedSelector,
            PulsarTopicGenerationSelectorValueV1 selectorValue,
            Sha256Digest attemptIdSha256) {
        public FenceRequest {
            authorityKey = requireKey(authorityKey);
            selectorKey = requireKey(selectorKey);
            Objects.requireNonNull(exactOpenAuthority, "exactOpenAuthority");
            Objects.requireNonNull(exactDeletedSelector, "exactDeletedSelector");
            Objects.requireNonNull(selectorValue, "selectorValue");
            M5RetentionRecordsV1.requireDigest(attemptIdSha256, "attemptIdSha256");
            requireExactKey(authorityKey, exactOpenAuthority);
            requireExactKey(selectorKey, exactDeletedSelector);
        }
    }

    public record RetirementRequest(
            String authorityKey,
            VersionedValue exactFencedAuthority,
            ReferenceFreeProofV1 proof,
            PhysicalCleanupSummaryV1 cleanup) {
        public RetirementRequest {
            authorityKey = requireKey(authorityKey);
            Objects.requireNonNull(exactFencedAuthority, "exactFencedAuthority");
            Objects.requireNonNull(proof, "proof");
            Objects.requireNonNull(cleanup, "cleanup");
            requireExactKey(authorityKey, exactFencedAuthority);
        }
    }

    @FunctionalInterface
    public interface AuditMutationAdmissionV1 {
        CompletionStage<Outcome> register(
                M5PulsarAuditDomainV1.Hold hold, java.util.function.Supplier<CompletionStage<Outcome>> mutation);
    }

    private final AuditMutationAdmissionV1 auditAdmission;
    private final ExactMetadataTransactionStoreV1 metadata;
    private final M5ReferenceFreshnessVerifierV1 freshness;

    public M5PulsarAggregateRetirementCoordinatorV1(ExactMetadataTransactionStoreV1 metadata) {
        this(
                metadata,
                (hold, mutation) -> CompletableFuture.failedFuture(new UnsupportedOperationException(
                        "native audit physical membership/writer admission is not installed")));
    }

    public M5PulsarAggregateRetirementCoordinatorV1(
            ExactMetadataTransactionStoreV1 metadata, AuditMutationAdmissionV1 auditAdmission) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.auditAdmission = Objects.requireNonNull(auditAdmission, "auditAdmission");
        freshness = new M5ReferenceFreshnessVerifierV1(metadata);
    }

    public CompletionStage<Optional<VersionedValue>> read(String authorityKey) {
        return metadata.read(requireKey(authorityKey));
    }

    /** Only an absent P1 key may enter this policy; initialization fences every writer until version binding. */
    public CompletionStage<Outcome> createNativeOnly(NativeOnlyCreationRequest request) {
        var initial = M5PulsarAggregateAuthorityCodecV1.initialNativeOnly(
                request.canonicalAggregate(), request.enrollment(), request.policy(), request.initialAudits());
        var bytes = M5PulsarAggregateAuthorityCodecV1.encodeAuthority(initial);
        return metadata.compareAndSet(Optional.empty(), request.authorityKey(), bytes)
                .exceptionally(failure -> MutationOutcome.RESPONSE_UNKNOWN)
                .thenCompose(outcome -> metadata.read(request.authorityKey())
                        .handle((observed, failure) -> failure == null ? observed : Optional.<VersionedValue>empty())
                        .thenCompose(observed -> {
                            if (observed.isEmpty()) {
                                return CompletableFuture.completedFuture(Outcome.RESPONSE_UNKNOWN);
                            }
                            var stored = observed.orElseThrow();
                            if (!M5PulsarAggregateAuthorityCodecV1.isAuthorityValue(stored.canonicalStoredBytes())) {
                                return CompletableFuture.completedFuture(Outcome.CONFLICT);
                            }
                            var current =
                                    M5PulsarAggregateAuthorityCodecV1.decodeAuthority(stored.canonicalStoredBytes());
                            if (!current.canonicalAggregateBytes().equals(initial.canonicalAggregateBytes())
                                    || !current.retentionPolicy().equals(initial.retentionPolicy())
                                    || !current.writerEnrollment().equals(initial.writerEnrollment())
                                    || !current.capability().equals(initial.capability())
                                    || current.auditDomain().isPresent()
                                            != initial.auditDomain().isPresent()) {
                                return CompletableFuture.completedFuture(Outcome.CONFLICT);
                            }
                            if (current.state() != PulsarAggregateAuthorityStateV1.NATIVE_PULSAR_ONLY_INITIALIZING_V5) {
                                return CompletableFuture.completedFuture(
                                        current.originalAggregateVersion().isPresent()
                                                ? Outcome.EXISTING_EXACT
                                                : Outcome.RETAIN);
                            }
                            var active = M5PulsarAggregateAuthorityCodecV1.activateNativeOnly(stored);
                            return mutate(
                                    request.authorityKey(),
                                    stored,
                                    M5PulsarAggregateAuthorityCodecV1.encodeAuthority(active));
                        }));
    }

    /** One durable first-factory identity; missing old parent metadata cannot later be treated as new. */
    public CompletionStage<Outcome> claimFirstNativeFactory(
            String authorityKey, VersionedValue exactOpen, Sha256Digest scope) {
        requireExactKey(requireKey(authorityKey), exactOpen);
        var current = exactAuthority(exactOpen);
        if (current.retentionPolicy().isEmpty() || current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        if (current.firstNativeFactoryScope().isPresent()) {
            return CompletableFuture.completedFuture(
                    current.firstNativeFactoryScope().equals(Optional.of(scope))
                            ? Outcome.EXISTING_EXACT
                            : Outcome.CONFLICT);
        }
        if (!current.referenceMutationTickets().isEmpty()) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        return mutate(
                authorityKey,
                exactOpen,
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(
                        M5PulsarAggregateAuthorityCodecV1.firstNativeFactorySuccessor(current, scope)));
    }

    /** Initial native caller must independently prove its original missing parent before SDK installation. */
    public CompletionStage<Outcome> prepareNativeRetention(
            VersionedValue exactOpen, M5PulsarRetentionConfigurationV1 desired) {
        var current = exactAuthority(exactOpen);
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1
                || current.retentionPolicy().isEmpty()) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        if (current.nativeRetention().isPresent()) {
            var present = current.nativeRetention().orElseThrow();
            return present.sameValues(desired.retentionTimeMillis(), desired.retentionSizeInMB())
                    ? requireFresh(exactOpen).thenApply(ignored -> Outcome.EXISTING_EXACT)
                    : CompletableFuture.completedFuture(Outcome.CONFLICT);
        }
        if (desired.generation() != 1
                || desired.installedByFactoryScope().isPresent()
                || !current.firstNativeFactoryScope().equals(Optional.of(desired.originalFactoryScope()))) {
            return CompletableFuture.completedFuture(Outcome.CONFLICT);
        }
        return mutate(
                exactOpen.key(),
                exactOpen,
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(
                        M5PulsarAggregateAuthorityCodecV1.retentionConfigurationSuccessor(current, desired)));
    }

    /** SDK installation is confirmed by the Native factory, never by a caller's readiness boolean. */
    public CompletionStage<Outcome> confirmNativeRetention(
            VersionedValue exactOpen, M5PulsarRetentionConfigurationV1 installed) {
        var current = exactAuthority(exactOpen);
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1
                || current.nativeRetention().isEmpty()) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        var pending = current.nativeRetention().orElseThrow();
        if (installed.installedByFactoryScope().isEmpty()
                || installed.generation() != pending.generation()
                || !installed.sameValues(pending.retentionTimeMillis(), pending.retentionSizeInMB())
                || !installed.originalFactoryScope().equals(pending.originalFactoryScope())) {
            return CompletableFuture.completedFuture(Outcome.CONFLICT);
        }
        if (pending.installedByFactoryScope().isPresent()) {
            return requireFresh(exactOpen).thenApply(ignored -> Outcome.EXISTING_EXACT);
        }
        return mutate(
                exactOpen.key(),
                exactOpen,
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(
                        M5PulsarAggregateAuthorityCodecV1.retentionConfigurationSuccessor(current, installed)));
    }

    /** Every SDK/reference writer shares P1; absence of all tickets plus this CAS excludes concurrent admission. */
    public CompletionStage<Outcome> updateNativeRetention(
            VersionedValue exactOpen, M5PulsarRetentionConfigurationV1 desired) {
        var current = exactAuthority(exactOpen);
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1
                || !current.referenceMutationTickets().isEmpty()
                || current.nativeRetention().isEmpty()) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        var previous = current.nativeRetention().orElseThrow();
        if (previous.installedByFactoryScope().isEmpty()
                || desired.installedByFactoryScope().isPresent()
                || desired.generation() != Math.addExact(previous.generation(), 1)
                || !desired.originalFactoryScope().equals(previous.originalFactoryScope())) {
            return CompletableFuture.completedFuture(Outcome.CONFLICT);
        }
        return mutate(
                exactOpen.key(),
                exactOpen,
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(
                        M5PulsarAggregateAuthorityCodecV1.retentionConfigurationSuccessor(current, desired)));
    }

    /** Register one durable policy obligation. It is not an SDK actor lease and has no time-based expiry. */
    public CompletionStage<Outcome> registerAuditHold(
            String authorityKey,
            VersionedValue exactOpen,
            M5ClosedWriterRegistryV1 registry,
            Sha256Digest obligationId,
            com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2 resource,
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 coverage) {
        requireExactKey(requireKey(authorityKey), exactOpen);
        var current = exactAuthority(exactOpen);
        if (!auditWritable(current, registry)) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        var holds = new ArrayList<>(current.auditDomain().orElseThrow().holds());
        var existing = holds.stream()
                .filter(hold -> hold.obligationId().equals(obligationId))
                .findFirst();
        if (existing.isPresent()) {
            var hold = existing.orElseThrow();
            return hold.resource().equals(resource) && hold.coverage().equals(coverage)
                    ? requireFresh(exactOpen).thenApply(ignored -> Outcome.EXISTING_EXACT)
                    : CompletableFuture.completedFuture(Outcome.CONFLICT);
        }
        var hold = new M5PulsarAuditDomainV1.Hold(
                obligationId, resource, coverage, Math.addExact(current.authorityGeneration(), 1));
        holds.add(hold);
        holds.sort(Comparator.comparing(value -> value.obligationId().toHex()));
        var candidate =
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(M5PulsarAggregateAuthorityCodecV1.auditSuccessor(
                        current, current.auditDomain().orElseThrow().withHolds(holds)));
        return auditAdmission.register(hold, () -> mutate(authorityKey, exactOpen, candidate));
    }

    public CompletionStage<Outcome> clearAuditHold(
            String authorityKey,
            VersionedValue exactOpen,
            M5ClosedWriterRegistryV1 registry,
            M5PulsarAuditDomainV1.Hold originalHold) {
        requireExactKey(requireKey(authorityKey), exactOpen);
        var current = exactAuthority(exactOpen);
        if (!auditWritable(current, registry)) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        var holds = new ArrayList<>(current.auditDomain().orElseThrow().holds());
        var existing = holds.stream()
                .filter(hold -> hold.obligationId().equals(originalHold.obligationId()))
                .findFirst();
        if (existing.isEmpty()) {
            return current.authorityGeneration() < originalHold.admissionGeneration()
                    ? CompletableFuture.completedFuture(Outcome.RETAIN)
                    : requireFresh(exactOpen).thenApply(ignored -> Outcome.EXISTING_EXACT);
        }
        if (!existing.orElseThrow().equals(originalHold)) {
            return CompletableFuture.completedFuture(Outcome.CONFLICT);
        }
        holds.remove(originalHold);
        return mutate(
                authorityKey,
                exactOpen,
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(M5PulsarAggregateAuthorityCodecV1.auditSuccessor(
                        current, current.auditDomain().orElseThrow().withHolds(holds))));
    }

    /** Excludes an original uncertain conditional audit write without changing any current obligation. */
    public CompletionStage<Outcome> fenceAuditAttempt(
            String authorityKey, VersionedValue exactOpen, M5ClosedWriterRegistryV1 registry) {
        requireExactKey(requireKey(authorityKey), exactOpen);
        var current = exactAuthority(exactOpen);
        if (!auditWritable(current, registry)) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        return mutate(
                authorityKey,
                exactOpen,
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(M5PulsarAggregateAuthorityCodecV1.auditSuccessor(
                        current, current.auditDomain().orElseThrow())));
    }

    private static boolean auditWritable(
            PulsarAggregateRetirementAuthorityV1 current, M5ClosedWriterRegistryV1 registry) {
        Objects.requireNonNull(registry, "registry");
        registry.requireRegistered(registry.writerFor(M5RetentionRecordsV1.ReferenceKindV1.AUDIT_GRACE));
        return current.state() == PulsarAggregateAuthorityStateV1.OPEN_V1
                && current.auditDomain().isPresent()
                && current.writerEnrollment().equals(Optional.of(registry.enrollment()));
    }

    public CompletionStage<Outcome> migrateLegacy(MigrationRequest request) {
        Objects.requireNonNull(request, "request");
        if (M5PulsarAggregateAuthorityCodecV1.isAuthorityValue(
                request.exactLegacyAggregate().canonicalStoredBytes())) {
            throw new IllegalArgumentException("migration predecessor is already a Pulsar authority envelope");
        }
        CanonicalBytes candidate = M5PulsarAggregateAuthorityCodecV1.encodeAuthority(
                M5PulsarAggregateAuthorityCodecV1.migrateLegacy(request.exactLegacyAggregate(), request.capability()));
        return mutate(request.authorityKey(), request.exactLegacyAggregate(), candidate);
    }

    public CompletionStage<Outcome> enrollWriters(EnrollmentRequest request) {
        Objects.requireNonNull(request, "request");
        PulsarAggregateRetirementAuthorityV1 current = exactAuthority(request.exactOpenAuthority());
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1
                || !current.referenceMutationTickets().isEmpty()) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        if (!request.enrollment().capability().equals(current.capability())) {
            throw new IllegalArgumentException("writer enrollment capability differs from Pulsar authority");
        }
        if (current.writerEnrollment().isPresent()) {
            return current.writerEnrollment().orElseThrow().equals(request.enrollment())
                    ? CompletableFuture.completedFuture(Outcome.EXISTING_EXACT)
                    : CompletableFuture.completedFuture(Outcome.CONFLICT);
        }
        PulsarAggregateRetirementAuthorityV1 candidate = M5PulsarAggregateAuthorityCodecV1.successor(
                current,
                PulsarAggregateAuthorityStateV1.OPEN_V1,
                Optional.empty(),
                List.of(),
                Optional.of(request.enrollment()));
        return mutate(
                request.authorityKey(),
                request.exactOpenAuthority(),
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(candidate));
    }

    public CompletionStage<Outcome> acquireTicket(TicketRequest request) {
        Objects.requireNonNull(request, "request");
        PulsarAggregateRetirementAuthorityV1 current = exactAuthority(request.exactAuthority());
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1
                || current.writerEnrollment().isEmpty()) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        if (current.auditDomain().isPresent()
                && request.ticket().referenceKind() == M5RetentionRecordsV1.ReferenceKindV1.AUDIT_GRACE) {
            return CompletableFuture.completedFuture(Outcome.CONFLICT);
        }
        if (current.retentionPolicy()
                .filter(policy -> policy.excludesReference(request.ticket().referenceKind()))
                .isPresent()) {
            return CompletableFuture.completedFuture(Outcome.CONFLICT);
        }
        if (current.referenceMutationTickets().stream().anyMatch(existing -> existing.operationIdSha256()
                .equals(request.ticket().operationIdSha256()))) {
            return current.referenceMutationTickets().contains(request.ticket())
                    ? CompletableFuture.completedFuture(Outcome.EXISTING_EXACT)
                    : CompletableFuture.completedFuture(Outcome.CONFLICT);
        }
        requireTicket(current, request.ticket());
        List<ReferenceMutationTicketV1> tickets = new ArrayList<>(current.referenceMutationTickets());
        tickets.add(request.ticket());
        tickets.sort(ticketOrder());
        PulsarAggregateRetirementAuthorityV1 candidate = M5PulsarAggregateAuthorityCodecV1.successor(
                current, PulsarAggregateAuthorityStateV1.OPEN_V1, Optional.empty(), tickets);
        return mutate(
                request.authorityKey(),
                request.exactAuthority(),
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(candidate));
    }

    public CompletionStage<Outcome> clearTicket(TicketRequest request) {
        return clearTicket(request, false);
    }

    /**
     * Close an original acquire attempt whose external mutation was never dispatched. Even an absent
     * ticket requires a confirmed successor version, so a delayed acquire cannot apply afterward.
     */
    public CompletionStage<Outcome> fenceUndispatchedTicket(TicketRequest request) {
        Objects.requireNonNull(request, "request");
        var current = exactAuthority(request.exactAuthority());
        requireTicket(current, request.ticket());
        if (current.state() == PulsarAggregateAuthorityStateV1.REFERENCE_SCAN_FENCED_V1
                && current.referenceMutationTickets().isEmpty()) {
            // The original acquire could only use an OPEN predecessor. This confirmed fence already
            // excludes its delayed CAS; do not alter a fence bound to a reference scan's version vector.
            return metadata.read(request.authorityKey())
                    .thenApply(observed ->
                            observed.filter(request.exactAuthority()::equals).isPresent()
                                    ? Outcome.EXISTING_EXACT
                                    : Outcome.CONFLICT);
        }
        return clearTicket(request, true);
    }

    private CompletionStage<Outcome> clearTicket(TicketRequest request, boolean advanceAbsentTicket) {
        Objects.requireNonNull(request, "request");
        PulsarAggregateRetirementAuthorityV1 current = exactAuthority(request.exactAuthority());
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        requireTicket(current, request.ticket());
        if (!advanceAbsentTicket && !current.referenceMutationTickets().contains(request.ticket())) {
            return CompletableFuture.completedFuture(Outcome.EXISTING_EXACT);
        }
        List<ReferenceMutationTicketV1> tickets = current.referenceMutationTickets().stream()
                .filter(ticket -> !ticket.equals(request.ticket()))
                .toList();
        PulsarAggregateRetirementAuthorityV1 candidate = M5PulsarAggregateAuthorityCodecV1.successor(
                current, PulsarAggregateAuthorityStateV1.OPEN_V1, Optional.empty(), tickets);
        return mutate(
                request.authorityKey(),
                request.exactAuthority(),
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(candidate));
    }

    /**
     * A first-admitted Native aggregate has no separate M5 aggregate-retirement batch namespace.
     * Its aggregate retirement
     * path consumes this same P1 key and a DELETED selector. The source-scan successor excludes that path and every
     * registered in-flight retirement writer; a prior retired aggregate would already be a permanent tombstone.
     * M4 source-retirement batches/protections must additionally be enumerated by the Native control producer.
     */
    public CompletionStage<Void> requireNativeRetirementQuiescent(
            VersionedValue exactScan, M5ClosedWriterRegistryV1 registry) {
        var current = exactAuthority(exactScan);
        registry.requireRegistered(
                registry.writerFor(M5RetentionRecordsV1.ReferenceKindV1.RETIREMENT_OR_DELETE_RECONCILIATION));
        if (current.state() != PulsarAggregateAuthorityStateV1.LOGICAL_TRIM_SOURCE_SCAN_FENCED_V3
                || current.logicalTrimSourceScan().isEmpty()
                || current.retentionPolicy().isEmpty()
                || current.firstNativeFactoryScope().isEmpty()
                || !current.referenceMutationTickets().isEmpty()
                || !current.writerEnrollment().equals(Optional.of(registry.enrollment()))) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Native retirement enumeration lacks its original closed P1 source scan"));
        }
        return requireFresh(exactScan);
    }

    public CompletionStage<Outcome> fence(FenceRequest request) {
        Objects.requireNonNull(request, "request");
        PulsarAggregateRetirementAuthorityV1 current = exactAuthority(request.exactOpenAuthority());
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1
                || !current.referenceMutationTickets().isEmpty()
                || current.auditDomain()
                        .filter(domain -> !domain.holds().isEmpty())
                        .isPresent()
                || current.writerEnrollment().isEmpty()) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        validateDeletedSelector(current, request.exactDeletedSelector(), request.selectorValue());
        AuthorityFactV1 selectorAuthority = fact(request.exactDeletedSelector());
        return requireFresh(request.exactDeletedSelector()).thenCompose(ignored -> {
            PulsarAggregateScanFenceV1 fence = new PulsarAggregateScanFenceV1(
                    current.originalAggregateSha256(),
                    request.attemptIdSha256(),
                    request.exactOpenAuthority().canonicalStoredSha256(),
                    selectorAuthority);
            PulsarAggregateRetirementAuthorityV1 candidate = M5PulsarAggregateAuthorityCodecV1.successor(
                    current, PulsarAggregateAuthorityStateV1.REFERENCE_SCAN_FENCED_V1, Optional.of(fence), List.of());
            return mutate(
                    request.authorityKey(),
                    request.exactOpenAuthority(),
                    M5PulsarAggregateAuthorityCodecV1.encodeAuthority(candidate));
        });
    }

    public CompletionStage<Outcome> abortFence(String authorityKey, VersionedValue exactFencedAuthority) {
        String key = requireKey(authorityKey);
        requireExactKey(key, exactFencedAuthority);
        PulsarAggregateRetirementAuthorityV1 current = exactAuthority(exactFencedAuthority);
        if (current.state() != PulsarAggregateAuthorityStateV1.REFERENCE_SCAN_FENCED_V1) {
            return CompletableFuture.completedFuture(Outcome.EXISTING_EXACT);
        }
        PulsarAggregateRetirementAuthorityV1 candidate = M5PulsarAggregateAuthorityCodecV1.successor(
                current, PulsarAggregateAuthorityStateV1.OPEN_V1, Optional.empty(), current.referenceMutationTickets());
        return mutate(key, exactFencedAuthority, M5PulsarAggregateAuthorityCodecV1.encodeAuthority(candidate));
    }

    public CompletionStage<Outcome> retire(RetirementRequest request) {
        Objects.requireNonNull(request, "request");
        PulsarAggregateRetirementAuthorityV1 current = exactAuthority(request.exactFencedAuthority());
        PulsarAggregateScanFenceV1 fence = current.scanFence()
                .filter(ignored -> current.state() == PulsarAggregateAuthorityStateV1.REFERENCE_SCAN_FENCED_V1)
                .orElseThrow(() -> new IllegalArgumentException("Pulsar retirement predecessor is not scan-fenced"));
        validateProofAndCleanup(current, fence, request.proof(), request.cleanup());
        RetiredTopicIncarnationTombstoneV1 tombstone =
                M5RetentionCodecV1.finalizeRetiredPulsar(new RetiredTopicIncarnationTombstoneV1(
                        current.incarnation(),
                        current.bindingId(),
                        current.originalAggregateSha256(),
                        request.proof().proofSha256(),
                        current.incarnation().bindingGeneration().value(),
                        PulsarTopicGenerationSelectorStateV1.DELETED,
                        fence.deletedSelectorAuthority().metadataVersion(),
                        request.exactFencedAuthority().metadataVersion(),
                        request.exactFencedAuthority().canonicalStoredSha256(),
                        current.capability(),
                        current.originalAggregateSha256()));
        CanonicalBytes candidate = M5RetentionCodecV1.encodeRetiredPulsar(tombstone);
        return metadata.read(request.authorityKey()).thenCompose(observed -> {
            if (observed.isPresent()
                    && observed.orElseThrow().canonicalStoredBytes().equals(candidate)) {
                return CompletableFuture.completedFuture(Outcome.EXISTING_EXACT);
            }
            if (!observed.equals(Optional.of(request.exactFencedAuthority()))) {
                return CompletableFuture.completedFuture(observed.isEmpty() ? Outcome.QUARANTINED : Outcome.CONFLICT);
            }
            return freshness
                    .requireFresh(request.proof())
                    .thenCompose(ignored -> requireFresh(request.cleanup().cleanupRoot()))
                    .thenCompose(ignored -> requireFresh(fence.deletedSelectorAuthority()))
                    .thenCompose(ignored -> mutate(request.authorityKey(), request.exactFencedAuthority(), candidate));
        });
    }

    private static void validateDeletedSelector(
            PulsarAggregateRetirementAuthorityV1 current,
            VersionedValue exactSelector,
            PulsarTopicGenerationSelectorValueV1 selector) {
        if (!exactSelector.canonicalStoredBytes().equals(selector.canonicalStoredBytes())
                || !exactSelector.canonicalStoredSha256().equals(selector.canonicalStoredDigest())
                || selector.state() != PulsarTopicGenerationSelectorStateV1.DELETED
                || !selector.persistenceName().equals(current.incarnation().persistenceName())
                || !selector.generation().equals(current.incarnation().bindingGeneration())
                || !selector.aggregateBindingId().equals(current.bindingId())
                || !selector.aggregateCanonicalStoredDigest().equals(current.originalAggregateSha256())) {
            throw new IllegalArgumentException("Pulsar authority fence lacks its exact DELETED generation selector");
        }
    }

    private static void validateProofAndCleanup(
            PulsarAggregateRetirementAuthorityV1 current,
            PulsarAggregateScanFenceV1 fence,
            ReferenceFreeProofV1 proof,
            PhysicalCleanupSummaryV1 cleanup) {
        if (proof.targetKind() != ReferenceTargetKindV1.PULSAR_AGGREGATE
                || !proof.targetIdentitySha256().equals(current.originalAggregateSha256())
                || !proof.selectorRoot().equals(fence.deletedSelectorAuthority())
                || !proof.identity().binding().bindingId().equals(current.bindingId())
                || !proof.identity().capability().equals(current.capability())
                || !cleanup.incarnation().equals(current.incarnation())
                || !cleanup.bindingId().equals(current.bindingId())
                || !cleanup.originalAggregateSha256().equals(current.originalAggregateSha256())
                || !cleanup.capability().equals(current.capability())) {
            throw new IllegalArgumentException("Pulsar proof or completed cleanup differs from the fenced authority");
        }
    }

    private CompletionStage<Void> requireFresh(VersionedValue expected) {
        return metadata.read(expected.key()).thenApply(observed -> {
            if (!observed.equals(Optional.of(expected))) {
                throw new M5ReferenceFreshnessVerifierV1.StaleAuthorityException(
                        "Pulsar selector changed from its exact DELETED predecessor");
            }
            return null;
        });
    }

    private CompletionStage<Void> requireFresh(AuthorityFactV1 expected) {
        return metadata.read(expected.key()).thenApply(observed -> {
            VersionedValue exact = observed.orElseThrow(
                    () -> new M5ReferenceFreshnessVerifierV1.StaleAuthorityException("required authority is missing"));
            if (!exact.metadataVersion().equals(expected.metadataVersion())
                    || !exact.canonicalStoredSha256().equals(expected.valueSha256())) {
                throw new M5ReferenceFreshnessVerifierV1.StaleAuthorityException(
                        "required authority changed from its proof-bound value");
            }
            return null;
        });
    }

    private CompletionStage<Outcome> mutate(String key, VersionedValue predecessor, CanonicalBytes candidate) {
        return metadata.compareAndSet(Optional.of(predecessor), key, candidate)
                .thenCompose(outcome -> metadata.read(key)
                        .handle((observed, failure) -> reconcile(predecessor, candidate, outcome, observed, failure)));
    }

    private static Outcome reconcile(
            VersionedValue predecessor,
            CanonicalBytes candidate,
            MutationOutcome mutation,
            Optional<VersionedValue> observed,
            Throwable readFailure) {
        if (readFailure != null || observed == null) {
            return Outcome.RESPONSE_UNKNOWN;
        }
        if (observed.isPresent()
                && observed.orElseThrow().canonicalStoredBytes().equals(candidate)) {
            return mutation == MutationOutcome.APPLIED_EXACT ? Outcome.APPLIED_EXACT : Outcome.EXISTING_EXACT;
        }
        if (observed.isPresent() && observed.orElseThrow().equals(predecessor)) {
            return mutation == MutationOutcome.PREDECESSOR_UNCHANGED || mutation == MutationOutcome.DEFINITIVE_CONFLICT
                    ? Outcome.DEFINITIVELY_NOT_APPLIED
                    : Outcome.RESPONSE_UNKNOWN;
        }
        return observed.isEmpty() ? Outcome.QUARANTINED : Outcome.CONFLICT;
    }

    private static PulsarAggregateRetirementAuthorityV1 exactAuthority(VersionedValue value) {
        if (!M5PulsarAggregateAuthorityCodecV1.isAuthorityValue(value.canonicalStoredBytes())) {
            throw new IllegalArgumentException("exact predecessor is not a Pulsar aggregate authority envelope");
        }
        return M5PulsarAggregateAuthorityCodecV1.decodeAuthority(value.canonicalStoredBytes());
    }

    private static void requireTicket(PulsarAggregateRetirementAuthorityV1 current, ReferenceMutationTicketV1 ticket) {
        if (ticket.targetKind() != ReferenceTargetKindV1.PULSAR_AGGREGATE
                || !ticket.targetIdentitySha256().equals(current.originalAggregateSha256())
                || !ticket.writerCapability().equals(current.capability())) {
            throw new IllegalArgumentException("ticket differs from the Pulsar aggregate authority target");
        }
    }

    private static Comparator<ReferenceMutationTicketV1> ticketOrder() {
        return Comparator.comparing((ReferenceMutationTicketV1 value) ->
                        value.targetIdentitySha256().toHex())
                .thenComparing(ReferenceMutationTicketV1::referenceKind)
                .thenComparing(value -> value.operationIdSha256().toHex());
    }

    private static AuthorityFactV1 fact(VersionedValue value) {
        return new AuthorityFactV1(value.key(), value.metadataVersion(), value.canonicalStoredSha256());
    }

    private static void requireExactKey(String key, VersionedValue value) {
        if (!key.equals(value.key())) {
            throw new IllegalArgumentException("exact predecessor key differs from its authority key");
        }
    }

    private static String requireKey(String value) {
        Objects.requireNonNull(value, "authorityKey");
        if (value.isBlank()) {
            throw new IllegalArgumentException("authority key must not be blank");
        }
        return value;
    }
}
