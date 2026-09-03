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

    private final ExactMetadataTransactionStoreV1 metadata;
    private final M5ReferenceFreshnessVerifierV1 freshness;

    public M5PulsarAggregateRetirementCoordinatorV1(ExactMetadataTransactionStoreV1 metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        freshness = new M5ReferenceFreshnessVerifierV1(metadata);
    }

    public CompletionStage<Optional<VersionedValue>> read(String authorityKey) {
        return metadata.read(requireKey(authorityKey));
    }

    public CompletionStage<Outcome> migrateLegacy(MigrationRequest request) {
        Objects.requireNonNull(request, "request");
        if (M5PulsarAggregateAuthorityCodecV1.isAuthorityValue(
                request.exactLegacyAggregate().canonicalStoredBytes())) {
            throw new IllegalArgumentException("migration predecessor is already a Pulsar authority envelope");
        }
        CanonicalBytes candidate =
                M5PulsarAggregateAuthorityCodecV1.encodeAuthority(M5PulsarAggregateAuthorityCodecV1.migrateLegacy(
                        request.exactLegacyAggregate().canonicalStoredBytes(), request.capability()));
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
        Objects.requireNonNull(request, "request");
        PulsarAggregateRetirementAuthorityV1 current = exactAuthority(request.exactAuthority());
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        requireTicket(current, request.ticket());
        if (!current.referenceMutationTickets().contains(request.ticket())) {
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

    public CompletionStage<Outcome> fence(FenceRequest request) {
        Objects.requireNonNull(request, "request");
        PulsarAggregateRetirementAuthorityV1 current = exactAuthority(request.exactOpenAuthority());
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1
                || !current.referenceMutationTickets().isEmpty()
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
