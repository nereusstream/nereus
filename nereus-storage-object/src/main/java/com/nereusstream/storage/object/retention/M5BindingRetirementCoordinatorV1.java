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
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BatchAuthoritySlotV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceScanFenceV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceWriterEnrollmentV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BatchMetadataStateV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.M4ReleaseBindingV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredSourceRetirementBatchTombstoneV1;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Ticket/fence/same-key retirement coordinator selected by ADR 0146. */
public final class M5BindingRetirementCoordinatorV1 {
    public enum Outcome {
        APPLIED_EXACT,
        EXISTING_EXACT,
        DEFINITIVELY_NOT_APPLIED,
        RESPONSE_UNKNOWN,
        RETAIN,
        CONFLICT,
        QUARANTINED
    }

    public record MigrationRequest(String authorityKey, VersionedValue exactLegacySelector) {
        public MigrationRequest {
            authorityKey = requireKey(authorityKey);
            Objects.requireNonNull(exactLegacySelector, "exactLegacySelector");
            if (!authorityKey.equals(exactLegacySelector.key())) {
                throw new IllegalArgumentException("legacy selector key differs from its authority key");
            }
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
            Sha256Digest batchIdSha256,
            Sha256Digest attemptIdSha256,
            List<M4ReleaseBindingV1> releases) {
        public FenceRequest {
            authorityKey = requireKey(authorityKey);
            Objects.requireNonNull(exactOpenAuthority, "exactOpenAuthority");
            M5BindingAuthorityRecordsV1.requireDigest(batchIdSha256, "batchIdSha256");
            M5BindingAuthorityRecordsV1.requireDigest(attemptIdSha256, "attemptIdSha256");
            releases = List.copyOf(Objects.requireNonNull(releases, "releases"));
            requireExactKey(authorityKey, exactOpenAuthority);
        }
    }

    public record RetirementRequest(
            String authorityKey, VersionedValue exactFencedAuthority, ReferenceFreeProofV1 proof) {
        public RetirementRequest {
            authorityKey = requireKey(authorityKey);
            Objects.requireNonNull(exactFencedAuthority, "exactFencedAuthority");
            Objects.requireNonNull(proof, "proof");
            requireExactKey(authorityKey, exactFencedAuthority);
        }
    }

    private final ExactMetadataTransactionStoreV1 metadata;
    private final M5ReferenceFreshnessVerifierV1 freshness;

    public M5BindingRetirementCoordinatorV1(ExactMetadataTransactionStoreV1 metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        freshness = new M5ReferenceFreshnessVerifierV1(metadata);
    }

    public CompletionStage<Optional<VersionedValue>> read(String authorityKey) {
        return metadata.read(requireKey(authorityKey));
    }

    public CompletionStage<Outcome> migrateLegacy(MigrationRequest request) {
        Objects.requireNonNull(request, "request");
        if (M5BindingAuthorityCodecV1.isAuthorityValue(
                request.exactLegacySelector().canonicalStoredBytes())) {
            throw new IllegalArgumentException("migration predecessor is already an authority envelope");
        }
        CanonicalBytes candidate = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.migrateLegacy(
                request.exactLegacySelector().canonicalStoredBytes()));
        return mutate(request.authorityKey(), request.exactLegacySelector(), candidate);
    }

    public CompletionStage<Outcome> acquireTicket(TicketRequest request) {
        Objects.requireNonNull(request, "request");
        BindingRetirementAuthorityV1 current = exactAuthority(request.exactAuthority());
        if (current.state() != BindingAuthorityStateV1.OPEN_V1
                || current.writerEnrollment().isEmpty()) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        if (current.referenceMutationTickets().stream().anyMatch(existing -> existing.operationIdSha256()
                .equals(request.ticket().operationIdSha256()))) {
            return current.referenceMutationTickets().contains(request.ticket())
                    ? CompletableFuture.completedFuture(Outcome.EXISTING_EXACT)
                    : CompletableFuture.completedFuture(Outcome.CONFLICT);
        }
        current.slot(request.ticket().targetIdentitySha256())
                .orElseThrow(() -> new IllegalArgumentException("ticket target has no authority slot"));
        List<ReferenceMutationTicketV1> tickets = new ArrayList<>(current.referenceMutationTickets());
        tickets.add(request.ticket());
        tickets.sort(ticketOrder());
        BindingRetirementAuthorityV1 candidate = M5BindingAuthorityCodecV1.successor(
                current,
                BindingAuthorityStateV1.OPEN_V1,
                current.selectorProjection(),
                current.batchSlots(),
                Optional.empty(),
                tickets);
        return mutate(
                request.authorityKey(), request.exactAuthority(), M5BindingAuthorityCodecV1.encodeAuthority(candidate));
    }

    public CompletionStage<Outcome> enrollWriters(EnrollmentRequest request) {
        Objects.requireNonNull(request, "request");
        BindingRetirementAuthorityV1 current = exactAuthority(request.exactOpenAuthority());
        if (current.state() != BindingAuthorityStateV1.OPEN_V1
                || !current.referenceMutationTickets().isEmpty()) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        if (!request.enrollment().capability().equals(current.capability())) {
            throw new IllegalArgumentException("writer enrollment capability differs from authority");
        }
        if (current.writerEnrollment().isPresent()) {
            return current.writerEnrollment().orElseThrow().equals(request.enrollment())
                    ? CompletableFuture.completedFuture(Outcome.EXISTING_EXACT)
                    : CompletableFuture.completedFuture(Outcome.CONFLICT);
        }
        BindingRetirementAuthorityV1 candidate = M5BindingAuthorityCodecV1.successor(
                current,
                BindingAuthorityStateV1.OPEN_V1,
                current.selectorProjection(),
                current.batchSlots(),
                Optional.empty(),
                List.of(),
                Optional.of(request.enrollment()));
        return mutate(
                request.authorityKey(),
                request.exactOpenAuthority(),
                M5BindingAuthorityCodecV1.encodeAuthority(candidate));
    }

    public CompletionStage<Outcome> clearTicket(TicketRequest request) {
        Objects.requireNonNull(request, "request");
        BindingRetirementAuthorityV1 current = exactAuthority(request.exactAuthority());
        if (current.state() != BindingAuthorityStateV1.OPEN_V1) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        if (!current.referenceMutationTickets().contains(request.ticket())) {
            return CompletableFuture.completedFuture(Outcome.EXISTING_EXACT);
        }
        List<ReferenceMutationTicketV1> tickets = current.referenceMutationTickets().stream()
                .filter(ticket -> !ticket.equals(request.ticket()))
                .toList();
        BindingRetirementAuthorityV1 candidate = M5BindingAuthorityCodecV1.successor(
                current,
                BindingAuthorityStateV1.OPEN_V1,
                current.selectorProjection(),
                current.batchSlots(),
                Optional.empty(),
                tickets);
        return mutate(
                request.authorityKey(), request.exactAuthority(), M5BindingAuthorityCodecV1.encodeAuthority(candidate));
    }

    public CompletionStage<Outcome> fence(FenceRequest request) {
        Objects.requireNonNull(request, "request");
        BindingRetirementAuthorityV1 current = exactAuthority(request.exactOpenAuthority());
        if (current.state() != BindingAuthorityStateV1.OPEN_V1
                || !current.referenceMutationTickets().isEmpty()
                || current.writerEnrollment().isEmpty()) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        BatchAuthoritySlotV1 slot = current.slot(request.batchIdSha256())
                .filter(value -> value.state() == BatchMetadataStateV1.FULL_V1)
                .orElseThrow(() -> new IllegalArgumentException("fence target is not one FULL_V1 slot"));
        SourceRetirementBatch batch = slot.fullBatch();
        validateReleases(batch, request.releases());
        return requireFreshReleases(request.releases()).thenCompose(ignored -> {
            ReferenceScanFenceV1 fence = new ReferenceScanFenceV1(
                    ReferenceTargetKindV1.RETIREMENT_BATCH,
                    request.batchIdSha256(),
                    request.attemptIdSha256(),
                    request.exactOpenAuthority().canonicalStoredSha256());
            BindingRetirementAuthorityV1 candidate = M5BindingAuthorityCodecV1.successor(
                    current,
                    BindingAuthorityStateV1.REFERENCE_SCAN_FENCED_V1,
                    current.selectorProjection(),
                    current.batchSlots(),
                    Optional.of(fence),
                    List.of());
            return mutate(
                    request.authorityKey(),
                    request.exactOpenAuthority(),
                    M5BindingAuthorityCodecV1.encodeAuthority(candidate));
        });
    }

    public CompletionStage<Outcome> abortFence(String authorityKey, VersionedValue exactFencedAuthority) {
        String key = requireKey(authorityKey);
        requireExactKey(key, exactFencedAuthority);
        BindingRetirementAuthorityV1 current = exactAuthority(exactFencedAuthority);
        if (current.state() != BindingAuthorityStateV1.REFERENCE_SCAN_FENCED_V1) {
            return CompletableFuture.completedFuture(Outcome.EXISTING_EXACT);
        }
        BindingRetirementAuthorityV1 candidate = M5BindingAuthorityCodecV1.successor(
                current,
                BindingAuthorityStateV1.OPEN_V1,
                current.selectorProjection(),
                current.batchSlots(),
                Optional.empty(),
                current.referenceMutationTickets());
        return mutate(key, exactFencedAuthority, M5BindingAuthorityCodecV1.encodeAuthority(candidate));
    }

    public CompletionStage<Outcome> retire(RetirementRequest request) {
        Objects.requireNonNull(request, "request");
        BindingRetirementAuthorityV1 current = exactAuthority(request.exactFencedAuthority());
        ReferenceScanFenceV1 fence = current.scanFence()
                .filter(ignored -> current.state() == BindingAuthorityStateV1.REFERENCE_SCAN_FENCED_V1)
                .orElseThrow(() -> new IllegalArgumentException("retirement predecessor is not scan-fenced"));
        ReferenceFreeProofV1 proof = request.proof();
        if (proof.targetKind() != ReferenceTargetKindV1.RETIREMENT_BATCH
                || !proof.targetIdentitySha256().equals(fence.targetIdentitySha256())
                || !proof.selectorRoot().key().equals(request.authorityKey())
                || !proof.selectorRoot()
                        .metadataVersion()
                        .equals(request.exactFencedAuthority().metadataVersion())
                || !proof.selectorRoot()
                        .valueSha256()
                        .equals(request.exactFencedAuthority().canonicalStoredSha256())) {
            throw new IllegalArgumentException("reference-free proof differs from the exact fenced authority");
        }
        BatchAuthoritySlotV1 fullSlot = current.slot(fence.targetIdentitySha256())
                .filter(slot -> slot.state() == BatchMetadataStateV1.FULL_V1)
                .orElseThrow(() -> new IllegalArgumentException("fenced target is not one FULL_V1 slot"));
        validateReleases(fullSlot.fullBatch(), proof.m4Releases());
        RetiredSourceRetirementBatchTombstoneV1 tombstone =
                M5RetentionCodecV1.finalizeRetiredBatch(new RetiredSourceRetirementBatchTombstoneV1(
                        BatchMetadataStateV1.RETIRED_V1,
                        current.binding(),
                        fullSlot.batchIdSha256(),
                        fullSlot.fullBatchSha256(),
                        proof.proofSha256(),
                        request.exactFencedAuthority().metadataVersion(),
                        request.exactFencedAuthority().canonicalStoredSha256(),
                        current.capability(),
                        Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1}))));
        BatchAuthoritySlotV1 retired = new BatchAuthoritySlotV1(
                fullSlot.activationOrdinal(),
                BatchMetadataStateV1.RETIRED_V1,
                fullSlot.batchIdSha256(),
                fullSlot.fullBatchSha256(),
                Optional.empty(),
                Optional.of(tombstone));
        List<BatchAuthoritySlotV1> slots = current.batchSlots().stream()
                .map(slot -> slot.equals(fullSlot) ? retired : slot)
                .toList();
        BindingReadSelector selector = withoutBatch(current.selectorProjection(), fullSlot.batchIdSha256());
        BindingRetirementAuthorityV1 candidate = M5BindingAuthorityCodecV1.successor(
                current,
                BindingAuthorityStateV1.OPEN_V1,
                selector,
                slots,
                Optional.empty(),
                current.referenceMutationTickets());
        CanonicalBytes candidateBytes = M5BindingAuthorityCodecV1.encodeAuthority(candidate);
        return metadata.read(request.authorityKey()).thenCompose(observed -> {
            if (observed.isPresent()
                    && observed.orElseThrow().canonicalStoredBytes().equals(candidateBytes)) {
                return CompletableFuture.completedFuture(Outcome.EXISTING_EXACT);
            }
            if (!observed.equals(Optional.of(request.exactFencedAuthority()))) {
                return CompletableFuture.completedFuture(observed.isEmpty() ? Outcome.QUARANTINED : Outcome.CONFLICT);
            }
            return freshness
                    .requireFresh(proof)
                    .thenCompose(
                            ignored -> mutate(request.authorityKey(), request.exactFencedAuthority(), candidateBytes));
        });
    }

    private CompletionStage<Void> requireFreshReleases(List<M4ReleaseBindingV1> releases) {
        CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
        for (M4ReleaseBindingV1 release : releases) {
            stage = stage.thenCompose(ignored -> metadata.read(
                            release.protectionAuthority().key())
                    .thenApply(observed -> {
                        VersionedValue exact = observed.orElseThrow(
                                () -> new IllegalStateException("M4 RELEASED authority is missing"));
                        if (!exact.metadataVersion()
                                        .equals(release.protectionAuthority().metadataVersion())
                                || !exact.canonicalStoredSha256()
                                        .equals(release.protectionAuthority().valueSha256())
                                || !exact.canonicalStoredBytes().equals(release.canonicalProtectionBytes())) {
                            throw new IllegalStateException("M4 RELEASED authority changed before fencing");
                        }
                        return null;
                    }));
        }
        return stage;
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

    private static BindingRetirementAuthorityV1 exactAuthority(VersionedValue value) {
        if (!M5BindingAuthorityCodecV1.isAuthorityValue(value.canonicalStoredBytes())) {
            throw new IllegalArgumentException("exact predecessor is not a Binding authority envelope");
        }
        return M5BindingAuthorityCodecV1.decodeAuthority(value.canonicalStoredBytes());
    }

    private static void validateReleases(SourceRetirementBatch batch, List<M4ReleaseBindingV1> releases) {
        if (releases.size() != batch.sources().size()) {
            throw new IllegalArgumentException("batch retirement lacks every exact M4 RELEASED member");
        }
        Map<Sha256Digest, M4ReleaseBindingV1> indexed = new HashMap<>();
        for (M4ReleaseBindingV1 release : releases) {
            if (!release.releasedByBatchSha256().equals(batch.batchIdSha256())
                    || indexed.put(release.sourceIdentitySha256(), release) != null) {
                throw new IllegalArgumentException("M4 release BatchId or source uniqueness differs");
            }
        }
        for (SourceProtectionIdentity source : batch.sources()) {
            M4ReleaseBindingV1 release = indexed.get(source.sourceIdentitySha256());
            if (release == null || release.protectionGeneration() != source.protectionGeneration()) {
                throw new IllegalArgumentException("batch member lacks its exact protection-generation release");
            }
        }
    }

    private static BindingReadSelector withoutBatch(BindingReadSelector selector, Sha256Digest batchId) {
        List<SourceRetirementBatch> survivors = selector.activeBatches().stream()
                .filter(batch -> !batch.batchIdSha256().equals(batchId))
                .toList();
        if (survivors.size() + 1 != selector.activeBatches().size()) {
            throw new IllegalArgumentException("selector projection does not contain exactly one fenced batch");
        }
        return new BindingReadSelector(
                selector.binding(),
                selector.selectedViewSha256(),
                selector.ownerEpoch(),
                selector.readAdmissionEpoch(),
                selector.sourceGeneration(),
                selector.mode(),
                selector.admissionState(),
                selector.fallbackSetSha256(),
                selector.capability(),
                selector.pendingAnchors(),
                survivors);
    }

    private static Comparator<ReferenceMutationTicketV1> ticketOrder() {
        return Comparator.comparing((ReferenceMutationTicketV1 value) ->
                        value.targetIdentitySha256().toHex())
                .thenComparing(ReferenceMutationTicketV1::referenceKind)
                .thenComparing(value -> value.operationIdSha256().toHex());
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
