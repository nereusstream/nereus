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
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BatchMetadataStateV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredSourceRetirementBatchTombstoneV1;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Closed records stored in the one M4-selector/M5-retirement authority cell. */
public final class M5BindingAuthorityRecordsV1 {
    public static final int MAX_BATCH_SLOTS = 1_024;
    public static final int MAX_REFERENCE_MUTATION_TICKETS = 256;
    public static final int MAX_AUTHORITY_BYTES = 1_048_576;

    private M5BindingAuthorityRecordsV1() {}

    public enum BindingAuthorityStateV1 {
        OPEN_V1,
        REFERENCE_SCAN_FENCED_V1
    }

    /** One never-reused BatchId slot. Full bytes and a tombstone are mutually exclusive. */
    public record BatchAuthoritySlotV1(
            long activationOrdinal,
            BatchMetadataStateV1 state,
            Sha256Digest batchIdSha256,
            Sha256Digest fullBatchSha256,
            Optional<CanonicalBytes> canonicalM4BatchBytes,
            Optional<RetiredSourceRetirementBatchTombstoneV1> retiredTombstone) {
        public BatchAuthoritySlotV1 {
            Objects.requireNonNull(state, "state");
            requireDigest(batchIdSha256, "batchIdSha256");
            requireDigest(fullBatchSha256, "fullBatchSha256");
            canonicalM4BatchBytes = Objects.requireNonNull(canonicalM4BatchBytes, "canonicalM4BatchBytes");
            retiredTombstone = Objects.requireNonNull(retiredTombstone, "retiredTombstone");
            if (activationOrdinal <= 0) {
                throw new IllegalArgumentException("batch-slot activation ordinal must be positive");
            }
            if (state == BatchMetadataStateV1.FULL_V1) {
                if (canonicalM4BatchBytes.isEmpty() || retiredTombstone.isPresent()) {
                    throw new IllegalArgumentException("FULL_V1 slot must contain only canonical M4 batch bytes");
                }
                CanonicalBytes bytes = canonicalM4BatchBytes.orElseThrow();
                SourceRetirementBatch batch = M4ReadControlCodecV1.decodeBatch(bytes);
                if (!batch.batchIdSha256().equals(batchIdSha256)
                        || !Sha256Digest.hash(bytes).equals(fullBatchSha256)) {
                    throw new IllegalArgumentException("FULL_V1 slot differs from its BatchId or full SHA-256");
                }
            } else {
                if (canonicalM4BatchBytes.isPresent() || retiredTombstone.isEmpty()) {
                    throw new IllegalArgumentException("RETIRED_V1 slot must contain only its permanent tombstone");
                }
                RetiredSourceRetirementBatchTombstoneV1 tombstone = retiredTombstone.orElseThrow();
                if (!tombstone.batchIdSha256().equals(batchIdSha256)
                        || !tombstone.fullBatchSha256().equals(fullBatchSha256)) {
                    throw new IllegalArgumentException("RETIRED_V1 slot differs from its BatchId or full SHA-256");
                }
            }
        }

        public static BatchAuthoritySlotV1 full(long activationOrdinal, SourceRetirementBatch batch) {
            Objects.requireNonNull(batch, "batch");
            CanonicalBytes bytes = M4ReadControlCodecV1.encodeBatch(batch);
            return new BatchAuthoritySlotV1(
                    activationOrdinal,
                    BatchMetadataStateV1.FULL_V1,
                    batch.batchIdSha256(),
                    Sha256Digest.hash(bytes),
                    Optional.of(bytes),
                    Optional.empty());
        }

        public SourceRetirementBatch fullBatch() {
            if (state != BatchMetadataStateV1.FULL_V1) {
                throw new IllegalStateException("retired batch slot has no full M4 batch");
            }
            return M4ReadControlCodecV1.decodeBatch(canonicalM4BatchBytes.orElseThrow());
        }
    }

    /** Durable reservation held across one proof-bound external authority mutation. */
    public record ReferenceMutationTicketV1(
            ReferenceTargetKindV1 targetKind,
            Sha256Digest targetIdentitySha256,
            ReferenceKindV1 referenceKind,
            CapabilityBinding writerCapability,
            Sha256Digest operationIdSha256,
            Sha256Digest externalAuthorityRootSha256) {
        public ReferenceMutationTicketV1 {
            Objects.requireNonNull(targetKind, "targetKind");
            requireDigest(targetIdentitySha256, "targetIdentitySha256");
            Objects.requireNonNull(referenceKind, "referenceKind");
            Objects.requireNonNull(writerCapability, "writerCapability");
            requireDigest(operationIdSha256, "operationIdSha256");
            requireDigest(externalAuthorityRootSha256, "externalAuthorityRootSha256");
        }
    }

    /** Exclusive durable fence that makes the complete target proof vector stable. */
    public record ReferenceScanFenceV1(
            ReferenceTargetKindV1 targetKind,
            Sha256Digest targetIdentitySha256,
            Sha256Digest attemptIdSha256,
            Sha256Digest openAuthorityValueSha256) {
        public ReferenceScanFenceV1 {
            Objects.requireNonNull(targetKind, "targetKind");
            requireDigest(targetIdentitySha256, "targetIdentitySha256");
            requireDigest(attemptIdSha256, "attemptIdSha256");
            requireDigest(openAuthorityValueSha256, "openAuthorityValueSha256");
        }
    }

    /** Exact closed writer inventory admitted to use the ticket protocol. */
    public record ReferenceWriterEnrollmentV1(
            CapabilityBinding capability,
            List<FloorClassV1> floorClasses,
            List<ReferenceKindV1> referenceKinds,
            Sha256Digest implementationRootSha256) {
        public ReferenceWriterEnrollmentV1 {
            Objects.requireNonNull(capability, "capability");
            floorClasses = List.copyOf(Objects.requireNonNull(floorClasses, "floorClasses"));
            referenceKinds = List.copyOf(Objects.requireNonNull(referenceKinds, "referenceKinds"));
            requireDigest(implementationRootSha256, "implementationRootSha256");
            if (!floorClasses.equals(List.of(FloorClassV1.values()))
                    || !referenceKinds.equals(List.of(ReferenceKindV1.values()))) {
                throw new IllegalArgumentException("writer enrollment does not cover the closed proof inventory");
            }
        }
    }

    /** Entire canonical value at the existing Binding selector key. */
    public record BindingRetirementAuthorityV1(
            BindingIdentity binding,
            long authorityGeneration,
            Optional<Sha256Digest> predecessorValueSha256,
            BindingAuthorityStateV1 state,
            BindingReadSelector selectorProjection,
            List<BatchAuthoritySlotV1> batchSlots,
            Optional<ReferenceScanFenceV1> scanFence,
            List<ReferenceMutationTicketV1> referenceMutationTickets,
            Optional<ReferenceWriterEnrollmentV1> writerEnrollment,
            CapabilityBinding capability,
            Sha256Digest authorityCanonicalSha256) {
        public BindingRetirementAuthorityV1 {
            Objects.requireNonNull(binding, "binding");
            predecessorValueSha256 = Objects.requireNonNull(predecessorValueSha256, "predecessorValueSha256");
            predecessorValueSha256.ifPresent(value -> requireDigest(value, "predecessorValueSha256"));
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(selectorProjection, "selectorProjection");
            batchSlots = List.copyOf(Objects.requireNonNull(batchSlots, "batchSlots"));
            scanFence = Objects.requireNonNull(scanFence, "scanFence");
            referenceMutationTickets =
                    List.copyOf(Objects.requireNonNull(referenceMutationTickets, "referenceMutationTickets"));
            writerEnrollment = Objects.requireNonNull(writerEnrollment, "writerEnrollment");
            Objects.requireNonNull(capability, "capability");
            Objects.requireNonNull(authorityCanonicalSha256, "authorityCanonicalSha256");
            if (authorityGeneration <= 0 || (authorityGeneration > 1 && predecessorValueSha256.isEmpty())) {
                throw new IllegalArgumentException("authority generation and predecessor presence disagree");
            }
            if (!binding.equals(selectorProjection.binding()) || !capability.equals(selectorProjection.capability())) {
                throw new IllegalArgumentException("authority Binding/capability differs from its selector projection");
            }
            if (batchSlots.size() > MAX_BATCH_SLOTS
                    || referenceMutationTickets.size() > MAX_REFERENCE_MUTATION_TICKETS) {
                throw new IllegalArgumentException("Binding authority exceeds a hard count cap");
            }
            validateSlots(selectorProjection, batchSlots, binding, capability);
            validateTickets(referenceMutationTickets, batchSlots, capability, writerEnrollment);
            if (writerEnrollment.isPresent()
                    && !writerEnrollment.orElseThrow().capability().equals(capability)) {
                throw new IllegalArgumentException("writer enrollment capability differs from Binding authority");
            }
            if ((state == BindingAuthorityStateV1.REFERENCE_SCAN_FENCED_V1) != scanFence.isPresent()) {
                throw new IllegalArgumentException("Binding authority state and scan fence disagree");
            }
            if (scanFence.isPresent()) {
                ReferenceScanFenceV1 fence = scanFence.orElseThrow();
                if (fence.targetKind() != ReferenceTargetKindV1.RETIREMENT_BATCH
                        || findSlot(batchSlots, fence.targetIdentitySha256())
                                .filter(slot -> slot.state() == BatchMetadataStateV1.FULL_V1)
                                .isEmpty()
                        || referenceMutationTickets.stream().anyMatch(ticket -> ticket.targetIdentitySha256()
                                .equals(fence.targetIdentitySha256()))) {
                    throw new IllegalArgumentException("scan fence lacks one ticket-free FULL_V1 target");
                }
            }
        }

        public Optional<BatchAuthoritySlotV1> slot(Sha256Digest batchId) {
            return findSlot(batchSlots, batchId);
        }
    }

    static BindingReadSelector withoutActiveBatches(BindingReadSelector selector) {
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
                List.of());
    }

    private static void validateSlots(
            BindingReadSelector selector,
            List<BatchAuthoritySlotV1> slots,
            BindingIdentity binding,
            CapabilityBinding capability) {
        long expectedOrdinal = 1;
        Set<Sha256Digest> identities = new HashSet<>();
        for (BatchAuthoritySlotV1 slot : slots) {
            if (slot.activationOrdinal() != expectedOrdinal++ || !identities.add(slot.batchIdSha256())) {
                throw new IllegalArgumentException("batch slots are not activation-ordered and BatchId-unique");
            }
            if (slot.state() == BatchMetadataStateV1.FULL_V1) {
                SourceRetirementBatch batch = slot.fullBatch();
                if (!batch.binding().equals(binding) || !batch.capability().equals(capability)) {
                    throw new IllegalArgumentException("FULL_V1 slot Binding/capability differs");
                }
            } else {
                RetiredSourceRetirementBatchTombstoneV1 tombstone =
                        slot.retiredTombstone().orElseThrow();
                if (!tombstone.binding().equals(binding)
                        || !tombstone.capability().equals(capability)) {
                    throw new IllegalArgumentException("RETIRED_V1 slot Binding/capability differs");
                }
            }
        }
        List<SourceRetirementBatch> projected = slots.stream()
                .filter(slot -> slot.state() == BatchMetadataStateV1.FULL_V1)
                .map(BatchAuthoritySlotV1::fullBatch)
                .toList();
        if (!selector.activeBatches().equals(projected)) {
            throw new IllegalArgumentException("selector active batches differ from FULL_V1 authority slots");
        }
    }

    private static void validateTickets(
            List<ReferenceMutationTicketV1> tickets,
            List<BatchAuthoritySlotV1> slots,
            CapabilityBinding capability,
            Optional<ReferenceWriterEnrollmentV1> writerEnrollment) {
        List<ReferenceMutationTicketV1> sorted = tickets.stream()
                .sorted(java.util.Comparator.comparing((ReferenceMutationTicketV1 value) ->
                                value.targetIdentitySha256().toHex())
                        .thenComparing(ReferenceMutationTicketV1::referenceKind)
                        .thenComparing(value -> value.operationIdSha256().toHex()))
                .toList();
        if (!tickets.equals(sorted) || (!tickets.isEmpty() && writerEnrollment.isEmpty())) {
            throw new IllegalArgumentException("reference tickets are not canonical or lack writer enrollment");
        }
        Set<Sha256Digest> operations = new HashSet<>();
        for (ReferenceMutationTicketV1 ticket : tickets) {
            if (ticket.targetKind() != ReferenceTargetKindV1.RETIREMENT_BATCH
                    || !ticket.writerCapability().equals(capability)
                    || !operations.add(ticket.operationIdSha256())
                    || findSlot(slots, ticket.targetIdentitySha256())
                            .filter(slot -> slot.state() == BatchMetadataStateV1.FULL_V1)
                            .isEmpty()) {
                throw new IllegalArgumentException("reference ticket lacks one unique admitted FULL_V1 target");
            }
        }
    }

    private static Optional<BatchAuthoritySlotV1> findSlot(List<BatchAuthoritySlotV1> slots, Sha256Digest batchId) {
        Objects.requireNonNull(batchId, "batchId");
        return slots.stream()
                .filter(slot -> slot.batchIdSha256().equals(batchId))
                .findFirst();
    }

    static void requireDigest(Sha256Digest value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero()) {
            throw new IllegalArgumentException(name + " must not be zero");
        }
    }
}
