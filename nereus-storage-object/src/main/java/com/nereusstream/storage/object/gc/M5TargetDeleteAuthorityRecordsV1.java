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

package com.nereusstream.storage.object.gc;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Closed, one-target-only records for the permanent M5-D physical-delete authority cell. */
public final class M5TargetDeleteAuthorityRecordsV1 {
    public static final int MAX_AUTHORITY_BYTES = ExactMetadataTransactionStoreV1.MAX_VALUE_BYTES;
    public static final int MAX_TARGET_IDENTITY_BYTES = 65_536;
    public static final int MAX_EXTERNAL_IDENTITY_BYTES = 262_144;
    public static final int MAX_WRITER_TICKETS = 256;

    private M5TargetDeleteAuthorityRecordsV1() {}

    public enum PhysicalDeleteTargetKindV1 {
        OBJECT_VERSION_V1,
        BOOKKEEPER_LEDGER_V1,
        PULSAR_ROOT_OBJECT_V1,
        PULSAR_DATA_OBJECT_V1,
        MULTIPART_UPLOAD_V1
    }

    public enum TargetDeleteAuthorityStateV1 {
        OPEN_V1,
        READ_FENCED_V1,
        DELETE_INTENT_V1,
        DELETE_DONE_V1
    }

    /** The complete set of writer classes that can change one M5-D eligibility proof. */
    public enum ProofBoundWriterClassV1 {
        M4_SOURCE_PROTECTION_RELEASE_V1,
        MANIFEST_SELECTOR_GENERATION_REPRESENTATION_V1,
        LOGICAL_TRIM_RETENTION_FLOOR_V1,
        REFERENCE_SHARED_PHYSICAL_MEMBER_V1,
        REPLICA_TOPOLOGY_V1,
        MULTIPART_INVENTORY_PUBLICATION_RESPONSE_LOSS_V1,
        TASK_PROJECTION_MIGRATION_RECOVERY_V1,
        OWNER_WORKER_LEASE_HANDLE_PIN_V1,
        PROVIDER_KMS_STORAGE_PROFILE_CAPABILITY_V1,
        DISPATCH_OWNER_CELL_RESERVATION_V1
    }

    public enum ExternalIdentityObservationV1 {
        PRESENT_EXACT_V1,
        ABSENT_EXACT_V1
    }

    public enum DeleteTerminalOutcomeV1 {
        DELETED_EXACT_V1,
        ALREADY_ABSENT_EXACT_V1
    }

    /** Immutable target bytes and their Cell-bound domain-separated identity. */
    public record PhysicalDeleteTargetV1(
            CellProviderScopeId cellProviderScopeId,
            PhysicalDeleteTargetKindV1 targetKind,
            CanonicalBytes exactTargetIdentity,
            Sha256Digest targetIdentitySha256) {
        public PhysicalDeleteTargetV1 {
            Objects.requireNonNull(cellProviderScopeId, "cellProviderScopeId");
            Objects.requireNonNull(targetKind, "targetKind");
            requireBytes(exactTargetIdentity, MAX_TARGET_IDENTITY_BYTES, "exactTargetIdentity");
            requireDigest(targetIdentitySha256, "targetIdentitySha256");
            Sha256Digest expected = M5TargetDeleteAuthorityKeysV1.targetIdentitySha256(
                    cellProviderScopeId, targetKind, exactTargetIdentity);
            if (!targetIdentitySha256.equals(expected)) {
                throw new IllegalArgumentException("target identity SHA-256 differs from its exact target bytes");
            }
        }

        public static PhysicalDeleteTargetV1 create(
                CellProviderScopeId cellProviderScopeId,
                PhysicalDeleteTargetKindV1 targetKind,
                CanonicalBytes exactTargetIdentity) {
            return new PhysicalDeleteTargetV1(
                    cellProviderScopeId,
                    targetKind,
                    exactTargetIdentity,
                    M5TargetDeleteAuthorityKeysV1.targetIdentitySha256(
                            cellProviderScopeId, targetKind, exactTargetIdentity));
        }
    }

    /** Exact closed inventory required before a target can become eligible. */
    public record ProofBoundWriterEnrollmentV1(
            List<ProofBoundWriterClassV1> writerClasses,
            Sha256Digest capabilitySetRootSha256,
            Sha256Digest implementationRootSha256,
            Sha256Digest policyRootSha256) {
        public ProofBoundWriterEnrollmentV1 {
            writerClasses = List.copyOf(Objects.requireNonNull(writerClasses, "writerClasses"));
            requireDigest(capabilitySetRootSha256, "capabilitySetRootSha256");
            requireDigest(implementationRootSha256, "implementationRootSha256");
            requireDigest(policyRootSha256, "policyRootSha256");
            if (!writerClasses.equals(List.of(ProofBoundWriterClassV1.values()))) {
                throw new IllegalArgumentException("writer enrollment does not cover the exact closed inventory");
            }
        }
    }

    /** Visible durable reservation held before one proof-changing external mutation dispatches. */
    public record ProofBoundWriterTicketV1(
            ProofBoundWriterClassV1 writerClass,
            Sha256Digest operationIdSha256,
            Sha256Digest capabilitySha256,
            Sha256Digest ownerFenceSha256,
            Sha256Digest externalFactsRootSha256,
            long predecessorAuthorityRevision) {
        public ProofBoundWriterTicketV1 {
            Objects.requireNonNull(writerClass, "writerClass");
            requireDigest(operationIdSha256, "operationIdSha256");
            requireDigest(capabilitySha256, "capabilitySha256");
            requireDigest(ownerFenceSha256, "ownerFenceSha256");
            requireDigest(externalFactsRootSha256, "externalFactsRootSha256");
            requirePositive(predecessorAuthorityRevision, "predecessorAuthorityRevision");
        }
    }

    /** CAS-1 result; its predecessor and proof vector remain byte-exact for the identity read. */
    public record TargetReadFenceV1(
            Sha256Digest attemptIdSha256,
            long fencedAuthorityRevision,
            long fenceEpoch,
            Sha256Digest openAuthorityValueSha256,
            Sha256Digest proofSnapshotDigest,
            Sha256Digest eligibilityRootSha256) {
        public TargetReadFenceV1 {
            requireDigest(attemptIdSha256, "attemptIdSha256");
            requirePositive(fencedAuthorityRevision, "fencedAuthorityRevision");
            requirePositive(fenceEpoch, "fenceEpoch");
            requireDigest(openAuthorityValueSha256, "openAuthorityValueSha256");
            requireDigest(proofSnapshotDigest, "proofSnapshotDigest");
            requireDigest(eligibilityRootSha256, "eligibilityRootSha256");
        }
    }

    /** Exact target-specific identity observation made only after CAS-1. */
    public record ExactExternalIdentityV1(
            PhysicalDeleteTargetKindV1 targetKind,
            ExternalIdentityObservationV1 observation,
            CanonicalBytes exactIdentityBytes,
            Sha256Digest externalIdentitySha256) {
        public ExactExternalIdentityV1 {
            Objects.requireNonNull(targetKind, "targetKind");
            Objects.requireNonNull(observation, "observation");
            requireBytes(exactIdentityBytes, MAX_EXTERNAL_IDENTITY_BYTES, "exactIdentityBytes");
            requireDigest(externalIdentitySha256, "externalIdentitySha256");
            Sha256Digest expected =
                    M5TargetDeleteAuthorityKeysV1.externalIdentitySha256(targetKind, observation, exactIdentityBytes);
            if (!externalIdentitySha256.equals(expected)) {
                throw new IllegalArgumentException("external identity SHA-256 differs from its exact observation");
            }
        }

        public static ExactExternalIdentityV1 create(
                PhysicalDeleteTargetKindV1 targetKind,
                ExternalIdentityObservationV1 observation,
                CanonicalBytes exactIdentityBytes) {
            return new ExactExternalIdentityV1(
                    targetKind,
                    observation,
                    exactIdentityBytes,
                    M5TargetDeleteAuthorityKeysV1.externalIdentitySha256(targetKind, observation, exactIdentityBytes));
        }
    }

    /** CAS-2 result and the complete token an external adapter must present before dispatch. */
    public record TargetDeleteIntentV1(
            Sha256Digest deleteAttemptIdSha256,
            long intentAuthorityRevision,
            long dispatchEpoch,
            Sha256Digest dispatchOwnerFenceSha256,
            Optional<Sha256Digest> ownerTakeoverProofSha256,
            Sha256Digest capabilityDigestSha256,
            Sha256Digest dispatchTokenSha256) {
        public TargetDeleteIntentV1 {
            requireDigest(deleteAttemptIdSha256, "deleteAttemptIdSha256");
            requirePositive(intentAuthorityRevision, "intentAuthorityRevision");
            requirePositive(dispatchEpoch, "dispatchEpoch");
            requireDigest(dispatchOwnerFenceSha256, "dispatchOwnerFenceSha256");
            ownerTakeoverProofSha256 = Objects.requireNonNull(ownerTakeoverProofSha256, "ownerTakeoverProofSha256");
            ownerTakeoverProofSha256.ifPresent(value -> requireDigest(value, "ownerTakeoverProofSha256"));
            requireDigest(capabilityDigestSha256, "capabilityDigestSha256");
            requireDigest(dispatchTokenSha256, "dispatchTokenSha256");
        }
    }

    /** Permanent exact terminal proof; it never authorizes another target or another attempt. */
    public record TargetDeleteDoneV1(
            Sha256Digest intentCanonicalSha256,
            long intentAuthorityRevision,
            Sha256Digest deleteAttemptIdSha256,
            long finalDispatchEpoch,
            Sha256Digest finalDispatchOwnerFenceSha256,
            DeleteTerminalOutcomeV1 terminalOutcome,
            Sha256Digest absenceInventoryRootSha256,
            Sha256Digest completionProofDigestSha256) {
        public TargetDeleteDoneV1 {
            requireDigest(intentCanonicalSha256, "intentCanonicalSha256");
            requirePositive(intentAuthorityRevision, "intentAuthorityRevision");
            requireDigest(deleteAttemptIdSha256, "deleteAttemptIdSha256");
            requirePositive(finalDispatchEpoch, "finalDispatchEpoch");
            requireDigest(finalDispatchOwnerFenceSha256, "finalDispatchOwnerFenceSha256");
            Objects.requireNonNull(terminalOutcome, "terminalOutcome");
            requireDigest(absenceInventoryRootSha256, "absenceInventoryRootSha256");
            requireDigest(completionProofDigestSha256, "completionProofDigestSha256");
        }
    }

    /** Entire permanent value at one target-scoped authority key. */
    public record TargetDeleteAuthorityV1(
            String authorityKey,
            PhysicalDeleteTargetV1 target,
            long authorityRevision,
            Optional<Sha256Digest> predecessorAuthoritySha256,
            TargetDeleteAuthorityStateV1 state,
            long closedWriterFenceEpoch,
            ProofBoundWriterEnrollmentV1 writerEnrollment,
            Sha256Digest proofSnapshotDigest,
            List<ProofBoundWriterTicketV1> activeWriterTickets,
            Optional<TargetReadFenceV1> readFence,
            Optional<ExactExternalIdentityV1> externalIdentity,
            Optional<TargetDeleteIntentV1> deleteIntent,
            Optional<TargetDeleteDoneV1> deleteDone,
            Sha256Digest authorityCanonicalSha256) {
        public TargetDeleteAuthorityV1 {
            authorityKey = requireKey(authorityKey);
            Objects.requireNonNull(target, "target");
            predecessorAuthoritySha256 =
                    Objects.requireNonNull(predecessorAuthoritySha256, "predecessorAuthoritySha256");
            predecessorAuthoritySha256.ifPresent(value -> requireDigest(value, "predecessorAuthoritySha256"));
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(writerEnrollment, "writerEnrollment");
            requireDigest(proofSnapshotDigest, "proofSnapshotDigest");
            activeWriterTickets = List.copyOf(Objects.requireNonNull(activeWriterTickets, "activeWriterTickets"));
            readFence = Objects.requireNonNull(readFence, "readFence");
            externalIdentity = Objects.requireNonNull(externalIdentity, "externalIdentity");
            deleteIntent = Objects.requireNonNull(deleteIntent, "deleteIntent");
            deleteDone = Objects.requireNonNull(deleteDone, "deleteDone");
            Objects.requireNonNull(authorityCanonicalSha256, "authorityCanonicalSha256");
            requirePositive(authorityRevision, "authorityRevision");
            if ((authorityRevision == 1) != predecessorAuthoritySha256.isEmpty()) {
                throw new IllegalArgumentException("authority revision and predecessor presence disagree");
            }
            if (!authorityKey.equals(M5TargetDeleteAuthorityKeysV1.authorityKey(target))) {
                throw new IllegalArgumentException("authority key differs from its immutable target");
            }
            validateTickets(activeWriterTickets, authorityRevision, writerEnrollment);
            validateState(
                    authorityKey,
                    target,
                    authorityRevision,
                    state,
                    closedWriterFenceEpoch,
                    proofSnapshotDigest,
                    activeWriterTickets,
                    readFence,
                    externalIdentity,
                    deleteIntent,
                    deleteDone);
        }
    }

    private static void validateTickets(
            List<ProofBoundWriterTicketV1> tickets, long authorityRevision, ProofBoundWriterEnrollmentV1 enrollment) {
        if (tickets.size() > MAX_WRITER_TICKETS) {
            throw new IllegalArgumentException("active writer ticket count exceeds its hard cap");
        }
        List<ProofBoundWriterTicketV1> sorted = tickets.stream()
                .sorted(Comparator.comparing(ProofBoundWriterTicketV1::writerClass)
                        .thenComparing(value -> value.operationIdSha256().toHex()))
                .toList();
        Set<Sha256Digest> operations = new HashSet<>();
        for (ProofBoundWriterTicketV1 ticket : tickets) {
            if (!enrollment.writerClasses().contains(ticket.writerClass())
                    || ticket.predecessorAuthorityRevision() >= authorityRevision
                    || !operations.add(ticket.operationIdSha256())) {
                throw new IllegalArgumentException("writer ticket is not uniquely enrolled before its authority");
            }
        }
        if (!tickets.equals(sorted)) {
            throw new IllegalArgumentException("writer tickets are not in canonical order");
        }
    }

    private static void validateState(
            String authorityKey,
            PhysicalDeleteTargetV1 target,
            long authorityRevision,
            TargetDeleteAuthorityStateV1 state,
            long closedWriterFenceEpoch,
            Sha256Digest proofSnapshotDigest,
            List<ProofBoundWriterTicketV1> activeWriterTickets,
            Optional<TargetReadFenceV1> readFence,
            Optional<ExactExternalIdentityV1> externalIdentity,
            Optional<TargetDeleteIntentV1> deleteIntent,
            Optional<TargetDeleteDoneV1> deleteDone) {
        boolean hasRead = readFence.isPresent();
        boolean hasExternal = externalIdentity.isPresent();
        boolean hasIntent = deleteIntent.isPresent();
        boolean hasDone = deleteDone.isPresent();
        if (state != TargetDeleteAuthorityStateV1.OPEN_V1 && !activeWriterTickets.isEmpty()) {
            throw new IllegalArgumentException("writer tickets may exist only while target authority is OPEN_V1");
        }
        switch (state) {
            case OPEN_V1 -> {
                if (closedWriterFenceEpoch != 0 || hasRead || hasExternal || hasIntent || hasDone) {
                    throw new IllegalArgumentException("OPEN_V1 authority contains a deletion phase");
                }
            }
            case READ_FENCED_V1 -> {
                if (closedWriterFenceEpoch <= 0 || !hasRead || hasExternal || hasIntent || hasDone) {
                    throw new IllegalArgumentException("READ_FENCED_V1 authority phase differs");
                }
                TargetReadFenceV1 fence = readFence.orElseThrow();
                if (fence.fencedAuthorityRevision() != authorityRevision
                        || fence.fenceEpoch() != closedWriterFenceEpoch
                        || !fence.proofSnapshotDigest().equals(proofSnapshotDigest)) {
                    throw new IllegalArgumentException("read fence differs from its authority revision/proof");
                }
            }
            case DELETE_INTENT_V1 -> {
                if (closedWriterFenceEpoch <= 0 || !hasRead || !hasExternal || !hasIntent || hasDone) {
                    throw new IllegalArgumentException("DELETE_INTENT_V1 authority phase differs");
                }
                validateIntent(
                        authorityKey,
                        target,
                        authorityRevision,
                        state,
                        readFence.orElseThrow(),
                        externalIdentity.orElseThrow(),
                        deleteIntent.orElseThrow());
            }
            case DELETE_DONE_V1 -> {
                if (closedWriterFenceEpoch <= 0 || !hasRead || !hasExternal || !hasIntent || !hasDone) {
                    throw new IllegalArgumentException("DELETE_DONE_V1 authority phase differs");
                }
                TargetReadFenceV1 fence = readFence.orElseThrow();
                ExactExternalIdentityV1 external = externalIdentity.orElseThrow();
                TargetDeleteIntentV1 intent = deleteIntent.orElseThrow();
                validateIntent(authorityKey, target, authorityRevision, state, fence, external, intent);
                TargetDeleteDoneV1 done = deleteDone.orElseThrow();
                if (done.intentAuthorityRevision() != intent.intentAuthorityRevision()
                        || !done.deleteAttemptIdSha256().equals(intent.deleteAttemptIdSha256())
                        || done.finalDispatchEpoch() != intent.dispatchEpoch()
                        || !done.finalDispatchOwnerFenceSha256().equals(intent.dispatchOwnerFenceSha256())
                        || done.intentAuthorityRevision() >= authorityRevision) {
                    throw new IllegalArgumentException("delete done differs from its fixed intent");
                }
            }
        }
    }

    private static void validateIntent(
            String authorityKey,
            PhysicalDeleteTargetV1 target,
            long authorityRevision,
            TargetDeleteAuthorityStateV1 state,
            TargetReadFenceV1 readFence,
            ExactExternalIdentityV1 external,
            TargetDeleteIntentV1 intent) {
        if (external.targetKind() != target.targetKind()) {
            throw new IllegalArgumentException("external identity target kind differs");
        }
        if (intent.intentAuthorityRevision() <= readFence.fencedAuthorityRevision()
                || intent.intentAuthorityRevision() > authorityRevision
                || (intent.dispatchEpoch() == 1
                        && intent.ownerTakeoverProofSha256().isPresent())
                || (intent.dispatchEpoch() > 1
                        && intent.ownerTakeoverProofSha256().isEmpty())) {
            throw new IllegalArgumentException("delete intent revision/ownership history differs from its authority");
        }
        Sha256Digest expected = M5TargetDeleteAuthorityKeysV1.dispatchTokenSha256(
                authorityKey,
                target.targetIdentitySha256(),
                intent.intentAuthorityRevision(),
                intent.deleteAttemptIdSha256(),
                intent.dispatchEpoch(),
                intent.dispatchOwnerFenceSha256(),
                external.externalIdentitySha256());
        if (!intent.dispatchTokenSha256().equals(expected)) {
            throw new IllegalArgumentException("delete intent dispatch token differs");
        }
    }

    static String requireKey(String value) {
        Objects.requireNonNull(value, "authorityKey");
        CanonicalBytes encoded = CanonicalUtf8.fromString(value).bytes();
        if (value.isBlank() || encoded.length() > ExactMetadataTransactionStoreV1.MAX_KEY_BYTES) {
            throw new IllegalArgumentException("authority key is blank or exceeds the metadata key hard cap");
        }
        return value;
    }

    static void requireBytes(CanonicalBytes value, int maximum, String label) {
        Objects.requireNonNull(value, label);
        if (value.isEmpty() || value.length() > maximum) {
            throw new IllegalArgumentException(label + " is empty or exceeds its hard cap");
        }
    }

    static void requireDigest(Sha256Digest value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isZero()) {
            throw new IllegalArgumentException(label + " must not be zero");
        }
    }

    static void requirePositive(long value, String label) {
        if (value <= 0) {
            throw new IllegalArgumentException(label + " must be positive");
        }
    }
}
