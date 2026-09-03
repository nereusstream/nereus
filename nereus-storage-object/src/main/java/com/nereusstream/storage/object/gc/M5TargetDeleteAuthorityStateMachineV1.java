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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.DeleteTerminalOutcomeV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterTicketV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteDoneV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteIntentV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetReadFenceV1;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Pure candidate construction for the irreversible one-target M5-D authority state machine. */
public final class M5TargetDeleteAuthorityStateMachineV1 {
    private M5TargetDeleteAuthorityStateMachineV1() {}

    public static TargetDeleteAuthorityV1 open(
            PhysicalDeleteTargetV1 target,
            ProofBoundWriterEnrollmentV1 writerEnrollment,
            Sha256Digest proofSnapshotDigest) {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(writerEnrollment, "writerEnrollment");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(proofSnapshotDigest, "proofSnapshotDigest");
        return M5TargetDeleteAuthorityCodecV1.finalizeAuthority(new TargetDeleteAuthorityV1(
                M5TargetDeleteAuthorityKeysV1.authorityKey(target),
                target,
                1,
                Optional.empty(),
                TargetDeleteAuthorityStateV1.OPEN_V1,
                0,
                writerEnrollment,
                proofSnapshotDigest,
                List.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                zeroDigest()));
    }

    public static TargetDeleteAuthorityV1 acquireWriterTicket(
            TargetDeleteAuthorityV1 current, ProofBoundWriterTicketV1 ticket) {
        requireOpen(current);
        Objects.requireNonNull(ticket, "ticket");
        if (ticket.predecessorAuthorityRevision() != current.authorityRevision()) {
            throw new IllegalArgumentException("writer ticket does not bind the exact predecessor revision");
        }
        ArrayList<ProofBoundWriterTicketV1> tickets = new ArrayList<>(current.activeWriterTickets());
        if (tickets.stream().anyMatch(value -> value.operationIdSha256().equals(ticket.operationIdSha256()))) {
            throw new IllegalArgumentException("writer operation already has one active ticket");
        }
        tickets.add(ticket);
        tickets.sort(Comparator.comparing(ProofBoundWriterTicketV1::writerClass)
                .thenComparing(value -> value.operationIdSha256().toHex()));
        return openSuccessor(current, current.proofSnapshotDigest(), tickets);
    }

    public static TargetDeleteAuthorityV1 completeWriterTicket(
            TargetDeleteAuthorityV1 current,
            Sha256Digest operationIdSha256,
            Sha256Digest reconciledProofSnapshotDigest) {
        requireOpen(current);
        M5TargetDeleteAuthorityRecordsV1.requireDigest(operationIdSha256, "operationIdSha256");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(reconciledProofSnapshotDigest, "reconciledProofSnapshotDigest");
        List<ProofBoundWriterTicketV1> tickets = current.activeWriterTickets().stream()
                .filter(value -> !value.operationIdSha256().equals(operationIdSha256))
                .toList();
        if (tickets.size() == current.activeWriterTickets().size()) {
            throw new IllegalArgumentException("writer operation has no active ticket to reconcile");
        }
        return openSuccessor(current, reconciledProofSnapshotDigest, tickets);
    }

    /** CAS-1 candidate: closes every writer and grants no external dispatch authority. */
    public static TargetDeleteAuthorityV1 prepareIdentityRead(
            TargetDeleteAuthorityV1 current, Sha256Digest attemptIdSha256, Sha256Digest eligibilityRootSha256) {
        requireOpen(current);
        M5TargetDeleteAuthorityRecordsV1.requireDigest(attemptIdSha256, "attemptIdSha256");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(eligibilityRootSha256, "eligibilityRootSha256");
        if (!current.activeWriterTickets().isEmpty()) {
            throw new IllegalStateException("active or unresolved writer ticket vetoes CAS-1");
        }
        long fencedRevision = Math.addExact(current.authorityRevision(), 1);
        long fenceEpoch = Math.addExact(current.closedWriterFenceEpoch(), 1);
        TargetReadFenceV1 fence = new TargetReadFenceV1(
                attemptIdSha256,
                fencedRevision,
                fenceEpoch,
                Sha256Digest.hash(M5TargetDeleteAuthorityCodecV1.encodeAuthority(current)),
                current.proofSnapshotDigest(),
                eligibilityRootSha256);
        return M5TargetDeleteAuthorityCodecV1.successor(
                current,
                TargetDeleteAuthorityStateV1.READ_FENCED_V1,
                fenceEpoch,
                current.proofSnapshotDigest(),
                List.of(),
                Optional.of(fence),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    /** CAS-2 candidate: binds one exact observation, fixed attempt, owner fence, and dispatch token. */
    public static TargetDeleteAuthorityV1 bindDeleteIntent(
            TargetDeleteAuthorityV1 current,
            ExactExternalIdentityV1 externalIdentity,
            Sha256Digest deleteAttemptIdSha256,
            Sha256Digest dispatchOwnerFenceSha256,
            Sha256Digest capabilityDigestSha256) {
        requireState(current, TargetDeleteAuthorityStateV1.READ_FENCED_V1);
        Objects.requireNonNull(externalIdentity, "externalIdentity");
        if (externalIdentity.targetKind() != current.target().targetKind()) {
            throw new IllegalArgumentException("external identity belongs to another target kind");
        }
        M5TargetDeleteAuthorityRecordsV1.requireDigest(deleteAttemptIdSha256, "deleteAttemptIdSha256");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(dispatchOwnerFenceSha256, "dispatchOwnerFenceSha256");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(capabilityDigestSha256, "capabilityDigestSha256");
        long intentRevision = Math.addExact(current.authorityRevision(), 1);
        long dispatchEpoch = 1;
        TargetDeleteIntentV1 intent = new TargetDeleteIntentV1(
                deleteAttemptIdSha256,
                intentRevision,
                dispatchEpoch,
                dispatchOwnerFenceSha256,
                Optional.empty(),
                capabilityDigestSha256,
                M5TargetDeleteAuthorityKeysV1.dispatchTokenSha256(
                        current.authorityKey(),
                        current.target().targetIdentitySha256(),
                        intentRevision,
                        deleteAttemptIdSha256,
                        dispatchEpoch,
                        dispatchOwnerFenceSha256,
                        externalIdentity.externalIdentitySha256()));
        return M5TargetDeleteAuthorityCodecV1.successor(
                current,
                TargetDeleteAuthorityStateV1.DELETE_INTENT_V1,
                current.closedWriterFenceEpoch(),
                current.proofSnapshotDigest(),
                List.of(),
                current.readFence(),
                Optional.of(externalIdentity),
                Optional.of(intent),
                Optional.empty());
    }

    /** A takeover keeps the exact target, CAS-2 revision, delete attempt, external identity, and capability. */
    public static TargetDeleteAuthorityV1 takeOverDispatch(
            TargetDeleteAuthorityV1 current,
            long nextDispatchEpoch,
            Sha256Digest nextDispatchOwnerFenceSha256,
            Sha256Digest oldOwnerFencedProofSha256) {
        requireState(current, TargetDeleteAuthorityStateV1.DELETE_INTENT_V1);
        TargetDeleteIntentV1 previous = current.deleteIntent().orElseThrow();
        if (nextDispatchEpoch <= previous.dispatchEpoch()) {
            throw new IllegalArgumentException("dispatch takeover epoch must strictly increase");
        }
        M5TargetDeleteAuthorityRecordsV1.requireDigest(nextDispatchOwnerFenceSha256, "nextDispatchOwnerFenceSha256");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(oldOwnerFencedProofSha256, "oldOwnerFencedProofSha256");
        ExactExternalIdentityV1 external = current.externalIdentity().orElseThrow();
        TargetDeleteIntentV1 successorIntent = new TargetDeleteIntentV1(
                previous.deleteAttemptIdSha256(),
                previous.intentAuthorityRevision(),
                nextDispatchEpoch,
                nextDispatchOwnerFenceSha256,
                Optional.of(oldOwnerFencedProofSha256),
                previous.capabilityDigestSha256(),
                M5TargetDeleteAuthorityKeysV1.dispatchTokenSha256(
                        current.authorityKey(),
                        current.target().targetIdentitySha256(),
                        previous.intentAuthorityRevision(),
                        previous.deleteAttemptIdSha256(),
                        nextDispatchEpoch,
                        nextDispatchOwnerFenceSha256,
                        external.externalIdentitySha256()));
        return M5TargetDeleteAuthorityCodecV1.successor(
                current,
                TargetDeleteAuthorityStateV1.DELETE_INTENT_V1,
                current.closedWriterFenceEpoch(),
                current.proofSnapshotDigest(),
                List.of(),
                current.readFence(),
                current.externalIdentity(),
                Optional.of(successorIntent),
                Optional.empty());
    }

    public static TargetDeleteAuthorityV1 completeDelete(
            TargetDeleteAuthorityV1 current,
            DeleteTerminalOutcomeV1 terminalOutcome,
            Sha256Digest absenceInventoryRootSha256,
            Sha256Digest completionProofDigestSha256) {
        requireState(current, TargetDeleteAuthorityStateV1.DELETE_INTENT_V1);
        Objects.requireNonNull(terminalOutcome, "terminalOutcome");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(absenceInventoryRootSha256, "absenceInventoryRootSha256");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(completionProofDigestSha256, "completionProofDigestSha256");
        ExactExternalIdentityV1 external = current.externalIdentity().orElseThrow();
        if (external.observation() == ExternalIdentityObservationV1.ABSENT_EXACT_V1
                && terminalOutcome != DeleteTerminalOutcomeV1.ALREADY_ABSENT_EXACT_V1) {
            throw new IllegalArgumentException("an initially absent target cannot claim an exact delete outcome");
        }
        TargetDeleteIntentV1 intent = current.deleteIntent().orElseThrow();
        TargetDeleteDoneV1 done = new TargetDeleteDoneV1(
                Sha256Digest.hash(M5TargetDeleteAuthorityCodecV1.encodeAuthority(current)),
                intent.intentAuthorityRevision(),
                intent.deleteAttemptIdSha256(),
                intent.dispatchEpoch(),
                intent.dispatchOwnerFenceSha256(),
                terminalOutcome,
                absenceInventoryRootSha256,
                completionProofDigestSha256);
        return M5TargetDeleteAuthorityCodecV1.successor(
                current,
                TargetDeleteAuthorityStateV1.DELETE_DONE_V1,
                current.closedWriterFenceEpoch(),
                current.proofSnapshotDigest(),
                List.of(),
                current.readFence(),
                current.externalIdentity(),
                current.deleteIntent(),
                Optional.of(done));
    }

    public static void requireExactDispatchAuthority(
            TargetDeleteAuthorityV1 current, Sha256Digest dispatchTokenSha256) {
        requireState(current, TargetDeleteAuthorityStateV1.DELETE_INTENT_V1);
        M5TargetDeleteAuthorityRecordsV1.requireDigest(dispatchTokenSha256, "dispatchTokenSha256");
        if (!current.deleteIntent().orElseThrow().dispatchTokenSha256().equals(dispatchTokenSha256)) {
            throw new IllegalArgumentException("dispatch token differs from the exact current intent");
        }
    }

    private static TargetDeleteAuthorityV1 openSuccessor(
            TargetDeleteAuthorityV1 current, Sha256Digest proofSnapshotDigest, List<ProofBoundWriterTicketV1> tickets) {
        return M5TargetDeleteAuthorityCodecV1.successor(
                current,
                TargetDeleteAuthorityStateV1.OPEN_V1,
                0,
                proofSnapshotDigest,
                tickets,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static void requireOpen(TargetDeleteAuthorityV1 current) {
        requireState(current, TargetDeleteAuthorityStateV1.OPEN_V1);
    }

    private static void requireState(TargetDeleteAuthorityV1 current, TargetDeleteAuthorityStateV1 expected) {
        Objects.requireNonNull(current, "current");
        if (current.state() != expected) {
            throw new IllegalStateException("target authority is not " + expected);
        }
        M5TargetDeleteAuthorityCodecV1.encodeAuthority(current);
    }

    private static Sha256Digest zeroDigest() {
        return Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]);
    }
}
