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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.ExactTransaction;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.TransactionOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1.Outcome;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.DeleteTerminalOutcomeV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterTicketV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteWriterGuardV1.ExternalMutationResultV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteWriterGuardV1.GuardOutcomeV1;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class M5TargetDeleteAuthorityCoordinatorV1Test {
    @Test
    void persistsEveryLifecycleTransitionAtOneKeyWithSameKeyCasOnly() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                store,
                M5DeleteEligibilityTestFixtures.syntheticObservationVerifier(),
                expected -> CompletableFuture.completedFuture(ExternalIdentityObservationV1.ABSENT_EXACT_V1));
        VersionedValue open = create(coordinator, 1);
        VersionedValue fenced = coordinator
                .prepareIdentityRead(open, digest(20), M5DeleteEligibilityTestFixtures.observation())
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        VersionedValue intent = coordinator
                .bindDeleteIntent(fenced, exactPresent(fenced, 30), digest(31))
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        var successor = nextObservation(true);
        var snapshot = M5DeleteEligibilityTestFixtures.replacement(
                decode(intent).target().resourceId(), 4);
        store.seedObservation(successor);
        store.seedSnapshot(snapshot);
        VersionedValue takeover = coordinator
                .refreshDispatch(intent, successor, snapshot)
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        VersionedValue done = coordinator
                .completeAbsent(takeover)
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();

        TargetDeleteAuthorityV1 decoded = M5TargetDeleteAuthorityCodecV1.decodeAuthority(done.canonicalStoredBytes());
        assertThat(decoded.authorityRevision()).isEqualTo(5);
        assertThat(decoded.state()).isEqualTo(TargetDeleteAuthorityStateV1.DELETE_DONE_V1);
        assertThat(store.casKeys).containsOnly(decoded.authorityKey());
        assertThat(store.transactionCalls).isZero();
        assertThat(ByteBuffer.wrap(done.canonicalStoredBytes().toByteArray()).getInt(4))
                .isEqualTo(6);
        var compact = coordinator.compactDone(done).toCompletableFuture().join();
        assertThat(compact.exactTerminalIsAuthoritative()).isTrue();
        assertThat(coordinator
                        .completeAbsent(takeover)
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(Outcome.EXISTING_TERMINAL);
        assertThat(M5TargetDeleteDoneV2.decode(compact.exactCandidate()).finalCapabilitySha256())
                .isEqualTo(successor.capability().valueSha256());
    }

    @Test
    void responseUnknownAfterApplyReconcilesTheExactCandidate() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        TargetDeleteAuthorityV1 initial = open(1);
        store.nextCas = NextCas.RESPONSE_UNKNOWN_AFTER_APPLY;

        var result = coordinator.create(initial).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(Outcome.EXISTING_EXACT);
        assertThat(result.exactCandidateIsAuthoritative()).isTrue();
    }

    @Test
    void responseUnknownWithoutApplyLeavesTheExactPredecessorAndDoesNotAdvance() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        VersionedValue open = create(coordinator, 1);
        store.nextCas = NextCas.RESPONSE_UNKNOWN_WITHOUT_APPLY;

        var result = coordinator
                .prepareIdentityRead(open, digest(20), M5DeleteEligibilityTestFixtures.observation())
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(Outcome.PREDECESSOR_UNCHANGED);
        assertThat(result.observed()).contains(open);
        assertThat(decode(store.readNow(open.key())).state()).isEqualTo(TargetDeleteAuthorityStateV1.OPEN_V1);
    }

    @Test
    void ticketWinningTheExactCasMakesTheCompetingFenceConflict() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        VersionedValue open = create(coordinator, 1);
        TargetDeleteAuthorityV1 decodedOpen = decode(open);
        ProofBoundWriterTicketV1 ticket = ticket(decodedOpen, 40);

        var ticketResult = coordinator
                .acquireWriterTicket(open, ticket)
                .toCompletableFuture()
                .join();
        var fenceResult = coordinator
                .prepareIdentityRead(open, digest(20), M5DeleteEligibilityTestFixtures.observation())
                .toCompletableFuture()
                .join();

        assertThat(ticketResult.outcome()).isEqualTo(Outcome.APPLIED_EXACT);
        assertThat(fenceResult.outcome()).isEqualTo(Outcome.DEFINITIVE_CONFLICT);
        assertThat(decode(store.readNow(open.key())).activeWriterTickets()).containsExactly(ticket);
    }

    @Test
    void missingPermanentAuthorityAfterTransitionAttemptQuarantines() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        VersionedValue open = create(coordinator, 1);
        store.nextCas = NextCas.REMOVE_AND_RESPONSE_UNKNOWN;

        var result = coordinator
                .prepareIdentityRead(open, digest(20), M5DeleteEligibilityTestFixtures.observation())
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(Outcome.QUARANTINED);
        assertThat(result.observed()).isEmpty();
    }

    @Test
    void writerGuardDispatchesOnlyAfterTicketIsAuthoritativelyVisibleThenClearsIt() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        M5TargetDeleteWriterGuardV1 guard = new M5TargetDeleteWriterGuardV1(coordinator);
        VersionedValue open = create(coordinator, 1);
        ProofBoundWriterTicketV1 ticket = ticket(decode(open), 50);
        AtomicInteger calls = new AtomicInteger();

        var result = guard.execute(open, ticket, dispatch -> {
                    calls.incrementAndGet();
                    assertThat(decode(dispatch.exactTicketedAuthority()).activeWriterTickets())
                            .containsExactly(ticket);
                    return CompletableFuture.completedFuture(ExternalMutationResultV1.applied(digest(60)));
                })
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(GuardOutcomeV1.COMPLETED_RECONCILED_V1);
        assertThat(result.externalMutationInvoked()).isTrue();
        assertThat(calls).hasValue(1);
        TargetDeleteAuthorityV1 current = decode(store.readNow(open.key()));
        assertThat(current.activeWriterTickets()).isEmpty();
        assertThat(current.proofSnapshotDigest()).isEqualTo(digest(60));
        assertThat(current.authorityRevision()).isEqualTo(3);
    }

    @Test
    void writerResponseLossRetainsTheDurableTicket() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        M5TargetDeleteWriterGuardV1 guard = new M5TargetDeleteWriterGuardV1(coordinator);
        VersionedValue open = create(coordinator, 1);
        ProofBoundWriterTicketV1 ticket = ticket(decode(open), 50);

        var result = guard.execute(
                        open,
                        ticket,
                        dispatch -> CompletableFuture.completedFuture(ExternalMutationResultV1.responseUnknown()))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(GuardOutcomeV1.TICKET_RETAINED_RESPONSE_UNKNOWN_V1);
        assertThat(decode(store.readNow(open.key())).activeWriterTickets()).containsExactly(ticket);
    }

    @Test
    void fenceWinningFirstPreventsGuardedExternalWriterDispatch() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        M5TargetDeleteWriterGuardV1 guard = new M5TargetDeleteWriterGuardV1(coordinator);
        VersionedValue open = create(coordinator, 1);
        coordinator
                .prepareIdentityRead(open, digest(20), M5DeleteEligibilityTestFixtures.observation())
                .toCompletableFuture()
                .join();
        AtomicInteger calls = new AtomicInteger();

        var result = guard.execute(open, ticket(decode(open), 70), dispatch -> {
                    calls.incrementAndGet();
                    return CompletableFuture.completedFuture(ExternalMutationResultV1.applied(digest(71)));
                })
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(GuardOutcomeV1.NOT_DISPATCHED_V1);
        assertThat(result.externalMutationInvoked()).isFalse();
        assertThat(calls).hasValue(0);
        assertThat(decode(store.readNow(open.key())).state()).isEqualTo(TargetDeleteAuthorityStateV1.READ_FENCED_V1);
    }

    @Test
    void malformedExistingValueAtPermanentKeyQuarantinesCreation() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        TargetDeleteAuthorityV1 initial = open(1);
        store.seed(initial.authorityKey(), bytes("foreign-value"));

        var result = coordinator.create(initial).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(Outcome.QUARANTINED);
        assertThat(store.transactionCalls).isZero();
    }

    @Test
    void rediscoveryUnderDifferentProofAndOwnerUsesTheSamePersistedFence() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 first = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        M5TargetDeleteAuthorityCoordinatorV1 second = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        TargetDeleteAuthorityV1 original = open(1);
        VersionedValue created =
                first.create(original).toCompletableFuture().join().observed().orElseThrow();
        TargetDeleteAuthorityV1 rediscovered = M5TargetDeleteAuthorityStateMachineV1.open(
                original.target(),
                new ProofBoundWriterEnrollmentV1(
                        List.of(ProofBoundWriterClassV1.values()), digest(110), digest(111), digest(112)),
                digest(113));
        assertThat(second.create(rediscovered).toCompletableFuture().join().outcome())
                .isEqualTo(Outcome.DEFINITIVE_CONFLICT);
        first.prepareIdentityRead(created, digest(114), M5DeleteEligibilityTestFixtures.observation())
                .toCompletableFuture()
                .join();
        var observed = second.read(rediscovered.authorityKey())
                .toCompletableFuture()
                .join()
                .orElseThrow();
        assertThat(observed.authority().state()).isEqualTo(TargetDeleteAuthorityStateV1.READ_FENCED_V1);
        assertThatThrownBy(() -> second.acquireWriterTicket(
                        observed.exactStoredValue(),
                        new ProofBoundWriterTicketV1(
                                ProofBoundWriterClassV1.REFERENCE_SHARED_PHYSICAL_MEMBER_V1,
                                digest(116),
                                digest(117),
                                digest(118),
                                digest(119),
                                observed.authority().authorityRevision())))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.casKeys).containsOnly(original.authorityKey());
    }

    @Test
    void qualifierInstallsTypedSnapshotAtExactRevisionAndRejectsMissingNamespaceAuthority() {
        InMemoryStore store = new InMemoryStore();
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        var template = open(1);
        var unqualified =
                M5TargetDeleteAuthorityStateMachineV1.open(template.target(), template.writerEnrollment(), digest(125));
        var created = coordinator
                .create(unqualified)
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        var snapshot =
                M5DeleteEligibilityTestFixtures.replacement(template.target().resourceId(), 2);
        var qualified = coordinator
                .qualifyEligibility(created, snapshot)
                .toCompletableFuture()
                .join();
        assertThat(qualified.outcome()).isEqualTo(Outcome.APPLIED_EXACT);
        assertThat(decode(qualified.observed().orElseThrow()).eligibilitySnapshot())
                .contains(snapshot);

        InMemoryStore missing = new InMemoryStore();
        missing.values.remove("/physical-namespace");
        var other = new M5TargetDeleteAuthorityCoordinatorV1(
                missing, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        assertThatThrownBy(() -> other.create(template).toCompletableFuture().join())
                .hasRootCauseMessage("eligibility authority is absent: /physical-namespace");
        assertThat(missing.values).doesNotContainKey(template.authorityKey());
    }

    @Test
    void changedSemanticOrReferenceAuthorityVetoesBothCasWindows() {
        InMemoryStore firstStore = new InMemoryStore();
        var first = new M5TargetDeleteAuthorityCoordinatorV1(
                firstStore, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        var firstOpen = create(first, 1);
        firstStore.seed("/semantic/RECOVERY", bytes("changed recovery authority"));
        assertThatThrownBy(() -> first.prepareIdentityRead(
                                firstOpen, digest(126), M5DeleteEligibilityTestFixtures.observation())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("eligibility authority changed: /semantic/RECOVERY");
        assertThat(firstStore.readNow(firstOpen.key())).isEqualTo(firstOpen);

        InMemoryStore secondStore = new InMemoryStore();
        var second = new M5TargetDeleteAuthorityCoordinatorV1(
                secondStore, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        var secondOpen = create(second, 1);
        var fenced = second.prepareIdentityRead(secondOpen, digest(127), M5DeleteEligibilityTestFixtures.observation())
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        secondStore.seed("/reference/READ_GENERATION_PIN_OR_OPEN_HANDLE", bytes("new source pin admission"));
        assertThatThrownBy(() -> second.bindDeleteIntent(fenced, exactPresent(fenced, 30), digest(31))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("eligibility authority changed: /reference/READ_GENERATION_PIN_OR_OPEN_HANDLE");
        assertVeto(fenced, secondStore.readNow(fenced.key()), DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED);
        assertThat(decode(fenced).deleteIntent()).isEmpty();
    }

    @Test
    void readFencedTakeoverAfterRestartRejectsLateObservationAndNeverReopensWriters() {
        var harness = new ObservationHarness();
        VersionedValue oldFence = harness.fenced;
        ExactExternalIdentityV1 late = exactPresent(oldFence, 141);
        var snapshot = M5DeleteEligibilityTestFixtures.replacement(
                decode(oldFence).target().resourceId(), 3);
        var context = nextObservation(true);
        harness.store.seedSnapshot(snapshot);
        harness.store.seedObservation(context);
        harness.store.nextCas = NextCas.RESPONSE_UNKNOWN_AFTER_APPLY;
        var restarted = new M5TargetDeleteAuthorityCoordinatorV1(
                harness.store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());

        var result = restarted
                .refreshIdentityRead(oldFence, context, snapshot)
                .toCompletableFuture()
                .join();
        assertThat(result.outcome()).isEqualTo(Outcome.EXISTING_EXACT);
        VersionedValue fresh = result.observed().orElseThrow();
        var decoded = decode(fresh);
        assertThat(decoded.state()).isEqualTo(TargetDeleteAuthorityStateV1.READ_FENCED_V1);
        assertThat(decoded.target()).isEqualTo(decode(oldFence).target());
        assertThat(decoded.closedWriterFenceEpoch()).isEqualTo(decode(oldFence).closedWriterFenceEpoch());
        assertThat(decoded.readFence().orElseThrow().attemptIdSha256())
                .isEqualTo(decode(oldFence).readFence().orElseThrow().attemptIdSha256());
        assertThat(decoded.readFence().orElseThrow().observationContext()).isEqualTo(context);
        assertThat(decoded.externalIdentity()).isEmpty();
        assertThatThrownBy(() -> restarted.bindDeleteIntent(fresh, late, digest(142)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("observation epoch");
        assertThatThrownBy(() -> restarted.acquireWriterTicket(fresh, ticket(decoded, 143)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(restarted
                        .bindDeleteIntent(oldFence, late, digest(144))
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(Outcome.DEFINITIVE_CONFLICT);
        VersionedValue intent = restarted
                .bindDeleteIntent(fresh, exactPresent(fresh, 145), digest(146))
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        assertThat(decode(intent).deleteIntent().orElseThrow().dispatchOwnerFenceSha256())
                .isEqualTo(context.coordinatorOwner().valueSha256());
        assertThat(harness.store.casKeys).containsOnly(oldFence.key());
        assertThat(harness.store.transactionCalls).isZero();
    }

    @Test
    void readFencedRefreshResponseLossWithoutApplyRetainsPredecessorAndRetriesExactly() {
        var harness = new ObservationHarness();
        var context = nextObservation(true);
        var snapshot = M5DeleteEligibilityTestFixtures.replacement(
                decode(harness.fenced).target().resourceId(), 3);
        harness.store.seedSnapshot(snapshot);
        harness.store.seedObservation(context);
        harness.store.nextCas = NextCas.RESPONSE_UNKNOWN_WITHOUT_APPLY;

        var first = harness.coordinator
                .refreshIdentityRead(harness.fenced, context, snapshot)
                .toCompletableFuture()
                .join();
        assertThat(first.outcome()).isEqualTo(Outcome.PREDECESSOR_UNCHANGED);
        assertThat(harness.store.readNow(harness.fenced.key())).isEqualTo(harness.fenced);
        var retried = harness.coordinator
                .refreshIdentityRead(harness.fenced, context, snapshot)
                .toCompletableFuture()
                .join();
        assertThat(retried.outcome()).isEqualTo(Outcome.APPLIED_EXACT);
        assertThat(retried.exactCandidate()).isEqualTo(first.exactCandidate());
        assertThat(decode(retried.observed().orElseThrow()).deleteIntent()).isEmpty();
    }

    @Test
    void nativeObservationAdapterIsRequiredEvenWhenEveryFactExists() {
        var store = new InMemoryStore();
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(store);
        VersionedValue initial = create(coordinator, 1);
        int calls = store.casCalls;

        assertThatThrownBy(() -> coordinator
                        .prepareIdentityRead(initial, digest(150), M5DeleteEligibilityTestFixtures.observation())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(UnsupportedOperationException.class);
        assertThat(store.casCalls).isEqualTo(calls);
        assertThat(store.readNow(initial.key())).isEqualTo(initial);
    }

    @Test
    void nativeOwnerFenceRejectionVetoesTakeoverDespiteExistingReceiptBytes() {
        var harness = new ObservationHarness();
        var context = nextObservation(true);
        var snapshot = M5DeleteEligibilityTestFixtures.replacement(
                decode(harness.fenced).target().resourceId(), 3);
        harness.store.seedSnapshot(snapshot);
        harness.store.seedObservation(context);
        var verifier = new DeleteObservationAuthorityVerifierV2() {
            @Override
            public CompletionStage<Void> requireCurrent(
                    PhysicalResourceIdV2 resource, DeleteObservationContextV2 current) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletionStage<Void> requirePredecessorFenced(
                    PhysicalResourceIdV2 resource,
                    DeleteObservationContextV2 previous,
                    DeleteObservationContextV2 successor) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("native previous owner is still active"));
            }
        };
        var successor = new M5TargetDeleteAuthorityCoordinatorV1(harness.store, verifier);
        int calls = harness.store.casCalls;

        assertThatThrownBy(() -> successor
                        .refreshIdentityRead(harness.fenced, context, snapshot)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("native previous owner is still active");
        assertThat(harness.store.casCalls).isEqualTo(calls + 1);
        assertVeto(
                harness.fenced,
                harness.store.readNow(harness.fenced.key()),
                DeleteRecoveryVetoV2.Reason.PREDECESSOR_OWNER_AUTHORITY_REJECTED);
    }

    @Test
    void capabilityRevocationBeforeCas2AndStaleRefreshKeepTheReadFenceClosed() {
        var harness = new ObservationHarness();
        var context = M5DeleteEligibilityTestFixtures.observation();
        harness.store.seed(context.capability().key(), bytes("revoked"));
        int calls = harness.store.casCalls;
        assertThatThrownBy(() -> harness.coordinator
                        .bindDeleteIntent(harness.fenced, exactPresent(harness.fenced, 151), digest(152))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage(
                        "eligibility authority changed: " + context.capability().key());
        assertThat(harness.store.casCalls).isEqualTo(calls + 1);
        assertVeto(
                harness.fenced,
                harness.store.readNow(harness.fenced.key()),
                DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED);

        var next = nextObservation(true);
        var snapshot = M5DeleteEligibilityTestFixtures.replacement(
                decode(harness.fenced).target().resourceId(), 3);
        harness.store.seedObservation(next);
        harness.store.seed("/semantic/RECOVERY", bytes("replacement no longer covers recovery"));
        assertThatThrownBy(() -> harness.coordinator
                        .refreshIdentityRead(harness.fenced, next, snapshot)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("eligibility authority changed: /semantic/RECOVERY");
        assertThat(harness.store.casCalls).isEqualTo(calls + 2);
        assertVeto(
                harness.fenced,
                harness.store.readNow(harness.fenced.key()),
                DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED);
    }

    @Test
    void sameOwnerCapabilityRefreshAdvancesEpochAndBindsOnlyNewObservation() {
        var harness = new ObservationHarness();
        var old = M5DeleteEligibilityTestFixtures.observation();
        var context = new DeleteObservationContextV2(
                2,
                old.coordinatorOwner(),
                M5DeleteEligibilityTestFixtures.fact("/dispatch/capability/two"),
                Optional.empty());
        var snapshot = M5DeleteEligibilityTestFixtures.replacement(
                decode(harness.fenced).target().resourceId(), 3);
        harness.store.seedSnapshot(snapshot);
        harness.store.seedObservation(context);
        VersionedValue refreshed = harness.coordinator
                .refreshIdentityRead(harness.fenced, context, snapshot)
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        assertThatThrownBy(() ->
                        harness.coordinator.bindDeleteIntent(refreshed, exactPresent(harness.fenced, 153), digest(154)))
                .isInstanceOf(IllegalArgumentException.class);
        VersionedValue intent = harness.coordinator
                .bindDeleteIntent(refreshed, exactPresent(refreshed, 155), digest(156))
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        assertThat(decode(intent).deleteIntent().orElseThrow().capabilityDigestSha256())
                .isEqualTo(context.capability().valueSha256());
    }

    @Test
    void readRefreshRejectsMissingFencingProofReusedEpochAndWrongSnapshotRevision() {
        var harness = new ObservationHarness();
        var previous = decode(harness.fenced);
        var context = nextObservation(true);
        var snapshot =
                M5DeleteEligibilityTestFixtures.replacement(previous.target().resourceId(), 3);
        var missing =
                new DeleteObservationContextV2(2, context.coordinatorOwner(), context.capability(), Optional.empty());
        var reused = new DeleteObservationContextV2(
                1, context.coordinatorOwner(), context.capability(), context.predecessorOwnerFenced());
        int calls = harness.store.casCalls;
        assertThatThrownBy(() -> harness.coordinator.refreshIdentityRead(harness.fenced, missing, snapshot))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> harness.coordinator.refreshIdentityRead(harness.fenced, reused, snapshot))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> harness.coordinator.refreshIdentityRead(
                        harness.fenced, context, previous.eligibilitySnapshot().orElseThrow()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(harness.store.casCalls).isEqualTo(calls);
    }

    @Test
    void vetoExtensionPreservesPreviouslyEncodedVersionFourAuthoritiesByteForByte() {
        // Captured from the ordinary V4 runtime/fixture JARs built at source 4167886e before this extension.
        var hashes = List.of(
                "80809392e51d7de465fe7e4b7a6559f77bfebd0a623399507e2011cf786ad348",
                "3ecb6993aaf160084a52dc7e0d7e86acb2264d735971dfaf6e3b86e67f5b5743",
                "106ba63af2d8171a6aff3948108c84e20b34e5899534c0e9d4f47bf62d550e21",
                "8ecc4fc242d03671c031efc3cad789e4ac9459a54c564d9ee98d5eabca48aeea");
        var phases = SyntheticDeleteAuthorityFixturesV2.phases(SyntheticDeleteAuthorityFixturesV2.resource(600));
        for (int i = 0; i < phases.size(); i++) {
            var encoded = M5TargetDeleteAuthorityCodecV1.encodeAuthority(phases.get(i));
            assertThat(Sha256Digest.hash(encoded).toHex()).isEqualTo(hashes.get(i));
            assertThat(ByteBuffer.wrap(encoded.toByteArray()).getInt(4)).isEqualTo(4);
            assertThat(M5TargetDeleteAuthorityCodecV1.decodeAuthority(encoded)).isEqualTo(phases.get(i));
        }
    }

    @Test
    void persistedVetoRequiresFreshQualifiedObservationBeforeIntent() {
        var harness = new ObservationHarness();
        var old = harness.fenced;
        var context = nextObservation(true);
        var snapshot =
                M5DeleteEligibilityTestFixtures.replacement(decode(old).target().resourceId(), 3);
        harness.store.seedSnapshot(snapshot);
        harness.store.seedObservation(context);
        harness.store.seed("/semantic/RECOVERY", bytes("lost replacement recovery"));
        var failure = rejected(harness.coordinator.refreshIdentityRead(old, context, snapshot));
        assertThat(failure.vetoResult().exactCandidateIsAuthoritative()).isTrue();
        var vetoed = harness.store.readNow(old.key());
        assertVeto(old, vetoed, DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED);
        assertThatThrownBy(() -> harness.coordinator.bindDeleteIntent(vetoed, exactPresent(vetoed, 161), digest(162)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("qualified observation refresh");
        var repaired =
                M5DeleteEligibilityTestFixtures.replacement(decode(old).target().resourceId(), 4);
        harness.store.seedSnapshot(repaired);
        var fresh = harness.coordinator
                .refreshIdentityRead(vetoed, context, repaired)
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        assertThat(decode(fresh).recoveryVeto()).isEmpty();
        assertThatThrownBy(() -> harness.coordinator.bindDeleteIntent(fresh, exactPresent(old, 163), digest(164)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(harness.coordinator
                        .bindDeleteIntent(fresh, exactPresent(fresh, 165), digest(166))
                        .toCompletableFuture()
                        .join()
                        .exactCandidateIsAuthoritative())
                .isTrue();
    }

    @Test
    void failedProofCollectionCanPersistVetoWithoutInventingACompleteEligibilitySnapshot() {
        var harness = new ObservationHarness();
        var old = harness.fenced;
        var attempted = nextObservation(true);
        // No successor snapshot or owner facts can be supplied when collection fails.
        var result = harness.coordinator
                .recordRecoveryVeto(old, DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED, attempted)
                .toCompletableFuture()
                .join();
        assertThat(result.exactCandidateIsAuthoritative()).isTrue();
        var vetoed = result.observed().orElseThrow();
        assertVeto(old, vetoed, DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED);
        assertThatThrownBy(() -> harness.coordinator.bindDeleteIntent(vetoed, exactPresent(vetoed, 167), digest(168)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("qualified observation refresh");
        var invalid = new DeleteObservationContextV2(
                3, attempted.coordinatorOwner(), attempted.capability(), attempted.predecessorOwnerFenced());
        int calls = harness.store.casCalls;
        assertThatThrownBy(() -> harness.coordinator.recordRecoveryVeto(
                        vetoed, DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED, invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("observation epoch");
        assertThat(harness.store.casCalls).isEqualTo(calls);
        assertThat(harness.store.readNow(old.key())).isEqualTo(vetoed);
    }

    @Test
    void repeatedIdenticalRecoveryRejectionRereadsOneVetoWithoutGrowingRevision() {
        var harness = new ObservationHarness();
        var context = nextObservation(true);
        var snapshot = M5DeleteEligibilityTestFixtures.replacement(
                decode(harness.fenced).target().resourceId(), 3);
        harness.store.seedSnapshot(snapshot);
        harness.store.seedObservation(context);
        var unsupported = new M5TargetDeleteAuthorityCoordinatorV1(harness.store);
        rejected(unsupported.refreshIdentityRead(harness.fenced, context, snapshot));
        var vetoed = harness.store.readNow(harness.fenced.key());
        int calls = harness.store.casCalls;
        var retrySnapshot = M5DeleteEligibilityTestFixtures.replacement(
                decode(vetoed).target().resourceId(), 4);
        for (int i = 0; i < 128; i++) {
            var failure = rejected(unsupported.refreshIdentityRead(vetoed, context, retrySnapshot));
            assertThat(failure.vetoResult().outcome()).isEqualTo(Outcome.EXISTING_EXACT);
        }
        assertThat(harness.store.readNow(vetoed.key())).isEqualTo(vetoed);
        assertThat(harness.store.casCalls).isEqualTo(calls);
    }

    @Test
    void unresolvedVetoWriteNeverTurnsRejectedRecoveryIntoSuccessOrInventsDurability() {
        for (boolean applied : new boolean[] {false, true}) {
            var harness = new ObservationHarness();
            var context = nextObservation(true);
            var snapshot = M5DeleteEligibilityTestFixtures.replacement(
                    decode(harness.fenced).target().resourceId(), 3);
            harness.store.seedSnapshot(snapshot);
            harness.store.seedObservation(context);
            harness.store.nextCas =
                    applied ? NextCas.RESPONSE_UNKNOWN_AFTER_APPLY : NextCas.RESPONSE_UNKNOWN_WITHOUT_APPLY;
            harness.store.failReadAfterCas = applied;
            var unsupported = new M5TargetDeleteAuthorityCoordinatorV1(harness.store);
            var failure = rejected(unsupported.refreshIdentityRead(harness.fenced, context, snapshot));
            assertThat(failure.vetoResult().outcome())
                    .isEqualTo(applied ? Outcome.RESPONSE_UNKNOWN : Outcome.PREDECESSOR_UNCHANGED);
            assertThat(failure.vetoResult().exactCandidateIsAuthoritative()).isFalse();
            var stored = harness.store.readNow(harness.fenced.key());
            assertThat(decode(stored).state()).isEqualTo(TargetDeleteAuthorityStateV1.READ_FENCED_V1);
            assertThat(decode(stored).recoveryVeto().isPresent()).isEqualTo(applied);
        }
    }

    @Test
    void delayedFailedValidationCannotOverwriteAnotherOwnersQualifiedRefresh() {
        var harness = new ObservationHarness();
        var context = nextObservation(true);
        var snapshot = M5DeleteEligibilityTestFixtures.replacement(
                decode(harness.fenced).target().resourceId(), 3);
        harness.store.seedSnapshot(snapshot);
        harness.store.seedObservation(context);
        var validation = new CompletableFuture<Void>();
        var delayed = new M5TargetDeleteAuthorityCoordinatorV1(harness.store, heldVerifier(validation));
        var failedLater = delayed.refreshIdentityRead(harness.fenced, context, snapshot);
        var winner = harness.coordinator
                .refreshIdentityRead(harness.fenced, context, snapshot)
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        validation.completeExceptionally(new IllegalStateException("old validation failed"));
        assertThat(rejected(failedLater).vetoResult().outcome()).isEqualTo(Outcome.DEFINITIVE_CONFLICT);
        assertThat(harness.store.readNow(winner.key())).isEqualTo(winner);
        assertThat(decode(winner).recoveryVeto()).isEmpty();
    }

    @Test
    void cancellingTheRecoveryObserverCannotAbandonPendingVetoPersistence() {
        var harness = new ObservationHarness();
        var context = nextObservation(true);
        var snapshot = M5DeleteEligibilityTestFixtures.replacement(
                decode(harness.fenced).target().resourceId(), 3);
        harness.store.seedSnapshot(snapshot);
        harness.store.seedObservation(context);
        var validation = new CompletableFuture<Void>();
        var delayed = new M5TargetDeleteAuthorityCoordinatorV1(harness.store, heldVerifier(validation));
        var observer =
                delayed.refreshIdentityRead(harness.fenced, context, snapshot).toCompletableFuture();
        assertThat(observer.cancel(true)).isTrue();
        validation.completeExceptionally(new IllegalStateException("native current owner rejected"));
        assertVeto(
                harness.fenced,
                harness.store.readNow(harness.fenced.key()),
                DeleteRecoveryVetoV2.Reason.CURRENT_OBSERVATION_AUTHORITY_REJECTED);
    }

    @Test
    void typedDispatchRefreshPreservesIdentityAndRejectsHashOnlyEntrypoints() {
        var harness = new DispatchHarness(false);
        var original = decode(harness.intent);
        var refreshed = harness.refresh().observed().orElseThrow();
        var current = decode(refreshed);
        assertThat(current.target()).isEqualTo(original.target());
        assertThat(current.readFence()).isEqualTo(original.readFence());
        assertThat(current.externalIdentity()).isEqualTo(original.externalIdentity());
        assertThat(current.closedWriterFenceEpoch()).isEqualTo(original.closedWriterFenceEpoch());
        assertThat(current.deleteIntent().orElseThrow().intentAuthorityRevision())
                .isEqualTo(original.deleteIntent().orElseThrow().intentAuthorityRevision());
        assertThat(current.deleteIntent().orElseThrow().ownerTakeoverProofSha256())
                .isEmpty();
        assertThat(current.deleteIntent().orElseThrow().capabilityDigestSha256())
                .isEqualTo(harness.context.capability().valueSha256());
        assertThat(current.proofSnapshotDigest()).isEqualTo(harness.snapshot.sha256());
        assertThat(harness.reads.get()).isOne();
        assertThatThrownBy(() -> harness.coordinator
                        .takeOverDispatch(refreshed, 3, digest(41), digest(42))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(UnsupportedOperationException.class);
        assertThat(harness.coordinator
                        .completeDelete(refreshed, DeleteTerminalOutcomeV1.DELETED_EXACT_V1, digest(43), digest(44))
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(Outcome.PREDECESSOR_UNCHANGED);
        assertThat(harness.store.readNow(refreshed.key())).isEqualTo(refreshed);
        assertThatThrownBy(() ->
                        M5TargetDeleteAuthorityStateMachineV1.takeOverDispatch(current, 3, digest(41), digest(42)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void dispatchTokensBindCompetingCapabilityAndFullEligibility() {
        var harness = new DispatchHarness(false);
        var first = M5TargetDeleteAuthorityStateMachineV1.refreshDispatch(
                decode(harness.intent),
                harness.context,
                harness.snapshot,
                ExternalIdentityObservationV1.PRESENT_EXACT_V1);
        var otherContext = new DeleteObservationContextV2(
                2,
                harness.context.coordinatorOwner(),
                M5DeleteEligibilityTestFixtures.fact("/dispatch/capability/other"),
                Optional.empty());
        var second = M5TargetDeleteAuthorityStateMachineV1.refreshDispatch(
                decode(harness.intent), otherContext, harness.snapshot, ExternalIdentityObservationV1.PRESENT_EXACT_V1);
        assertThat(first.deleteIntent().orElseThrow().dispatchTokenSha256())
                .isNotEqualTo(second.deleteIntent().orElseThrow().dispatchTokenSha256());
        assertThatThrownBy(() -> M5TargetDeleteAuthorityStateMachineV1.refreshDispatch(
                        decode(harness.intent),
                        otherContext,
                        M5DeleteEligibilityTestFixtures.replacement(harness.snapshot.resource(), 3),
                        ExternalIdentityObservationV1.PRESENT_EXACT_V1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nativeDispatchOwnerRejectionPreventsExternalReadAndCas() {
        var harness = new DispatchHarness(true);
        var verifier = new DeleteObservationAuthorityVerifierV2() {
            public CompletionStage<Void> requireCurrent(PhysicalResourceIdV2 resource, DeleteObservationContextV2 ctx) {
                return CompletableFuture.failedFuture(new IllegalStateException("owner rejected"));
            }

            public CompletionStage<Void> requirePredecessorFenced(
                    PhysicalResourceIdV2 resource,
                    DeleteObservationContextV2 previous,
                    DeleteObservationContextV2 successor) {
                return CompletableFuture.failedFuture(new IllegalStateException("predecessor not fenced"));
            }
        };
        var rejected = new M5TargetDeleteAuthorityCoordinatorV1(harness.store, verifier, harness.reader);
        assertThatThrownBy(() -> rejected.refreshDispatch(harness.intent, harness.context, harness.snapshot)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("predecessor not fenced");
        assertThat(harness.reads.get()).isZero();
        assertThat(harness.store.readNow(harness.intent.key())).isEqualTo(harness.intent);
    }

    @Test
    void dispatchRechecksAllFactsAfterNativeReadAndRetainsIntentOnFailure() {
        var harness = new DispatchHarness(false);
        var read = new CompletableFuture<ExternalIdentityObservationV1>();
        var delayed = new M5TargetDeleteAuthorityCoordinatorV1(
                harness.store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier(), expected -> read);
        var pending = delayed.refreshDispatch(harness.intent, harness.context, harness.snapshot);
        harness.store.seed(harness.context.capability().key(), bytes("revoked"));
        read.complete(ExternalIdentityObservationV1.PRESENT_EXACT_V1);
        assertThatThrownBy(() -> pending.toCompletableFuture().join())
                .hasRootCauseMessage("eligibility authority changed: "
                        + harness.context.capability().key());
        assertThat(harness.store.readNow(harness.intent.key())).isEqualTo(harness.intent);
    }

    @Test
    void failedOrMissingNativeIdentityReaderNeverAdvancesIntent() {
        var harness = new DispatchHarness(false);
        var missing = new M5TargetDeleteAuthorityCoordinatorV1(
                harness.store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        assertThatThrownBy(() -> missing.refreshDispatch(harness.intent, harness.context, harness.snapshot)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(UnsupportedOperationException.class);
        var failed = new M5TargetDeleteAuthorityCoordinatorV1(
                harness.store,
                M5DeleteEligibilityTestFixtures.syntheticObservationVerifier(),
                expected -> CompletableFuture.failedFuture(new IllegalStateException("changed native identity")));
        assertThatThrownBy(() -> failed.refreshDispatch(harness.intent, harness.context, harness.snapshot)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("changed native identity");
        assertThat(harness.store.readNow(harness.intent.key())).isEqualTo(harness.intent);
    }

    @Test
    void cancelledDispatchObserverDoesNotCancelNativeReadOrAcceptedCas() {
        var harness = new DispatchHarness(false);
        var read = new CompletableFuture<ExternalIdentityObservationV1>();
        var delayed = new M5TargetDeleteAuthorityCoordinatorV1(
                harness.store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier(), expected -> read);
        var observer = delayed.refreshDispatch(harness.intent, harness.context, harness.snapshot)
                .toCompletableFuture();
        assertThat(observer.cancel(true)).isTrue();
        assertThat(read.isCancelled()).isFalse();
        read.complete(ExternalIdentityObservationV1.PRESENT_EXACT_V1);
        assertThat(decode(harness.store.readNow(harness.intent.key())).authorityRevision())
                .isEqualTo(4);
    }

    @Test
    void delayedDispatchRefreshCannotOverwriteCompetingExactWinner() {
        var harness = new DispatchHarness(false);
        var read = new CompletableFuture<ExternalIdentityObservationV1>();
        var delayed = new M5TargetDeleteAuthorityCoordinatorV1(
                harness.store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier(), expected -> read);
        var pending = delayed.refreshDispatch(harness.intent, harness.context, harness.snapshot);
        var winner = harness.refresh().observed().orElseThrow();
        read.complete(ExternalIdentityObservationV1.ABSENT_EXACT_V1);
        assertThat(pending.toCompletableFuture().join().outcome()).isEqualTo(Outcome.DEFINITIVE_CONFLICT);
        assertThat(harness.store.readNow(harness.intent.key())).isEqualTo(winner);
    }

    @Test
    void dispatchResponseLossReconcilesExactCandidateAndHistoryStaysBounded() {
        var harness = new DispatchHarness(false);
        harness.store.nextCas = NextCas.RESPONSE_UNKNOWN_AFTER_APPLY;
        var result = harness.refresh();
        assertThat(result.outcome()).isEqualTo(Outcome.EXISTING_EXACT);
        var first = result.observed().orElseThrow();
        var latest = first;
        for (int epoch = 3; epoch < 30; epoch++) {
            var context = new DeleteObservationContextV2(
                    epoch, harness.context.coordinatorOwner(), harness.context.capability(), Optional.empty());
            var snapshot = M5DeleteEligibilityTestFixtures.replacement(harness.snapshot.resource(), epoch + 2);
            harness.store.seedSnapshot(snapshot);
            latest = harness.coordinator
                    .refreshDispatch(latest, context, snapshot)
                    .toCompletableFuture()
                    .join()
                    .observed()
                    .orElseThrow();
            assertThat(latest.canonicalStoredBytes().length())
                    .isEqualTo(first.canonicalStoredBytes().length());
        }
        assertThat(decode(latest).deleteIntent().orElseThrow().dispatchEpoch()).isEqualTo(29);
        var absent = M5TargetDeleteAuthorityStateMachineV1.refreshDispatch(
                decode(latest),
                new DeleteObservationContextV2(
                        30, harness.context.coordinatorOwner(), harness.context.capability(), Optional.empty()),
                M5DeleteEligibilityTestFixtures.replacement(harness.snapshot.resource(), 32),
                ExternalIdentityObservationV1.ABSENT_EXACT_V1);
        var next = new DeleteObservationContextV2(
                31, harness.context.coordinatorOwner(), harness.context.capability(), Optional.empty());
        var snapshot = M5DeleteEligibilityTestFixtures.replacement(harness.snapshot.resource(), 33);
        assertThatThrownBy(() -> M5TargetDeleteAuthorityStateMachineV1.refreshDispatch(
                        absent, next, snapshot, ExternalIdentityObservationV1.PRESENT_EXACT_V1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot reappear");
        assertThat(M5TargetDeleteAuthorityStateMachineV1.refreshDispatch(
                                absent, next, snapshot, ExternalIdentityObservationV1.ABSENT_EXACT_V1)
                        .deleteIntent()
                        .orElseThrow()
                        .dispatchEpoch())
                .isEqualTo(31);
    }

    @Test
    void nativeAbsenceCompletionRejectsPresentUnknownAndStaleAuthority() {
        var harness = new DispatchHarness(false);
        var refreshed = harness.refresh().observed().orElseThrow();
        assertThatThrownBy(() -> harness.coordinator
                        .completeAbsent(refreshed)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("native target absence was not established");
        assertThatThrownBy(() -> harness.coordinator
                        .completeAbsent(harness.intent)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("dispatch authority changed before native identity read");
        assertThat(harness.store.readNow(refreshed.key())).isEqualTo(refreshed);
    }

    private static final class DispatchHarness {
        private final InMemoryStore store = new InMemoryStore();
        private final AtomicInteger reads = new AtomicInteger();
        private final DeleteExternalIdentityReaderV2 reader = expected -> {
            reads.incrementAndGet();
            return CompletableFuture.completedFuture(ExternalIdentityObservationV1.PRESENT_EXACT_V1);
        };
        private final M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier(), reader);
        private final VersionedValue intent;
        private final DeleteObservationContextV2 context;
        private final DeleteEligibilitySnapshotV2 snapshot;

        private DispatchHarness(boolean newOwner) {
            var fenced = coordinator
                    .prepareIdentityRead(
                            create(coordinator, 1), digest(140), M5DeleteEligibilityTestFixtures.observation())
                    .toCompletableFuture()
                    .join()
                    .observed()
                    .orElseThrow();
            intent = coordinator
                    .bindDeleteIntent(fenced, exactPresent(fenced, 30), digest(31))
                    .toCompletableFuture()
                    .join()
                    .observed()
                    .orElseThrow();
            context = nextObservation(newOwner);
            snapshot = M5DeleteEligibilityTestFixtures.replacement(
                    decode(intent).target().resourceId(), 4);
            store.seedSnapshot(snapshot);
            store.seedObservation(context);
        }

        private M5TargetDeleteAuthorityCoordinatorV1.MutationResultV1 refresh() {
            return coordinator
                    .refreshDispatch(intent, context, snapshot)
                    .toCompletableFuture()
                    .join();
        }
    }

    private static DeleteObservationAuthorityVerifierV2 heldVerifier(CompletionStage<Void> validation) {
        return new DeleteObservationAuthorityVerifierV2() {
            public CompletionStage<Void> requireCurrent(
                    PhysicalResourceIdV2 resource, DeleteObservationContextV2 current) {
                return validation;
            }

            public CompletionStage<Void> requirePredecessorFenced(
                    PhysicalResourceIdV2 resource,
                    DeleteObservationContextV2 previous,
                    DeleteObservationContextV2 successor) {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private static M5TargetDeleteAuthorityCoordinatorV1.RecoveryRejectedException rejected(CompletionStage<?> stage) {
        try {
            stage.toCompletableFuture().join();
            throw new AssertionError("rejected recovery unexpectedly completed successfully");
        } catch (java.util.concurrent.CompletionException failure) {
            Throwable cause = failure;
            while (!(cause instanceof M5TargetDeleteAuthorityCoordinatorV1.RecoveryRejectedException)
                    && cause.getCause() != null) {
                cause = cause.getCause();
            }
            assertThat(cause).isInstanceOf(M5TargetDeleteAuthorityCoordinatorV1.RecoveryRejectedException.class);
            return (M5TargetDeleteAuthorityCoordinatorV1.RecoveryRejectedException) cause;
        }
    }

    private static void assertVeto(VersionedValue previous, VersionedValue stored, DeleteRecoveryVetoV2.Reason reason) {
        var old = decode(previous);
        var current = decode(stored);
        assertThat(current.state()).isEqualTo(TargetDeleteAuthorityStateV1.READ_FENCED_V1);
        assertThat(current.target()).isEqualTo(old.target());
        assertThat(current.closedWriterFenceEpoch()).isEqualTo(old.closedWriterFenceEpoch());
        assertThat(current.readFence().orElseThrow().attemptIdSha256())
                .isEqualTo(old.readFence().orElseThrow().attemptIdSha256());
        assertThat(current.readFence().orElseThrow().observationContext())
                .isEqualTo(old.readFence().orElseThrow().observationContext());
        assertThat(current.recoveryVeto().orElseThrow().reason()).isEqualTo(reason);
        assertThat(ByteBuffer.wrap(stored.canonicalStoredBytes().toByteArray()).getInt(4))
                .isEqualTo(5);
        assertThat(current.recoveryVeto().orElseThrow().rejectedAuthoritySha256())
                .isEqualTo(previous.canonicalStoredSha256());
        assertThat(stored.canonicalStoredBytes().length()
                        - previous.canonicalStoredBytes().length())
                .isEqualTo(73);
        assertThat(current.authorityRevision()).isEqualTo(old.authorityRevision() + 1);
    }

    private static DeleteObservationContextV2 nextObservation(boolean newOwner) {
        var previous = M5DeleteEligibilityTestFixtures.observation();
        return new DeleteObservationContextV2(
                2,
                newOwner ? M5DeleteEligibilityTestFixtures.fact("/dispatch/owner/two") : previous.coordinatorOwner(),
                M5DeleteEligibilityTestFixtures.fact("/dispatch/capability/two"),
                newOwner
                        ? Optional.of(M5DeleteEligibilityTestFixtures.fact("/dispatch/owner/one-fenced"))
                        : Optional.empty());
    }

    private static final class ObservationHarness {
        private final InMemoryStore store = new InMemoryStore();
        private final M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                store, M5DeleteEligibilityTestFixtures.syntheticObservationVerifier());
        private final VersionedValue fenced;

        private ObservationHarness() {
            fenced = coordinator
                    .prepareIdentityRead(
                            create(coordinator, 1), digest(140), M5DeleteEligibilityTestFixtures.observation())
                    .toCompletableFuture()
                    .join()
                    .observed()
                    .orElseThrow();
        }
    }

    private static VersionedValue create(M5TargetDeleteAuthorityCoordinatorV1 coordinator, int cell) {
        var result = coordinator.create(open(cell)).toCompletableFuture().join();
        assertThat(result.outcome()).isEqualTo(Outcome.APPLIED_EXACT);
        return result.observed().orElseThrow();
    }

    private static TargetDeleteAuthorityV1 open(int cell) {
        PhysicalDeleteTargetV1 target = PhysicalDeleteTargetV1.create(new PhysicalResourceIdV2.ObjectVersion(
                new PhysicalResourceIdV2.Namespace(
                        PhysicalResourceIdV2.ProviderKind.OBJECT_PROVIDER,
                        CanonicalUtf8.fromString("physical-cluster-" + cell),
                        CanonicalUtf8.fromString("bucket-incarnation")),
                CanonicalUtf8.fromString("object"),
                PhysicalResourceIdV2.ObjectIdentityKind.IMMUTABLE_VERSION,
                CanonicalUtf8.fromString("version-7")));
        ProofBoundWriterEnrollmentV1 enrollment = new ProofBoundWriterEnrollmentV1(
                List.of(ProofBoundWriterClassV1.values()), digest(4), digest(5), digest(6));
        return M5TargetDeleteAuthorityStateMachineV1.open(
                target, enrollment, M5DeleteEligibilityTestFixtures.replacement(target.resourceId(), 1));
    }

    private static ExactExternalIdentityV1 exactPresent(VersionedValue fenced, int suffix) {
        return ExactExternalIdentityV1.create(
                decode(fenced),
                ExternalIdentityObservationV1.PRESENT_EXACT_V1,
                bytes("object/version-" + suffix + "/length/body/root/footer"));
    }

    private static ProofBoundWriterTicketV1 ticket(TargetDeleteAuthorityV1 value, int suffix) {
        return new ProofBoundWriterTicketV1(
                ProofBoundWriterClassV1.REPLICA_TOPOLOGY_V1,
                digest(suffix),
                digest(suffix + 1),
                digest(suffix + 2),
                digest(suffix + 3),
                value.authorityRevision());
    }

    private static TargetDeleteAuthorityV1 decode(VersionedValue value) {
        return M5TargetDeleteAuthorityCodecV1.decodeAuthority(value.canonicalStoredBytes());
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Sha256Digest digest(int lastByte) {
        byte[] value = new byte[Sha256Digest.LENGTH];
        value[value.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(value);
    }

    private enum NextCas {
        NORMAL,
        RESPONSE_UNKNOWN_AFTER_APPLY,
        RESPONSE_UNKNOWN_WITHOUT_APPLY,
        REMOVE_AND_RESPONSE_UNKNOWN
    }

    private static final class InMemoryStore implements ExactMetadataTransactionStoreV1 {
        private final Map<String, VersionedValue> values = new LinkedHashMap<>();
        private final java.util.Set<String> casKeys = new java.util.HashSet<>();
        private long version;
        private int casCalls;
        private int transactionCalls;
        private NextCas nextCas = NextCas.NORMAL;
        private boolean failReadAfterCas;
        private boolean failNextRead;

        InMemoryStore() {
            seedEligibility(open(1));
            M5DeleteEligibilityTestFixtures.observation()
                    .authorityFacts()
                    .forEach(fact -> values.put(
                            fact.key(), VersionedValue.of(fact.key(), bytes(fact.key()), fact.metadataVersion())));
        }

        void seedEligibility(TargetDeleteAuthorityV1 authority) {
            authority.eligibilitySnapshot().ifPresent(snapshot -> M5DeleteEligibilityTestFixtures.metadataValues(
                            snapshot)
                    .forEach(value -> values.put(value.key(), value)));
        }

        void seedSnapshot(DeleteEligibilitySnapshotV2 snapshot) {
            M5DeleteEligibilityTestFixtures.metadataValues(snapshot).forEach(value -> values.put(value.key(), value));
        }

        void seedObservation(DeleteObservationContextV2 context) {
            context.authorityFacts()
                    .forEach(fact -> values.put(
                            fact.key(), VersionedValue.of(fact.key(), bytes(fact.key()), fact.metadataVersion())));
        }

        synchronized VersionedValue seed(String key, CanonicalBytes value) {
            VersionedValue stored = stored(key, value);
            values.put(key, stored);
            return stored;
        }

        synchronized VersionedValue readNow(String key) {
            return Optional.ofNullable(values.get(key)).orElseThrow();
        }

        @Override
        public synchronized CompletionStage<Optional<VersionedValue>> read(String key) {
            if (failNextRead) {
                failNextRead = false;
                return CompletableFuture.failedFuture(new IllegalStateException("injected veto reread loss"));
            }
            return CompletableFuture.completedFuture(Optional.ofNullable(values.get(key)));
        }

        @Override
        public synchronized CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> exactPredecessor, String key, CanonicalBytes exactCandidate) {
            casKeys.add(key);
            casCalls++;
            Optional<VersionedValue> current = Optional.ofNullable(values.get(key));
            if (!current.equals(exactPredecessor)) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            NextCas behavior = nextCas;
            nextCas = NextCas.NORMAL;
            if (behavior == NextCas.RESPONSE_UNKNOWN_WITHOUT_APPLY) {
                return CompletableFuture.completedFuture(MutationOutcome.RESPONSE_UNKNOWN);
            }
            if (behavior == NextCas.REMOVE_AND_RESPONSE_UNKNOWN) {
                values.remove(key);
                return CompletableFuture.completedFuture(MutationOutcome.RESPONSE_UNKNOWN);
            }
            values.put(key, stored(key, exactCandidate));
            failNextRead = failReadAfterCas;
            failReadAfterCas = false;
            return CompletableFuture.completedFuture(
                    behavior == NextCas.RESPONSE_UNKNOWN_AFTER_APPLY
                            ? MutationOutcome.RESPONSE_UNKNOWN
                            : MutationOutcome.APPLIED_EXACT);
        }

        @Override
        public synchronized CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
            transactionCalls++;
            return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
        }

        @Override
        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }

        private VersionedValue stored(String key, CanonicalBytes value) {
            MetadataVersion metadataVersion = new MetadataVersion(CanonicalBytes.copyOf(
                    ByteBuffer.allocate(Long.BYTES).putLong(++version).array()));
            return VersionedValue.of(key, value, metadataVersion);
        }
    }
}
