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

package com.nereusstream.metadata.oxia.v2.retention;

import static com.nereusstream.metadata.oxia.v2.retention.M5PermanentDoneOxiaIntegrationTest.Fixture.await;
import static com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2.digest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.metadata.oxia.v2.retention.M5PermanentDoneOxiaIntegrationTest.Fixture;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2;
import com.nereusstream.storage.object.gc.DeleteObservationAuthorityVerifierV2;
import com.nereusstream.storage.object.gc.DeleteObservationContextV2;
import com.nereusstream.storage.object.gc.DeleteRecoveryVetoV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1.Outcome;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Actual Oxia fact versions/CAS, synthetic eligibility/owner/external observations; no physical deletion. */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class M5ReadFencedOxiaIntegrationTest {
    @Test
    void appliedRefreshWithLostDeliveryRejectsOldObservationAndKeepsAdmissionClosed() throws Exception {
        try (var first = new RecoveryFixture(Fixture.root());
                var successor = new RecoveryFixture(first.nativeFixture.root)) {
            var old = first.fence();
            var late = external(old);
            var context = successor.next(old, true);
            var snapshot = successor.snapshot(decode(old).authorityRevision() + 1);
            successor.nativeFixture.faults.loseCas = true;
            var result = await(successor.coordinator.refreshIdentityRead(old, context, snapshot));
            assertThat(result.outcome()).isEqualTo(Outcome.APPLIED_EXACT);
            assertThat(successor.nativeFixture.faults.loseCas).isFalse();
            var fresh = result.observed().orElseThrow();
            verifyRefresh(old, fresh, context);
            assertThatThrownBy(() -> successor.coordinator.bindDeleteIntent(fresh, late, digest("stale")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("observation epoch");
            assertThat(await(first.coordinator.bindDeleteIntent(old, late, digest("old-callback")))
                            .outcome())
                    .isEqualTo(Outcome.DEFINITIVE_CONFLICT);
            assertThatThrownBy(() -> M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(
                            decode(fresh), digest("reopen"), context))
                    .isInstanceOf(IllegalStateException.class);
            var intent = await(successor.coordinator.bindDeleteIntent(fresh, external(fresh), digest("fresh")))
                    .observed()
                    .orElseThrow();
            assertThat(decode(intent).deleteIntent().orElseThrow().dispatchOwnerFenceSha256())
                    .isEqualTo(context.coordinatorOwner().valueSha256());
            assertThat(await(first.nativeFixture.route.read(old.key()))).contains(intent);
        }
    }

    @Test
    void heldOldRefreshLosesToAnotherNativeClientWithoutOverwritingItsEpoch() throws Exception {
        try (var first = new RecoveryFixture(Fixture.root());
                var winner = new RecoveryFixture(first.nativeFixture.root)) {
            var old = first.fence();
            var loserContext = first.next(old, true);
            var snapshot = first.snapshot(decode(old).authorityRevision() + 1);
            first.nativeFixture.faults.holdCas = true;
            var delayed = first.coordinator.refreshIdentityRead(old, loserContext, snapshot);
            await(first.nativeFixture.faults.held);
            try {
                var oldContext = decode(old).readFence().orElseThrow().observationContext();
                var winnerContext = new DeleteObservationContextV2(
                        oldContext.observationEpoch() + 1,
                        oldContext.coordinatorOwner(),
                        winner.fact("/dispatch/winning-capability", bytes("winning capability")),
                        Optional.empty());
                var fresh = await(winner.coordinator.refreshIdentityRead(old, winnerContext, snapshot))
                        .observed()
                        .orElseThrow();
                first.nativeFixture.faults.release.getAndSet(null).run();
                assertThat(await(delayed).outcome()).isEqualTo(Outcome.DEFINITIVE_CONFLICT);
                assertThat(await(first.nativeFixture.route.read(old.key()))).contains(fresh);
                verifyRefresh(old, fresh, winnerContext);
            } finally {
                var release = first.nativeFixture.faults.release.getAndSet(null);
                if (release != null) {
                    release.run();
                }
            }
        }
    }

    @Test
    void missingNativeVerifierRejectsRecoveryDespiteEveryPersistedSyntheticFact() throws Exception {
        try (var fixture = new RecoveryFixture(Fixture.root())) {
            var old = fixture.fence();
            var context = fixture.next(old, true);
            var snapshot = fixture.snapshot(decode(old).authorityRevision() + 1);
            var unsupported = new M5TargetDeleteAuthorityCoordinatorV1(fixture.nativeFixture.route);
            assertThatThrownBy(() -> await(unsupported.refreshIdentityRead(old, context, snapshot)))
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> await(unsupported.bindDeleteIntent(old, external(old), digest("unsupported"))))
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class);
            var stored = await(fixture.nativeFixture.route.read(old.key())).orElseThrow();
            verifyVeto(old, stored, DeleteRecoveryVetoV2.Reason.PREDECESSOR_OWNER_AUTHORITY_REJECTED);
        }
    }

    @Test
    void changedNativeFactVersionVetoesRefreshAndIntentAndPreservesTheFence() throws Exception {
        try (var fixture = new RecoveryFixture(Fixture.root())) {
            var old = fixture.fence();
            var context = fixture.next(old, false);
            var snapshot = fixture.snapshot(decode(old).authorityRevision() + 1);
            var capability =
                    decode(old).readFence().orElseThrow().observationContext().capability();
            var before =
                    await(fixture.nativeFixture.route.read(capability.key())).orElseThrow();
            // Even identical bytes with a new native metadata version invalidate the captured authority.
            assertThat(await(fixture.facts.compareAndSet(
                            Optional.of(before), before.key(), before.canonicalStoredBytes())))
                    .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.APPLIED_EXACT);
            assertThatThrownBy(() -> await(fixture.coordinator.bindDeleteIntent(old, external(old), digest("revoked"))))
                    .hasRootCauseMessage("eligibility authority changed: " + capability.key());
            var semantic = snapshot.authorityFacts().stream()
                    .filter(fact -> fact.key().endsWith("/semantic/RECOVERY"))
                    .findFirst()
                    .orElseThrow();
            var semanticBefore =
                    await(fixture.nativeFixture.route.read(semantic.key())).orElseThrow();
            await(fixture.facts.compareAndSet(
                    Optional.of(semanticBefore), semantic.key(), bytes("different recovery semantics")));
            assertThatThrownBy(() -> await(fixture.coordinator.refreshIdentityRead(old, context, snapshot)))
                    .hasRootCauseMessage("eligibility authority changed: " + semantic.key());
            verifyVeto(
                    old,
                    await(fixture.nativeFixture.route.read(old.key())).orElseThrow(),
                    DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED);
        }
    }

    @Test
    void delayedRejectionCannotPersistVetoOverAnotherNativeClientsSuccessfulRefresh() throws Exception {
        try (var first = new RecoveryFixture(Fixture.root());
                var other = new RecoveryFixture(first.nativeFixture.root)) {
            var old = first.fence();
            var context = first.next(old, true);
            var snapshot = first.snapshot(decode(old).authorityRevision() + 1);
            var validation = new CompletableFuture<Void>();
            var heldOwner = new DeleteObservationAuthorityVerifierV2() {
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
            var delayed = new M5TargetDeleteAuthorityCoordinatorV1(first.nativeFixture.route, heldOwner);
            var pending = delayed.refreshIdentityRead(old, context, snapshot);
            var winner = await(other.coordinator.refreshIdentityRead(old, context, snapshot))
                    .observed()
                    .orElseThrow();
            validation.completeExceptionally(new IllegalStateException("delayed synthetic owner rejection"));
            try {
                await(pending);
                throw new AssertionError("rejected native recovery unexpectedly succeeded");
            } catch (java.util.concurrent.ExecutionException failure) {
                Throwable cause = failure;
                while (!(cause instanceof M5TargetDeleteAuthorityCoordinatorV1.RecoveryRejectedException)
                        && cause.getCause() != null) {
                    cause = cause.getCause();
                }
                assertThat(cause).isInstanceOf(M5TargetDeleteAuthorityCoordinatorV1.RecoveryRejectedException.class);
                assertThat(((M5TargetDeleteAuthorityCoordinatorV1.RecoveryRejectedException) cause)
                                .vetoResult()
                                .outcome())
                        .isEqualTo(Outcome.DEFINITIVE_CONFLICT);
            }
            assertThat(await(first.nativeFixture.route.read(old.key()))).contains(winner);
            assertThat(decode(winner).recoveryVeto()).isEmpty();
        }
    }

    @Test
    void typedIntentRefreshUsesNativeCasAndRechecksChangedFactsAfterExternalRead() throws Exception {
        try (var first = new RecoveryFixture(Fixture.root());
                var other = new RecoveryFixture(first.nativeFixture.root)) {
            var fenced = first.fence();
            var intent = await(first.coordinator.bindDeleteIntent(fenced, external(fenced), digest("intent")))
                    .observed()
                    .orElseThrow();
            var context = first.next(fenced, true);
            var snapshot = first.snapshot(4);
            var read = new CompletableFuture<ExternalIdentityObservationV1>();
            var delayed = new M5TargetDeleteAuthorityCoordinatorV1(
                    first.nativeFixture.route, first.syntheticOwner, expected -> read);
            var pending = delayed.refreshDispatch(intent, context, snapshot);
            var winnerCoordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                    other.nativeFixture.route,
                    other.syntheticOwner,
                    expected -> CompletableFuture.completedFuture(ExternalIdentityObservationV1.PRESENT_EXACT_V1));
            var winner = await(winnerCoordinator.refreshDispatch(intent, context, snapshot))
                    .observed()
                    .orElseThrow();
            read.complete(ExternalIdentityObservationV1.ABSENT_EXACT_V1);
            assertThat(await(pending).outcome()).isEqualTo(Outcome.DEFINITIVE_CONFLICT);
            assertThat(await(first.nativeFixture.route.read(intent.key()))).contains(winner);
            assertThat(decode(winner).externalIdentity())
                    .isEqualTo(decode(intent).externalIdentity());
            var previous = M5TargetDeleteAuthorityStateMachineV1.dispatchContext(decode(winner));
            var third = new DeleteObservationContextV2(
                    3, previous.coordinatorOwner(), previous.capability(), Optional.empty());
            var nextSnapshot = first.snapshot(5);
            var held = new CompletableFuture<ExternalIdentityObservationV1>();
            var stale = new M5TargetDeleteAuthorityCoordinatorV1(
                    first.nativeFixture.route, first.syntheticOwner, expected -> held);
            var rejected = stale.refreshDispatch(winner, third, nextSnapshot);
            var cap = await(first.facts.read(third.capability().key())).orElseThrow();
            await(first.facts.compareAndSet(Optional.of(cap), cap.key(), cap.canonicalStoredBytes()));
            held.complete(ExternalIdentityObservationV1.PRESENT_EXACT_V1);
            assertThatThrownBy(() -> await(rejected))
                    .hasRootCauseMessage("eligibility authority changed: " + cap.key());
            assertThat(await(first.nativeFixture.route.read(intent.key()))).contains(winner);
        }
    }

    static void verifyVeto(VersionedValue old, VersionedValue stored, DeleteRecoveryVetoV2.Reason reason) {
        var previous = decode(old);
        var current = decode(stored);
        assertThat(current.state()).isEqualTo(TargetDeleteAuthorityStateV1.READ_FENCED_V1);
        assertThat(current.target()).isEqualTo(previous.target());
        assertThat(current.closedWriterFenceEpoch()).isEqualTo(previous.closedWriterFenceEpoch());
        assertThat(current.readFence().orElseThrow().attemptIdSha256())
                .isEqualTo(previous.readFence().orElseThrow().attemptIdSha256());
        assertThat(current.readFence().orElseThrow().observationContext())
                .isEqualTo(previous.readFence().orElseThrow().observationContext());
        assertThat(current.recoveryVeto().orElseThrow().reason()).isEqualTo(reason);
        assertThat(current.recoveryVeto().orElseThrow().rejectedAuthoritySha256())
                .isEqualTo(old.canonicalStoredSha256());
        assertThat(stored.canonicalStoredBytes().length()
                        - old.canonicalStoredBytes().length())
                .isEqualTo(73);
    }

    static void verifyRefresh(VersionedValue old, VersionedValue fresh, DeleteObservationContextV2 context) {
        var before = decode(old);
        var after = decode(fresh);
        assertThat(fresh.metadataVersion()).isNotEqualTo(old.metadataVersion());
        assertThat(after.state()).isEqualTo(TargetDeleteAuthorityStateV1.READ_FENCED_V1);
        assertThat(after.target()).isEqualTo(before.target());
        assertThat(after.closedWriterFenceEpoch()).isEqualTo(before.closedWriterFenceEpoch());
        assertThat(after.readFence().orElseThrow().attemptIdSha256())
                .isEqualTo(before.readFence().orElseThrow().attemptIdSha256());
        assertThat(after.readFence().orElseThrow().observationContext()).isEqualTo(context);
        assertThat(after.authorityRevision()).isEqualTo(before.authorityRevision() + 1);
        assertThat(after.externalIdentity()).isEmpty();
        assertThat(after.deleteIntent()).isEmpty();
        assertThat(after.recoveryVeto()).isEmpty();
    }

    static TargetDeleteAuthorityV1 decode(VersionedValue value) {
        return M5TargetDeleteAuthorityCodecV1.decodeAuthority(value.canonicalStoredBytes());
    }

    static ExactExternalIdentityV1 external(VersionedValue value) {
        return ExactExternalIdentityV1.create(
                decode(value), ExternalIdentityObservationV1.PRESENT_EXACT_V1, bytes("synthetic external identity"));
    }

    static CanonicalBytes bytes(String value) {
        return CanonicalUtf8.fromString(value).bytes();
    }

    static final class RecoveryFixture implements AutoCloseable {
        final Fixture nativeFixture;
        final DeleteObservationAuthorityVerifierV2 syntheticOwner;
        final PhysicalResourceIdV2 resource = SyntheticDeleteAuthorityFixturesV2.resource(600);
        final M5TargetDeleteAuthorityCoordinatorV1 coordinator;
        final Oxia09ExactMetadataTransactionStoreV1 facts;

        RecoveryFixture(String root) throws Exception {
            nativeFixture = new Fixture(root);
            facts = new Oxia09ExactMetadataTransactionStoreV1(nativeFixture.faults);
            // Deliberately test-only: this admits synthetic owner statements, not native protocol authority.
            syntheticOwner = new DeleteObservationAuthorityVerifierV2() {
                public CompletionStage<Void> requireCurrent(
                        PhysicalResourceIdV2 resource, DeleteObservationContextV2 context) {
                    return CompletableFuture.completedFuture(null);
                }

                public CompletionStage<Void> requirePredecessorFenced(
                        PhysicalResourceIdV2 resource,
                        DeleteObservationContextV2 previous,
                        DeleteObservationContextV2 successor) {
                    return CompletableFuture.completedFuture(null);
                }
            };
            coordinator = new M5TargetDeleteAuthorityCoordinatorV1(nativeFixture.route, syntheticOwner);
        }

        AuthorityFactV1 fact(String suffix, CanonicalBytes value) {
            String key = nativeFixture.root + "/synthetic-facts" + suffix;
            try {
                var existing = await(nativeFixture.route.read(key));
                if (existing.isEmpty()) {
                    await(facts.compareAndSet(Optional.empty(), key, value));
                }
                var stored = await(nativeFixture.route.read(key)).orElseThrow();
                assertThat(stored.canonicalStoredBytes()).isEqualTo(value);
                return new AuthorityFactV1(key, stored.metadataVersion(), stored.canonicalStoredSha256());
            } catch (Exception failure) {
                throw new IllegalStateException("native synthetic fact fixture failed", failure);
            }
        }

        DeleteEligibilitySnapshotV2 snapshot(long revision) {
            return SyntheticDeleteAuthorityFixturesV2.replacement(resource, revision, this::fact);
        }

        VersionedValue fence() throws Exception {
            var template = SyntheticDeleteAuthorityFixturesV2.phases(resource).get(0);
            var initial = M5TargetDeleteAuthorityStateMachineV1.open(
                    PhysicalDeleteTargetV1.create(resource), template.writerEnrollment(), snapshot(1));
            var open = await(coordinator.create(initial)).observed().orElseThrow();
            var context = new DeleteObservationContextV2(
                    1,
                    fact("/dispatch/owner-1", bytes("synthetic owner 1")),
                    fact("/dispatch/capability-1", bytes("synthetic capability 1")),
                    Optional.empty());
            return await(coordinator.prepareIdentityRead(open, digest("native-metadata-read-attempt"), context))
                    .observed()
                    .orElseThrow();
        }

        DeleteObservationContextV2 next(VersionedValue fence, boolean ownerChanges) {
            var previous = decode(fence).readFence().orElseThrow().observationContext();
            long epoch = previous.observationEpoch() + 1;
            return new DeleteObservationContextV2(
                    epoch,
                    ownerChanges
                            ? fact("/dispatch/owner-" + epoch, bytes("synthetic owner " + epoch))
                            : previous.coordinatorOwner(),
                    fact("/dispatch/capability-" + epoch, bytes("synthetic capability " + epoch)),
                    ownerChanges
                            ? Optional.of(fact("/dispatch/fenced-" + epoch, bytes("synthetic fence " + epoch)))
                            : Optional.empty());
        }

        public void close() throws Exception {
            nativeFixture.close();
        }
    }
}
