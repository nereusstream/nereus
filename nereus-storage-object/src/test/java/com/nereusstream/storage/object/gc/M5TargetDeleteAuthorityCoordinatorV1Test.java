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
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetKindV1;
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
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(store);
        VersionedValue open = create(coordinator, 1);
        VersionedValue fenced = coordinator
                .prepareIdentityRead(open, digest(20), digest(21))
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        VersionedValue intent = coordinator
                .bindDeleteIntent(fenced, exactPresent(30), digest(31), digest(32), digest(33))
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        VersionedValue takeover = coordinator
                .takeOverDispatch(intent, 2, digest(34), digest(35))
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();
        VersionedValue done = coordinator
                .completeDelete(takeover, DeleteTerminalOutcomeV1.DELETED_EXACT_V1, digest(36), digest(37))
                .toCompletableFuture()
                .join()
                .observed()
                .orElseThrow();

        TargetDeleteAuthorityV1 decoded = M5TargetDeleteAuthorityCodecV1.decodeAuthority(done.canonicalStoredBytes());
        assertThat(decoded.authorityRevision()).isEqualTo(5);
        assertThat(decoded.state()).isEqualTo(TargetDeleteAuthorityStateV1.DELETE_DONE_V1);
        assertThat(store.casKeys).containsOnly(decoded.authorityKey());
        assertThat(store.transactionCalls).isZero();
    }

    @Test
    void responseUnknownAfterApplyReconcilesTheExactCandidate() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(store);
        TargetDeleteAuthorityV1 initial = open(1);
        store.nextCas = NextCas.RESPONSE_UNKNOWN_AFTER_APPLY;

        var result = coordinator.create(initial).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(Outcome.EXISTING_EXACT);
        assertThat(result.exactCandidateIsAuthoritative()).isTrue();
    }

    @Test
    void responseUnknownWithoutApplyLeavesTheExactPredecessorAndDoesNotAdvance() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(store);
        VersionedValue open = create(coordinator, 1);
        store.nextCas = NextCas.RESPONSE_UNKNOWN_WITHOUT_APPLY;

        var result = coordinator
                .prepareIdentityRead(open, digest(20), digest(21))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(Outcome.PREDECESSOR_UNCHANGED);
        assertThat(result.observed()).contains(open);
        assertThat(decode(store.readNow(open.key())).state()).isEqualTo(TargetDeleteAuthorityStateV1.OPEN_V1);
    }

    @Test
    void ticketWinningTheExactCasMakesTheCompetingFenceConflict() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(store);
        VersionedValue open = create(coordinator, 1);
        TargetDeleteAuthorityV1 decodedOpen = decode(open);
        ProofBoundWriterTicketV1 ticket = ticket(decodedOpen, 40);

        var ticketResult = coordinator
                .acquireWriterTicket(open, ticket)
                .toCompletableFuture()
                .join();
        var fenceResult = coordinator
                .prepareIdentityRead(open, digest(20), digest(21))
                .toCompletableFuture()
                .join();

        assertThat(ticketResult.outcome()).isEqualTo(Outcome.APPLIED_EXACT);
        assertThat(fenceResult.outcome()).isEqualTo(Outcome.DEFINITIVE_CONFLICT);
        assertThat(decode(store.readNow(open.key())).activeWriterTickets()).containsExactly(ticket);
    }

    @Test
    void missingPermanentAuthorityAfterTransitionAttemptQuarantines() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(store);
        VersionedValue open = create(coordinator, 1);
        store.nextCas = NextCas.REMOVE_AND_RESPONSE_UNKNOWN;

        var result = coordinator
                .prepareIdentityRead(open, digest(20), digest(21))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(Outcome.QUARANTINED);
        assertThat(result.observed()).isEmpty();
    }

    @Test
    void writerGuardDispatchesOnlyAfterTicketIsAuthoritativelyVisibleThenClearsIt() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(store);
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
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(store);
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
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(store);
        M5TargetDeleteWriterGuardV1 guard = new M5TargetDeleteWriterGuardV1(coordinator);
        VersionedValue open = create(coordinator, 1);
        coordinator
                .prepareIdentityRead(open, digest(20), digest(21))
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
        M5TargetDeleteAuthorityCoordinatorV1 coordinator = new M5TargetDeleteAuthorityCoordinatorV1(store);
        TargetDeleteAuthorityV1 initial = open(1);
        store.seed(initial.authorityKey(), bytes("foreign-value"));

        var result = coordinator.create(initial).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(Outcome.QUARANTINED);
        assertThat(store.transactionCalls).isZero();
    }

    @Test
    void rediscoveryUnderDifferentProofAndOwnerUsesTheSamePersistedFence() {
        InMemoryStore store = new InMemoryStore();
        M5TargetDeleteAuthorityCoordinatorV1 first = new M5TargetDeleteAuthorityCoordinatorV1(store);
        M5TargetDeleteAuthorityCoordinatorV1 second = new M5TargetDeleteAuthorityCoordinatorV1(store);
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
        first.prepareIdentityRead(created, digest(114), digest(115))
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
        return M5TargetDeleteAuthorityStateMachineV1.open(target, enrollment, digest(9));
    }

    private static ExactExternalIdentityV1 exactPresent(int suffix) {
        return ExactExternalIdentityV1.create(
                PhysicalDeleteTargetKindV1.OBJECT_VERSION_V1,
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
        private int transactionCalls;
        private NextCas nextCas = NextCas.NORMAL;

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
            return CompletableFuture.completedFuture(Optional.ofNullable(values.get(key)));
        }

        @Override
        public synchronized CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> exactPredecessor, String key, CanonicalBytes exactCandidate) {
            casKeys.add(key);
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
