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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterTicketV1;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class M5PermanentDoneV2Test {
    @Test
    void compactDonePreservesResourceAttemptRevisionOwnerCapabilityAndAbsenceProof() {
        var full = SyntheticDeleteAuthorityFixturesV2.phases(SyntheticDeleteAuthorityFixturesV2.resource(1))
                .get(3);
        var original = M5TargetDeleteAuthorityCodecV1.encodeAuthority(full);
        var compact = M5TargetDeleteDoneV2.from(full);
        assertThat(compact.resource()).isEqualTo(full.target().resourceId());
        assertThat(compact.authorityKey()).isEqualTo(full.authorityKey());
        assertThat(compact.authorityRevision()).isEqualTo(full.authorityRevision() + 1);
        assertThat(compact.fullDoneAuthoritySha256()).isEqualTo(Sha256Digest.hash(original));
        assertThat(compact.done()).isEqualTo(full.deleteDone().orElseThrow());
        assertThat(compact.finalCapabilitySha256())
                .isEqualTo(full.deleteIntent().orElseThrow().capabilityDigestSha256());
        assertThat(M5TargetDeleteDoneV2.decode(compact.encode())).isEqualTo(compact);
        assertThat(compact.encode().length()).isLessThan(original.length() / 2);
        assertThat(M5TargetDeleteAuthorityCodecV1.encodeAuthority(full)).isEqualTo(original);
    }

    @Test
    void activeAuthorityMalformedResourceTruncationAndAnyChangedByteCannotProduceDone() {
        var phases = SyntheticDeleteAuthorityFixturesV2.phases(SyntheticDeleteAuthorityFixturesV2.resource(1));
        for (int i = 0; i < 3; i++) {
            var active = phases.get(i);
            assertThatThrownBy(() -> M5TargetDeleteDoneV2.from(active)).hasMessageContaining("full DELETE_DONE");
        }
        var bytes = M5TargetDeleteDoneV2.from(phases.get(3)).encode().toByteArray();
        for (int i = 0; i < bytes.length; i++) {
            var changed = bytes.clone();
            changed[i] ^= 1;
            assertThatThrownBy(() -> M5TargetDeleteDoneV2.decode(CanonicalBytes.copyOf(changed)))
                    .isInstanceOf(IllegalArgumentException.class);
            var truncated = CanonicalBytes.copyOf(Arrays.copyOf(bytes, i));
            assertThatThrownBy(() -> M5TargetDeleteDoneV2.decode(truncated))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void lostAppliedCompactionReconcilesExactPermanentStoredBytesAndRetry() {
        var store = new Store();
        var before = store.fullDone(1);
        store.loseResponse = true;
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(store);
        var first = coordinator.compactDone(before).toCompletableFuture().join();
        assertThat(first.outcome()).isEqualTo(M5TargetDeleteAuthorityCoordinatorV1.Outcome.EXISTING_EXACT);
        assertThat(first.exactCandidateIsAuthoritative()).isTrue();
        assertThat(coordinator.compactDone(before).toCompletableFuture().join().exactCandidateIsAuthoritative())
                .isTrue();
        var terminal =
                coordinator.inspect(before.key()).toCompletableFuture().join().orElseThrow();
        assertThat(terminal.compactDone()).isPresent();
        assertThat(terminal.fullAuthority()).isEmpty();
    }

    @Test
    void exactCompletionRetryRecognizesCompactedDoneWithoutPretendingTheFullCandidateRemainsStored() {
        var store = new Store();
        var phases = SyntheticDeleteAuthorityFixturesV2.phases(SyntheticDeleteAuthorityFixturesV2.resource(1));
        var intent =
                store.seed(phases.get(2).authorityKey(), M5TargetDeleteAuthorityCodecV1.encodeAuthority(phases.get(2)));
        var full = store.fullDone(1);
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(store);
        assertThat(coordinator.compactDone(full).toCompletableFuture().join().exactTerminalIsAuthoritative())
                .isTrue();
        var done = phases.get(3).deleteDone().orElseThrow();
        var result = coordinator
                .completeDelete(
                        intent,
                        done.terminalOutcome(),
                        done.absenceInventoryRootSha256(),
                        done.completionProofDigestSha256())
                .toCompletableFuture()
                .join();
        assertThat(result.outcome()).isEqualTo(M5TargetDeleteAuthorityCoordinatorV1.Outcome.EXISTING_TERMINAL);
        assertThat(result.exactTerminalIsAuthoritative()).isTrue();
        assertThat(result.exactCandidateIsAuthoritative()).isFalse();
        assertThat(coordinator
                        .completeDelete(
                                intent,
                                done.terminalOutcome(),
                                digest("wrong absence"),
                                done.completionProofDigestSha256())
                        .toCompletableFuture()
                        .join()
                        .exactTerminalIsAuthoritative())
                .isFalse();
    }

    @Test
    void completionsExceedResidentCapAndEvictedTerminalsReloadBeforeAnyRecreation() {
        var store = new Store();
        var cache = new M5PermanentDoneCacheV2(store, 2, 4096);
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(cache);
        for (int i = 0; i < 130; i++) {
            var full = store.fullDone(i);
            assertThat(coordinator
                            .compactDone(full)
                            .toCompletableFuture()
                            .join()
                            .exactCandidateIsAuthoritative())
                    .isTrue();
            assertThat(cache.snapshot().entries()).isLessThanOrEqualTo(2);
        }
        assertThat(cache.snapshot().evictions()).isGreaterThan(120);
        var old = SyntheticDeleteAuthorityFixturesV2.resource(0);
        long reads = cache.snapshot().authoritativeReads();
        assertThat(coordinator
                        .inspect(old.authorityKey())
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .compactDone())
                .isPresent();
        assertThat(cache.snapshot().authoritativeReads()).isEqualTo(reads + 1);
        var open = SyntheticDeleteAuthorityFixturesV2.phases(old).get(0);
        SyntheticDeleteAuthorityFixturesV2.facts(old).forEach(value -> store.values.put(value.key(), value));
        assertThat(coordinator.create(open).toCompletableFuture().join().outcome())
                .isEqualTo(M5TargetDeleteAuthorityCoordinatorV1.Outcome.DEFINITIVE_CONFLICT);
        var restarted = new M5TargetDeleteAuthorityCoordinatorV1(new M5PermanentDoneCacheV2(store, 1, 4096));
        assertThat(restarted
                        .inspect(old.authorityKey())
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .compactDone())
                .isPresent();
        assertThat(restarted.create(open).toCompletableFuture().join().exactCandidateIsAuthoritative())
                .isFalse();
    }

    @Test
    void staleWriterCannotDispatchAfterPermanentCompactionAndCacheEviction() {
        var store = new Store();
        var resource = SyntheticDeleteAuthorityFixturesV2.resource(1);
        var phases = SyntheticDeleteAuthorityFixturesV2.phases(resource);
        var old = store.seed(resource.authorityKey(), M5TargetDeleteAuthorityCodecV1.encodeAuthority(phases.get(0)));
        var cache = new M5PermanentDoneCacheV2(store, 1, 4096);
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(cache);
        coordinator.compactDone(store.fullDone(1)).toCompletableFuture().join();
        coordinator.compactDone(store.fullDone(2)).toCompletableFuture().join();
        var calls = new AtomicInteger();
        var ticket = new ProofBoundWriterTicketV1(
                ProofBoundWriterClassV1.REPLICA_TOPOLOGY_V1,
                digest("operation"),
                digest("owner"),
                digest("proof"),
                digest("successor"),
                1);
        var result = new M5TargetDeleteWriterGuardV1(coordinator)
                .execute(old, ticket, ignored -> {
                    calls.incrementAndGet();
                    return CompletableFuture.completedFuture(
                            M5TargetDeleteWriterGuardV1.ExternalMutationResultV1.applied(digest("p")));
                })
                .toCompletableFuture()
                .join();
        assertThat(calls).hasValue(0);
        assertThat(result.externalMutationInvoked()).isFalse();
        assertThat(result.outcome()).isEqualTo(M5TargetDeleteWriterGuardV1.GuardOutcomeV1.NOT_DISPATCHED_V1);
    }

    @Test
    void absenceUnknownAndMalformedDoneAreNeverCachedAsACompletedResource() {
        var store = new Store();
        var cache = new M5PermanentDoneCacheV2(store, 1, 4096);
        String key = SyntheticDeleteAuthorityFixturesV2.resource(1).authorityKey();
        assertThat(cache.read(key).toCompletableFuture().join()).isEmpty();
        assertThat(cache.read(key).toCompletableFuture().join()).isEmpty();
        assertThat(cache.snapshot().authoritativeReads()).isEqualTo(2);
        store.failReads = true;
        assertThatThrownBy(() -> cache.read(key).toCompletableFuture().join())
                .hasRootCauseMessage("unknown native read");
        store.failReads = false;
        store.seed(
                key,
                CanonicalBytes.copyOf(ByteBuffer.allocate(4).putInt(0x4d354443).array()));
        assertThatThrownBy(() -> cache.read(key).toCompletableFuture().join())
                .hasCauseInstanceOf(IllegalArgumentException.class);
        assertThat(cache.snapshot().entries()).isZero();
    }

    @Test
    void inspectingOneResourceCannotAcceptAnotherResourceReturnedByTheStore() {
        var store = new Store();
        var other = store.fullDone(2);
        String requested = SyntheticDeleteAuthorityFixturesV2.resource(1).authorityKey();
        store.values.put(requested, other);
        assertThatThrownBy(() -> new M5TargetDeleteAuthorityCoordinatorV1(store)
                        .inspect(requested)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("observed delete authority key differs");
    }

    @Test
    void byteBoundCanBypassCachingWithoutLosingPermanentAuthority() {
        var store = new Store();
        var cache = new M5PermanentDoneCacheV2(store, 100, 1);
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(cache);
        var full = store.fullDone(1);
        coordinator.compactDone(full).toCompletableFuture().join();
        assertThat(coordinator
                        .inspect(full.key())
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .compactDone())
                .isPresent();
        assertThat(cache.snapshot().entries()).isZero();
        assertThat(cache.snapshot().encodedBytes()).isZero();
    }

    private static Sha256Digest digest(String value) {
        return SyntheticDeleteAuthorityFixturesV2.digest(value);
    }

    private static final class Store implements ExactMetadataTransactionStoreV1 {
        private final HashMap<String, VersionedValue> values = new HashMap<>();
        private long version;
        private boolean loseResponse;
        private boolean failReads;

        VersionedValue fullDone(int id) {
            var done = SyntheticDeleteAuthorityFixturesV2.phases(SyntheticDeleteAuthorityFixturesV2.resource(id))
                    .get(3);
            return seed(done.authorityKey(), M5TargetDeleteAuthorityCodecV1.encodeAuthority(done));
        }

        VersionedValue seed(String key, CanonicalBytes value) {
            var exact = VersionedValue.of(
                    key,
                    value,
                    new MetadataVersion(CanonicalBytes.copyOf(
                            ByteBuffer.allocate(8).putLong(version++).array())));
            values.put(key, exact);
            return exact;
        }

        public CompletionStage<Optional<VersionedValue>> read(String key) {
            return failReads
                    ? CompletableFuture.failedFuture(new IllegalStateException("unknown native read"))
                    : CompletableFuture.completedFuture(Optional.ofNullable(values.get(key)));
        }

        public CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> before, String key, CanonicalBytes value) {
            if (!Optional.ofNullable(values.get(key)).equals(before)) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            seed(key, value);
            return CompletableFuture.completedFuture(
                    loseResponse ? MutationOutcome.RESPONSE_UNKNOWN : MutationOutcome.APPLIED_EXACT);
        }

        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
            return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
        }

        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }
    }
}
