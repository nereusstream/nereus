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

package com.nereusstream.storage.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.DeleteOutcome;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateV2RealTest.NativeFaults;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Actual server-enforced native fencing and ledger absence. M5 eligibility/intent composition is not certified. */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class M5BookKeeperNativeDeleteV2RealTest {
    static final com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1 CAPABILITY =
            M5BookKeeperNativeCreateV2RealTest.capability();

    @Test
    void oldPausedDeleteIsRejectedByServerAfterAnotherClientTakesOwnership() throws Exception {
        var spec = spec();
        var owner = UUID.randomUUID();
        try (var first = connect(spec);
                var second = connect(spec)) {
            assertThatThrownBy(() -> await(first.requireNamespaceBinding()))
                    .hasRootCauseMessage("native client was not opened bound");
            var handle = sealed(first, spec);
            var target = await(first.captureExactTarget(handle)).exactTarget().orElseThrow();
            var authority = first.deleteAuthority(handle);
            assertThatThrownBy(() -> await(authority.claim(Optional.empty(), owner)))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class);
            await(first.fenceCreates());
            var epoch = await(authority.claim(Optional.empty(), owner));
            var foreignTask =
                    M5BookKeeperNativeCreateSpecV2.of(spec.nativeInstanceId(), spec().taskId(), spec.configurations());
            try (var foreign = connect(foreignTask)) {
                await(foreign.fenceCreates());
                var foreignAuthority = foreign.deleteAuthority(handle);
                assertThat(await(foreignAuthority.read())).contains(epoch);
                assertThatThrownBy(() -> await(foreignAuthority.deleteExact(epoch, target)))
                        .hasRootCauseInstanceOf(
                                org.apache.bookkeeper.client.BKException.BKUnauthorizedAccessException.class);
                assertThat(await(first.captureExactTarget(handle)).exactTarget())
                        .contains(target);
            }
            var gate = new CompletableFuture<Void>();
            var ready = new CompletableFuture<Void>();
            var delayed = first.deleteAuthority(handle, () -> {
                ready.complete(null);
                return gate;
            });
            var observer = delayed.deleteExact(epoch, target).toCompletableFuture();
            await(ready);
            var successor = second.deleteAuthority(handle);
            var current = await(successor.claim(Optional.of(epoch), UUID.randomUUID()));
            assertThat(current.nativeVersion()).isEqualTo(epoch.nativeVersion() + 1);
            assertThat(current.encode().length()).isEqualTo(epoch.encode().length());
            gate.complete(null);
            assertThatThrownBy(() -> await(observer))
                    .hasRootCauseInstanceOf(org.apache.zookeeper.KeeperException.BadVersionException.class);
            assertThat(await(second.captureExactTarget(handle)).exactTarget()).contains(target);
            assertThatThrownBy(() -> await(authority.requireCurrent(epoch)))
                    .hasRootCauseMessage("native delete owner is fenced");
            assertThat(await(successor.deleteExact(current, target)).outcome())
                    .isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
            assertThat(await(successor.read())).contains(current);
            assertThat(await(successor.deleteExact(current, target)).outcome())
                    .isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
            try (var restarted = connect(spec)) {
                var recovered = restarted.deleteAuthority(handle);
                assertThat(await(recovered.read())).contains(current);
                assertThat(await(recovered.deleteExact(current, target)).outcome())
                        .isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
            }
        }
    }

    @Test
    void nativeLedgerVersionChangeAfterIdentityReadPreventsDeleteEvenWhenBytesMatch() throws Exception {
        var spec = spec();
        try (var nativeState = new NativeFaults(uri(), CAPABILITY, spec)) {
            var handle = sealed(nativeState);
            var metadata = await(nativeState.manager.readLedgerMetadata(
                    handle.ledgerIdentity().ledgerId()));
            var target = M5BookKeeperDeleteAdapterV1.exactTarget(metadata.getValue(), handle, CAPABILITY, new byte[0])
                    .orElseThrow();
            var gate = new CompletableFuture<Void>();
            var ready = new CompletableFuture<Void>();
            var authority = authority(nativeState, handle, () -> {
                ready.complete(null);
                return gate;
            });
            var epoch = await(authority.claim(Optional.empty(), UUID.randomUUID()));
            var pending = authority.deleteExact(epoch, target);
            await(ready);
            var updated = await(nativeState.manager.writeLedgerMetadata(
                    handle.ledgerIdentity().ledgerId(), metadata.getValue(), metadata.getVersion()));
            assertThat(updated.getVersion()).isNotEqualTo(metadata.getVersion());
            gate.complete(null);
            assertThatThrownBy(() -> await(pending))
                    .hasRootCauseInstanceOf(org.apache.zookeeper.KeeperException.BadVersionException.class);
            assertThat(await(nativeState.manager.readLedgerMetadata(
                                    handle.ledgerIdentity().ledgerId()))
                            .getValue()
                            .getCToken())
                    .isEqualTo(metadata.getValue().getCToken());
            assertThat(await(authority.deleteExact(epoch, target)).outcome())
                    .isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
        }
    }

    @Test
    void lostAppliedEpochAndDeleteRepliesReconcileNativeStateAndObserverCancellationCannotCancelDelete()
            throws Exception {
        var spec = spec();
        try (var nativeState = new NativeFaults(uri(), CAPABILITY, spec)) {
            var handle = sealed(nativeState);
            var metadata = await(nativeState.manager.readLedgerMetadata(
                    handle.ledgerIdentity().ledgerId()));
            var target = M5BookKeeperDeleteAdapterV1.exactTarget(metadata.getValue(), handle, CAPABILITY, new byte[0])
                    .orElseThrow();
            var gate = new CompletableFuture<Void>();
            var ready = new CompletableFuture<Void>();
            var authority = authority(nativeState, handle, () -> {
                ready.complete(null);
                return gate.thenRun(() -> nativeState.zk.loseNextMulti = true);
            });
            nativeState.zk.loseNextMulti = true;
            var epoch = await(authority.claim(Optional.empty(), UUID.randomUUID()));
            assertThat(nativeState.zk.lost.get()).isOne();
            var delivered = new CompletableFuture<Void>();
            nativeState.zk.lostMultiDelivery.set(delivered);
            var observer = authority.deleteExact(epoch, target).toCompletableFuture();
            await(ready);
            assertThat(observer.cancel(true)).isTrue();
            gate.complete(null);
            await(delivered);
            assertThatThrownBy(() -> await(nativeState.manager.readLedgerMetadata(
                            handle.ledgerIdentity().ledgerId())))
                    .hasRootCauseInstanceOf(
                            org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                    .class);
            assertThat(nativeState.zk.lost.get()).isEqualTo(2);
            assertThat(await(authority.read())).contains(epoch);
            assertThat(await(authority.deleteExact(epoch, target)).outcome())
                    .isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
        }
    }

    @Test
    void nativeIntentBindingRejectsTokenReuseAndFencesPausedDispatchAfterSameOwnerEpochAdvance() throws Exception {
        var spec = spec();
        try (var state = new NativeFaults(uri(), CAPABILITY, spec)) {
            var handle = sealed(state);
            var target = M5BookKeeperDeleteAdapterV1.exactTarget(
                            await(state.manager.readLedgerMetadata(
                                            handle.ledgerIdentity().ledgerId()))
                                    .getValue(),
                            handle,
                            CAPABILITY,
                            new byte[0])
                    .orElseThrow();
            var ready = new CompletableFuture<Void>();
            var gate = new CompletableFuture<Void>();
            var delayed = authority(state, handle, () -> {
                ready.complete(null);
                return gate;
            });
            var immediate = authority(state, handle, () -> CompletableFuture.completedFuture(null));
            var owner = UUID.randomUUID();
            var epoch = await(immediate.claim(Optional.empty(), owner));
            var token = digest("token-1");
            var intentSha = digest("intent-1");
            state.zk.loseNextIntentMutation = true;
            var intent = await(immediate.bindIntent(epoch, token, intentSha, target.metadataSha256()));
            assertThat(state.zk.lost.get()).isOne();
            assertThat(await(immediate.bindIntent(epoch, token, intentSha, target.metadataSha256())))
                    .isEqualTo(intent);
            assertThatThrownBy(() ->
                            await(immediate.bindIntent(epoch, digest("token-2"), intentSha, target.metadataSha256())))
                    .hasRootCauseMessage("changed native intent requires a newer GC epoch");
            assertThatThrownBy(() -> await(
                            immediate.bindIntent(epoch, token, digest("changed intent"), target.metadataSha256())))
                    .hasRootCauseMessage("changed native intent requires a newer GC epoch");
            assertThatThrownBy(() -> await(immediate.bindIntent(epoch, token, intentSha, digest("changed metadata"))))
                    .hasRootCauseMessage("changed native intent requires a newer GC epoch");
            var pending = delayed.deleteExact(intent, target);
            await(ready);
            var nextEpoch = await(immediate.claim(Optional.of(epoch), owner));
            var next = await(
                    immediate.bindIntent(nextEpoch, digest("token-2"), digest("intent-2"), target.metadataSha256()));
            assertThat(next.nativeVersion()).isEqualTo(intent.nativeVersion() + 1);
            assertThat(next.encode().length()).isEqualTo(intent.encode().length());
            gate.complete(null);
            assertThatThrownBy(() -> await(pending))
                    .hasRootCauseInstanceOf(org.apache.zookeeper.KeeperException.BadVersionException.class);
            assertThat(await(state.manager.readLedgerMetadata(
                            handle.ledgerIdentity().ledgerId())))
                    .isNotNull();
            assertThat(await(immediate.deleteExact(next, target)).outcome())
                    .isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
            assertThat(await(immediate.readIntent())).contains(next);
            assertThat(await(immediate.deleteExact(next, target)).outcome())
                    .isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
        }
    }

    static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    private static M5BookKeeperNativeDeleteAuthorityV2 authority(
            NativeFaults state,
            RunLedgerHandleV1 handle,
            java.util.function.Supplier<CompletionStage<Void>> beforeDelete) {
        var configuration = RealBookKeeperClientConfigurationV1.from(uri(), CAPABILITY);
        return new M5BookKeeperNativeDeleteAuthorityV2(
                state.zk,
                state.manager,
                state.guard,
                org.apache.bookkeeper.util.ZkUtils.getACLs(configuration),
                handle,
                state.spec.namespace(),
                CAPABILITY,
                beforeDelete);
    }

    static RunLedgerHandleV1 sealed(NativeFaults state) throws Exception {
        long id = state.allocate();
        await(state.guard.reserve(id));
        var created = await(state.manager.createLedgerMetadata(id, state.metadata(id)));
        var closed = LedgerMetadataBuilder.from(created.getValue())
                .withClosedState()
                .withLastEntryId(-1)
                .withLength(0)
                .build();
        await(state.manager.writeLedgerMetadata(id, closed, created.getVersion()));
        await(state.guard.fenceCreates());
        var run = state.spec.configurations().get(0);
        return new RunLedgerHandleV1(
                run.providerScopeId(), run.runId(), new BookKeeperLedgerIdentity(id), run.configurationDigest());
    }

    static RunLedgerHandleV1 sealed(M5BookKeeperNativeCreateClientV2 client, M5BookKeeperNativeCreateSpecV2 spec)
            throws Exception {
        var session = client.newSession();
        try {
            var id = await(session.reserveLedgerIdentity()).exactProof().orElseThrow();
            var handle = await(session.createReservedRunLedger(
                            spec.configurations().get(0), id))
                    .exactProof()
                    .orElseThrow();
            var payload = ImmutableRetainedStoragePayload.copyOf(new byte[] {1, 2, 3});
            try {
                assertThat(await(session.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, 0, payload)))
                                .exactProof())
                        .isPresent();
            } finally {
                payload.release();
            }
            assertThat(await(session.closeRunLedger(handle)).exactProof()).isPresent();
            return handle;
        } finally {
            await(session.closeAsync());
        }
    }

    static M5BookKeeperNativeCreateClientV2 connect(M5BookKeeperNativeCreateSpecV2 spec) throws Exception {
        requireExactClientArtifact();
        return M5BookKeeperNativeCreateClientV2.connect(uri(), CAPABILITY, spec);
    }

    static M5BookKeeperNativeCreateSpecV2 spec() throws Exception {
        requireExactClientArtifact();
        String instance = M5BookKeeperNativeCreateClientV2.discoverInstanceId(uri(), CAPABILITY);
        UUID id = UUID.randomUUID();
        return M5BookKeeperNativeCreateSpecV2.of(
                instance,
                Sha256Digest.hash(
                        CanonicalBytes.copyOf(id.toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII))),
                List.of(RunLedgerConfigurationV1.from(
                        CAPABILITY,
                        new StorageRunId(new Id128(id.getMostSignificantBits(), id.getLeastSignificantBits())))));
    }

    private static String uri() {
        return System.getProperty("nereus.bookkeeper.metadataServiceUri");
    }

    private static void requireExactClientArtifact() throws Exception {
        var jar = java.nio.file.Path.of(org.apache.bookkeeper.client.BookKeeper.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
        assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(java.nio.file.Files.readAllBytes(jar))))
                .isEqualTo(CAPABILITY.clientArtifactSha256());
    }

    static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
}
