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
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
class M5BookKeeperNativeCreateV2RealTest {
    private static String uri;
    private static String instance;
    private static final BookKeeperCapabilitySnapshotV1 CAPABILITY = capability();

    @BeforeAll
    static void discoverExactNativeNamespace() throws Exception {
        uri = System.getProperty("nereus.bookkeeper.metadataServiceUri");
        var jar = Path.of(BookKeeper.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
        assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(jar))))
                .isEqualTo(CAPABILITY.clientArtifactSha256());
        instance = M5BookKeeperNativeCreateClientV2.discoverInstanceId(uri, CAPABILITY);
    }

    @Test
    void endpointAliasesResolveSameNativeIncarnationAndReservationSurvivesClientRestart() throws Exception {
        assertThat(M5BookKeeperNativeCreateClientV2.discoverInstanceId(uri.replace("127.0.0.1", "127.1"), CAPABILITY))
                .isEqualTo(instance);
        var spec = spec();
        long id;
        try (var client = connect(spec)) {
            var session = client.newSession();
            id = session.reserveLedgerIdentity()
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS)
                    .exactProof()
                    .orElseThrow()
                    .ledgerId();
            session.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
        try (var client = connect(spec)) {
            client.driver().guard().requireOwned(id).get(10, TimeUnit.SECONDS);
            assertThatThrownBy(() -> client.driver().guard().reserve(id).get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(org.apache.bookkeeper.client.BKException.BKLedgerExistException.class);
        }
    }

    @Test
    void sealedNativeLedgerCannotBeRecreatedByLateOldOrFreshClientsAfterCreateFenceAndTestDelete() throws Exception {
        var spec = spec();
        var run = spec.configurations().get(0);
        try (var client = connect(spec)) {
            var session = client.newSession();
            var id = session.reserveLedgerIdentity()
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS)
                    .exactProof()
                    .orElseThrow();
            var handle = session.createReservedRunLedger(run, id)
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS)
                    .exactProof()
                    .orElseThrow();
            var payload = ImmutableRetainedStoragePayload.copyOf(new byte[] {1, 2, 3});
            assertThat(session.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, 0, payload))
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .outcome())
                    .isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
            assertThat(payload.release()).isTrue();
            assertThat(session.closeRunLedger(handle)
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .outcome())
                    .isEqualTo(ProviderMutationOutcomeV1.APPLIED_EXACT);
            client.fenceCreates().get(10, TimeUnit.SECONDS);
            client.fenceCreates().get(10, TimeUnit.SECONDS);
            var reader = client.newSession();
            assertThat(reader.openRunLedger(handle)
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactHandle())
                    .contains(handle);
            assertThat(reader.readExactEntry(handle, 0)
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactEntry()
                            .orElseThrow()
                            .payload()
                            .toByteArray())
                    .containsExactly(1, 2, 3);
            reader.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            // Test-owned resource deletion is fault injection; this create fence grants no M5 delete authority.
            client.nativeClient()
                    .newDeleteLedgerOp()
                    .withLedgerId(id.ledgerId())
                    .execute()
                    .get(30, TimeUnit.SECONDS);
            var late = client.newSession();
            assertThat(late.createReservedRunLedger(run, id)
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactProof())
                    .isEmpty();
            try (var restarted = connect(spec)) {
                var fresh = restarted.newSession();
                assertThat(fresh.createReservedRunLedger(run, id)
                                .toCompletableFuture()
                                .get(10, TimeUnit.SECONDS)
                                .exactProof())
                        .isEmpty();
                assertThat(fresh.reserveLedgerIdentity()
                                .toCompletableFuture()
                                .get(10, TimeUnit.SECONDS)
                                .exactProof())
                        .isEmpty();
                assertThatThrownBy(() -> restarted
                                .nativeClient()
                                .getLedgerManager()
                                .readLedgerMetadata(id.ledgerId())
                                .get(10, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(
                                org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                        .class);
                fresh.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
            late.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            session.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void reservationBeforeFenceCannotCreateAfterNativeFence() throws Exception {
        var spec = spec();
        try (var client = connect(spec)) {
            var session = client.newSession();
            var id = session.reserveLedgerIdentity()
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS)
                    .exactProof()
                    .orElseThrow();
            client.fenceCreates().get(10, TimeUnit.SECONDS);
            assertThat(session.createReservedRunLedger(spec.configurations().get(0), id)
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactProof())
                    .isEmpty();
            assertThatThrownBy(() -> client.nativeClient()
                            .getLedgerManager()
                            .readLedgerMetadata(id.ledgerId())
                            .get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(
                            org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                    .class);
            session.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void foreignTaskCannotAdoptReservedIdAndUnadmittedRunCannotCreate() throws Exception {
        var spec = spec();
        var other = spec();
        try (var client = connect(spec);
                var foreign = connect(other)) {
            var session = client.newSession();
            var id = session.reserveLedgerIdentity()
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS)
                    .exactProof()
                    .orElseThrow();
            var foreignSession = foreign.newSession();
            assertThat(foreignSession
                            .createReservedRunLedger(other.configurations().get(0), id)
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactProof())
                    .isEmpty();
            assertThat(session.createReservedRunLedger(other.configurations().get(0), id)
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactProof())
                    .isEmpty();
            foreignSession.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            session.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void changedNativeInstanceAndConflictingTaskScopeCannotConnect() throws Exception {
        var spec = spec();
        var wrong =
                M5BookKeeperNativeCreateSpecV2.of(UUID.randomUUID().toString(), spec.taskId(), spec.configurations());
        assertThatThrownBy(() -> connect(wrong))
                .hasRootCauseMessage("native BookKeeper INSTANCEID differs from the task create scope");
        try (var client = connect(spec)) {
            var changed = M5BookKeeperNativeCreateSpecV2.of(instance, spec.taskId(), spec().configurations());
            assertThatThrownBy(() -> connect(changed))
                    .hasRootCauseMessage("native BK task create fence differs or has an invalid revision");
        }
    }

    @Test
    void missingOrChangedNativeGuardCannotBeTreatedAsOpen() throws Exception {
        var spec = spec();
        try (var client = connect(spec)) {
            var guard = client.driver().guard();
            var session = client.newSession();
            var id = session.reserveLedgerIdentity()
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS)
                    .exactProof()
                    .orElseThrow();
            client.driver().getZk().delete(guard.taskPath(), 0);
            assertThat(session.createReservedRunLedger(spec.configurations().get(0), id)
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactProof())
                    .isEmpty();
            assertThatThrownBy(() -> client.fenceCreates().get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(org.apache.zookeeper.KeeperException.NoNodeException.class);
            session.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
        var corrupt = spec();
        try (var client = connect(corrupt)) {
            client.driver().getZk().setData(client.driver().guard().taskPath(), new byte[] {1}, 0);
            assertThatThrownBy(() -> client.fenceCreates().get(10, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class);
            assertThatThrownBy(() -> connect(corrupt))
                    .hasRootCauseMessage("native BK task create fence differs or has an invalid revision");
        }
    }

    @Test
    void delayedNativeCreateTransactionCannotCrossThePermanentFence() throws Exception {
        try (var fault = new NativeFaults(spec())) {
            long id = fault.allocate();
            fault.guard.reserve(id).get(10, TimeUnit.SECONDS);
            fault.zk.holdNextMulti = true;
            var pending = fault.manager.createLedgerMetadata(id, fault.metadata(id));
            fault.zk.held.get(10, TimeUnit.SECONDS);
            fault.guard.fenceCreates().get(10, TimeUnit.SECONDS);
            assertThat(pending).isNotDone();
            fault.zk.release.get().run();
            assertThatThrownBy(() -> pending.get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(org.apache.zookeeper.KeeperException.BadVersionException.class);
            assertThatThrownBy(() -> fault.manager.readLedgerMetadata(id).get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(
                            org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                    .class);
            fault.guard.requireOwned(id).get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void lostAppliedNativeReservationCreateAndFenceResponsesReconcileExactDurableState() throws Exception {
        try (var fault = new NativeFaults(spec())) {
            long id = fault.allocate();
            fault.zk.loseNextMulti = true;
            fault.guard.reserve(id).get(10, TimeUnit.SECONDS);
            fault.zk.loseNextMulti = true;
            var created =
                    fault.manager.createLedgerMetadata(id, fault.metadata(id)).get(10, TimeUnit.SECONDS);
            assertThat(fault.manager
                            .readLedgerMetadata(id)
                            .get(10, TimeUnit.SECONDS)
                            .getValue()
                            .getCToken())
                    .isEqualTo(created.getValue().getCToken());
            fault.zk.loseNextSet = true;
            fault.guard.fenceCreates().get(10, TimeUnit.SECONDS);
            assertThat(fault.zk.lost.get()).isEqualTo(3);
            assertThat(fault.zk.multis.get()).isEqualTo(2);
            fault.client.newDeleteLedgerOp().withLedgerId(id).execute().get(30, TimeUnit.SECONDS);
            assertThatThrownBy(() -> fault.manager
                            .createLedgerMetadata(id, fault.metadata(id))
                            .get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(org.apache.zookeeper.KeeperException.BadVersionException.class);
        }
    }

    /** Only callback delivery is controlled; every reservation, fence and ledger transaction reaches real ZooKeeper. */
    private static final class NativeFaults implements AutoCloseable {
        final BookKeeper client;
        final FaultZooKeeper zk;
        final M5BookKeeperNativeCreateSpecV2 spec;
        final M5BookKeeperNativeCreateGuardV2 guard;
        final M5BookKeeperNativeLedgerManagerV2 manager;

        NativeFaults(M5BookKeeperNativeCreateSpecV2 spec) throws Exception {
            this.spec = spec;
            var configuration = RealBookKeeperClientConfigurationV1.from(uri, CAPABILITY);
            client = (BookKeeper) org.apache.bookkeeper.client.api.BookKeeper.newBuilder(configuration)
                    .build();
            var connected = new java.util.concurrent.CountDownLatch(1);
            zk = new FaultZooKeeper(java.net.URI.create(uri).getAuthority(), event -> {
                if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected) {
                    connected.countDown();
                }
            });
            assertThat(connected.await(10, TimeUnit.SECONDS)).isTrue();
            var acls = org.apache.bookkeeper.util.ZkUtils.getACLs(configuration);
            guard = new M5BookKeeperNativeCreateGuardV2(
                    zk, java.net.URI.create(uri).getPath(), spec, acls);
            var delegate = (org.apache.bookkeeper.meta.AbstractZkLedgerManager)
                    client.getLedgerManagerFactory().newLedgerManager();
            manager = new M5BookKeeperNativeLedgerManagerV2(delegate, guard, spec, zk, acls);
        }

        long allocate() throws Exception {
            try (var generator = client.getLedgerManagerFactory().newLedgerIdGenerator()) {
                var result = new java.util.concurrent.CompletableFuture<Long>();
                generator.generateLedgerId((rc, id) -> {
                    if (rc == org.apache.bookkeeper.client.BKException.Code.OK) {
                        result.complete(id);
                    } else {
                        result.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                    }
                });
                return result.get(10, TimeUnit.SECONDS);
            }
        }

        org.apache.bookkeeper.client.api.LedgerMetadata metadata(long id) {
            return org.apache.bookkeeper.client.LedgerMetadataBuilder.create()
                    .withId(id)
                    .withMetadataFormatVersion(3)
                    .withEnsembleSize(3)
                    .withWriteQuorumSize(3)
                    .withAckQuorumSize(2)
                    .withPassword(new byte[0])
                    .withDigestType(org.apache.bookkeeper.client.api.DigestType.CRC32C)
                    .withCustomMetadata(RealBookKeeperCellSessionV1.metadata(
                            spec.configurations().get(0)))
                    .newEnsembleEntry(
                            0,
                            List.of(
                                    org.apache.bookkeeper.net.BookieId.parse("127.0.0.1:3181"),
                                    org.apache.bookkeeper.net.BookieId.parse("127.0.0.1:3182"),
                                    org.apache.bookkeeper.net.BookieId.parse("127.0.0.1:3183")))
                    .build();
        }

        @Override
        public void close() throws Exception {
            try {
                manager.close();
            } finally {
                try {
                    zk.close();
                } finally {
                    client.close();
                }
            }
        }
    }

    private static final class FaultZooKeeper extends org.apache.zookeeper.ZooKeeper {
        volatile boolean holdNextMulti;
        volatile boolean loseNextMulti;
        volatile boolean loseNextSet;
        final java.util.concurrent.atomic.AtomicInteger lost = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger multis = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.CompletableFuture<Void> held = new java.util.concurrent.CompletableFuture<>();
        final java.util.concurrent.atomic.AtomicReference<Runnable> release =
                new java.util.concurrent.atomic.AtomicReference<>();

        FaultZooKeeper(String connect, org.apache.zookeeper.Watcher watcher) throws java.io.IOException {
            super(connect, 10_000, watcher);
        }

        @Override
        public void multi(
                Iterable<org.apache.zookeeper.Op> ops,
                org.apache.zookeeper.AsyncCallback.MultiCallback callback,
                Object context) {
            multis.incrementAndGet();
            boolean lose = loseNextMulti;
            loseNextMulti = false;
            Runnable dispatch = () -> super.multi(
                    ops,
                    (rc, path, ctx, results) -> {
                        if (lose && rc == 0) {
                            lost.incrementAndGet();
                            callback.processResult(
                                    org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue(),
                                    path,
                                    ctx,
                                    null);
                        } else {
                            callback.processResult(rc, path, ctx, results);
                        }
                    },
                    context);
            if (holdNextMulti) {
                holdNextMulti = false;
                release.set(dispatch);
                held.complete(null);
            } else {
                dispatch.run();
            }
        }

        @Override
        public void setData(
                String path,
                byte[] data,
                int version,
                org.apache.zookeeper.AsyncCallback.StatCallback callback,
                Object context) {
            boolean lose = loseNextSet;
            loseNextSet = false;
            super.setData(
                    path,
                    data,
                    version,
                    (rc, actualPath, ctx, stat) -> {
                        if (lose && rc == 0) {
                            lost.incrementAndGet();
                            callback.processResult(
                                    org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue(),
                                    actualPath,
                                    ctx,
                                    null);
                        } else {
                            callback.processResult(rc, actualPath, ctx, stat);
                        }
                    },
                    context);
        }
    }

    private static M5BookKeeperNativeCreateClientV2 connect(M5BookKeeperNativeCreateSpecV2 spec) throws Exception {
        return M5BookKeeperNativeCreateClientV2.connect(uri, CAPABILITY, spec);
    }

    private static M5BookKeeperNativeCreateSpecV2 spec() {
        var id = UUID.randomUUID();
        return M5BookKeeperNativeCreateSpecV2.of(
                instance,
                Sha256Digest.hash(
                        CanonicalBytes.copyOf(id.toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII))),
                List.of(RunLedgerConfigurationV1.from(
                        CAPABILITY,
                        new StorageRunId(new Id128(id.getMostSignificantBits(), id.getLeastSignificantBits())))));
    }

    private static BookKeeperCapabilitySnapshotV1 capability() {
        int frameLimit = 5_242_880;
        return new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(digest(1)),
                "cd06340851d6d657b7c7546df01df365c18980de",
                Sha256Digest.copyOf(java.util.HexFormat.of()
                        .parseHex("8e64f2b7436bb814705f611eb0ac48d64d90de7a50d295905c459d89bc3f9d8f")),
                "cd06340851d6d657b7c7546df01df365c18980de",
                Sha256Digest.copyOf(java.util.HexFormat.of()
                        .parseHex("c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d")),
                BookKeeperProtocolModeV1.V3,
                frameLimit,
                frameLimit,
                BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(frameLimit, frameLimit),
                true,
                3,
                3,
                2,
                BookKeeperDigestTypeV1.CRC32C,
                true,
                true,
                new BookKeeperTimeoutClassV1(10_000, 5_000, 5_000, 30_000),
                "bk-k0-no-auth:v1",
                Sha256Digest.copyOf(java.util.HexFormat.of()
                        .parseHex("eaf41c4b42b767b8ea6e86023a784425b8073f174dbade92b4249c8f3d301dbd")));
    }

    private static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }
}
