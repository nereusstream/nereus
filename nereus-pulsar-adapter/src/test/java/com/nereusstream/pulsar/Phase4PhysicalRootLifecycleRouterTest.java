/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.managedledger.cursor.CursorIdentity;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorSnapshotKeys;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.ManagedLedgerStreamProjection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedVirtualLedgerProjection;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.VirtualLedgerProjectionRecord;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class Phase4PhysicalRootLifecycleRouterTest {
    private static final String CLUSTER = "cluster-a";
    private static final String TOPIC = "persistent://tenant/ns/gc-router";
    private static final Checksum SHA =
            new Checksum(ChecksumType.SHA256, "a".repeat(64));

    @Test
    void routesEveryLifecycleAndDeduplicatesActiveCursorInventoryByStream() {
        CursorLedgerIdentity ledger = ledger();
        ManagedLedgerProjectionMetadataStore projections = projections(ledger, true);
        AtomicInteger cursorScans = new AtomicInteger();
        AtomicInteger cursorRecoveries = new AtomicInteger();
        AtomicInteger ownerlessActive = new AtomicInteger();
        AtomicInteger ownerlessMarked = new AtomicInteger();
        AtomicInteger deleting = new AtomicInteger();
        AtomicInteger deleted = new AtomicInteger();
        ArrayList<CursorLedgerIdentity> routedLedgers = new ArrayList<>();
        Phase4PhysicalRootLifecycleRouter router =
                new Phase4PhysicalRootLifecycleRouter(
                        CLUSTER,
                        projections,
                        exact -> {
                            cursorScans.incrementAndGet();
                            routedLedgers.add(exact);
                            return CompletableFuture.completedFuture(null);
                        },
                        (exact, root) -> {
                            cursorRecoveries.incrementAndGet();
                            routedLedgers.add(exact);
                            return CompletableFuture.completedFuture(null);
                        },
                        root -> {
                            ownerlessActive.incrementAndGet();
                            return CompletableFuture.completedFuture(null);
                        },
                        root -> {
                            ownerlessMarked.incrementAndGet();
                            return CompletableFuture.completedFuture(null);
                        },
                        root -> {
                            deleting.incrementAndGet();
                            return CompletableFuture.completedFuture(null);
                        },
                        root -> {
                            deleted.incrementAndGet();
                            return CompletableFuture.completedFuture(null);
                        });

        router.visit(root(cursorKey(ledger, "0".repeat(32)),
                PhysicalObjectKind.CURSOR_SNAPSHOT, PhysicalObjectLifecycle.ACTIVE)).join();
        router.visit(root(cursorKey(ledger, "1".repeat(32)),
                PhysicalObjectKind.CURSOR_SNAPSHOT, PhysicalObjectLifecycle.ACTIVE)).join();
        router.visit(root(new ObjectKey("objects/plain-active"),
                PhysicalObjectKind.COMMITTED_COMPACTED, PhysicalObjectLifecycle.ACTIVE)).join();
        router.visit(root(cursorKey(ledger, "2".repeat(32)),
                PhysicalObjectKind.CURSOR_SNAPSHOT, PhysicalObjectLifecycle.MARKED)).join();
        router.visit(root(new ObjectKey("objects/plain-marked"),
                PhysicalObjectKind.COMMITTED_COMPACTED, PhysicalObjectLifecycle.MARKED)).join();
        router.visit(root(new ObjectKey("objects/plain-deleting"),
                PhysicalObjectKind.COMMITTED_COMPACTED, PhysicalObjectLifecycle.DELETING)).join();
        router.visit(root(new ObjectKey("objects/plain-deleted"),
                PhysicalObjectKind.COMMITTED_COMPACTED, PhysicalObjectLifecycle.DELETED)).join();
        router.visit(root(new ObjectKey("objects/plain-quarantined"),
                PhysicalObjectKind.COMMITTED_COMPACTED, PhysicalObjectLifecycle.QUARANTINED)).join();

        assertThat(cursorScans).hasValue(1);
        assertThat(cursorRecoveries).hasValue(1);
        assertThat(ownerlessActive).hasValue(1);
        assertThat(ownerlessMarked).hasValue(1);
        assertThat(deleting).hasValue(1);
        assertThat(deleted).hasValue(1);
        assertThat(routedLedgers).containsExactly(ledger, ledger);
    }

    @Test
    void missingHistoricalBindingFallsBackOnlyToOwnerlessGlobalProof() {
        CursorLedgerIdentity ledger = ledger();
        AtomicInteger ownerless = new AtomicInteger();
        Phase4PhysicalRootLifecycleRouter router =
                new Phase4PhysicalRootLifecycleRouter(
                        CLUSTER,
                        projections(ledger, false),
                        ignored -> CompletableFuture.failedFuture(
                                new AssertionError("cursor scan must not run")),
                        (ignored, root) -> CompletableFuture.failedFuture(
                                new AssertionError("cursor recovery must not run")),
                        root -> {
                            ownerless.incrementAndGet();
                            return CompletableFuture.completedFuture(null);
                        },
                        root -> CompletableFuture.completedFuture(null),
                        root -> CompletableFuture.completedFuture(null),
                        root -> CompletableFuture.completedFuture(null));

        router.visit(root(cursorKey(ledger, "3".repeat(32)),
                PhysicalObjectKind.CURSOR_SNAPSHOT, PhysicalObjectLifecycle.ACTIVE)).join();

        assertThat(ownerless).hasValue(1);
    }

    private static ManagedLedgerProjectionMetadataStore projections(
            CursorLedgerIdentity ledger, boolean bindingPresent) {
        return (ManagedLedgerProjectionMetadataStore) Proxy.newProxyInstance(
                Phase4PhysicalRootLifecycleRouterTest.class.getClassLoader(),
                new Class<?>[] {ManagedLedgerProjectionMetadataStore.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("getProjectionByStream")) {
                        StreamId stream = (StreamId) arguments[1];
                        Optional<VersionedVirtualLedgerProjection> binding = bindingPresent
                                ? Optional.of(binding(ledger))
                                : Optional.empty();
                        return CompletableFuture.completedFuture(
                                new ManagedLedgerStreamProjection(
                                        stream, binding, Optional.empty()));
                    }
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    return CompletableFuture.failedFuture(new UnsupportedOperationException(
                            method.getName()));
                });
    }

    private static VersionedVirtualLedgerProjection binding(
            CursorLedgerIdentity ledger) {
        return new VersionedVirtualLedgerProjection(
                "/projection/binding",
                new VirtualLedgerProjectionRecord(
                        ledger.managedLedgerName(),
                        ledger.managedLedgerNameHash(),
                        ledger.projection(),
                        0,
                        ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION,
                        0),
                1,
                SHA);
    }

    private static CursorLedgerIdentity ledger() {
        StreamId stream = ManagedLedgerProjectionNames.streamId(TOPIC, 1);
        ManagedLedgerProjectionIdentity projection =
                new ManagedLedgerProjectionIdentity(
                        1,
                        1,
                        stream.value(),
                        ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 1);
        return new CursorLedgerIdentity(
                TOPIC,
                ManagedLedgerProjectionNames.managedLedgerNameHash(TOPIC),
                projection);
    }

    private static ObjectKey cursorKey(
            CursorLedgerIdentity ledger, String snapshotId) {
        String cursor = "subscription-a";
        return CursorSnapshotKeys.objectKey(
                CLUSTER,
                new CursorIdentity(
                        ledger,
                        cursor,
                        CursorNames.cursorNameHash(cursor),
                        1),
                snapshotId);
    }

    private static VersionedPhysicalObjectRoot root(
            ObjectKey key,
            PhysicalObjectKind kind,
            PhysicalObjectLifecycle lifecycle) {
        long epoch = switch (lifecycle) {
            case ACTIVE, QUARANTINED -> 1;
            case MARKED -> 2;
            case DELETING -> 3;
            case DELETED -> 4;
        };
        boolean gc = lifecycle == PhysicalObjectLifecycle.MARKED
                || lifecycle == PhysicalObjectLifecycle.DELETING
                || lifecycle == PhysicalObjectLifecycle.DELETED;
        long started = lifecycle == PhysicalObjectLifecycle.DELETING
                        || lifecycle == PhysicalObjectLifecycle.DELETED
                ? 300
                : 0;
        long deleted = lifecycle == PhysicalObjectLifecycle.DELETED ? 400 : 0;
        PhysicalObjectRootRecord value = new PhysicalObjectRootRecord(
                1,
                ObjectKeyHash.from(key).value(),
                key.value(),
                "",
                kind.wireId(),
                42,
                ChecksumType.CRC32C.name(),
                "01020304",
                "",
                "",
                lifecycle,
                epoch,
                100,
                100,
                gc ? "b".repeat(52) : "",
                gc ? "c".repeat(64) : "",
                gc ? 200 : 0,
                gc ? 300 : 0,
                started,
                deleted,
                0,
                "",
                lifecycle == PhysicalObjectLifecycle.QUARANTINED
                        ? "test quarantine"
                        : "",
                0);
        return new VersionedPhysicalObjectRoot(
                "/root/" + ObjectKeyHash.from(key).value(),
                value,
                0,
                SHA);
    }
}
