/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PhysicalObjectRootScannerTest {
    private static final String CLUSTER = "cluster-a";
    private static final String ATTEMPT_ID = "a".repeat(52);
    private static final String REFERENCE_SHA = "b".repeat(64);

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void closeScheduler() {
        scheduler.shutdownNow();
    }

    @Test
    void scansAllShardsWithBoundedPagesAndExactLifecycleCounts() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        VersionedPhysicalObjectRoot active = create(store, "objects/active");
        VersionedPhysicalObjectRoot marked = mark(create(store, "objects/marked"), store);
        VersionedPhysicalObjectRoot deleting = deleting(
                mark(create(store, "objects/deleting"), store), store);
        VersionedPhysicalObjectRoot deleted = deleted(
                deleting(mark(create(store, "objects/deleted"), store), store), store);
        VersionedPhysicalObjectRoot quarantined = quarantine(
                create(store, "objects/quarantined"), store);
        ArrayList<String> visited = new ArrayList<>();
        PhysicalObjectRootScanner scanner = new PhysicalObjectRootScanner(
                CLUSTER, config(), store, scheduler);

        PhysicalObjectRootScanResult result = scanner.scan(root -> {
            visited.add(root.key());
            return CompletableFuture.completedFuture(null);
        }).join();

        assertThat(result.totalRoots()).isEqualTo(5);
        assertThat(result.activeRoots()).isEqualTo(1);
        assertThat(result.markedRoots()).isEqualTo(1);
        assertThat(result.deletingRoots()).isEqualTo(1);
        assertThat(result.deletedRoots()).isEqualTo(1);
        assertThat(result.quarantinedRoots()).isEqualTo(1);
        assertThat(visited).containsExactlyInAnyOrder(
                active.key(), marked.key(), deleting.key(), deleted.key(), quarantined.key());
    }

    @Test
    void visitorFailureDoesNotLeaveTheScannerPermanentlyOverlapping() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        create(store, "objects/one");
        PhysicalObjectRootScanner scanner = new PhysicalObjectRootScanner(
                CLUSTER, config(), store, scheduler);

        assertThatThrownBy(() -> scanner.scan(root -> CompletableFuture.failedFuture(
                        new RuntimeException("injected visitor failure"))).join())
                .hasRootCauseMessage("injected visitor failure");
        assertThat(scanner.scan(root -> CompletableFuture.completedFuture(null)).join().totalRoots())
                .isEqualTo(1);
    }

    @Test
    void closeRejectsNewPassesWithoutClosingBorrowedMetadataStore() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        VersionedPhysicalObjectRoot root = create(store, "objects/one");
        PhysicalObjectRootScanner scanner = new PhysicalObjectRootScanner(
                CLUSTER, config(), store, scheduler);

        scanner.close();

        assertThatThrownBy(() -> scanner.scan(ignored -> CompletableFuture.completedFuture(null)).join())
                .satisfies(failure -> assertThat(unwrap(failure))
                        .isInstanceOfSatisfying(NereusException.class, error ->
                                assertThat(error.code()).isEqualTo(ErrorCode.STORAGE_CLOSED)));
        assertThat(store.getRoot(CLUSTER, new ObjectKeyHash(root.value().objectKeyHash())).join())
                .isPresent();
    }

    private static VersionedPhysicalObjectRoot create(
            FakePhysicalObjectMetadataStore store, String keyValue) {
        ObjectKey key = new ObjectKey(keyValue);
        ObjectKeyHash hash = ObjectKeyHash.from(key);
        return store.createRoot(CLUSTER, new PhysicalObjectRootRecord(
                1,
                hash.value(),
                key.value(),
                "",
                2,
                42,
                ChecksumType.CRC32C.name(),
                "01020304",
                "",
                "",
                PhysicalObjectLifecycle.ACTIVE,
                1,
                100,
                200,
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                "",
                "",
                0)).join();
    }

    private static VersionedPhysicalObjectRoot mark(
            VersionedPhysicalObjectRoot root,
            FakePhysicalObjectMetadataStore store) {
        return store.compareAndSetRoot(CLUSTER, copy(
                root.value(),
                PhysicalObjectLifecycle.MARKED,
                root.value().lifecycleEpoch() + 1,
                ATTEMPT_ID,
                REFERENCE_SHA,
                300,
                400,
                0,
                0,
                ""), root.metadataVersion()).join();
    }

    private static VersionedPhysicalObjectRoot deleting(
            VersionedPhysicalObjectRoot root,
            FakePhysicalObjectMetadataStore store) {
        return store.compareAndSetRoot(CLUSTER, copy(
                root.value(),
                PhysicalObjectLifecycle.DELETING,
                root.value().lifecycleEpoch() + 1,
                root.value().gcAttemptId(),
                root.value().referenceSetSha256(),
                root.value().markedAtMillis(),
                root.value().deleteNotBeforeMillis(),
                400,
                0,
                ""), root.metadataVersion()).join();
    }

    private static VersionedPhysicalObjectRoot deleted(
            VersionedPhysicalObjectRoot root,
            FakePhysicalObjectMetadataStore store) {
        return store.compareAndSetRoot(CLUSTER, copy(
                root.value(),
                PhysicalObjectLifecycle.DELETED,
                root.value().lifecycleEpoch() + 1,
                root.value().gcAttemptId(),
                root.value().referenceSetSha256(),
                root.value().markedAtMillis(),
                root.value().deleteNotBeforeMillis(),
                root.value().deleteStartedAtMillis(),
                500,
                ""), root.metadataVersion()).join();
    }

    private static VersionedPhysicalObjectRoot quarantine(
            VersionedPhysicalObjectRoot root,
            FakePhysicalObjectMetadataStore store) {
        return store.compareAndSetRoot(CLUSTER, copy(
                root.value(),
                PhysicalObjectLifecycle.QUARANTINED,
                root.value().lifecycleEpoch() + 1,
                "",
                "",
                0,
                0,
                0,
                0,
                "audit mismatch"), root.metadataVersion()).join();
    }

    private static PhysicalObjectRootRecord copy(
            PhysicalObjectRootRecord root,
            PhysicalObjectLifecycle lifecycle,
            long lifecycleEpoch,
            String attemptId,
            String referenceSha,
            long markedAt,
            long deleteNotBefore,
            long deleteStarted,
            long deletedAt,
            String reason) {
        return new PhysicalObjectRootRecord(
                root.schemaVersion(),
                root.objectKeyHash(),
                root.objectKey(),
                root.objectId(),
                root.objectKindId(),
                root.objectLength(),
                root.storageChecksumType(),
                root.storageChecksumValue(),
                root.contentSha256(),
                root.etag(),
                lifecycle,
                lifecycleEpoch,
                root.createdAtMillis(),
                root.orphanNotBeforeMillis(),
                attemptId,
                referenceSha,
                markedAt,
                deleteNotBefore,
                deleteStarted,
                deletedAt,
                0,
                "",
                reason,
                0);
    }

    private static PhysicalGcConfig config() {
        return new PhysicalGcConfig(
                false,
                true,
                1,
                1,
                1,
                4_096,
                10,
                10,
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ofMillis(5),
                Duration.ofSeconds(11),
                Duration.ofHours(1),
                Duration.ofHours(25),
                Duration.ofDays(7),
                Duration.ofSeconds(2),
                Duration.ofSeconds(2));
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
