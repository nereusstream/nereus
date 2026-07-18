/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.DeleteObjectResult;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ListObjectsOptions;
import com.nereusstream.objectstore.ListObjectsResult;
import com.nereusstream.objectstore.ListedObject;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import com.nereusstream.objectstore.ReplayableObjectUpload;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ObjectInventoryScannerTest {
    private static final String CLUSTER = "cluster/a";
    private static final ObjectKeyPrefix PREFIX = new ObjectKeyPrefix("cluster-a/inventory/");
    private static final Instant NOW = Instant.parse("2026-07-18T12:00:00Z");
    private static final Checksum CRC32C = new Checksum(ChecksumType.CRC32C, "01020304");

    @Test
    void registersOnlyOldExactHeadObjectsAndWaitsAnotherFullGrace() {
        FakePhysicalObjectMetadataStore metadata = new FakePhysicalObjectMetadataStore();
        InventoryObjectStore objects = new InventoryObjectStore();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            ObjectKey valid = key("valid.obj");
            ObjectKey malformed = key("malformed.bin");
            ObjectKey young = key("young.obj");
            ObjectKey stale = key("stale.obj");
            ObjectKey mismatch = key("mismatch.obj");
            Instant old = NOW.minus(Duration.ofHours(3));
            objects.list(valid, 7, "etag-valid", old);
            objects.head(valid, 7, "etag-valid", CRC32C);
            objects.list(malformed, 3, "etag-malformed", old);
            objects.head(malformed, 3, "etag-malformed", CRC32C);
            objects.list(young, 5, "etag-young", NOW.minus(Duration.ofMinutes(5)));
            objects.head(young, 5, "etag-young", CRC32C);
            objects.list(stale, 6, "etag-stale", old);
            objects.list(mismatch, 8, "etag-list", old);
            objects.head(mismatch, 9, "etag-head", CRC32C);

            try (ObjectInventoryScanner scanner = scanner(
                    enabledConfig(true, false), metadata, objects, scheduler)) {
                ObjectInventoryScanResult result = scanner.scan().join();

                assertThat(result).isEqualTo(new ObjectInventoryScanResult(
                        1, 5, 5, 0, 0, 1, 0, 1, 1, 1, 1, 0));
                VersionedPhysicalObjectRoot root = metadata.getRoot(
                                CLUSTER, ObjectKeyHash.from(valid))
                        .join().orElseThrow();
                assertThat(root.value().objectKey()).isEqualTo(valid.value());
                assertThat(root.value().objectKindId())
                        .isEqualTo(PhysicalObjectKind.INDEX_OBJECT.wireId());
                assertThat(root.value().createdAtMillis()).isEqualTo(old.toEpochMilli());
                assertThat(root.value().orphanNotBeforeMillis()).isEqualTo(
                        NOW.plus(Duration.ofHours(2).plus(Duration.ofMillis(5))).toEpochMilli());

                ObjectInventoryScanResult repeated = scanner.scan().join();
                assertThat(repeated.alreadyRooted()).isEqualTo(1);
                assertThat(repeated.rootsRegistered()).isZero();
                assertThat(repeated.objectsListed()).isEqualTo(5);
            }
        } finally {
            scheduler.shutdownNow();
            objects.close();
            metadata.close();
        }
    }

    @Test
    void disabledAndDryRunPassesReportWithoutCreatingMetadata() {
        for (PhysicalGcConfig config : List.of(
                enabledConfig(false, true),
                enabledConfig(true, true))) {
            FakePhysicalObjectMetadataStore metadata = new FakePhysicalObjectMetadataStore();
            InventoryObjectStore objects = new InventoryObjectStore();
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            try {
                ObjectKey key = key("dry-run.obj");
                objects.list(key, 4, "etag", NOW.minus(Duration.ofHours(3)));
                objects.head(key, 4, "etag", CRC32C);
                try (ObjectInventoryScanner scanner = scanner(
                        config, metadata, objects, scheduler)) {
                    ObjectInventoryScanResult result = scanner.scan().join();
                    assertThat(result.wouldRegister()).isEqualTo(1);
                    assertThat(result.rootsRegistered()).isZero();
                    assertThat(metadata.getRoot(CLUSTER, ObjectKeyHash.from(key)).join())
                            .isEmpty();
                }
            } finally {
                scheduler.shutdownNow();
                objects.close();
                metadata.close();
            }
        }
    }

    @Test
    void lostCreateResponseConvergesOnlyThroughTheExactDurableRoot() {
        LostCreateResponseStore metadata = new LostCreateResponseStore();
        InventoryObjectStore objects = new InventoryObjectStore();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            ObjectKey key = key("lost-response.obj");
            objects.list(key, 12, "etag", NOW.minus(Duration.ofHours(3)));
            objects.head(key, 12, "etag", CRC32C);
            try (ObjectInventoryScanner scanner = scanner(
                    enabledConfig(true, false), metadata, objects, scheduler)) {
                ObjectInventoryScanResult result = scanner.scan().join();
                assertThat(result.rootsConverged()).isEqualTo(1);
                assertThat(result.rootsRegistered()).isZero();
                assertThat(metadata.getRoot(CLUSTER, ObjectKeyHash.from(key)).join())
                        .isPresent();
            }
        } finally {
            scheduler.shutdownNow();
            objects.close();
            metadata.close();
        }
    }

    @Test
    void concurrentDifferentRootIsReportedWithoutOverwritingOrAbortingPass() {
        ConcurrentRootStore metadata = new ConcurrentRootStore();
        InventoryObjectStore objects = new InventoryObjectStore();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            ObjectKey key = key("concurrent-root.obj");
            objects.list(key, 12, "etag", NOW.minus(Duration.ofHours(3)));
            objects.head(key, 12, "etag", CRC32C);
            try (ObjectInventoryScanner scanner = scanner(
                    enabledConfig(true, false), metadata, objects, scheduler)) {
                ObjectInventoryScanResult result = scanner.scan().join();
                assertThat(result.rootConflicts()).isEqualTo(1);
                assertThat(result.rootsRegistered()).isZero();
                assertThat(metadata.getRoot(CLUSTER, ObjectKeyHash.from(key)).join())
                        .get()
                        .extracting(value -> value.value().orphanNotBeforeMillis())
                        .isEqualTo(NOW.plus(Duration.ofHours(2).plus(Duration.ofMillis(6)))
                                .toEpochMilli());
            }
        } finally {
            scheduler.shutdownNow();
            objects.close();
            metadata.close();
        }
    }

    @Test
    void rejectsDuplicateFamiliesAndPostCloseScans() {
        FakePhysicalObjectMetadataStore metadata = new FakePhysicalObjectMetadataStore();
        InventoryObjectStore objects = new InventoryObjectStore();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            ObjectInventoryFamily family = family();
            assertThatThrownBy(() -> new ObjectInventoryScanner(
                            CLUSTER,
                            enabledConfig(false, true),
                            metadata,
                            objects,
                            List.of(family, family),
                            Clock.fixed(NOW, ZoneOffset.UTC),
                            scheduler))
                    .isInstanceOf(IllegalArgumentException.class);
            ObjectInventoryScanner scanner = scanner(
                    enabledConfig(false, true), metadata, objects, scheduler);
            scanner.close();
            assertThatThrownBy(() -> scanner.scan().join())
                    .hasRootCauseInstanceOf(NereusException.class)
                    .rootCause()
                    .extracting(value -> ((NereusException) value).code())
                    .isEqualTo(ErrorCode.STORAGE_CLOSED);
        } finally {
            scheduler.shutdownNow();
            objects.close();
            metadata.close();
        }
    }

    @Test
    void opaqueContinuationDoesNotImplyCrossPageLogicalOrdering() {
        FakePhysicalObjectMetadataStore metadata = new FakePhysicalObjectMetadataStore();
        InventoryObjectStore objects = new InventoryObjectStore();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            ObjectKey lower = key("a.obj");
            ObjectKey higher = key("z.obj");
            Instant old = NOW.minus(Duration.ofHours(3));
            objects.list(lower, 1, "etag-a", old);
            objects.head(lower, 1, "etag-a", CRC32C);
            objects.list(higher, 1, "etag-z", old);
            objects.head(higher, 1, "etag-z", CRC32C);
            objects.pageInDescendingLogicalOrder();

            try (ObjectInventoryScanner scanner = scanner(
                    enabledConfig(true, false), metadata, objects, scheduler)) {
                ObjectInventoryScanResult result = scanner.scan().join();
                assertThat(result.pagesScanned()).isEqualTo(2);
                assertThat(result.objectsListed()).isEqualTo(2);
                assertThat(result.rootsRegistered()).isEqualTo(2);
            }
        } finally {
            scheduler.shutdownNow();
            objects.close();
            metadata.close();
        }
    }

    @Test
    void repeatedOpaqueContinuationFailsClosed() {
        FakePhysicalObjectMetadataStore metadata = new FakePhysicalObjectMetadataStore();
        InventoryObjectStore objects = new InventoryObjectStore();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            Instant old = NOW.minus(Duration.ofHours(3));
            ObjectKey first = key("a.obj");
            ObjectKey second = key("b.obj");
            objects.list(first, 1, "etag-a", old);
            objects.head(first, 1, "etag-a", CRC32C);
            objects.list(second, 1, "etag-b", old);
            objects.head(second, 1, "etag-b", CRC32C);
            objects.repeatOpaqueContinuation();

            try (ObjectInventoryScanner scanner = scanner(
                    enabledConfig(true, false), metadata, objects, scheduler)) {
                assertThatThrownBy(() -> scanner.scan().join())
                        .hasRootCauseInstanceOf(NereusException.class)
                        .hasRootCauseMessage(
                                "object inventory listing escaped its prefix or repeated the supplied opaque token");
            }
        } finally {
            scheduler.shutdownNow();
            objects.close();
            metadata.close();
        }
    }

    private static ObjectInventoryScanner scanner(
            PhysicalGcConfig config,
            FakePhysicalObjectMetadataStore metadata,
            InventoryObjectStore objects,
            ScheduledExecutorService scheduler) {
        return new ObjectInventoryScanner(
                CLUSTER,
                config,
                metadata,
                objects,
                List.of(family()),
                Clock.fixed(NOW, ZoneOffset.UTC),
                scheduler);
    }

    private static ObjectInventoryFamily family() {
        return new ObjectInventoryFamily() {
            @Override
            public String familyId() {
                return "test-object-v1";
            }

            @Override
            public ObjectKeyPrefix prefix() {
                return PREFIX;
            }

            @Override
            public ObjectInventoryKey parse(ObjectKey key) {
                if (!key.value().startsWith(PREFIX.value()) || !key.value().endsWith(".obj")) {
                    throw new IllegalArgumentException("malformed test object key");
                }
                String id = key.value().substring(
                        PREFIX.value().length(), key.value().length() - ".obj".length());
                return new ObjectInventoryKey(
                        key,
                        Optional.of(new ObjectId(id)),
                        PhysicalObjectKind.INDEX_OBJECT,
                        Optional.empty());
            }
        };
    }

    private static ObjectKey key(String suffix) {
        return new ObjectKey(PREFIX.value() + suffix);
    }

    private static PhysicalGcConfig enabledConfig(boolean enabled, boolean dryRun) {
        return new PhysicalGcConfig(
                enabled,
                dryRun,
                1,
                1,
                1,
                10,
                10,
                10,
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ofMillis(5),
                Duration.ofSeconds(11),
                Duration.ofHours(1),
                Duration.ofHours(2),
                Duration.ofDays(7),
                Duration.ofSeconds(2),
                Duration.ofSeconds(2));
    }

    private static final class LostCreateResponseStore extends FakePhysicalObjectMetadataStore {
        private final AtomicBoolean fail = new AtomicBoolean(true);

        @Override
        public synchronized CompletableFuture<VersionedPhysicalObjectRoot> createRoot(
                String cluster, PhysicalObjectRootRecord root) {
            CompletableFuture<VersionedPhysicalObjectRoot> created = super.createRoot(cluster, root);
            if (fail.compareAndSet(true, false)) {
                return created.thenCompose(ignored -> CompletableFuture.failedFuture(
                        new NereusException(
                                ErrorCode.METADATA_UNAVAILABLE,
                                true,
                                "lost root create response")));
            }
            return created;
        }
    }

    private static final class ConcurrentRootStore extends FakePhysicalObjectMetadataStore {
        private final AtomicBoolean race = new AtomicBoolean(true);

        @Override
        public synchronized CompletableFuture<VersionedPhysicalObjectRoot> createRoot(
                String cluster, PhysicalObjectRootRecord root) {
            if (race.compareAndSet(true, false)) {
                super.createRoot(cluster, withOrphanNotBefore(
                        root, Math.addExact(root.orphanNotBeforeMillis(), 1))).join();
            }
            return super.createRoot(cluster, root);
        }

        private static PhysicalObjectRootRecord withOrphanNotBefore(
                PhysicalObjectRootRecord root, long value) {
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
                    root.lifecycle(),
                    root.lifecycleEpoch(),
                    root.createdAtMillis(),
                    value,
                    root.gcAttemptId(),
                    root.referenceSetSha256(),
                    root.markedAtMillis(),
                    root.deleteNotBeforeMillis(),
                    root.deleteStartedAtMillis(),
                    root.deletedAtMillis(),
                    root.tombstoneFirstAbsentAtMillis(),
                    root.tombstoneProofSha256(),
                    root.stateReason(),
                    root.metadataVersion());
        }
    }

    private static final class InventoryObjectStore implements ObjectStore {
        private enum PagingMode {
            LOGICAL,
            DESCENDING_OPAQUE,
            REPEATED_OPAQUE_TOKEN
        }

        private final List<ListedObject> listed = new ArrayList<>();
        private final Map<ObjectKey, HeadObjectResult> heads = new HashMap<>();
        private PagingMode pagingMode = PagingMode.LOGICAL;
        private boolean closed;

        private void pageInDescendingLogicalOrder() {
            pagingMode = PagingMode.DESCENDING_OPAQUE;
        }

        private void repeatOpaqueContinuation() {
            pagingMode = PagingMode.REPEATED_OPAQUE_TOKEN;
        }

        private void list(
                ObjectKey key,
                long length,
                String etag,
                Instant lastModified) {
            listed.add(new ListedObject(
                    key, length, Optional.of(etag), Optional.of(lastModified)));
            listed.sort(Comparator.comparing(value -> value.key().value()));
        }

        private void head(
                ObjectKey key,
                long length,
                String etag,
                Checksum checksum) {
            heads.put(key, new HeadObjectResult(
                    key, length, checksum, Optional.of(etag), Map.of()));
        }

        @Override
        public CompletableFuture<ListObjectsResult> listObjects(
                ObjectKeyPrefix prefix,
                Optional<String> continuationToken,
                ListObjectsOptions options) {
            ensureOpen();
            if (pagingMode == PagingMode.DESCENDING_OPAQUE) {
                List<ListedObject> page = continuationToken.isEmpty()
                        ? List.of(listed.getLast())
                        : List.of(listed.getFirst());
                Optional<String> next = continuationToken.isEmpty()
                        ? Optional.of("opaque-descending-page-2")
                        : Optional.empty();
                return CompletableFuture.completedFuture(
                        new ListObjectsResult(prefix, page, next));
            }
            if (pagingMode == PagingMode.REPEATED_OPAQUE_TOKEN) {
                List<ListedObject> page = continuationToken.isEmpty()
                        ? List.of(listed.getFirst())
                        : List.of(listed.getLast());
                return CompletableFuture.completedFuture(new ListObjectsResult(
                        prefix, page, Optional.of("opaque-repeated-token")));
            }
            List<ListedObject> remaining = listed.stream()
                    .filter(value -> value.key().value().startsWith(prefix.value()))
                    .filter(value -> continuationToken.isEmpty()
                            || value.key().value().compareTo(continuationToken.orElseThrow()) > 0)
                    .toList();
            List<ListedObject> page = remaining.stream()
                    .limit(options.maxKeys())
                    .toList();
            Optional<String> next = remaining.size() > page.size()
                    ? Optional.of(page.get(page.size() - 1).key().value())
                    : Optional.empty();
            return CompletableFuture.completedFuture(
                    new ListObjectsResult(prefix, page, next));
        }

        @Override
        public CompletableFuture<HeadObjectResult> headObject(
                ObjectKey key, HeadObjectOptions options) {
            ensureOpen();
            HeadObjectResult head = heads.get(key);
            if (head == null) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_NOT_FOUND, true, "object not found"));
            }
            return CompletableFuture.completedFuture(head);
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key,
                ReplayableObjectUpload source,
                PutObjectOptions options) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(
                ObjectKey key,
                long offset,
                long length,
                RangeReadOptions options) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<DeleteObjectResult> deleteObject(
                ObjectKey key, DeleteObjectOptions options) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public void close() {
            closed = true;
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("test object store is closed");
            }
        }
    }
}
