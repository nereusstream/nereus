/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationActivationSubject;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OwnerlessObjectGcExecutorTest {
    private static final String CLUSTER = "cluster-a";
    private static final Checksum SHA =
            new Checksum(ChecksumType.SHA256, "a".repeat(64));

    @TempDir
    Path temporary;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void closeScheduler() {
        scheduler.shutdownNow();
    }

    @Test
    void restartReconstructsMarkedOwnerlessPlanAndDeletesExactBytes() {
        MutableClock clock = new MutableClock(1_000);
        FakePhysicalObjectMetadataStore metadata = new FakePhysicalObjectMetadataStore();
        ClearDomain domain = new ClearDomain();
        try (LocalFileObjectStore objects = new LocalFileObjectStore(temporary.resolve("objects"))) {
            VersionedPhysicalObjectRoot active = root(metadata, put(objects));
            OwnerlessObjectGcExecutor executor = executor(
                    metadata, objects, domain, clock);

            OwnerlessObjectGcExecutor.ExecutionResult first =
                    executor.executeActive(active).join();

            assertThat(first.mark().orElseThrow().status())
                    .isEqualTo(PhysicalGcMarkStatus.MARKED);
            assertThat(first.advance().orElseThrow().status())
                    .isEqualTo(PhysicalGcAdvanceStatus.WAITING_FOR_GRACE);
            VersionedPhysicalObjectRoot marked = metadata.getRoot(
                            CLUSTER, new ObjectKeyHash(active.value().objectKeyHash()))
                    .join()
                    .orElseThrow();
            assertThat(marked.value().lifecycle()).isEqualTo(PhysicalObjectLifecycle.MARKED);

            clock.setMillis(12_000);
            OwnerlessObjectGcExecutor.ExecutionResult recovered = executor(
                            metadata, objects, domain, clock)
                    .recoverMarked(marked)
                    .join();

            assertThat(recovered.advance().orElseThrow().status())
                    .isEqualTo(PhysicalGcAdvanceStatus.DELETE_INTENT);
            assertThat(recovered.deletion().orElseThrow().status())
                    .isEqualTo(PhysicalGcDeletionStatus.DELETED);
            assertThat(metadata.getRoot(
                                    CLUSTER,
                                    new ObjectKeyHash(active.value().objectKeyHash()))
                            .join()
                            .orElseThrow()
                            .value()
                            .lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.DELETED);
        }
    }

    @Test
    void newAuthoritativeVetoUnmarksInsteadOfEnteringDeleteIntent() {
        MutableClock clock = new MutableClock(1_000);
        FakePhysicalObjectMetadataStore metadata = new FakePhysicalObjectMetadataStore();
        ClearDomain domain = new ClearDomain();
        try (LocalFileObjectStore objects = new LocalFileObjectStore(temporary.resolve("veto"))) {
            VersionedPhysicalObjectRoot active = root(metadata, put(objects));
            OwnerlessObjectGcExecutor executor = executor(
                    metadata, objects, domain, clock);
            executor.executeActive(active).join();
            VersionedPhysicalObjectRoot marked = metadata.getRoot(
                            CLUSTER, new ObjectKeyHash(active.value().objectKeyHash()))
                    .join()
                    .orElseThrow();
            domain.veto = true;
            clock.setMillis(12_000);

            OwnerlessObjectGcExecutor.ExecutionResult result =
                    executor.recoverMarked(marked).join();

            assertThat(result.advance().orElseThrow().status())
                    .isEqualTo(PhysicalGcAdvanceStatus.PLAN_DRIFT_UNMARKED);
            assertThat(metadata.getRoot(
                                    CLUSTER,
                                    new ObjectKeyHash(active.value().objectKeyHash()))
                            .join()
                            .orElseThrow()
                            .value()
                            .lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
        }
    }

    @Test
    void anyDurableProtectionSkipsTheExpensiveOwnerlessGlobalProof() {
        MutableClock clock = new MutableClock(1_000);
        FakePhysicalObjectMetadataStore metadata = new FakePhysicalObjectMetadataStore();
        ClearDomain domain = new ClearDomain();
        try (LocalFileObjectStore objects = new LocalFileObjectStore(temporary.resolve("protected"))) {
            VersionedPhysicalObjectRoot active = root(metadata, put(objects));
            metadata.createProtection(CLUSTER, new ObjectProtectionRecord(
                            1,
                            active.value().objectKeyHash(),
                            ObjectProtectionType.VISIBLE_GENERATION.wireId(),
                            "generation-one",
                            "/owner/generation-one",
                            1,
                            SHA.value(),
                            active.value().lifecycleEpoch(),
                            1_000,
                            0,
                            0))
                    .join();

            OwnerlessObjectGcExecutor.ExecutionResult result = executor(
                            metadata, objects, domain, clock)
                    .executeActive(active)
                    .join();

            assertThat(result.mark()).isEmpty();
            assertThat(result.advance()).isEmpty();
            assertThat(domain.snapshotCalls).hasValue(0);
            assertThat(metadata.getRoot(
                                    CLUSTER,
                                    new ObjectKeyHash(active.value().objectKeyHash()))
                            .join()
                            .orElseThrow()
                            .value()
                            .lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
        }
    }

    private OwnerlessObjectGcExecutor executor(
            FakePhysicalObjectMetadataStore metadata,
            LocalFileObjectStore objects,
            ClearDomain domain,
            Clock clock) {
        PhysicalGcConfig config = config();
        SourceRetirementMetadataStore sources = emptySourceStore();
        ObjectProtectionManager protections = new DefaultObjectProtectionManager(
                CLUSTER,
                metadata,
                config.pendingProtectionDuration(),
                config.maximumClockSkew(),
                config.orphanGrace(),
                clock);
        AbandonedAppendIntentPlanBuilder abandonedAppendIntents =
                new AbandonedAppendIntentPlanBuilder(
                        CLUSTER,
                        metadata,
                        sources,
                        protections,
                        config,
                        scheduler);
        GcReferenceDomainRegistry domains = new GcReferenceDomainRegistry(
                config, scheduler, List.of(domain));
        DefaultGcRetirementJournal journal = new DefaultGcRetirementJournal(
                CLUSTER, metadata, config);
        PhysicalObjectGarbageCollector collector = new PhysicalObjectGarbageCollector(
                CLUSTER,
                config,
                metadata,
                domains,
                activationGuard(),
                abandonedAppendIntents,
                journal,
                new SecureGcIdGenerator(),
                clock,
                scheduler);
        SourceRetirementCoordinator retirement = new SourceRetirementCoordinator(
                CLUSTER,
                config,
                metadata,
                journal,
                new GcMetadataRetirementRegistry(List.of()),
                objects,
                clock,
                scheduler);
        return new OwnerlessObjectGcExecutor(
                CLUSTER,
                config,
                abandonedAppendIntents,
                domains,
                collector,
                retirement,
                new SecureGcIdGenerator(),
                clock);
    }

    private static SourceRetirementMetadataStore emptySourceStore() {
        return (SourceRetirementMetadataStore) Proxy.newProxyInstance(
                SourceRetirementMetadataStore.class.getClassLoader(),
                new Class<?>[] {SourceRetirementMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "empty-source-retirement-store";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    }
                    return switch (method.getName()) {
                        case "getCommitNodeByKey", "getCommittedMarkerByKey", "getCommittedMarker" ->
                                CompletableFuture.completedFuture(Optional.empty());
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static GenerationProtocolActivationGuard activationGuard() {
        return new GenerationProtocolActivationGuard() {
            @Override
            public CompletableFuture<GenerationActivationProof> requireReady(
                    GenerationOperation operation,
                    GenerationActivationSubject subject,
                    boolean activateLiveProjectionIfAbsent) {
                return CompletableFuture.completedFuture(
                        GenerationActivationProof.create(
                                operation,
                                subject,
                                0,
                                1,
                                1,
                                SHA,
                                false,
                                true,
                                1_000));
            }

            @Override
            public CompletableFuture<Void> revalidate(
                    GenerationActivationProof proof) {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private static PutObjectResult put(LocalFileObjectStore objects) {
        byte[] bytes = "ownerless-object".getBytes(StandardCharsets.UTF_8);
        return objects.putObject(
                        new ObjectKey("objects/ownerless-one"),
                        ByteBuffer.wrap(bytes),
                        new PutObjectOptions(
                                "application/octet-stream",
                                Crc32cChecksums.checksum(bytes),
                                true,
                                Map.of(),
                                Duration.ofSeconds(1)))
                .join();
    }

    private static VersionedPhysicalObjectRoot root(
            FakePhysicalObjectMetadataStore metadata, PutObjectResult object) {
        return metadata.createRoot(CLUSTER, new PhysicalObjectRootRecord(
                        1,
                        ObjectKeyHash.from(object.key()).value(),
                        object.key().value(),
                        "",
                        PhysicalObjectKind.COMMITTED_COMPACTED.wireId(),
                        object.objectLength(),
                        object.checksum().type().name(),
                        object.checksum().value(),
                        "",
                        object.etag(),
                        PhysicalObjectLifecycle.ACTIVE,
                        1,
                        100,
                        100,
                        "",
                        "",
                        0,
                        0,
                        0,
                        0,
                        0,
                        "",
                        "",
                        0))
                .join();
    }

    private static PhysicalGcConfig config() {
        return new PhysicalGcConfig(
                true,
                false,
                10,
                10,
                1,
                4_096,
                100,
                100,
                Duration.ofMinutes(1),
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ZERO,
                Duration.ofSeconds(10),
                Duration.ofMinutes(1),
                Duration.ofHours(25),
                Duration.ofDays(7),
                Duration.ofSeconds(1),
                Duration.ofSeconds(2));
    }

    private static final class ClearDomain implements GcReferenceDomain {
        private volatile boolean veto;
        private final AtomicInteger snapshotCalls = new AtomicInteger();

        @Override
        public String domainId() {
            return PhysicalObjectGarbageCollector.PROJECTION_REFERENCE_DOMAIN;
        }

        @Override
        public int protocolVersion() {
            return PhysicalObjectGarbageCollector.PROJECTION_REFERENCE_DOMAIN_VERSION;
        }

        @Override
        public CompletableFuture<GcReferenceSnapshot> snapshot(
                GcReferenceQuery query) {
            snapshotCalls.incrementAndGet();
            return CompletableFuture.completedFuture(GcReferenceSnapshot.create(
                    domainId(),
                    protocolVersion(),
                    query.queryIdentitySha256(),
                    true,
                    veto,
                    1,
                    0,
                    List.of(new GcAuthorityToken("/projection", 1, SHA)),
                    List.of()));
        }

        @Override
        public CompletableFuture<Boolean> stillMatches(
                GcReferenceQuery query, GcReferenceSnapshot snapshot) {
            return CompletableFuture.completedFuture(!veto);
        }
    }

    private static final class MutableClock extends Clock {
        private volatile long millis;

        private MutableClock(long millis) {
            this.millis = millis;
        }

        private void setMillis(long value) {
            millis = value;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long millis() {
            return millis;
        }
    }
}
