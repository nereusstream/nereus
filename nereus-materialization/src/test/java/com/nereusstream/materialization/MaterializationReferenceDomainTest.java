/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.materialization.gc.MaterializationReferenceDomain;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.TaskScanPage;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class MaterializationReferenceDomainTest {
    private static final Checksum EVIDENCE = new Checksum(
            ChecksumType.SHA256, "e".repeat(64));

    @Test
    void scansExactTaskSourcesAndDetectsAuthorityDrift() {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero(
                        "/index/task-domain-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero(
                        "/index/task-domain-4", 2, 4, 100, 100, 4));
        MaterializationTask task = MaterializationPlannerTestSupport.planner(
                        candidates, List.of(), 0, 4)
                .plan(
                        MaterializationPlannerTestSupport.STREAM,
                        new OffsetRange(0, 4),
                        MaterializationPlannerTestSupport.policy(),
                        1)
                .join()
                .get(0);
        VersionedMaterializationTask durable =
                MaterializationPlannerTestSupport.durableTask(task, 9);
        AtomicReference<List<VersionedMaterializationTask>> tasks =
                new AtomicReference<>(List.of(durable));
        AtomicInteger scans = new AtomicInteger();
        MaterializationReferenceDomain domain = new MaterializationReferenceDomain(
                MaterializationPlannerTestSupport.CLUSTER,
                store(tasks, scans),
                PhysicalGcConfig.defaults());
        GcReferenceQuery query = query(task);

        var snapshot = domain.snapshot(query).join();

        assertThat(snapshot.complete()).isTrue();
        assertThat(snapshot.veto()).isTrue();
        assertThat(snapshot.authorities()).singleElement().satisfies(authority -> {
            assertThat(authority.authorityKey()).isEqualTo(durable.key());
            assertThat(authority.metadataVersion()).isEqualTo(9);
            assertThat(authority.identitySha256()).isEqualTo(durable.durableValueSha256());
        });
        assertThat(snapshot.references()).singleElement().satisfies(reference -> {
            assertThat(reference.referenceType()).isEqualTo("materialization-source");
            assertThat(reference.referenceId()).endsWith("/source/0");
            assertThat(reference.ownerKey()).isEqualTo(durable.key());
            assertThat(reference.ownerMetadataVersion()).isEqualTo(9);
            assertThat(reference.ownerIdentitySha256()).isEqualTo(durable.durableValueSha256());
        });
        assertThat(domain.stillMatches(query, snapshot).join()).isTrue();
        tasks.set(List.of());
        assertThat(domain.stillMatches(query, snapshot).join()).isFalse();
        assertThat(scans).hasValue(3);
    }

    @Test
    void ownerlessQueryNeverInfersCompletenessFromTheTaskRegistry() {
        AtomicInteger scans = new AtomicInteger();
        MaterializationReferenceDomain domain = new MaterializationReferenceDomain(
                MaterializationPlannerTestSupport.CLUSTER,
                store(new AtomicReference<>(List.of()), scans),
                PhysicalGcConfig.defaults());
        VersionedGenerationCandidate candidate = MaterializationPlannerTestSupport.zero(
                "/index/ownerless-task", 0, 2, 0, 100, 2);
        ObjectSliceReadTarget target = (ObjectSliceReadTarget)
                ((com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex) candidate)
                        .value()
                        .readTarget();
        PhysicalObjectIdentity object = object(target);
        GcReferenceQuery ownerless = GcReferenceQuery.create(
                GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE,
                object,
                List.of(),
                EVIDENCE);

        var snapshot = domain.snapshot(ownerless).join();

        assertThat(snapshot.complete()).isFalse();
        assertThat(snapshot.veto()).isTrue();
        assertThat(scans).hasValue(0);
    }

    private static GcReferenceQuery query(MaterializationTask task) {
        ObjectSliceReadTarget target = (ObjectSliceReadTarget) task.sources().get(0).readTarget();
        return GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object(target),
                List.of(task.streamId()),
                EVIDENCE);
    }

    private static PhysicalObjectIdentity object(ObjectSliceReadTarget target) {
        return PhysicalObjectIdentity.create(
                target.objectKey(),
                Optional.of(target.objectId()),
                PhysicalObjectKind.OBJECT_WAL,
                100,
                new Checksum(ChecksumType.CRC32C, "11223344"),
                Optional.empty(),
                Optional.empty());
    }

    private static GenerationMetadataStore store(
            AtomicReference<List<VersionedMaterializationTask>> tasks,
            AtomicInteger scans) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "materialization-reference-domain-store";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    }
                    return switch (method.getName()) {
                        case "scanTasks" -> {
                            scans.incrementAndGet();
                            yield CompletableFuture.completedFuture(new TaskScanPage(
                                    tasks.get().stream()
                                            .sorted(java.util.Comparator.comparing(
                                                    VersionedMaterializationTask::key))
                                            .toList(),
                                    Optional.empty()));
                        }
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }
}
