/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.GcGlobalReferenceScopeSnapshot;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.materialization.gc.GenerationReferenceDomain;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class GenerationReferenceDomainTest {
    private static final Checksum EVIDENCE = sha('e');

    @Test
    void scansBothViewsAndRevalidatesTheExactQueryWithoutProcessLocalContext() {
        VersionedGenerationZeroIndex first = MaterializationPlannerTestSupport.zero(
                "/index/domain-2", 0, 2, 0, 100, 2);
        VersionedGenerationZeroIndex second = MaterializationPlannerTestSupport.zero(
                "/index/domain-4", 2, 4, 100, 100, 4);
        AtomicReference<List<VersionedGenerationCandidate>> candidates =
                new AtomicReference<>(List.of(first, second));
        AtomicInteger scans = new AtomicInteger();
        GenerationReferenceDomain domain = new GenerationReferenceDomain(
                MaterializationPlannerTestSupport.CLUSTER,
                store(candidates, scans),
                PhysicalGcConfig.defaults());
        GcReferenceQuery query = query(first);

        var snapshot = domain.snapshot(query).join();

        assertThat(snapshot.complete()).isTrue();
        assertThat(snapshot.veto()).isFalse();
        assertThat(snapshot.authorityCount()).isEqualTo(2);
        assertThat(snapshot.references()).singleElement().satisfies(reference -> {
            assertThat(reference.referenceType()).isEqualTo("generation-zero-index");
            assertThat(reference.ownerKey()).isEqualTo(first.key());
            assertThat(reference.ownerMetadataVersion()).isEqualTo(first.metadataVersion());
            assertThat(reference.ownerIdentitySha256()).isEqualTo(first.durableValueSha256());
        });
        assertThat(scans).hasValue(2);
        assertThat(domain.stillMatches(query, snapshot).join()).isTrue();
        candidates.set(List.of(second));
        assertThat(domain.stillMatches(query, snapshot).join()).isFalse();
    }

    @Test
    void multiStreamWalObjectRemainsReferencedWhileAnyStreamSliceIsLive() {
        VersionedGenerationZeroIndex first = MaterializationPlannerTestSupport.zero(
                "/index/shared-object-first", 0, 2, 0, 100, 2);
        StreamId secondStream = new StreamId("stream-planner-second");
        OffsetIndexEntry firstValue = first.value();
        OffsetIndexEntry secondValue = new OffsetIndexEntry(
                secondStream,
                firstValue.range(),
                firstValue.generation(),
                firstValue.cumulativeSize(),
                firstValue.readTarget(),
                firstValue.payloadFormat(),
                firstValue.recordCount(),
                firstValue.entryCount(),
                firstValue.logicalBytes(),
                firstValue.schemaRefs(),
                firstValue.projectionRef(),
                firstValue.commitVersion(),
                firstValue.tombstoned(),
                firstValue.metadataVersion());
        VersionedGenerationZeroIndex second = new VersionedGenerationZeroIndex(
                "/index/shared-object-second",
                first.encoding(),
                secondValue,
                first.metadataVersion(),
                sha('f'));
        AtomicReference<List<VersionedGenerationCandidate>> candidates =
                new AtomicReference<>(List.of(first, second));
        GenerationReferenceDomain domain = new GenerationReferenceDomain(
                MaterializationPlannerTestSupport.CLUSTER,
                store(candidates, new AtomicInteger()),
                PhysicalGcConfig.defaults());
        GcReferenceQuery query = GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object(first),
                List.of(MaterializationPlannerTestSupport.STREAM, secondStream),
                EVIDENCE);

        var bothLive = domain.snapshot(query).join();
        assertThat(bothLive.complete()).isTrue();
        assertThat(bothLive.references())
                .extracting(value -> value.ownerKey())
                .containsExactlyInAnyOrder(first.key(), second.key());

        candidates.set(List.of(second));
        var oneSliceStillLive = domain.snapshot(query).join();
        assertThat(oneSliceStillLive.complete()).isTrue();
        assertThat(oneSliceStillLive.references())
                .extracting(value -> value.ownerKey())
                .containsExactly(second.key());

        candidates.set(List.of());
        assertThat(domain.snapshot(query).join().references()).isEmpty();
    }

    @Test
    void configuredLimitAndOwnerlessEnumerationFailClosed() {
        VersionedGenerationZeroIndex first = MaterializationPlannerTestSupport.zero(
                "/index/limit-2", 0, 2, 0, 100, 2);
        VersionedGenerationZeroIndex second = MaterializationPlannerTestSupport.zero(
                "/index/limit-4", 2, 4, 100, 100, 4);
        AtomicReference<List<VersionedGenerationCandidate>> candidates =
                new AtomicReference<>(List.of(first, second));
        AtomicInteger scans = new AtomicInteger();
        GenerationReferenceDomain domain = new GenerationReferenceDomain(
                MaterializationPlannerTestSupport.CLUSTER,
                store(candidates, scans),
                withLimits(1, 1));

        var limited = domain.snapshot(query(first)).join();

        assertThat(limited.complete()).isFalse();
        assertThat(limited.veto()).isTrue();
        assertThat(limited.authorityCount()).isEqualTo(2);
        assertThat(limited.authorities()).hasSize(1);

        GcReferenceQuery ownerless = GcReferenceQuery.create(
                GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE,
                object(first),
                List.of(),
                EVIDENCE);
        int beforeOwnerless = scans.get();
        var blocked = domain.snapshot(ownerless).join();
        assertThat(blocked.complete()).isFalse();
        assertThat(blocked.veto()).isTrue();
        assertThat(scans).hasValue(beforeOwnerless);
    }

    @Test
    void higherGenerationMustBeDrainingBeforeItsReferenceCanEnterAPlan() {
        VersionedGenerationIndex committed = MaterializationPlannerTestSupport.higher(
                "/index/higher-domain",
                0,
                2,
                3,
                0,
                100,
                2,
                MaterializationPlannerTestSupport.policy().digestSha256(),
                MaterializationPolicy.COMMITTED_FORMAT);
        AtomicReference<List<VersionedGenerationCandidate>> candidates =
                new AtomicReference<>(List.of(committed));
        GenerationReferenceDomain domain = new GenerationReferenceDomain(
                MaterializationPlannerTestSupport.CLUSTER,
                store(candidates, new AtomicInteger()),
                PhysicalGcConfig.defaults());
        GcReferenceQuery query = query(committed);

        var visible = domain.snapshot(query).join();
        assertThat(visible.references()).hasSize(1);
        assertThat(visible.veto()).isTrue();

        candidates.set(List.of(draining(committed)));
        var draining = domain.snapshot(query).join();
        assertThat(draining.references()).hasSize(1);
        assertThat(draining.veto()).isFalse();
    }

    @Test
    void ownerlessGlobalScopeScansEveryAuthoritativeStreamAndRevalidatesScope() {
        VersionedGenerationZeroIndex first = MaterializationPlannerTestSupport.zero(
                "/index/global-domain", 0, 2, 0, 100, 2);
        AtomicReference<List<VersionedGenerationCandidate>> candidates =
                new AtomicReference<>(List.of(first));
        AtomicInteger scans = new AtomicInteger();
        AtomicReference<GcGlobalReferenceScopeSnapshot> scope =
                new AtomicReference<>(GcGlobalScopeTestSupport.snapshot(
                        List.of(MaterializationPlannerTestSupport.STREAM),
                        1,
                        GcGlobalScopeTestSupport.sha('b')));
        GenerationReferenceDomain domain = new GenerationReferenceDomain(
                MaterializationPlannerTestSupport.CLUSTER,
                store(candidates, scans),
                PhysicalGcConfig.defaults(),
                () -> CompletableFuture.completedFuture(scope.get()));
        GcReferenceQuery ownerless = GcReferenceQuery.create(
                GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE,
                object(first),
                List.of(),
                EVIDENCE);

        var snapshot = domain.snapshot(ownerless).join();

        assertThat(snapshot.complete()).isTrue();
        assertThat(snapshot.veto()).isFalse();
        assertThat(snapshot.references()).singleElement()
                .satisfies(reference -> assertThat(reference.referenceType())
                        .isEqualTo("generation-zero-index"));
        assertThat(snapshot.authorities())
                .extracting(value -> value.authorityKey())
                .contains("/global/reference-scope", first.key());
        assertThat(scans).hasValue(2);
        assertThat(domain.stillMatches(ownerless, snapshot).join()).isTrue();

        scope.set(GcGlobalScopeTestSupport.snapshot(
                List.of(MaterializationPlannerTestSupport.STREAM),
                2,
                GcGlobalScopeTestSupport.sha('c')));
        assertThat(domain.stillMatches(ownerless, snapshot).join()).isFalse();
    }

    private static GcReferenceQuery query(VersionedGenerationZeroIndex candidate) {
        return query((VersionedGenerationCandidate) candidate);
    }

    private static GcReferenceQuery query(VersionedGenerationCandidate candidate) {
        return GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object(candidate),
                List.of(MaterializationPlannerTestSupport.STREAM),
                EVIDENCE);
    }

    private static PhysicalObjectIdentity object(VersionedGenerationZeroIndex candidate) {
        return object((VersionedGenerationCandidate) candidate);
    }

    private static PhysicalObjectIdentity object(VersionedGenerationCandidate candidate) {
        ObjectSliceReadTarget target = candidate instanceof VersionedGenerationZeroIndex zero
                ? (ObjectSliceReadTarget) zero.value().readTarget()
                : (ObjectSliceReadTarget) ReadTargetCodecRegistry.phase15()
                        .decode(((VersionedGenerationIndex) candidate).value().readTarget());
        return PhysicalObjectIdentity.create(
                target.objectKey(),
                Optional.of(target.objectId()),
                candidate instanceof VersionedGenerationZeroIndex
                        ? PhysicalObjectKind.OBJECT_WAL
                        : PhysicalObjectKind.COMMITTED_COMPACTED,
                100,
                new Checksum(ChecksumType.CRC32C, "11223344"),
                Optional.empty(),
                Optional.empty());
    }

    private static VersionedGenerationIndex draining(VersionedGenerationIndex source) {
        GenerationIndexRecord value = source.value();
        long version = Math.addExact(source.metadataVersion(), 1);
        GenerationIndexRecord draining = new GenerationIndexRecord(
                value.schemaVersion(),
                value.streamId(),
                value.readViewId(),
                value.offsetStart(),
                value.offsetEnd(),
                value.generation(),
                value.publicationId(),
                value.taskId(),
                GenerationLifecycle.DRAINING,
                value.sourceSetSha256(),
                value.policySha256(),
                value.readTarget(),
                value.targetIdentitySha256(),
                value.materializationPolicySha256(),
                value.payloadFormat(),
                value.sourceRecordCount(),
                value.outputRecordCount(),
                value.entryCount(),
                value.logicalBytes(),
                value.cumulativeSizeAtStart(),
                value.cumulativeSizeAtEnd(),
                value.firstCommitVersion(),
                value.lastCommitVersion(),
                value.schemaRefs(),
                value.projectionRef(),
                value.createdAtMillis(),
                value.committedAtMillis(),
                "gc-drain",
                Math.addExact(value.stateChangedAtMillis(), 1),
                version);
        return new VersionedGenerationIndex(
                source.key(), draining, version, sha('d'));
    }

    private static GenerationMetadataStore store(
            AtomicReference<List<VersionedGenerationCandidate>> candidates,
            AtomicInteger scans) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "generation-reference-domain-store";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    }
                    return switch (method.getName()) {
                        case "scanIndex" -> {
                            scans.incrementAndGet();
                            StreamId stream = (StreamId) args[1];
                            ReadView view = (ReadView) args[2];
                            List<VersionedGenerationCandidate> values = candidates.get().stream()
                                    .filter(candidate -> stream(candidate).equals(stream))
                                    .filter(candidate -> view(candidate) == view)
                                    .sorted(java.util.Comparator.comparing(
                                            VersionedGenerationCandidate::key))
                                    .toList();
                            yield CompletableFuture.completedFuture(
                                    new GenerationScanPage(values, Optional.empty()));
                        }
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static ReadView view(VersionedGenerationCandidate candidate) {
        return candidate instanceof VersionedGenerationZeroIndex
                ? ReadView.COMMITTED
                : ReadView.fromWireId(((VersionedGenerationIndex) candidate).value().readViewId());
    }

    private static StreamId stream(VersionedGenerationCandidate candidate) {
        return candidate instanceof VersionedGenerationZeroIndex zero
                ? zero.value().streamId()
                : new StreamId(((VersionedGenerationIndex) candidate).value().streamId());
    }

    private static PhysicalGcConfig withLimits(int authorities, int references) {
        PhysicalGcConfig defaults = PhysicalGcConfig.defaults();
        return new PhysicalGcConfig(
                defaults.enabled(),
                defaults.dryRun(),
                defaults.metadataScanPageSize(),
                defaults.objectListPageSize(),
                defaults.maxConcurrentDeletes(),
                defaults.maxStreamsPerCandidate(),
                authorities,
                references,
                defaults.scanInterval(),
                defaults.readerLeaseDuration(),
                defaults.readerLeaseRenewInterval(),
                defaults.maximumClockSkew(),
                defaults.drainGrace(),
                defaults.pendingProtectionDuration(),
                defaults.orphanGrace(),
                defaults.tombstoneAuditGrace(),
                defaults.operationTimeout(),
                defaults.closeTimeout());
    }

    private static Checksum sha(char character) {
        return new Checksum(
                ChecksumType.SHA256, Character.toString(character).repeat(64));
    }
}
