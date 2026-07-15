/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.materialization.gc.AppendRecoveryReferenceDomain;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.AppendRecoveryHead;
import com.nereusstream.metadata.oxia.AppendRecoveryTailPage;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AppendRecoveryReferenceDomainTest {
    private static final Checksum EVIDENCE = sha('e');

    @Test
    void liveTailReferenceVetoesAndExactRescanDetectsHeadOrCommitDrift() {
        VersionedGenerationZeroIndex source = MaterializationPlannerTestSupport.zero(
                "/index/append-domain", 0, 2, 0, 100, 2);
        ObjectSliceReadTarget target = (ObjectSliceReadTarget) source.value().readTarget();
        AppendRecoveryTailPage live = page(target);
        AtomicReference<AppendRecoveryTailPage> pages = new AtomicReference<>(live);
        AtomicInteger rootReads = new AtomicInteger();
        AtomicInteger tailReads = new AtomicInteger();
        AppendRecoveryReferenceDomain domain = new AppendRecoveryReferenceDomain(
                MaterializationPlannerTestSupport.CLUSTER,
                l0Store(pages, tailReads),
                generationStore(rootReads),
                PhysicalGcConfig.defaults());
        GcReferenceQuery query = query(target);

        var snapshot = domain.snapshot(query).join();

        assertThat(snapshot.complete()).isTrue();
        assertThat(snapshot.veto()).isTrue();
        assertThat(snapshot.authorityCount()).isEqualTo(3);
        assertThat(snapshot.references()).singleElement().satisfies(reference -> {
            assertThat(reference.referenceType()).isEqualTo("append-recovery-commit");
            assertThat(reference.referenceId()).isEqualTo("commit-1");
            assertThat(reference.ownerKey()).isEqualTo("/commit/1");
            assertThat(reference.ownerMetadataVersion()).isEqualTo(7);
        });
        assertThat(domain.stillMatches(query, snapshot).join()).isTrue();

        pages.set(genesisPage());
        assertThat(domain.stillMatches(query, snapshot).join()).isFalse();
        assertThat(rootReads).hasValue(3);
        assertThat(tailReads).hasValue(3);
    }

    @Test
    void unrelatedLiveTailAndOwnerlessQueriesRemainConservative() {
        VersionedGenerationZeroIndex source = MaterializationPlannerTestSupport.zero(
                "/index/append-source", 0, 2, 0, 100, 2);
        VersionedGenerationZeroIndex unrelated = MaterializationPlannerTestSupport.zero(
                "/index/append-unrelated", 2, 4, 100, 100, 4);
        ObjectSliceReadTarget sourceTarget = (ObjectSliceReadTarget) source.value().readTarget();
        ObjectSliceReadTarget unrelatedTarget =
                (ObjectSliceReadTarget) unrelated.value().readTarget();
        AtomicInteger rootReads = new AtomicInteger();
        AtomicInteger tailReads = new AtomicInteger();
        AppendRecoveryReferenceDomain domain = new AppendRecoveryReferenceDomain(
                MaterializationPlannerTestSupport.CLUSTER,
                l0Store(new AtomicReference<>(page(sourceTarget)), tailReads),
                generationStore(rootReads),
                PhysicalGcConfig.defaults());

        var clear = domain.snapshot(query(unrelatedTarget)).join();
        assertThat(clear.complete()).isTrue();
        assertThat(clear.veto()).isFalse();
        assertThat(clear.references()).isEmpty();

        GcReferenceQuery ownerless = GcReferenceQuery.create(
                GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE,
                object(unrelatedTarget),
                List.of(),
                EVIDENCE);
        int rootsBefore = rootReads.get();
        int tailsBefore = tailReads.get();
        var blocked = domain.snapshot(ownerless).join();
        assertThat(blocked.complete()).isFalse();
        assertThat(blocked.veto()).isTrue();
        assertThat(rootReads).hasValue(rootsBefore);
        assertThat(tailReads).hasValue(tailsBefore);
    }

    private static AppendRecoveryTailPage page(ObjectSliceReadTarget target) {
        AppendRecoveryAnchor anchor = AppendRecoveryAnchor.genesis(
                MaterializationPlannerTestSupport.STREAM);
        AppendRecoveryHead head = new AppendRecoveryHead(
                MaterializationPlannerTestSupport.STREAM,
                "commit-1",
                2,
                100,
                1,
                11);
        StreamCommitTargetRecord record = new StreamCommitTargetRecord(
                MaterializationPlannerTestSupport.STREAM.value(),
                "commit-1",
                "",
                0,
                2,
                0,
                100,
                1,
                "writer",
                "run-hash",
                1,
                "fence-hash",
                ReadTargetCodecRegistry.phase15().encode(target),
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                2,
                2,
                100,
                MaterializationPlannerTestSupport.SCHEMAS,
                "",
                1,
                2,
                10,
                0);
        byte[] canonical = new byte[] {1, 2, 3};
        AppendRecoveryCommit commit = new AppendRecoveryCommit(
                "/commit/1",
                AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1,
                record,
                7,
                sha('a'),
                ByteBuffer.wrap(canonical),
                sha(canonical));
        return new AppendRecoveryTailPage(
                anchor, head, List.of(commit), true, Optional.empty());
    }

    private static AppendRecoveryTailPage genesisPage() {
        AppendRecoveryAnchor anchor = AppendRecoveryAnchor.genesis(
                MaterializationPlannerTestSupport.STREAM);
        return new AppendRecoveryTailPage(
                anchor,
                new AppendRecoveryHead(
                        MaterializationPlannerTestSupport.STREAM,
                        "",
                        0,
                        0,
                        0,
                        12),
                List.of(),
                true,
                Optional.empty());
    }

    private static GcReferenceQuery query(ObjectSliceReadTarget target) {
        return GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object(target),
                List.of(MaterializationPlannerTestSupport.STREAM),
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

    private static GenerationMetadataStore generationStore(AtomicInteger rootReads) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getRecoveryRoot" -> {
                        rootReads.incrementAndGet();
                        yield CompletableFuture.completedFuture(Optional.empty());
                    }
                    case "close" -> null;
                    case "toString" -> "append-reference-generation-store";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static OxiaMetadataStore l0Store(
            AtomicReference<AppendRecoveryTailPage> pages, AtomicInteger tailReads) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "readAppendRecoveryTail" -> {
                        tailReads.incrementAndGet();
                        yield CompletableFuture.completedFuture(pages.get());
                    }
                    case "close" -> null;
                    case "toString" -> "append-reference-l0-store";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Checksum sha(char character) {
        return new Checksum(
                ChecksumType.SHA256, Character.toString(character).repeat(64));
    }

    private static Checksum sha(byte[] value) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }
}
