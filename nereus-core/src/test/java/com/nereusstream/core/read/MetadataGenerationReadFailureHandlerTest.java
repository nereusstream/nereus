/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class MetadataGenerationReadFailureHandlerTest {
    private static final String CLUSTER = F4MetadataTestValues.CLUSTER;
    private static final StreamId STREAM = new StreamId(F4MetadataTestValues.STREAM);

    @Test
    void quarantinesPhysicalRootAndCommittedGenerationOnImmutableCorruption() {
        FakePhysicalObjectMetadataStore physical = new FakePhysicalObjectMetadataStore();
        AtomicReference<VersionedGenerationIndex> index = new AtomicReference<>(versionedIndex(10));
        ObjectSliceReadTarget target = target(index.get());
        var createdRoot = physical.createRoot(
                CLUSTER,
                root(target)).join();
        GenerationMetadataStore generations = generationStore(index);
        MetadataGenerationReadFailureHandler handler = new MetadataGenerationReadFailureHandler(
                CLUSTER,
                generations,
                physical,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));

        handler.handle(
                        STREAM,
                        candidate(index.get()),
                        new NereusException(
                                ErrorCode.OBJECT_CHECKSUM_MISMATCH,
                                false,
                                "bad checksum"))
                .join();

        assertThat(physical.getRoot(
                        CLUSTER,
                        new com.nereusstream.api.ObjectKeyHash(
                                createdRoot.value().objectKeyHash()))
                .join().orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.QUARANTINED);
        assertThat(index.get().value().lifecycle()).isEqualTo(GenerationLifecycle.QUARANTINED);
        assertThat(index.get().value().stateReason()).isEqualTo("read-object_checksum_mismatch");
    }

    @Test
    void transientObjectReadFailureDoesNotQuarantineHealthyMetadata() {
        FakePhysicalObjectMetadataStore physical = new FakePhysicalObjectMetadataStore();
        AtomicReference<VersionedGenerationIndex> index = new AtomicReference<>(versionedIndex(10));
        physical.createRoot(
                CLUSTER,
                root(target(index.get()))).join();
        MetadataGenerationReadFailureHandler handler = new MetadataGenerationReadFailureHandler(
                CLUSTER,
                generationStore(index),
                physical,
                Clock.systemUTC());

        handler.handle(
                        STREAM,
                        candidate(index.get()),
                        new NereusException(ErrorCode.OBJECT_READ_FAILED, true, "temporary"))
                .join();

        assertThat(index.get().value().lifecycle()).isEqualTo(GenerationLifecycle.COMMITTED);
    }

    @Test
    void quarantinesEveryDiscoveredCommittedIndexThatReferencesTheCorruptObject() {
        FakePhysicalObjectMetadataStore physical = new FakePhysicalObjectMetadataStore();
        AtomicReference<VersionedGenerationIndex> first = new AtomicReference<>(
                versionedIndex(10, 3, "/generation/index/3"));
        AtomicReference<VersionedGenerationIndex> second = new AtomicReference<>(
                versionedIndex(20, 4, "/generation/index/4"));
        ObjectSliceReadTarget target = target(first.get());
        physical.createRoot(CLUSTER, root(target)).join();
        MetadataGenerationReadFailureHandler handler = new MetadataGenerationReadFailureHandler(
                CLUSTER,
                generationStore(List.of(first, second)),
                physical,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));

        handler.handle(
                STREAM,
                candidate(first.get()),
                new NereusException(
                        ErrorCode.OBJECT_CHECKSUM_MISMATCH,
                        false,
                        "bad checksum")).join();

        assertThat(first.get().value().lifecycle()).isEqualTo(GenerationLifecycle.QUARANTINED);
        assertThat(second.get().value().lifecycle()).isEqualTo(GenerationLifecycle.QUARANTINED);
    }

    private static GenerationMetadataStore generationStore(
            AtomicReference<VersionedGenerationIndex> index) {
        return generationStore(List.of(index));
    }

    private static GenerationMetadataStore generationStore(
            List<AtomicReference<VersionedGenerationIndex>> indexes) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                MetadataGenerationReadFailureHandlerTest.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getIndex" -> {
                        com.nereusstream.metadata.oxia.GenerationIndexIdentity identity =
                                (com.nereusstream.metadata.oxia.GenerationIndexIdentity) args[1];
                        yield CompletableFuture.completedFuture(indexes.stream()
                                .map(AtomicReference::get)
                                .filter(value -> value.value().generation() == identity.generation()
                                        && value.value().offsetEnd() == identity.offsetEnd())
                                .findFirst());
                    }
                    case "scanIndex" -> CompletableFuture.completedFuture(
                            new GenerationScanPage(
                                    indexes.stream()
                                            .<VersionedGenerationCandidate>map(AtomicReference::get)
                                            .toList(),
                                    Optional.empty()));
                    case "compareAndSetIndex" -> {
                        GenerationIndexRecord replacement = (GenerationIndexRecord) args[1];
                        long expectedVersion = (long) args[2];
                        AtomicReference<VersionedGenerationIndex> reference = indexes.stream()
                                .filter(candidate -> candidate.get().value().generation()
                                        == replacement.generation())
                                .findFirst()
                                .orElseThrow();
                        VersionedGenerationIndex current = reference.get();
                        if (current.metadataVersion() != expectedVersion) {
                            yield CompletableFuture.failedFuture(new IllegalStateException("version mismatch"));
                        }
                        VersionedGenerationIndex updated = new VersionedGenerationIndex(
                                current.key(),
                                replacement.withMetadataVersion(expectedVersion + 1),
                                expectedVersion + 1,
                                new Checksum(ChecksumType.SHA256, "f".repeat(64)));
                        reference.set(updated);
                        yield CompletableFuture.completedFuture(updated);
                    }
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static VersionedGenerationIndex versionedIndex(long version) {
        return versionedIndex(version, 3, "/generation/index");
    }

    private static VersionedGenerationIndex versionedIndex(
            long version,
            long generation,
            String key) {
        GenerationIndexRecord sample = F4MetadataTestValues.generation(
                GenerationLifecycle.COMMITTED);
        GenerationIndexRecord value = new GenerationIndexRecord(
                sample.schemaVersion(),
                sample.streamId(),
                sample.readViewId(),
                sample.offsetStart(),
                sample.offsetEnd(),
                generation,
                generation == 3 ? sample.publicationId() : "q".repeat(26),
                generation == 3 ? sample.taskId() : "task-f4-second",
                sample.lifecycle(),
                sample.sourceSetSha256(),
                sample.policySha256(),
                sample.readTarget(),
                sample.targetIdentitySha256(),
                sample.materializationPolicySha256(),
                sample.payloadFormat(),
                sample.sourceRecordCount(),
                sample.outputRecordCount(),
                sample.entryCount(),
                sample.logicalBytes(),
                sample.cumulativeSizeAtStart(),
                sample.cumulativeSizeAtEnd(),
                sample.firstCommitVersion(),
                sample.lastCommitVersion(),
                sample.schemaRefs(),
                sample.projectionRef(),
                sample.createdAtMillis(),
                sample.committedAtMillis(),
                sample.stateReason(),
                sample.stateChangedAtMillis(),
                version);
        return new VersionedGenerationIndex(
                key,
                value,
                version,
                new Checksum(ChecksumType.SHA256, "e".repeat(64)));
    }

    private static GenerationReadCandidate candidate(VersionedGenerationIndex index) {
        ObjectSliceReadTarget target = target(index);
        ResolvedRange range = new ResolvedRange(
                new OffsetRange(index.value().offsetStart(), index.value().offsetEnd()),
                index.value().generation(),
                target,
                PayloadFormat.valueOf(index.value().payloadFormat()),
                index.value().outputRecordCount(),
                index.value().entryCount(),
                index.value().logicalBytes(),
                index.value().schemaRefs(),
                Optional.empty(),
                index.value().lastCommitVersion());
        return new GenerationReadCandidate(
                ReadView.COMMITTED,
                range,
                index.key(),
                index.metadataVersion(),
                index.durableValueSha256(),
                false,
                Optional.of(new PublicationId(index.value().publicationId())));
    }

    private static ObjectSliceReadTarget target(VersionedGenerationIndex index) {
        return (ObjectSliceReadTarget) ReadTargetCodecRegistry.phase15()
                .decode(index.value().readTarget());
    }

    private static PhysicalObjectRootRecord root(ObjectSliceReadTarget target) {
        return new PhysicalObjectRootRecord(
                1,
                com.nereusstream.api.ObjectKeyHash.from(target.objectKey()).value(),
                target.objectKey().value(),
                target.objectId().value(),
                2,
                target.objectLength(),
                ChecksumType.CRC32C.name(),
                F4MetadataTestValues.output().storageCrc32c(),
                F4MetadataTestValues.output().contentSha256(),
                F4MetadataTestValues.output().etag(),
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
                0);
    }
}
