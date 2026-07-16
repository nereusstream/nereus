/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationZeroIndexEncoding;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.VersionedGcRetirementManifest;
import com.nereusstream.metadata.oxia.VersionedGcRetirementRemoval;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GcDomainSnapshotProofRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class GenerationIndexRetirementHandlerTest {
    private static final String CLUSTER = "cluster-a";
    private static final String ATTEMPT_ID = "b".repeat(52);
    private static final StreamId STREAM = new StreamId("tenant/ns/stream-a");
    private static final ObjectKey PHYSICAL_OBJECT = new ObjectKey("objects/wal-a");
    private static final int SOURCE_VERSION = 7;
    private static final Checksum SOURCE_DIGEST = sha('e');

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void closeScheduler() {
        scheduler.shutdownNow();
    }

    @Test
    void generationZeroDeleteUsesExactJournalFactsAndCanonicalRouting() {
        String key = new F4Keyspace(CLUSTER).generationIndexKey(
                STREAM, ReadView.COMMITTED, 12, 0);
        VersionedGenerationZeroIndex zero = generationZero(key, SOURCE_VERSION, SOURCE_DIGEST);
        GcPlannedMetadataRemoval removal = new GcPlannedMetadataRemoval(
                GenerationZeroIndexRetirementHandler.REMOVAL_TYPE,
                key,
                SOURCE_VERSION,
                SOURCE_DIGEST);
        AtomicReference<VersionedGenerationCandidate> current = new AtomicReference<>(zero);
        AtomicInteger deletes = new AtomicInteger();
        GenerationZeroIndexRetirementHandler handler = new GenerationZeroIndexRetirementHandler(
                CLUSTER,
                (stream, view, suppliedKey) -> CompletableFuture.completedFuture(
                        Optional.ofNullable(current.get())),
                (stream, offsetEnd, expectedVersion, expectedDigest) -> {
                    assertThat(stream).isEqualTo(STREAM);
                    assertThat(offsetEnd).isEqualTo(12);
                    assertThat(expectedVersion).isEqualTo(SOURCE_VERSION);
                    assertThat(expectedDigest).isEqualTo(SOURCE_DIGEST);
                    deletes.incrementAndGet();
                    current.set(null);
                    return CompletableFuture.completedFuture(null);
                });

        GcMetadataRetirementOutcome outcome;
        try (MaterializationDeadline deadline = deadline()) {
            outcome = handler.retire(context(removal), removal, deadline).join();
        }

        assertThat(outcome).isEqualTo(GcMetadataRetirementOutcome.RETIRED);
        assertThat(deletes).hasValue(1);
        assertThat(current).hasValue(null);
    }

    @Test
    void generationZeroLostDeleteResponseConvergesOnlyAfterExactReloadIsAbsent() {
        String key = new F4Keyspace(CLUSTER).generationIndexKey(
                STREAM, ReadView.COMMITTED, 12, 0);
        VersionedGenerationZeroIndex zero = generationZero(key, SOURCE_VERSION, SOURCE_DIGEST);
        GcPlannedMetadataRemoval removal = new GcPlannedMetadataRemoval(
                GenerationZeroIndexRetirementHandler.REMOVAL_TYPE,
                key,
                SOURCE_VERSION,
                SOURCE_DIGEST);
        AtomicReference<VersionedGenerationCandidate> current = new AtomicReference<>(zero);
        GenerationZeroIndexRetirementHandler handler = new GenerationZeroIndexRetirementHandler(
                CLUSTER,
                (stream, view, suppliedKey) -> CompletableFuture.completedFuture(
                        Optional.ofNullable(current.get())),
                (stream, offsetEnd, expectedVersion, expectedDigest) -> {
                    current.set(null);
                    return CompletableFuture.failedFuture(
                            new RuntimeException("injected lost delete response"));
                });

        GcMetadataRetirementOutcome outcome;
        try (MaterializationDeadline deadline = deadline()) {
            outcome = handler.retire(context(removal), removal, deadline).join();
        }

        assertThat(outcome).isEqualTo(GcMetadataRetirementOutcome.ALREADY_ABSENT);
    }

    @Test
    void generationZeroDriftFailsBeforeDelete() {
        String key = new F4Keyspace(CLUSTER).generationIndexKey(
                STREAM, ReadView.COMMITTED, 12, 0);
        VersionedGenerationZeroIndex zero = generationZero(key, SOURCE_VERSION, sha('f'));
        GcPlannedMetadataRemoval removal = new GcPlannedMetadataRemoval(
                GenerationZeroIndexRetirementHandler.REMOVAL_TYPE,
                key,
                SOURCE_VERSION,
                SOURCE_DIGEST);
        AtomicInteger deletes = new AtomicInteger();
        GenerationZeroIndexRetirementHandler handler = new GenerationZeroIndexRetirementHandler(
                CLUSTER,
                (stream, view, suppliedKey) -> CompletableFuture.completedFuture(Optional.of(zero)),
                (stream, offsetEnd, expectedVersion, expectedDigest) -> {
                    deletes.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });

        try (MaterializationDeadline deadline = deadline()) {
            assertThatThrownBy(() -> handler.retire(context(removal), removal, deadline).join())
                    .hasRootCauseMessage(
                            "generation-zero index no longer matches the sealed journal");
        }
        assertThat(deletes).hasValue(0);
    }

    @Test
    void higherGenerationRetiresExactDrainingIndexAndRestartRecognizesSameAttempt() {
        String key = new F4Keyspace(CLUSTER).generationIndexKey(
                STREAM, ReadView.TOPIC_COMPACTED, 12, 3);
        VersionedGenerationIndex draining = higher(
                key, SOURCE_VERSION, SOURCE_DIGEST, GenerationLifecycle.DRAINING, "draining", 150);
        GcPlannedMetadataRemoval removal = new GcPlannedMetadataRemoval(
                HigherGenerationIndexRetirementHandler.REMOVAL_TYPE,
                key,
                SOURCE_VERSION,
                SOURCE_DIGEST);
        GcMetadataRetirementContext context = context(removal);
        AtomicReference<VersionedGenerationCandidate> current = new AtomicReference<>(draining);
        AtomicInteger casCalls = new AtomicInteger();
        HigherGenerationIndexRetirementHandler handler = new HigherGenerationIndexRetirementHandler(
                CLUSTER,
                (stream, view, suppliedKey) -> CompletableFuture.completedFuture(
                        Optional.ofNullable(current.get())),
                (replacement, expectedVersion) -> {
                    assertThat(expectedVersion).isEqualTo(SOURCE_VERSION);
                    casCalls.incrementAndGet();
                    VersionedGenerationIndex retired = hydratedReplacement(
                            key, replacement, SOURCE_VERSION + 1, sha('f'));
                    current.set(retired);
                    return CompletableFuture.completedFuture(retired);
                });

        GcMetadataRetirementOutcome first;
        GcMetadataRetirementOutcome restarted;
        try (MaterializationDeadline deadline = deadline()) {
            first = handler.retire(context, removal, deadline).join();
        }
        try (MaterializationDeadline deadline = deadline()) {
            restarted = handler.retire(context, removal, deadline).join();
        }

        assertThat(first).isEqualTo(GcMetadataRetirementOutcome.RETIRED);
        assertThat(restarted).isEqualTo(GcMetadataRetirementOutcome.ALREADY_ABSENT);
        assertThat(casCalls).hasValue(1);
        VersionedGenerationIndex retired = (VersionedGenerationIndex) current.get();
        assertThat(retired.value().lifecycle()).isEqualTo(GenerationLifecycle.RETIRED);
        assertThat(retired.value().stateChangedAtMillis()).isEqualTo(201);
        assertThat(retired.value().stateReason())
                .isEqualTo("physical-gc:"
                        + ATTEMPT_ID
                        + ":"
                        + context.journal().referenceSetSha256().value());
    }

    @Test
    void higherGenerationLostCasResponseConvergesOnExactAttemptBoundReplacement() {
        String key = new F4Keyspace(CLUSTER).generationIndexKey(
                STREAM, ReadView.COMMITTED, 12, 3);
        VersionedGenerationIndex draining = higher(
                key, SOURCE_VERSION, SOURCE_DIGEST, GenerationLifecycle.DRAINING, "draining", 150);
        GcPlannedMetadataRemoval removal = new GcPlannedMetadataRemoval(
                HigherGenerationIndexRetirementHandler.REMOVAL_TYPE,
                key,
                SOURCE_VERSION,
                SOURCE_DIGEST);
        GcMetadataRetirementContext context = context(removal);
        AtomicReference<VersionedGenerationCandidate> current = new AtomicReference<>(draining);
        HigherGenerationIndexRetirementHandler handler = new HigherGenerationIndexRetirementHandler(
                CLUSTER,
                (stream, view, suppliedKey) -> CompletableFuture.completedFuture(
                        Optional.ofNullable(current.get())),
                (replacement, expectedVersion) -> {
                    current.set(hydratedReplacement(
                            key, replacement, SOURCE_VERSION + 1, sha('f')));
                    return CompletableFuture.failedFuture(
                            new RuntimeException("injected lost CAS response"));
                });

        GcMetadataRetirementOutcome outcome;
        try (MaterializationDeadline deadline = deadline()) {
            outcome = handler.retire(context, removal, deadline).join();
        }

        assertThat(outcome).isEqualTo(GcMetadataRetirementOutcome.RETIRED);
    }

    @Test
    void higherGenerationRejectsNonDrainingOrPostIntentDrift() {
        String key = new F4Keyspace(CLUSTER).generationIndexKey(
                STREAM, ReadView.COMMITTED, 12, 3);
        GcPlannedMetadataRemoval removal = new GcPlannedMetadataRemoval(
                HigherGenerationIndexRetirementHandler.REMOVAL_TYPE,
                key,
                SOURCE_VERSION,
                SOURCE_DIGEST);
        VersionedGenerationIndex committed = higher(
                key, SOURCE_VERSION, SOURCE_DIGEST, GenerationLifecycle.COMMITTED, "", 110);
        VersionedGenerationIndex lateDraining = higher(
                key, SOURCE_VERSION, SOURCE_DIGEST, GenerationLifecycle.DRAINING, "draining", 202);
        AtomicReference<VersionedGenerationCandidate> current = new AtomicReference<>(committed);
        AtomicInteger casCalls = new AtomicInteger();
        HigherGenerationIndexRetirementHandler handler = new HigherGenerationIndexRetirementHandler(
                CLUSTER,
                (stream, view, suppliedKey) -> CompletableFuture.completedFuture(Optional.of(current.get())),
                (replacement, expectedVersion) -> {
                    casCalls.incrementAndGet();
                    return CompletableFuture.failedFuture(new AssertionError("unexpected CAS"));
                });

        try (MaterializationDeadline deadline = deadline()) {
            assertThatThrownBy(() -> handler.retire(context(removal), removal, deadline).join())
                    .hasRootCauseMessage("journaled higher-generation index is not DRAINING");
        }
        current.set(lateDraining);
        try (MaterializationDeadline deadline = deadline()) {
            assertThatThrownBy(() -> handler.retire(context(removal), removal, deadline).join())
                    .hasRootCauseMessage(
                            "higher-generation DRAINING timestamp follows delete intent");
        }
        assertThat(casCalls).hasValue(0);
    }

    private MaterializationDeadline deadline() {
        return new MaterializationDeadline(Duration.ofSeconds(5), scheduler);
    }

    private static VersionedGenerationZeroIndex generationZero(
            String key,
            long metadataVersion,
            Checksum durableDigest) {
        OffsetIndexEntry value = new OffsetIndexEntry(
                STREAM,
                new OffsetRange(0, 12),
                0,
                12,
                target(ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1"),
                PayloadFormat.PULSAR_ENTRY_BATCH,
                12,
                1,
                12,
                List.of(),
                Optional.empty(),
                1,
                false,
                metadataVersion);
        return new VersionedGenerationZeroIndex(
                key,
                GenerationZeroIndexEncoding.GENERIC_OFFSET_INDEX_TARGET_RECORD,
                value,
                metadataVersion,
                durableDigest);
    }

    private static VersionedGenerationIndex higher(
            String key,
            long metadataVersion,
            Checksum durableDigest,
            GenerationLifecycle lifecycle,
            String reason,
            long changedAt) {
        ObjectSliceReadTarget target = target(
                ObjectType.STREAM_COMPACTED_OBJECT,
                ReadView.TOPIC_COMPACTED == viewFromKey(key)
                        ? "NEREUS_TOPIC_COMPACTED_PARQUET_V1"
                        : "NEREUS_COMPACTED_PARQUET_V1");
        var encoded = ReadTargetCodecRegistry.phase15().encode(target);
        GenerationIndexRecord value = new GenerationIndexRecord(
                1,
                STREAM.value(),
                viewFromKey(key).wireId(),
                0,
                12,
                3,
                "p".repeat(26),
                "task-3",
                lifecycle,
                sha('a').value(),
                sha('b').value(),
                encoded,
                encoded.identityChecksumValue(),
                sha('b').value(),
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                12,
                viewFromKey(key) == ReadView.COMMITTED ? 12 : 6,
                1,
                12,
                0,
                12,
                1,
                1,
                List.of(),
                "",
                100,
                110,
                reason,
                changedAt,
                metadataVersion);
        return new VersionedGenerationIndex(key, value, metadataVersion, durableDigest);
    }

    private static ReadView viewFromKey(String key) {
        return new F4Keyspace(CLUSTER).parseGenerationIndexKey(key).view();
    }

    private static VersionedGenerationIndex hydratedReplacement(
            String key,
            GenerationIndexRecord replacement,
            long metadataVersion,
            Checksum durableDigest) {
        GenerationIndexRecord hydrated = replacement.withMetadataVersion(metadataVersion);
        return new VersionedGenerationIndex(key, hydrated, metadataVersion, durableDigest);
    }

    private static ObjectSliceReadTarget target(
            ObjectType objectType,
            String physicalFormat) {
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.INLINE,
                Optional.empty(),
                Optional.empty(),
                Optional.of(new byte[] {1, 2, 3}),
                0,
                0,
                new Checksum(ChecksumType.CRC32C, "01020304"));
        return new ObjectSliceReadTarget(
                1,
                new ObjectId("object-target"),
                new ObjectKey("objects/target"),
                objectType,
                physicalFormat,
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                "slice-target",
                0,
                100,
                new Checksum(ChecksumType.CRC32C, "11223344"),
                index);
    }

    private static GcMetadataRetirementContext context(
            GcPlannedMetadataRemoval removal) {
        Checksum query = sha('a');
        GcDomainSnapshotProof proof = new GcDomainSnapshotProof(
                "generation-v1", 1, query, sha('c'));
        Checksum referenceSet = GcPlanValidation.referenceSetSha256(
                query, List.of(proof), List.of(), List.of(removal));
        GcRetirementRemovalRecord removalValue = new GcRetirementRemovalRecord(
                1,
                ObjectKeyHash.from(PHYSICAL_OBJECT).value(),
                ATTEMPT_ID,
                removal.removalType(),
                removal.key(),
                removal.metadataVersion(),
                removal.durableValueSha256().value(),
                1);
        VersionedGcRetirementRemoval removalEntry = new VersionedGcRetirementRemoval(
                "/journal/removal", removalValue, 1, sha('f'));
        GcRetirementManifestRecord manifestValue = new GcRetirementManifestRecord(
                1,
                ObjectKeyHash.from(PHYSICAL_OBJECT).value(),
                ATTEMPT_ID,
                GcRetirementManifestRecord.REFERENCE_SET_PROTOCOL_VERSION,
                query.value(),
                List.of(new GcDomainSnapshotProofRecord(
                        proof.domainId(),
                        proof.protocolVersion(),
                        proof.queryIdentitySha256().value(),
                        proof.snapshotSha256().value())),
                0,
                1,
                referenceSet.value(),
                100,
                1);
        GcRetirementJournalSnapshot journal = new GcRetirementJournalSnapshot(
                new VersionedGcRetirementManifest(
                        "/journal/manifest", manifestValue, 1, sha('d')),
                List.of(),
                List.of(removalEntry));
        PhysicalObjectRootRecord rootValue = new PhysicalObjectRootRecord(
                1,
                ObjectKeyHash.from(PHYSICAL_OBJECT).value(),
                PHYSICAL_OBJECT.value(),
                "object-a",
                1,
                4,
                ChecksumType.CRC32C.name(),
                "01020304",
                "",
                "",
                PhysicalObjectLifecycle.DELETING,
                3,
                1,
                2,
                ATTEMPT_ID,
                referenceSet.value(),
                100,
                200,
                201,
                0,
                0,
                "",
                "",
                2);
        VersionedPhysicalObjectRoot root = new VersionedPhysicalObjectRoot(
                "/physical/root", rootValue, 2, sha('d'));
        return new GcMetadataRetirementContext(root, journal);
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, String.valueOf(value).repeat(64));
    }
}
