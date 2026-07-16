/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.StreamId;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.VersionedGcRetirementManifest;
import com.nereusstream.metadata.oxia.VersionedGcRetirementRemoval;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.GcDomainSnapshotProofRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.retirement.GenericCommittedAppendIdentity;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroCommit;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroMarker;
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

class GenerationZeroSourceRetirementHandlerTest {
    private static final String CLUSTER = "cluster-a";
    private static final String ATTEMPT_ID = "b".repeat(52);
    private static final StreamId STREAM = new StreamId("tenant/ns/stream-a");
    private static final String COMMIT_ID = "commit-a";
    private static final ObjectKey PHYSICAL_OBJECT = new ObjectKey("objects/wal-a");
    private static final int SOURCE_VERSION = 7;
    private static final Checksum SOURCE_DIGEST = sha('e');

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void closeScheduler() {
        scheduler.shutdownNow();
    }

    @Test
    void markerDeleteUsesExactJournalKeyVersionAndDigest() {
        String key = new OxiaKeyspace(CLUSTER).committedAppendKey(STREAM, COMMIT_ID);
        VersionedGenerationZeroMarker marker = marker(key, SOURCE_VERSION, SOURCE_DIGEST);
        GcPlannedMetadataRemoval removal = removal(
                GenerationZeroMarkerRetirementHandler.REMOVAL_TYPE, key);
        AtomicReference<VersionedGenerationZeroMarker> current = new AtomicReference<>(marker);
        AtomicInteger deletes = new AtomicInteger();
        GenerationZeroMarkerRetirementHandler handler =
                new GenerationZeroMarkerRetirementHandler(
                        suppliedKey -> CompletableFuture.completedFuture(
                                Optional.ofNullable(current.get())),
                        (suppliedKey, expectedVersion, expectedDigest) -> {
                            assertThat(suppliedKey).isEqualTo(key);
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
    }

    @Test
    void markerLostDeleteResponseConvergesOnlyAfterExactKeyIsAbsent() {
        String key = new OxiaKeyspace(CLUSTER).committedAppendKey(STREAM, COMMIT_ID);
        GcPlannedMetadataRemoval removal = removal(
                GenerationZeroMarkerRetirementHandler.REMOVAL_TYPE, key);
        AtomicReference<VersionedGenerationZeroMarker> current = new AtomicReference<>(
                marker(key, SOURCE_VERSION, SOURCE_DIGEST));
        GenerationZeroMarkerRetirementHandler handler =
                new GenerationZeroMarkerRetirementHandler(
                        suppliedKey -> CompletableFuture.completedFuture(
                                Optional.ofNullable(current.get())),
                        (suppliedKey, expectedVersion, expectedDigest) -> {
                            current.set(null);
                            return CompletableFuture.failedFuture(
                                    new RuntimeException("injected marker response loss"));
                        });

        GcMetadataRetirementOutcome outcome;
        try (MaterializationDeadline deadline = deadline()) {
            outcome = handler.retire(context(removal), removal, deadline).join();
        }

        assertThat(outcome).isEqualTo(GcMetadataRetirementOutcome.ALREADY_ABSENT);
    }

    @Test
    void markerDriftFailsBeforeDelete() {
        String key = new OxiaKeyspace(CLUSTER).committedAppendKey(STREAM, COMMIT_ID);
        GcPlannedMetadataRemoval removal = removal(
                GenerationZeroMarkerRetirementHandler.REMOVAL_TYPE, key);
        AtomicInteger deletes = new AtomicInteger();
        GenerationZeroMarkerRetirementHandler handler =
                new GenerationZeroMarkerRetirementHandler(
                        suppliedKey -> CompletableFuture.completedFuture(Optional.of(
                                marker(key, SOURCE_VERSION, sha('f')))),
                        (suppliedKey, expectedVersion, expectedDigest) -> {
                            deletes.incrementAndGet();
                            return CompletableFuture.completedFuture(null);
                        });

        try (MaterializationDeadline deadline = deadline()) {
            assertThatThrownBy(() -> handler.retire(
                            context(removal), removal, deadline).join())
                    .hasRootCauseMessage(
                            "generation-zero marker no longer matches the sealed journal");
        }
        assertThat(deletes).hasValue(0);
    }

    @Test
    void commitDeleteAndRestartUseOnlyTheJournaledExactKey() {
        String key = new OxiaKeyspace(CLUSTER).streamCommitKey(STREAM, COMMIT_ID);
        GcPlannedMetadataRemoval removal = removal(
                GenerationZeroCommitRetirementHandler.REMOVAL_TYPE, key);
        AtomicReference<VersionedGenerationZeroCommit> current = new AtomicReference<>(
                commit(key, SOURCE_VERSION, SOURCE_DIGEST));
        AtomicInteger deletes = new AtomicInteger();
        GenerationZeroCommitRetirementHandler handler =
                new GenerationZeroCommitRetirementHandler(
                        suppliedKey -> CompletableFuture.completedFuture(
                                Optional.ofNullable(current.get())),
                        (suppliedKey, expectedVersion, expectedDigest) -> {
                            assertThat(suppliedKey).isEqualTo(key);
                            deletes.incrementAndGet();
                            current.set(null);
                            return CompletableFuture.completedFuture(null);
                        });

        GcMetadataRetirementOutcome first;
        GcMetadataRetirementOutcome restarted;
        try (MaterializationDeadline deadline = deadline()) {
            first = handler.retire(context(removal), removal, deadline).join();
        }
        try (MaterializationDeadline deadline = deadline()) {
            restarted = handler.retire(context(removal), removal, deadline).join();
        }

        assertThat(first).isEqualTo(GcMetadataRetirementOutcome.RETIRED);
        assertThat(restarted).isEqualTo(GcMetadataRetirementOutcome.ALREADY_ABSENT);
        assertThat(deletes).hasValue(1);
    }

    @Test
    void uncertainCommitDeleteRethrowsWhileExactSourceStillExists() {
        String key = new OxiaKeyspace(CLUSTER).streamCommitKey(STREAM, COMMIT_ID);
        GcPlannedMetadataRemoval removal = removal(
                GenerationZeroCommitRetirementHandler.REMOVAL_TYPE, key);
        VersionedGenerationZeroCommit commit = commit(
                key, SOURCE_VERSION, SOURCE_DIGEST);
        GenerationZeroCommitRetirementHandler handler =
                new GenerationZeroCommitRetirementHandler(
                        suppliedKey -> CompletableFuture.completedFuture(Optional.of(commit)),
                        (suppliedKey, expectedVersion, expectedDigest) ->
                                CompletableFuture.failedFuture(
                                        new RuntimeException("commit delete did not apply")));

        try (MaterializationDeadline deadline = deadline()) {
            assertThatThrownBy(() -> handler.retire(
                            context(removal), removal, deadline).join())
                    .hasRootCauseMessage("commit delete did not apply");
        }
    }

    private MaterializationDeadline deadline() {
        return new MaterializationDeadline(Duration.ofSeconds(5), scheduler);
    }

    private static VersionedGenerationZeroMarker marker(
            String key,
            long metadataVersion,
            Checksum durableDigest) {
        return new VersionedGenerationZeroMarker(
                key,
                STREAM,
                new GenericCommittedAppendIdentity(COMMIT_ID),
                0,
                12,
                1,
                Optional.of(sha('a')),
                metadataVersion,
                durableDigest);
    }

    private static VersionedGenerationZeroCommit commit(
            String key,
            long metadataVersion,
            Checksum durableDigest) {
        return new VersionedGenerationZeroCommit(
                key,
                STREAM,
                COMMIT_ID,
                AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1,
                new GenericCommittedAppendIdentity(COMMIT_ID),
                canonicalCommit(),
                0,
                12,
                1,
                sha256(MetadataRecordCodecFactory.encodeEnvelope(
                        canonicalCommit(), StreamCommitTargetRecord.class)),
                metadataVersion,
                durableDigest);
    }

    private static StreamCommitTargetRecord canonicalCommit() {
        return new StreamCommitTargetRecord(
                STREAM.value(),
                COMMIT_ID,
                "",
                0,
                12,
                0,
                12,
                1,
                "writer",
                "writer-run",
                1,
                "fence",
                new ReadTargetRecord(
                        "OBJECT_SLICE",
                        1,
                        "BINARY_V1",
                        new byte[] {1},
                        "SHA256",
                        sha('a').value()),
                "PULSAR_ENTRY_BATCH",
                12,
                1,
                12,
                List.of(),
                "",
                0,
                0,
                1,
                0);
    }

    private static GcPlannedMetadataRemoval removal(String type, String key) {
        return new GcPlannedMetadataRemoval(
                type, key, SOURCE_VERSION, SOURCE_DIGEST);
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

    private static Checksum sha256(byte[] value) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    java.util.HexFormat.of().formatHex(
                            java.security.MessageDigest.getInstance("SHA-256").digest(value)));
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
