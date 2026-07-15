/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.GenerationZeroIndexEncoding;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.StreamRegistrationScanPage;
import com.nereusstream.metadata.oxia.TaskScanPage;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

final class MaterializationPlannerTestSupport {
    static final String CLUSTER = "cluster-planner";
    static final StreamId STREAM = new StreamId("stream-planner");
    static final ProjectionRef PROJECTION = new ProjectionRef(
            ProjectionType.VIRTUAL_LEDGER, "projection-planner");
    static final List<SchemaRef> SCHEMAS = List.of(new SchemaRef("pulsar", "bytes", 1));

    private MaterializationPlannerTestSupport() {
    }

    static MaterializationPolicy policy() {
        return MaterializationPolicyFactory.losslessCommitted(
                2, 16, 1_000, 1_000_000, 128, "ZSTD");
    }

    static DefaultMaterializationPlanner planner(
            List<VersionedGenerationCandidate> candidates,
            List<VersionedMaterializationTask> tasks,
            long trimOffset,
            long committedEndOffset) {
        GenerationMetadataStore store = generationStore(candidates, tasks, null);
        return new DefaultMaterializationPlanner(
                CLUSTER,
                l0Store(snapshot(trimOffset, committedEndOffset)),
                store,
                2);
    }

    static GenerationMetadataStore generationStore(
            List<VersionedGenerationCandidate> candidates,
            List<VersionedMaterializationTask> tasks,
            GenerationMetadataStore delegate) {
        List<VersionedGenerationCandidate> orderedCandidates = candidates.stream()
                .sorted(Comparator.comparing(VersionedGenerationCandidate::key))
                .toList();
        List<VersionedMaterializationTask> orderedTasks = tasks.stream()
                .sorted(Comparator.comparing(VersionedMaterializationTask::key))
                .toList();
        VersionedMaterializationStreamRegistration registration = registration();
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "planner-generation-store";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    }
                    return switch (method.getName()) {
                        case "getStreamRegistration" -> CompletableFuture.completedFuture(Optional.of(registration));
                        case "scanStreamRegistrations" -> CompletableFuture.completedFuture(
                                new StreamRegistrationScanPage(
                                        ((int) args[1]) == new F4Keyspace(CLUSTER)
                                                        .materializationRegistryShard(STREAM)
                                                ? List.of(registration)
                                                : List.of(),
                                        Optional.empty()));
                        case "scanIndex" -> CompletableFuture.completedFuture(
                                new GenerationScanPage(orderedCandidates, Optional.empty()));
                        case "scanTasks" -> delegate == null
                                ? CompletableFuture.completedFuture(
                                        new TaskScanPage(orderedTasks, Optional.empty()))
                                : invokeDelegate(delegate, method, args);
                        case "getCandidate" -> CompletableFuture.completedFuture(orderedCandidates.stream()
                                .filter(candidate -> candidateMatches(candidate, args))
                                .findFirst());
                        case "close" -> null;
                        default -> invokeDelegate(delegate, method, args);
                    };
                });
    }

    static VersionedGenerationZeroIndex zero(
            String key,
            long start,
            long end,
            long cumulativeStart,
            long logicalBytes,
            long commitVersion) {
        long metadataVersion = 10 + commitVersion;
        ObjectSliceReadTarget target = target(
                "l0-" + start + "-" + end,
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1");
        OffsetIndexEntry value = new OffsetIndexEntry(
                STREAM,
                new OffsetRange(start, end),
                0,
                cumulativeStart + logicalBytes,
                target,
                PayloadFormat.PULSAR_ENTRY_BATCH,
                Math.toIntExact(end - start),
                Math.toIntExact(end - start),
                logicalBytes,
                SCHEMAS,
                Optional.empty(),
                commitVersion,
                false,
                metadataVersion);
        return new VersionedGenerationZeroIndex(
                key,
                GenerationZeroIndexEncoding.GENERIC_OFFSET_INDEX_TARGET_RECORD,
                value,
                metadataVersion,
                sha(hexCharacter(key)));
    }

    static VersionedGenerationIndex higher(
            String key,
            long start,
            long end,
            long generation,
            long cumulativeStart,
            long logicalBytes,
            long commitVersion,
            Checksum policyDigest,
            String physicalFormat) {
        long metadataVersion = 100 + generation;
        ObjectSliceReadTarget target = target(
                "g" + generation + "-" + start + "-" + end,
                ObjectType.STREAM_COMPACTED_OBJECT,
                physicalFormat);
        var encodedTarget = ReadTargetCodecRegistry.phase15().encode(target);
        GenerationIndexRecord record = new GenerationIndexRecord(
                1,
                STREAM.value(),
                ReadView.COMMITTED.wireId(),
                start,
                end,
                generation,
                "p".repeat(26),
                "source-task-" + generation + "-" + start + "-" + end,
                GenerationLifecycle.COMMITTED,
                sha('a').value(),
                policyDigest.value(),
                encodedTarget,
                encodedTarget.identityChecksumValue(),
                policyDigest.value(),
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                Math.toIntExact(end - start),
                Math.toIntExact(end - start),
                Math.toIntExact(end - start),
                logicalBytes,
                cumulativeStart,
                cumulativeStart + logicalBytes,
                commitVersion,
                commitVersion,
                SCHEMAS,
                MaterializationRecordMapper.projectionIdentity(Optional.of(PROJECTION)),
                100,
                110,
                "",
                110,
                metadataVersion);
        return new VersionedGenerationIndex(key, record, metadataVersion, sha(hexCharacter(key)));
    }

    static VersionedMaterializationTask durableTask(MaterializationTask task, long metadataVersion) {
        var record = MaterializationRecordMapper.plannedTask(task, 500).withMetadataVersion(metadataVersion);
        return new VersionedMaterializationTask(
                "/task/" + task.taskId(), record, metadataVersion, sha('f'));
    }

    static Checksum sha(char character) {
        return new Checksum(ChecksumType.SHA256, Character.toString(character).repeat(64));
    }

    private static Object invokeDelegate(
            GenerationMetadataStore delegate,
            java.lang.reflect.Method method,
            Object[] args) throws Throwable {
        if (delegate == null) {
            throw new UnsupportedOperationException(method.getName());
        }
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    private static boolean candidateMatches(VersionedGenerationCandidate candidate, Object[] args) {
        ReadView view = (ReadView) args[2];
        long offsetEnd = (long) args[3];
        long generation = (long) args[4];
        if (view != ReadView.COMMITTED) {
            return false;
        }
        if (candidate instanceof VersionedGenerationZeroIndex zero) {
            return zero.value().offsetEnd() == offsetEnd && generation == 0;
        }
        VersionedGenerationIndex higher = (VersionedGenerationIndex) candidate;
        return higher.value().offsetEnd() == offsetEnd && higher.value().generation() == generation;
    }

    static VersionedMaterializationStreamRegistration registration() {
        long metadataVersion = 7;
        MaterializationStreamRegistrationRecord record = new MaterializationStreamRegistrationRecord(
                1,
                STREAM.value(),
                MaterializationRecordMapper.projectionIdentity(Optional.of(PROJECTION)),
                sha('e').value(),
                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                100,
                4,
                100,
                metadataVersion);
        return new VersionedMaterializationStreamRegistration(
                new F4Keyspace(CLUSTER).materializationRegistryKey(STREAM),
                record,
                metadataVersion,
                sha('d'));
    }

    static StreamMetadataSnapshot snapshot(long trimOffset, long committedEndOffset) {
        long metadataVersion = 5;
        long commitVersion = committedEndOffset == 0 ? 0 : committedEndOffset;
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM.value(),
                        "persistent://tenant/ns/topic",
                        "stream-name-hash",
                        StreamState.ACTIVE.name(),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        Map.of(),
                        1,
                        1,
                        metadataVersion),
                new CommittedEndOffsetRecord(
                        STREAM.value(),
                        committedEndOffset,
                        committedEndOffset * 50,
                        commitVersion,
                        metadataVersion),
                new TrimRecord(STREAM.value(), trimOffset, "", 1, metadataVersion));
    }

    static OxiaMetadataStore l0Store(StreamMetadataSnapshot snapshot) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getStreamSnapshot" -> CompletableFuture.completedFuture(snapshot);
                    case "close" -> null;
                    case "toString" -> "planner-l0-store";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ObjectSliceReadTarget target(
            String id,
            ObjectType objectType,
            String physicalFormat) {
        byte[] indexBytes = new byte[] {1, 2, 3};
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.INLINE,
                Optional.empty(),
                Optional.empty(),
                Optional.of(indexBytes),
                0,
                0,
                new Checksum(ChecksumType.CRC32C, "01020304"));
        return new ObjectSliceReadTarget(
                1,
                new ObjectId("object-" + id),
                new ObjectKey("objects/planner/" + id),
                objectType,
                physicalFormat,
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                "slice-" + id,
                0,
                100,
                new Checksum(ChecksumType.CRC32C, "11223344"),
                index);
    }

    private static char hexCharacter(String value) {
        int hash = Math.floorMod(value.hashCode(), 16);
        return "0123456789abcdef".charAt(hash);
    }
}
