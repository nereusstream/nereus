/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.GenerationId;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationSequenceRecord;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexTargetRecord;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Production Phase 4 generation/workflow adapter over a caller-owned shared Oxia runtime. */
public final class OxiaJavaGenerationMetadataStore implements GenerationMetadataStore {
    private static final int MAX_ALLOCATION_ATTEMPTS = 64;
    private final F4MetadataStoreSupport support;

    public static OxiaJavaGenerationMetadataStore usingSharedRuntime(
            OxiaClientConfiguration clientConfig,
            SharedOxiaClientRuntime runtime,
            Clock clock) {
        Objects.requireNonNull(clientConfig, "clientConfig");
        Objects.requireNonNull(runtime, "runtime");
        runtime.requireCompatible(clientConfig);
        return new OxiaJavaGenerationMetadataStore(runtime.client(), clock);
    }

    OxiaJavaGenerationMetadataStore(PartitionedOxiaClient client, Clock clock) {
        this.support = new F4MetadataStoreSupport(client, clock);
    }

    @Override
    public CompletableFuture<Optional<VersionedGenerationSequence>> getSequence(
            String cluster, StreamId streamId, ReadView view) {
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = Objects.requireNonNull(streamId, "streamId");
        ReadView exactView = Objects.requireNonNull(view, "view");
        String key = keys.generationSequenceKey(stream, exactView);
        return support.get(key, keys.streamPartitionKey(stream), GenerationSequenceRecord.class)
                .thenApply(value -> value.map(item -> sequence(keys, stream, exactView, item)));
    }

    @Override
    public CompletableFuture<VersionedGenerationSequence> getOrCreateSequence(
            String cluster, StreamId streamId, ReadView view) {
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = Objects.requireNonNull(streamId, "streamId");
        ReadView exactView = Objects.requireNonNull(view, "view");
        return getSequence(cluster, stream, exactView).thenCompose(existing -> {
            if (existing.isPresent()) {
                return CompletableFuture.completedFuture(existing.orElseThrow());
            }
            GenerationSequenceRecord bootstrap = new GenerationSequenceRecord(
                    1, stream.value(), exactView.wireId(), 0, 0, "", support.now(), 0);
            return recoverCreate(
                    support.create(
                            keys.generationSequenceKey(stream, exactView),
                            keys.streamPartitionKey(stream),
                            bootstrap,
                            GenerationSequenceRecord.class)
                            .thenApply(item -> sequence(keys, stream, exactView, item)),
                    () -> getSequence(cluster, stream, exactView).thenApply(value -> value.orElseThrow(
                            () -> F4MetadataStoreSupport.invariant("sequence disappeared after create conflict"))));
        });
    }

    @Override
    public CompletableFuture<AllocatedGeneration> allocateGeneration(
            String cluster, StreamId streamId, ReadView view, PublicationId publicationId) {
        Objects.requireNonNull(publicationId, "publicationId");
        return getOrCreateSequence(cluster, streamId, view)
                .thenCompose(sequence -> allocate(cluster, streamId, view, publicationId, sequence, 0));
    }

    private CompletableFuture<AllocatedGeneration> allocate(
            String cluster,
            StreamId streamId,
            ReadView view,
            PublicationId publicationId,
            VersionedGenerationSequence current,
            int attempt) {
        if (current.value().lastPublicationId().equals(publicationId.value())) {
            return CompletableFuture.completedFuture(allocation(current, publicationId));
        }
        if (attempt >= MAX_ALLOCATION_ATTEMPTS
                || current.value().lastAllocatedGeneration() == Long.MAX_VALUE
                || current.value().allocationSequence() == Long.MAX_VALUE) {
            return F4MetadataStoreSupport.failed(new IllegalStateException(
                    "generation allocation exhausted its retry or numeric bound"));
        }
        F4Keyspace keys = new F4Keyspace(cluster);
        GenerationSequenceRecord replacement = new GenerationSequenceRecord(
                1,
                streamId.value(),
                view.wireId(),
                Math.addExact(current.value().lastAllocatedGeneration(), 1),
                Math.addExact(current.value().allocationSequence(), 1),
                publicationId.value(),
                support.now(),
                0);
        CompletableFuture<VersionedGenerationSequence> write = support.compareAndSet(
                        current.key(),
                        keys.streamPartitionKey(streamId),
                        replacement,
                        GenerationSequenceRecord.class,
                        current.metadataVersion())
                .thenApply(item -> sequence(keys, streamId, view, item));
        return write.handle((value, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(allocation(value, publicationId));
            }
            return getSequence(cluster, streamId, view).thenCompose(reloaded -> {
                VersionedGenerationSequence next = reloaded.orElseThrow(
                        () -> F4MetadataStoreSupport.invariant("generation sequence disappeared during allocation"));
                return allocate(cluster, streamId, view, publicationId, next, attempt + 1);
            });
        }).thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<Void> deleteSequence(
            String cluster, StreamId streamId, ReadView view, long expectedVersion) {
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.delete(
                keys.generationSequenceKey(streamId, view), keys.streamPartitionKey(streamId), expectedVersion);
    }

    @Override
    public CompletableFuture<VersionedGenerationIndex> createPrepared(
            String cluster, GenerationIndexRecord record) {
        GenerationIndexRecord value = Objects.requireNonNull(record, "record");
        if (value.lifecycle() != GenerationLifecycle.PREPARED || value.metadataVersion() != 0) {
            throw new IllegalArgumentException("createPrepared requires an unhydrated PREPARED index");
        }
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = new StreamId(value.streamId());
        ReadView view = ReadView.fromWireId(value.readViewId());
        String key = keys.generationIndexKey(
                stream, view, value.offsetEnd(), value.generation());
        CompletableFuture<VersionedGenerationIndex> create = support.create(
                        key, keys.streamPartitionKey(stream), value, GenerationIndexRecord.class)
                .thenApply(item -> index(keys, item));
        GenerationIndexIdentity identity = new GenerationIndexIdentity(
                stream, view, value.offsetEnd(), value.generation());
        return recoverCreate(create, () -> getIndex(cluster, identity).thenApply(existing -> {
            VersionedGenerationIndex result = existing.orElseThrow(
                    () -> F4MetadataStoreSupport.invariant(
                            "generation index disappeared after create conflict"));
            if (!GenerationMetadataTransitions.sameGenerationPublicationIdentity(value, result.value())) {
                throw F4MetadataStoreSupport.invariant(
                        "generation index key collided with another publication identity");
            }
            return result;
        }));
    }

    @Override
    public CompletableFuture<VersionedGenerationIndex> restoreCommittedFromCheckpoint(
            String cluster,
            GenerationIndexRecord record,
            Checksum canonicalRecordSha256) {
        GenerationIndexRecord value = Objects.requireNonNull(record, "record");
        Checksum expectedRawDigest = Objects.requireNonNull(
                canonicalRecordSha256, "canonicalRecordSha256");
        if (value.lifecycle() != GenerationLifecycle.COMMITTED
                || value.metadataVersion() != 0
                || ReadView.fromWireId(value.readViewId()) != ReadView.COMMITTED) {
            throw new IllegalArgumentException(
                    "checkpoint restore requires an unhydrated COMMITTED-view index");
        }
        if (!GenerationIndexDigests.canonicalRecordSha256(value)
                .equals(expectedRawDigest)) {
            throw new IllegalArgumentException(
                    "checkpoint generation-index raw digest does not match canonical bytes");
        }
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = new StreamId(value.streamId());
        ReadView view = ReadView.COMMITTED;
        String key = keys.generationIndexKey(
                stream, view, value.offsetEnd(), value.generation());
        GenerationIndexIdentity identity = new GenerationIndexIdentity(
                stream, view, value.offsetEnd(), value.generation());
        Checksum expectedDurableDigest = GenerationIndexDigests.durableValueSha256(value);
        CompletableFuture<VersionedGenerationIndex> create = support.create(
                        key,
                        keys.streamPartitionKey(stream),
                        value,
                        GenerationIndexRecord.class)
                .thenApply(item -> index(keys, item));
        return recoverCreate(create, () -> getIndex(cluster, identity)
                        .thenApply(existing -> existing.orElseThrow(() ->
                                F4MetadataStoreSupport.invariant(
                                        "restored generation index disappeared after create conflict"))))
                .thenApply(actual -> requireExactRestoredIndex(
                        value, expectedDurableDigest, actual));
    }

    @Override
    public CompletableFuture<VersionedGenerationIndex> compareAndSetIndex(
            String cluster, GenerationIndexRecord replacement, long expectedVersion) {
        GenerationIndexRecord value = Objects.requireNonNull(replacement, "replacement");
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = new StreamId(value.streamId());
        ReadView view = ReadView.fromWireId(value.readViewId());
        GenerationIndexIdentity identity = new GenerationIndexIdentity(
                stream, view, value.offsetEnd(), value.generation());
        String key = keys.generationIndexKey(stream, view, value.offsetEnd(), value.generation());
        return getIndex(cluster, identity).thenCompose(currentOptional -> {
            VersionedGenerationIndex current = currentOptional.orElseThrow(
                    () -> new F4MetadataConditionFailedException("generation index is absent"));
            if (current.metadataVersion() != expectedVersion) {
                return F4MetadataStoreSupport.failed(
                        new F4MetadataConditionFailedException("generation index version mismatch"));
            }
            GenerationMetadataTransitions.requireValidIndexReplacement(current.value(), value);
            return support.compareAndSet(
                            key, keys.streamPartitionKey(stream), value,
                            GenerationIndexRecord.class, expectedVersion)
                    .thenApply(item -> index(keys, item));
        });
    }

    @Override
    public CompletableFuture<Optional<VersionedGenerationIndex>> getIndex(
            String cluster, GenerationIndexIdentity identity) {
        GenerationIndexIdentity exact = Objects.requireNonNull(identity, "identity");
        F4Keyspace keys = new F4Keyspace(cluster);
        String key = keys.generationIndexKey(
                exact.streamId(), exact.view(), exact.offsetEnd(), exact.generation());
        return support.get(key, keys.streamPartitionKey(exact.streamId()), GenerationIndexRecord.class)
                .thenApply(value -> value.map(item -> index(keys, item)));
    }

    @Override
    public CompletableFuture<Optional<VersionedGenerationCandidate>> getCandidate(
            String cluster,
            StreamId streamId,
            ReadView view,
            long offsetEnd,
            long generation) {
        if (offsetEnd <= 0 || generation < 0 || (view != ReadView.COMMITTED && generation == 0)) {
            throw new IllegalArgumentException("generation candidate identity is invalid");
        }
        F4Keyspace keys = new F4Keyspace(cluster);
        String key = keys.generationIndexKey(streamId, view, offsetEnd, generation);
        return support.client().get(key, keys.streamPartitionKey(streamId))
                .thenApply(value -> value.map(item -> generationCandidate(keys, streamId, view, item)));
    }

    @Override
    public CompletableFuture<Optional<VersionedGenerationCandidate>> getCandidateByKey(
            String cluster,
            StreamId streamId,
            ReadView view,
            String indexKey) {
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = Objects.requireNonNull(streamId, "streamId");
        ReadView exactView = Objects.requireNonNull(view, "view");
        String key = Objects.requireNonNull(indexKey, "indexKey");
        String prefix = keys.generationIndexPrefix(stream, exactView) + "/";
        if (!key.startsWith(prefix) || key.length() == prefix.length()) {
            throw new IllegalArgumentException(
                    "indexKey is outside the requested stream/view generation prefix");
        }
        return support.client().get(key, keys.streamPartitionKey(stream))
                .thenApply(value -> value.map(item ->
                        generationCandidate(keys, stream, exactView, item)));
    }

    @Override
    public CompletableFuture<GenerationScanPage> scanIndex(
            String cluster,
            StreamId streamId,
            ReadView view,
            long minOffsetEndInclusive,
            long maxOffsetEndInclusive,
            Optional<F4ScanToken> continuation,
            int limit) {
        F4MetadataStoreSupport.requirePageLimit(limit);
        if (minOffsetEndInclusive < 0 || maxOffsetEndInclusive < minOffsetEndInclusive) {
            throw new IllegalArgumentException("generation scan bounds are invalid");
        }
        F4Keyspace keys = new F4Keyspace(cluster);
        String prefix = keys.generationIndexPrefix(streamId, view) + "/";
        String scope = support.scopeSha256(
                "generation-index\0" + streamId.value() + "\0" + view.wireId() + "\0"
                        + minOffsetEndInclusive + "\0" + maxOffsetEndInclusive);
        F4ScanToken token = support.validateToken(
                continuation, keys.cluster(), F4ScanKind.GENERATION_INDEX, scope, prefix);
        String from = token == null
                ? keys.generationIndexScanFrom(streamId, view, minOffsetEndInclusive)
                : token.resumeFromInclusive();
        String to = keys.generationIndexScanToAfterEnd(streamId, view, maxOffsetEndInclusive);
        return support.client().rangeScan(from, to, limit, keys.streamPartitionKey(streamId)).thenApply(stored -> {
            List<VersionedGenerationCandidate> values = new ArrayList<>(stored.size());
            for (PartitionedOxiaClient.VersionedValue item : stored) {
                values.add(generationCandidate(keys, streamId, view, item));
            }
            Optional<F4ScanToken> next = stored.size() == limit
                    ? Optional.of(new F4ScanToken(
                            keys.cluster(), F4ScanKind.GENERATION_INDEX, scope, prefix,
                            stored.get(stored.size() - 1).key()))
                    : Optional.empty();
            return new GenerationScanPage(values, next);
        });
    }

    @Override
    public CompletableFuture<Void> deleteIndex(
            String cluster, GenerationIndexIdentity identity, long expectedVersion) {
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.delete(
                keys.generationIndexKey(
                        identity.streamId(), identity.view(), identity.offsetEnd(), identity.generation()),
                keys.streamPartitionKey(identity.streamId()),
                expectedVersion);
    }

    @Override
    public CompletableFuture<VersionedMaterializationTask> createTask(
            String cluster, MaterializationTaskRecord task) {
        MaterializationTaskRecord value = Objects.requireNonNull(task, "task");
        if (value.lifecycle() != com.nereusstream.metadata.oxia.records.TaskLifecycle.PLANNED
                || value.metadataVersion() != 0) {
            throw new IllegalArgumentException("createTask requires an unhydrated PLANNED task");
        }
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = new StreamId(value.streamId());
        String key = keys.taskKey(stream, value.taskId());
        CompletableFuture<VersionedMaterializationTask> create = support.create(
                        key, keys.streamPartitionKey(stream), value, MaterializationTaskRecord.class)
                .thenApply(item -> task(keys, item));
        return recoverCreate(create, () -> getTask(cluster, stream, value.taskId()).thenApply(existing -> {
            VersionedMaterializationTask result = existing.orElseThrow(
                    () -> F4MetadataStoreSupport.invariant("task disappeared after create conflict"));
            if (!GenerationMetadataTransitions.sameTaskCreateIdentity(value, result.value())) {
                throw F4MetadataStoreSupport.invariant(
                        "task key collided with another planning identity");
            }
            return result;
        }));
    }

    @Override
    public CompletableFuture<Optional<VersionedMaterializationTask>> getTask(
            String cluster, StreamId streamId, String taskId) {
        F4Keyspace keys = new F4Keyspace(cluster);
        String key = keys.taskKey(streamId, taskId);
        return support.get(key, keys.streamPartitionKey(streamId), MaterializationTaskRecord.class)
                .thenApply(value -> value.map(item -> task(keys, item)));
    }

    @Override
    public CompletableFuture<VersionedMaterializationTask> compareAndSetTask(
            String cluster, MaterializationTaskRecord task, long expectedVersion) {
        MaterializationTaskRecord value = Objects.requireNonNull(task, "task");
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = new StreamId(value.streamId());
        return getTask(cluster, stream, value.taskId()).thenCompose(currentOptional -> {
            VersionedMaterializationTask current = currentOptional.orElseThrow(
                    () -> new F4MetadataConditionFailedException("materialization task is absent"));
            if (current.metadataVersion() != expectedVersion) {
                return F4MetadataStoreSupport.failed(
                        new F4MetadataConditionFailedException("materialization task version mismatch"));
            }
            GenerationMetadataTransitions.requireValidTaskReplacement(current.value(), value, support.now());
            return support.compareAndSet(
                            keys.taskKey(stream, value.taskId()), keys.streamPartitionKey(stream), value,
                            MaterializationTaskRecord.class, expectedVersion)
                    .thenApply(item -> task(keys, item));
        });
    }

    @Override
    public CompletableFuture<TaskScanPage> scanTasks(
            String cluster, StreamId streamId, Optional<F4ScanToken> continuation, int limit) {
        F4Keyspace keys = new F4Keyspace(cluster);
        String prefix = F4MetadataStoreSupport.prefixStart(keys.taskPrefix(streamId));
        return scanSimple(
                keys,
                streamId,
                F4ScanKind.MATERIALIZATION_TASK,
                prefix,
                continuation,
                limit,
                MaterializationTaskRecord.class,
                item -> task(keys, item),
                TaskScanPage::new);
    }

    @Override
    public CompletableFuture<Void> deleteTask(
            String cluster, StreamId streamId, String taskId, long expectedVersion) {
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.delete(
                keys.taskKey(streamId, taskId), keys.streamPartitionKey(streamId), expectedVersion);
    }

    @Override
    public CompletableFuture<Optional<VersionedMaterializationCheckpoint>> getMaterializationCheckpoint(
            String cluster, StreamId streamId, String policyId, long policyVersion) {
        F4Keyspace keys = new F4Keyspace(cluster);
        String key = keys.checkpointKey(streamId, policyId, policyVersion);
        return support.get(key, keys.streamPartitionKey(streamId), MaterializationCheckpointRecord.class)
                .thenApply(value -> value.map(item -> checkpoint(keys, item)));
    }

    @Override
    public CompletableFuture<VersionedMaterializationCheckpoint> getOrCreateMaterializationCheckpoint(
            String cluster,
            StreamId streamId,
            String policyId,
            long policyVersion,
            Checksum policySha256) {
        Objects.requireNonNull(policySha256, "policySha256");
        if (policySha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("policySha256 must use SHA256");
        }
        F4Keyspace keys = new F4Keyspace(cluster);
        return getMaterializationCheckpoint(cluster, streamId, policyId, policyVersion).thenCompose(existing -> {
            if (existing.isPresent()) {
                if (!existing.orElseThrow().value().policySha256().equals(policySha256.value())) {
                    return F4MetadataStoreSupport.failed(F4MetadataStoreSupport.invariant(
                            "checkpoint policy version collided with another digest"));
                }
                return CompletableFuture.completedFuture(existing.orElseThrow());
            }
            MaterializationCheckpointRecord bootstrap = new MaterializationCheckpointRecord(
                    1, streamId.value(), policyId, policyVersion, policySha256.value(),
                    0, 0, 0, "", support.now(), 0);
            CompletableFuture<VersionedMaterializationCheckpoint> create = support.create(
                            keys.checkpointKey(streamId, policyId, policyVersion),
                            keys.streamPartitionKey(streamId),
                            bootstrap,
                            MaterializationCheckpointRecord.class)
                    .thenApply(item -> checkpoint(keys, item));
            return recoverCreate(create, () -> getMaterializationCheckpoint(
                            cluster, streamId, policyId, policyVersion)
                    .thenApply(value -> {
                        VersionedMaterializationCheckpoint result = value.orElseThrow(
                                () -> F4MetadataStoreSupport.invariant(
                                        "checkpoint disappeared after create conflict"));
                        if (!result.value().policySha256().equals(policySha256.value())) {
                            throw F4MetadataStoreSupport.invariant(
                                    "checkpoint policy version collided with another digest");
                        }
                        return result;
                    }));
        });
    }

    @Override
    public CompletableFuture<VersionedMaterializationCheckpoint> compareAndSetMaterializationCheckpoint(
            String cluster, MaterializationCheckpointRecord checkpoint, long expectedVersion) {
        MaterializationCheckpointRecord value = Objects.requireNonNull(checkpoint, "checkpoint");
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = new StreamId(value.streamId());
        return getMaterializationCheckpoint(
                        cluster, stream, value.policyId(), value.policyVersion())
                .thenCompose(currentOptional -> {
                    VersionedMaterializationCheckpoint current = currentOptional.orElseThrow(
                            () -> new F4MetadataConditionFailedException(
                                    "materialization checkpoint is absent"));
                    if (current.metadataVersion() != expectedVersion) {
                        return F4MetadataStoreSupport.failed(
                                new F4MetadataConditionFailedException(
                                        "materialization checkpoint version mismatch"));
                    }
                    GenerationMetadataTransitions.requireValidCheckpointReplacement(current.value(), value);
                    return support.compareAndSet(
                                    keys.checkpointKey(stream, value.policyId(), value.policyVersion()),
                                    keys.streamPartitionKey(stream), value,
                                    MaterializationCheckpointRecord.class, expectedVersion)
                            .thenApply(item -> checkpoint(keys, item));
                });
    }

    @Override
    public CompletableFuture<MaterializationCheckpointScanPage> scanMaterializationCheckpoints(
            String cluster,
            StreamId streamId,
            Optional<F4ScanToken> continuation,
            int limit) {
        F4Keyspace keys = new F4Keyspace(cluster);
        String prefix = F4MetadataStoreSupport.prefixStart(keys.checkpointPrefix(streamId));
        return scanSimple(
                keys,
                streamId,
                F4ScanKind.MATERIALIZATION_CHECKPOINT,
                prefix,
                continuation,
                limit,
                MaterializationCheckpointRecord.class,
                item -> checkpoint(keys, item),
                MaterializationCheckpointScanPage::new);
    }

    @Override
    public CompletableFuture<Void> deleteMaterializationCheckpoint(
            String cluster, StreamId streamId, String policyId, long policyVersion, long expectedVersion) {
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.delete(
                keys.checkpointKey(streamId, policyId, policyVersion),
                keys.streamPartitionKey(streamId),
                expectedVersion);
    }

    @Override
    public CompletableFuture<VersionedRangeRetentionStats> createRangeRetentionStats(
            String cluster, RangeRetentionStatsRecord stats) {
        RangeRetentionStatsRecord value = Objects.requireNonNull(stats, "stats");
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = new StreamId(value.streamId());
        String key = keys.retentionStatsKey(stream, value.offsetEnd(), value.commitVersion());
        CompletableFuture<VersionedRangeRetentionStats> create = support.create(
                        key, keys.streamPartitionKey(stream), value, RangeRetentionStatsRecord.class)
                .thenApply(item -> stats(keys, item));
        return recoverCreate(create, () -> getRangeRetentionStats(
                        cluster, stream, value.offsetEnd(), value.commitVersion())
                .thenApply(existing -> {
                    VersionedRangeRetentionStats result = existing.orElseThrow(
                            () -> F4MetadataStoreSupport.invariant("retention stats disappeared after create conflict"));
                    if (!result.value().withMetadataVersion(0).equals(value)) {
                        throw F4MetadataStoreSupport.invariant("retention stats identity conflict");
                    }
                    return result;
                }));
    }

    @Override
    public CompletableFuture<Optional<VersionedRangeRetentionStats>> getRangeRetentionStats(
            String cluster, StreamId streamId, long offsetEnd, long commitVersion) {
        F4Keyspace keys = new F4Keyspace(cluster);
        String key = keys.retentionStatsKey(streamId, offsetEnd, commitVersion);
        return support.get(key, keys.streamPartitionKey(streamId), RangeRetentionStatsRecord.class)
                .thenApply(value -> value.map(item -> stats(keys, item)));
    }

    @Override
    public CompletableFuture<VersionedRangeRetentionStats> compareAndSetRangeRetentionStats(
            String cluster, RangeRetentionStatsRecord stats, long expectedVersion) {
        RangeRetentionStatsRecord value = Objects.requireNonNull(stats, "stats");
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = new StreamId(value.streamId());
        return getRangeRetentionStats(cluster, stream, value.offsetEnd(), value.commitVersion())
                .thenCompose(currentOptional -> {
                    VersionedRangeRetentionStats current = currentOptional.orElseThrow(
                            () -> new F4MetadataConditionFailedException("retention stats are absent"));
                    if (current.metadataVersion() != expectedVersion) {
                        return F4MetadataStoreSupport.failed(
                                new F4MetadataConditionFailedException("retention stats version mismatch"));
                    }
                    GenerationMetadataTransitions.requireValidRetentionStatsReplacement(current.value(), value);
                    return support.compareAndSet(
                                    keys.retentionStatsKey(stream, value.offsetEnd(), value.commitVersion()),
                                    keys.streamPartitionKey(stream), value,
                                    RangeRetentionStatsRecord.class, expectedVersion)
                            .thenApply(item -> stats(keys, item));
                });
    }

    @Override
    public CompletableFuture<RangeRetentionStatsScanPage> scanRangeRetentionStats(
            String cluster,
            StreamId streamId,
            long minOffsetEndInclusive,
            long maxOffsetEndInclusive,
            Optional<F4ScanToken> continuation,
            int limit) {
        F4MetadataStoreSupport.requirePageLimit(limit);
        if (minOffsetEndInclusive < 0 || maxOffsetEndInclusive < minOffsetEndInclusive) {
            throw new IllegalArgumentException("retention stats scan bounds are invalid");
        }
        F4Keyspace keys = new F4Keyspace(cluster);
        String prefix = keys.retentionStatsPrefix(streamId) + "/";
        String scope = support.scopeSha256(
                "retention-stats\0" + streamId.value() + "\0"
                        + minOffsetEndInclusive + "\0" + maxOffsetEndInclusive);
        F4ScanToken token = support.validateToken(
                continuation, keys.cluster(), F4ScanKind.RETENTION_STATS, scope, prefix);
        String from = token == null
                ? keys.retentionStatsScanFrom(streamId, minOffsetEndInclusive) : token.resumeFromInclusive();
        String to = keys.retentionStatsScanToAfterEnd(streamId, maxOffsetEndInclusive);
        return support.client().rangeScan(from, to, limit, keys.streamPartitionKey(streamId)).thenApply(stored -> {
            List<VersionedRangeRetentionStats> values = stored.stream()
                    .map(item -> stats(keys, support.decode(item, RangeRetentionStatsRecord.class)))
                    .toList();
            Optional<F4ScanToken> next = stored.size() == limit
                    ? Optional.of(new F4ScanToken(
                            keys.cluster(), F4ScanKind.RETENTION_STATS, scope, prefix,
                            stored.get(stored.size() - 1).key()))
                    : Optional.empty();
            return new RangeRetentionStatsScanPage(values, next);
        });
    }

    @Override
    public CompletableFuture<Void> deleteRangeRetentionStats(
            String cluster, StreamId streamId, long offsetEnd, long commitVersion, long expectedVersion) {
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.delete(
                keys.retentionStatsKey(streamId, offsetEnd, commitVersion),
                keys.streamPartitionKey(streamId), expectedVersion);
    }

    @Override
    public CompletableFuture<VersionedMaterializationStreamRegistration> createOrVerifyStreamRegistration(
            String cluster, MaterializationStreamRegistrationRecord registration) {
        MaterializationStreamRegistrationRecord value = Objects.requireNonNull(registration, "registration");
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = new StreamId(value.streamId());
        CompletableFuture<VersionedMaterializationStreamRegistration> create = support.create(
                        keys.materializationRegistryKey(stream),
                        keys.materializationRegistryPartitionKey(keys.materializationRegistryShard(stream)),
                        value,
                        MaterializationStreamRegistrationRecord.class)
                .thenApply(item -> registration(keys, item));
        return recoverCreate(create, () -> getStreamRegistration(cluster, stream).thenApply(existing -> {
            VersionedMaterializationStreamRegistration result = existing.orElseThrow(
                    () -> F4MetadataStoreSupport.invariant("registration disappeared after create conflict"));
            MaterializationStreamRegistrationRecord stored = result.value();
            if (!stored.streamId().equals(value.streamId())
                    || !stored.projectionRef().equals(value.projectionRef())
                    || !stored.projectionIdentitySha256().equals(value.projectionIdentitySha256())
                    || !stored.storageProfile().equals(value.storageProfile())) {
                throw F4MetadataStoreSupport.invariant("stream registration identity conflict");
            }
            return result;
        }));
    }

    @Override
    public CompletableFuture<Optional<VersionedMaterializationStreamRegistration>> getStreamRegistration(
            String cluster, StreamId streamId) {
        F4Keyspace keys = new F4Keyspace(cluster);
        int shard = keys.materializationRegistryShard(streamId);
        return support.get(
                        keys.materializationRegistryKey(streamId),
                        keys.materializationRegistryPartitionKey(shard),
                        MaterializationStreamRegistrationRecord.class)
                .thenApply(value -> value.map(item -> registration(keys, item)));
    }

    @Override
    public CompletableFuture<VersionedMaterializationStreamRegistration> compareAndSetStreamRegistration(
            String cluster, MaterializationStreamRegistrationRecord registration, long expectedVersion) {
        MaterializationStreamRegistrationRecord value = Objects.requireNonNull(registration, "registration");
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = new StreamId(value.streamId());
        int shard = keys.materializationRegistryShard(stream);
        return getStreamRegistration(cluster, stream).thenCompose(currentOptional -> {
            VersionedMaterializationStreamRegistration current = currentOptional.orElseThrow(
                    () -> new F4MetadataConditionFailedException("stream registration is absent"));
            if (current.metadataVersion() != expectedVersion) {
                return F4MetadataStoreSupport.failed(
                        new F4MetadataConditionFailedException("stream registration version mismatch"));
            }
            GenerationMetadataTransitions.requireValidRegistrationReplacement(current.value(), value);
            return support.compareAndSet(
                            keys.materializationRegistryKey(stream),
                            keys.materializationRegistryPartitionKey(shard),
                            value,
                            MaterializationStreamRegistrationRecord.class,
                            expectedVersion)
                    .thenApply(item -> registration(keys, item));
        });
    }

    @Override
    public CompletableFuture<StreamRegistrationScanPage> scanStreamRegistrations(
            String cluster, int shard, Optional<F4ScanToken> continuation, int limit) {
        F4MetadataStoreSupport.requirePageLimit(limit);
        F4Keyspace keys = new F4Keyspace(cluster);
        String prefix = F4MetadataStoreSupport.prefixStart(keys.materializationRegistryPrefix(shard));
        String scope = support.scopeSha256("stream-registration\0" + shard);
        F4ScanToken token = support.validateToken(
                continuation, keys.cluster(), F4ScanKind.STREAM_REGISTRATION, scope, prefix);
        String from = token == null ? prefix : token.resumeFromInclusive();
        String to = F4MetadataStoreSupport.prefixEnd(keys.materializationRegistryPrefix(shard));
        return support.client().rangeScan(
                        from, to, limit, keys.materializationRegistryPartitionKey(shard))
                .thenApply(stored -> {
                    List<VersionedMaterializationStreamRegistration> values = stored.stream()
                            .map(item -> registration(
                                    keys, support.decode(item, MaterializationStreamRegistrationRecord.class)))
                            .toList();
                    Optional<F4ScanToken> next = stored.size() == limit
                            ? Optional.of(new F4ScanToken(
                                    keys.cluster(), F4ScanKind.STREAM_REGISTRATION, scope, prefix,
                                    stored.get(stored.size() - 1).key()))
                            : Optional.empty();
                    return new StreamRegistrationScanPage(values, next);
                });
    }

    @Override
    public CompletableFuture<Void> deleteStreamRegistration(
            String cluster, StreamId streamId, long expectedVersion) {
        F4Keyspace keys = new F4Keyspace(cluster);
        int shard = keys.materializationRegistryShard(streamId);
        return support.delete(
                keys.materializationRegistryKey(streamId),
                keys.materializationRegistryPartitionKey(shard), expectedVersion);
    }

    @Override
    public CompletableFuture<Optional<VersionedRecoveryCheckpointRoot>> getRecoveryRoot(
            String cluster, StreamId streamId) {
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.get(
                        keys.recoveryRootKey(streamId), keys.streamPartitionKey(streamId),
                        RecoveryCheckpointRootRecord.class)
                .thenApply(value -> value.map(item -> recovery(keys, item)));
    }

    @Override
    public CompletableFuture<VersionedRecoveryCheckpointRoot> getOrCreateRecoveryRoot(
            String cluster, StreamId streamId) {
        F4Keyspace keys = new F4Keyspace(cluster);
        return getRecoveryRoot(cluster, streamId).thenCompose(existing -> {
            if (existing.isPresent()) {
                return CompletableFuture.completedFuture(existing.orElseThrow());
            }
            RecoveryCheckpointRootRecord bootstrap = new RecoveryCheckpointRootRecord(
                    1, streamId.value(), 0, 0, 0, 0, 0, 0, 0,
                    "", "", List.of(), "", "", 0, 0, 0);
            CompletableFuture<VersionedRecoveryCheckpointRoot> create = support.create(
                            keys.recoveryRootKey(streamId), keys.streamPartitionKey(streamId),
                            bootstrap, RecoveryCheckpointRootRecord.class)
                    .thenApply(item -> recovery(keys, item));
            return recoverCreate(create, () -> getRecoveryRoot(cluster, streamId).thenApply(value -> value.orElseThrow(
                    () -> F4MetadataStoreSupport.invariant("recovery root disappeared after create conflict"))));
        });
    }

    @Override
    public CompletableFuture<VersionedRecoveryCheckpointRoot> compareAndSetRecoveryRoot(
            String cluster, RecoveryCheckpointRootRecord root, long expectedVersion) {
        RecoveryCheckpointRootRecord value = Objects.requireNonNull(root, "root");
        F4Keyspace keys = new F4Keyspace(cluster);
        StreamId stream = new StreamId(value.streamId());
        return getRecoveryRoot(cluster, stream).thenCompose(currentOptional -> {
            VersionedRecoveryCheckpointRoot current = currentOptional.orElseThrow(
                    () -> new F4MetadataConditionFailedException("recovery root is absent"));
            if (current.metadataVersion() != expectedVersion) {
                return F4MetadataStoreSupport.failed(
                        new F4MetadataConditionFailedException("recovery root version mismatch"));
            }
            GenerationMetadataTransitions.requireValidRecoveryRootReplacement(current.value(), value);
            return support.compareAndSet(
                            keys.recoveryRootKey(stream), keys.streamPartitionKey(stream), value,
                            RecoveryCheckpointRootRecord.class, expectedVersion)
                    .thenApply(item -> recovery(keys, item));
        });
    }

    @Override
    public CompletableFuture<Void> deleteRecoveryRoot(
            String cluster, StreamId streamId, long expectedVersion) {
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.delete(keys.recoveryRootKey(streamId), keys.streamPartitionKey(streamId), expectedVersion);
    }

    @Override
    public void close() {
        support.close();
    }

    private <R, V, P> CompletableFuture<P> scanSimple(
            F4Keyspace keys,
            StreamId streamId,
            F4ScanKind kind,
            String prefix,
            Optional<F4ScanToken> continuation,
            int limit,
            Class<R> recordType,
            Function<F4MetadataStoreSupport.Decoded<R>, V> wrapper,
            java.util.function.BiFunction<List<V>, Optional<F4ScanToken>, P> pageFactory) {
        F4MetadataStoreSupport.requirePageLimit(limit);
        String scope = support.scopeSha256(kind.name() + "\0" + streamId.value());
        F4ScanToken token = support.validateToken(continuation, keys.cluster(), kind, scope, prefix);
        String from = token == null ? prefix : token.resumeFromInclusive();
        String base = prefix.substring(0, prefix.length() - 1);
        String to = F4MetadataStoreSupport.prefixEnd(base);
        return support.client().rangeScan(from, to, limit, keys.streamPartitionKey(streamId)).thenApply(stored -> {
            List<V> values = stored.stream().map(item -> wrapper.apply(support.decode(item, recordType))).toList();
            Optional<F4ScanToken> next = stored.size() == limit
                    ? Optional.of(new F4ScanToken(
                            keys.cluster(), kind, scope, prefix, stored.get(stored.size() - 1).key()))
                    : Optional.empty();
            return pageFactory.apply(values, next);
        });
    }

    private VersionedGenerationCandidate generationCandidate(
            F4Keyspace keys,
            StreamId streamId,
            ReadView view,
            PartitionedOxiaClient.VersionedValue stored) {
        String type = MetadataRecordCodecFactory.recordType(stored.value());
        if (type.equals(GenerationIndexRecord.class.getSimpleName())) {
            if (view == ReadView.TOPIC_COMPACTED || view == ReadView.COMMITTED) {
                return index(keys, support.decode(stored, GenerationIndexRecord.class));
            }
        }
        if (view != ReadView.COMMITTED) {
            throw F4MetadataStoreSupport.invariant("generation-zero record appeared in a non-committed view");
        }
        OffsetIndexEntry entry;
        GenerationZeroIndexEncoding encoding;
        if (type.equals(OffsetIndexRecord.class.getSimpleName())) {
            OffsetIndexRecord raw = MetadataRecordCodecFactory.decodeEnvelope(stored.value(), OffsetIndexRecord.class);
            OffsetIndexRecord record = copy(raw, stored.version());
            entry = legacyEntry(streamId, record);
            encoding = GenerationZeroIndexEncoding.LEGACY_OFFSET_INDEX_RECORD;
        } else if (type.equals(OffsetIndexTargetRecord.class.getSimpleName())) {
            OffsetIndexTargetRecord raw = MetadataRecordCodecFactory.decodeEnvelope(
                    stored.value(), OffsetIndexTargetRecord.class);
            OffsetIndexTargetRecord record = new OffsetIndexTargetRecord(
                    raw.streamId(), raw.offsetStart(), raw.offsetEnd(), raw.generation(), raw.cumulativeSize(),
                    raw.readTarget(), raw.payloadFormat(), raw.recordCount(), raw.entryCount(), raw.logicalBytes(),
                    raw.schemaRefs(), raw.projectionRef(), raw.minEventTimeMillis(), raw.maxEventTimeMillis(),
                    raw.commitVersion(), raw.tombstoned(), stored.version());
            entry = new OffsetIndexEntry(
                    streamId, new OffsetRange(record.offsetStart(), record.offsetEnd()), record.generation(),
                    record.cumulativeSize(), ReadTargetCodecRegistry.phase15().decode(record.readTarget()),
                    PayloadFormat.valueOf(record.payloadFormat()), record.recordCount(), record.entryCount(),
                    record.logicalBytes(), record.schemaRefs(), ProjectionIdentity.decode(record.projectionRef()),
                    record.commitVersion(), record.tombstoned(), stored.version());
            encoding = GenerationZeroIndexEncoding.GENERIC_OFFSET_INDEX_TARGET_RECORD;
        } else {
            throw F4MetadataStoreSupport.invariant("unknown record type in generation-index scan: " + type);
        }
        String expected = keys.generationIndexKey(streamId, ReadView.COMMITTED, entry.offsetEnd(), 0);
        if (!entry.streamId().equals(streamId) || !stored.key().equals(expected) || entry.generation() != 0) {
            throw F4MetadataStoreSupport.invariant("generation-zero index key/value identity mismatch");
        }
        return new VersionedGenerationZeroIndex(
                stored.key(), encoding, entry, stored.version(), support.decodeDigest(stored.value()));
    }

    private static OffsetIndexEntry legacyEntry(StreamId streamId, OffsetIndexRecord record) {
        EntryIndexReferenceRecord rawIndex = record.entryIndexRef();
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.valueOf(rawIndex.location()),
                rawIndex.objectId().isEmpty() ? Optional.empty() : Optional.of(new ObjectId(rawIndex.objectId())),
                rawIndex.objectKey().isEmpty() ? Optional.empty() : Optional.of(new ObjectKey(rawIndex.objectKey())),
                rawIndex.inlineData().length == 0 ? Optional.empty() : Optional.of(rawIndex.inlineData()),
                rawIndex.offset(), rawIndex.length(),
                new Checksum(ChecksumType.valueOf(rawIndex.checksumType()), rawIndex.checksumValue()));
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1, new ObjectId(record.objectId()), new ObjectKey(record.objectKey()),
                ObjectType.valueOf(record.objectType()), record.physicalFormat(), record.logicalFormat(),
                record.sliceId(), record.objectOffset(), record.objectLength(),
                new Checksum(ChecksumType.valueOf(record.sliceChecksumType()), record.sliceChecksumValue()), index);
        return new OffsetIndexEntry(
                streamId, new OffsetRange(record.offsetStart(), record.offsetEnd()), record.generation(),
                record.cumulativeSize(), target, PayloadFormat.valueOf(record.payloadFormat()),
                record.recordCount(), record.entryCount(), record.logicalBytes(), record.schemaRefs(),
                ProjectionIdentity.decode(record.projectionRef()), record.commitVersion(), record.tombstoned(),
                record.metadataVersion());
    }

    private static OffsetIndexRecord copy(OffsetIndexRecord value, long version) {
        return new OffsetIndexRecord(
                value.streamId(), value.offsetStart(), value.offsetEnd(), value.generation(), value.cumulativeSize(),
                value.objectId(), value.objectKey(), value.sliceId(), value.objectType(), value.physicalFormat(),
                value.logicalFormat(), value.payloadFormat(), value.objectOffset(), value.objectLength(),
                value.recordCount(), value.entryCount(), value.logicalBytes(), value.schemaRefs(), value.entryIndexRef(),
                value.projectionRef(), value.sliceChecksumType(), value.sliceChecksumValue(), value.minEventTimeMillis(),
                value.maxEventTimeMillis(), value.commitVersion(), value.tombstoned(), version);
    }

    private static VersionedGenerationSequence sequence(
            F4Keyspace keys,
            StreamId stream,
            ReadView view,
            F4MetadataStoreSupport.Decoded<GenerationSequenceRecord> item) {
        if (!item.key().equals(keys.generationSequenceKey(stream, view))
                || !item.value().streamId().equals(stream.value())
                || item.value().readViewId() != view.wireId()) {
            throw F4MetadataStoreSupport.invariant("generation sequence key/value identity mismatch");
        }
        return new VersionedGenerationSequence(item.key(), item.value(), item.version(), item.durableSha256());
    }

    private static VersionedGenerationIndex index(
            F4Keyspace keys, F4MetadataStoreSupport.Decoded<GenerationIndexRecord> item) {
        GenerationIndexRecord value = item.value();
        StreamId stream = new StreamId(value.streamId());
        String expected = keys.generationIndexKey(
                stream, ReadView.fromWireId(value.readViewId()), value.offsetEnd(), value.generation());
        if (!item.key().equals(expected)) {
            throw F4MetadataStoreSupport.invariant("generation index key/value identity mismatch");
        }
        return new VersionedGenerationIndex(item.key(), value, item.version(), item.durableSha256());
    }

    private static VersionedGenerationIndex requireExactRestoredIndex(
            GenerationIndexRecord expected,
            Checksum expectedDurableDigest,
            VersionedGenerationIndex actual) {
        if (!expected.withMetadataVersion(actual.metadataVersion())
                        .equals(actual.value())
                || !actual.durableValueSha256().equals(expectedDurableDigest)) {
            throw F4MetadataStoreSupport.invariant(
                    "checkpoint restore collided with a different generation index");
        }
        return actual;
    }

    private static VersionedMaterializationTask task(
            F4Keyspace keys, F4MetadataStoreSupport.Decoded<MaterializationTaskRecord> item) {
        MaterializationTaskRecord value = item.value();
        if (!item.key().equals(keys.taskKey(new StreamId(value.streamId()), value.taskId()))) {
            throw F4MetadataStoreSupport.invariant("materialization task key/value identity mismatch");
        }
        return new VersionedMaterializationTask(item.key(), value, item.version(), item.durableSha256());
    }

    private static VersionedMaterializationCheckpoint checkpoint(
            F4Keyspace keys, F4MetadataStoreSupport.Decoded<MaterializationCheckpointRecord> item) {
        MaterializationCheckpointRecord value = item.value();
        if (!item.key().equals(keys.checkpointKey(
                new StreamId(value.streamId()), value.policyId(), value.policyVersion()))) {
            throw F4MetadataStoreSupport.invariant("materialization checkpoint key/value identity mismatch");
        }
        return new VersionedMaterializationCheckpoint(item.key(), value, item.version(), item.durableSha256());
    }

    private static VersionedRangeRetentionStats stats(
            F4Keyspace keys, F4MetadataStoreSupport.Decoded<RangeRetentionStatsRecord> item) {
        RangeRetentionStatsRecord value = item.value();
        if (!item.key().equals(keys.retentionStatsKey(
                new StreamId(value.streamId()), value.offsetEnd(), value.commitVersion()))) {
            throw F4MetadataStoreSupport.invariant("retention stats key/value identity mismatch");
        }
        return new VersionedRangeRetentionStats(item.key(), value, item.version(), item.durableSha256());
    }

    private static VersionedMaterializationStreamRegistration registration(
            F4Keyspace keys,
            F4MetadataStoreSupport.Decoded<MaterializationStreamRegistrationRecord> item) {
        MaterializationStreamRegistrationRecord value = item.value();
        if (!item.key().equals(keys.materializationRegistryKey(new StreamId(value.streamId())))) {
            throw F4MetadataStoreSupport.invariant("stream registration key/value identity mismatch");
        }
        return new VersionedMaterializationStreamRegistration(
                item.key(), value, item.version(), item.durableSha256());
    }

    private static VersionedRecoveryCheckpointRoot recovery(
            F4Keyspace keys, F4MetadataStoreSupport.Decoded<RecoveryCheckpointRootRecord> item) {
        RecoveryCheckpointRootRecord value = item.value();
        if (!item.key().equals(keys.recoveryRootKey(new StreamId(value.streamId())))) {
            throw F4MetadataStoreSupport.invariant("recovery root key/value identity mismatch");
        }
        return new VersionedRecoveryCheckpointRoot(item.key(), value, item.version(), item.durableSha256());
    }

    private static AllocatedGeneration allocation(
            VersionedGenerationSequence sequence, PublicationId publicationId) {
        GenerationSequenceRecord value = sequence.value();
        return new AllocatedGeneration(
                new StreamId(value.streamId()),
                ReadView.fromWireId(value.readViewId()),
                new GenerationId(value.lastAllocatedGeneration()),
                publicationId,
                value.allocationSequence(),
                sequence.metadataVersion(),
                sequence.durableValueSha256());
    }

    private static <T> CompletableFuture<T> recoverCreate(
            CompletableFuture<T> create,
            java.util.function.Supplier<CompletableFuture<T>> onConflict) {
        return create.handle((value, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(value);
            }
            if (!F4MetadataStoreSupport.isConditionFailure(failure)) {
                return F4MetadataStoreSupport.<T>failed(F4MetadataStoreSupport.unwrap(failure));
            }
            return onConflict.get();
        }).thenCompose(Function.identity());
    }
}
