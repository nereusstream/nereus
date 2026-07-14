/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.metadata.oxia.codec.MetadataCodecException;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.LedgerIdAllocatorRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.PositionIndexRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import com.nereusstream.metadata.oxia.records.VirtualLedgerProjectionRecord;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/** Shared single-key CAS protocol used by the fake and Java Oxia F2 metadata adapters. */
final class ProjectionMetadataStoreCore implements ManagedLedgerProjectionMetadataStore {
    enum WriteKind {
        ALLOCATOR,
        TOPIC,
        VIRTUAL_LEDGER,
        POSITION_INDEX
    }

    @FunctionalInterface
    interface WriteObserver {
        void afterWrite(WriteKind kind);
    }

    private static final long INITIAL_BACKOFF_NANOS = TimeUnit.MICROSECONDS.toNanos(100);
    private static final long MAX_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    private final PartitionedOxiaClient client;
    private final ProjectionMetadataStoreConfig config;
    private final WriteObserver writeObserver;
    private final ExecutorService operationExecutor;
    private final Semaphore admission;
    private final AtomicBoolean closed = new AtomicBoolean();

    ProjectionMetadataStoreCore(
            PartitionedOxiaClient client,
            ProjectionMetadataStoreConfig config,
            Clock clock,
            WriteObserver writeObserver) {
        this.client = Objects.requireNonNull(client, "client");
        this.config = Objects.requireNonNull(config, "config");
        Objects.requireNonNull(clock, "clock");
        this.writeObserver = Objects.requireNonNull(writeObserver, "writeObserver");
        this.admission = new Semaphore(config.maxPendingOperations());
        this.operationExecutor = Executors.newFixedThreadPool(
                Math.min(4, config.maxPendingOperations()), namedThreadFactory());
    }

    @Override
    public CompletableFuture<Optional<TopicProjectionRecord>> getProjection(
            String cluster,
            String managedLedgerName) {
        ManagedLedgerProjectionKeyspace keyspace = new ManagedLedgerProjectionKeyspace(cluster);
        String exactName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        return submit(deadline -> readTopic(keyspace, exactName, deadline));
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> createFirstProjection(
            String cluster,
            ProjectionCreateRequest request,
            ProjectionPublishGuard publishGuard) {
        ManagedLedgerProjectionKeyspace keyspace = new ManagedLedgerProjectionKeyspace(cluster);
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(publishGuard, "publishGuard");
        return submit(deadline -> {
            Optional<TopicProjectionRecord> raced = readTopic(
                    keyspace, request.managedLedgerName(), deadline);
            if (raced.isPresent()) {
                TopicProjectionRecord authoritative = raced.orElseThrow();
                repairInside(keyspace, authoritative, deadline);
                return authoritative;
            }
            requireNoOrphanDerivedRecords(keyspace, request, deadline);
            long ledgerId = allocateLedgerId(keyspace, deadline);
            TopicProjectionRecord candidate = topicCandidate(request, ledgerId);
            String key = keyspace.topicProjectionKey(request.managedLedgerName());
            PartitionKey partitionKey = keyspace.topicProjectionPartitionKey(request.managedLedgerName());
            deadline.await(publishGuard.validateBeforePublish());
            Optional<Long> version = putIfAbsent(key, partitionKey, candidate, TopicProjectionRecord.class, deadline);
            TopicProjectionRecord authoritative;
            if (version.isPresent()) {
                writeObserver.afterWrite(WriteKind.TOPIC);
                authoritative = withMetadataVersion(candidate, version.orElseThrow());
            } else {
                authoritative = readTopic(keyspace, request.managedLedgerName(), deadline)
                        .orElseThrow(() -> invariant("topic projection disappeared after a create race"));
            }
            repairInside(keyspace, authoritative, deadline);
            return authoritative;
        });
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> recreateDeletedProjection(
            String cluster,
            ManagedLedgerProjectionIdentity expectedDeletedIdentity,
            long expectedTopicMetadataVersion,
            ProjectionCreateRequest request,
            ProjectionPublishGuard publishGuard) {
        ManagedLedgerProjectionKeyspace keyspace = new ManagedLedgerProjectionKeyspace(cluster);
        Objects.requireNonNull(expectedDeletedIdentity, "expectedDeletedIdentity");
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(publishGuard, "publishGuard");
        requireVersion(expectedTopicMetadataVersion);
        validateRecreationRequest(expectedDeletedIdentity, request);
        return submit(deadline -> {
            TopicProjectionRecord deleted = readTopic(keyspace, request.managedLedgerName(), deadline)
                    .orElseThrow(() -> invariant("deleted topic projection is missing"));
            if (!expectedDeletedIdentity.equals(deleted.projectionIdentity())) {
                if (matchesCreation(deleted, request)) {
                    repairInside(keyspace, deleted, deadline);
                    return deleted;
                }
                throw new ManagedLedgerProjectionIdentityMismatchException(
                        expectedDeletedIdentity, deleted.projectionIdentity());
            }
            if (deleted.metadataVersion() != expectedTopicMetadataVersion) {
                throw conditionFailed("deleted topic metadata version changed before recreation");
            }
            if (deleted.parsedFacadeState() != ManagedLedgerFacadeState.DELETED) {
                throw invariant("only a DELETED projection can be recreated");
            }

            long ledgerId = allocateLedgerId(keyspace, deadline);
            TopicProjectionRecord candidate = withProperties(
                    topicCandidate(request, ledgerId),
                    ManagedLedgerCursorProtocol.replaceExternalProperties(
                            deleted.properties(), request.initialProperties()));
            String key = keyspace.topicProjectionKey(request.managedLedgerName());
            PartitionKey partitionKey = keyspace.topicProjectionPartitionKey(request.managedLedgerName());
            deadline.await(publishGuard.validateBeforePublish());
            Optional<Long> version = putIfVersion(
                    key, partitionKey, expectedTopicMetadataVersion, candidate, TopicProjectionRecord.class, deadline);
            TopicProjectionRecord authoritative;
            if (version.isPresent()) {
                writeObserver.afterWrite(WriteKind.TOPIC);
                authoritative = withMetadataVersion(candidate, version.orElseThrow());
            } else {
                authoritative = readTopic(keyspace, request.managedLedgerName(), deadline)
                        .orElseThrow(() -> invariant("topic projection disappeared after recreation race"));
                if (!matchesCreation(authoritative, request)) {
                    throw new ManagedLedgerProjectionIdentityMismatchException(
                            expectedDeletedIdentity, authoritative.projectionIdentity());
                }
            }
            repairInside(keyspace, authoritative, deadline);
            return authoritative;
        });
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> updateProperties(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedVersion,
            Map<String, String> properties) {
        ManagedLedgerProjectionKeyspace keyspace = new ManagedLedgerProjectionKeyspace(cluster);
        String exactName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        Objects.requireNonNull(expectedIdentity, "expectedIdentity");
        requireVersion(expectedVersion);
        Map<String, String> canonical = ProjectionCreateRequest.canonicalProperties(properties);
        return submit(deadline -> updateTopic(
                keyspace,
                exactName,
                expectedIdentity,
                expectedVersion,
                deadline,
                current -> withProperties(
                        current,
                        ManagedLedgerCursorProtocol.replaceExternalProperties(current.properties(), canonical))));
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> activateCursorProtocol(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedMetadataVersion) {
        ManagedLedgerProjectionKeyspace keyspace = new ManagedLedgerProjectionKeyspace(cluster);
        String exactName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        Objects.requireNonNull(expectedIdentity, "expectedIdentity");
        requireVersion(expectedMetadataVersion);
        return submit(deadline -> {
            boolean firstRead = true;
            long backoff = INITIAL_BACKOFF_NANOS;
            while (true) {
                TopicProjectionRecord current = readTopic(keyspace, exactName, deadline)
                        .orElseThrow(() -> invariant("topic projection is missing"));
                requireIdentity(expectedIdentity, current);
                if (ManagedLedgerCursorProtocol.isActivated(current)) {
                    return current;
                }
                if (firstRead && current.metadataVersion() != expectedMetadataVersion) {
                    throw conditionFailed("topic metadata version changed before cursor activation");
                }
                if (current.parsedFacadeState() == ManagedLedgerFacadeState.DELETING
                        || current.parsedFacadeState() == ManagedLedgerFacadeState.DELETED) {
                    throw invariant("cursor protocol cannot activate a deleting or deleted projection");
                }
                TopicProjectionRecord candidate = withProperties(
                        current, ManagedLedgerCursorProtocol.activate(current.properties()));
                Optional<Long> version = putIfVersion(
                        keyspace.topicProjectionKey(exactName),
                        keyspace.topicProjectionPartitionKey(exactName),
                        current.metadataVersion(),
                        candidate,
                        TopicProjectionRecord.class,
                        deadline);
                if (version.isPresent()) {
                    writeObserver.afterWrite(WriteKind.TOPIC);
                    return withMetadataVersion(candidate, version.orElseThrow());
                }
                firstRead = false;
                backoff = deadline.backoff(backoff);
            }
        });
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> mirrorFacadeState(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedVersion,
            ManagedLedgerFacadeState state) {
        ManagedLedgerProjectionKeyspace keyspace = new ManagedLedgerProjectionKeyspace(cluster);
        String exactName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        Objects.requireNonNull(expectedIdentity, "expectedIdentity");
        Objects.requireNonNull(state, "state");
        requireVersion(expectedVersion);
        return submit(deadline -> updateTopic(
                keyspace,
                exactName,
                expectedIdentity,
                expectedVersion,
                deadline,
                current -> withFacadeState(current, state)));
    }

    @Override
    public CompletableFuture<ProjectionRepairResult> repairProjectionIndexes(
            String cluster,
            TopicProjectionRecord authoritative) {
        ManagedLedgerProjectionKeyspace keyspace = new ManagedLedgerProjectionKeyspace(cluster);
        Objects.requireNonNull(authoritative, "authoritative");
        return submit(deadline -> {
            TopicProjectionRecord current = readTopic(
                            keyspace, authoritative.managedLedgerName(), deadline)
                    .orElseThrow(() -> invariant("cannot repair derived records without topic authority"));
            requireIdentity(authoritative.projectionIdentity(), current);
            return repairInside(keyspace, current, deadline);
        });
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            operationExecutor.shutdownNow();
        }
    }

    private TopicProjectionRecord updateTopic(
            ManagedLedgerProjectionKeyspace keyspace,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedVersion,
            Deadline deadline,
            java.util.function.UnaryOperator<TopicProjectionRecord> mutation) {
        boolean firstRead = true;
        long backoff = INITIAL_BACKOFF_NANOS;
        while (true) {
            TopicProjectionRecord current = readTopic(keyspace, managedLedgerName, deadline)
                    .orElseThrow(() -> invariant("topic projection is missing"));
            requireIdentity(expectedIdentity, current);
            if (firstRead && current.metadataVersion() != expectedVersion) {
                throw conditionFailed("topic metadata version changed before mutation");
            }
            TopicProjectionRecord candidate = mutation.apply(current);
            if (candidate == current) {
                return current;
            }
            Optional<Long> version = putIfVersion(
                    keyspace.topicProjectionKey(managedLedgerName),
                    keyspace.topicProjectionPartitionKey(managedLedgerName),
                    current.metadataVersion(),
                    candidate,
                    TopicProjectionRecord.class,
                    deadline);
            if (version.isPresent()) {
                writeObserver.afterWrite(WriteKind.TOPIC);
                return withMetadataVersion(candidate, version.orElseThrow());
            }
            firstRead = false;
            backoff = deadline.backoff(backoff);
        }
    }

    private ProjectionRepairResult repairInside(
            ManagedLedgerProjectionKeyspace keyspace,
            TopicProjectionRecord authoritative,
            Deadline deadline) {
        ManagedLedgerProjectionIdentity identity = authoritative.projectionIdentity();
        VirtualLedgerProjectionRecord virtualCandidate = new VirtualLedgerProjectionRecord(
                authoritative.managedLedgerName(),
                authoritative.managedLedgerNameHash(),
                identity,
                0,
                ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION,
                0);
        PositionIndexRecord positionCandidate = new PositionIndexRecord(
                authoritative.managedLedgerName(),
                authoritative.managedLedgerNameHash(),
                identity,
                ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION,
                ManagedLedgerProjectionNames.POSITION_FORMULA_V1,
                0);

        ProjectionRepairStatus virtualStatus = repairOne(
                keyspace.virtualLedgerProjectionKey(authoritativeStreamId(authoritative)),
                keyspace.streamPartitionKey(authoritativeStreamId(authoritative)),
                virtualCandidate,
                VirtualLedgerProjectionRecord.class,
                WriteKind.VIRTUAL_LEDGER,
                deadline);
        ProjectionRepairStatus positionStatus = repairOne(
                keyspace.positionIndexKey(authoritativeStreamId(authoritative)),
                keyspace.streamPartitionKey(authoritativeStreamId(authoritative)),
                positionCandidate,
                PositionIndexRecord.class,
                WriteKind.POSITION_INDEX,
                deadline);
        return new ProjectionRepairResult(virtualStatus, positionStatus);
    }

    private <T> ProjectionRepairStatus repairOne(
            String key,
            PartitionKey partitionKey,
            T candidate,
            Class<T> recordClass,
            WriteKind kind,
            Deadline deadline) {
        Optional<PartitionedOxiaClient.VersionedValue> existing = get(key, partitionKey, deadline);
        boolean created = false;
        if (existing.isEmpty()) {
            Optional<Long> version = putIfAbsent(key, partitionKey, candidate, recordClass, deadline);
            if (version.isPresent()) {
                created = true;
                writeObserver.afterWrite(kind);
            }
            existing = get(key, partitionKey, deadline);
        }
        T actual = existing.map(value -> decode(value, recordClass))
                .orElseThrow(() -> invariant("derived projection disappeared during repair"));
        if (!equalIgnoringMetadataVersion(candidate, actual)) {
            throw invariant("derived projection conflicts with topic authority: " + recordClass.getSimpleName());
        }
        return created ? ProjectionRepairStatus.CREATED : ProjectionRepairStatus.ALREADY_VALID;
    }

    private long allocateLedgerId(ManagedLedgerProjectionKeyspace keyspace, Deadline deadline) {
        long backoff = INITIAL_BACKOFF_NANOS;
        while (true) {
            Optional<PartitionedOxiaClient.VersionedValue> stored = get(
                    keyspace.ledgerIdAllocatorKey(), keyspace.ledgerIdAllocatorPartitionKey(), deadline);
            if (stored.isEmpty()) {
                LedgerIdAllocatorRecord initialized = new LedgerIdAllocatorRecord(
                        Math.addExact(ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID, 1), 1, 0);
                Optional<Long> version = putIfAbsent(
                        keyspace.ledgerIdAllocatorKey(), keyspace.ledgerIdAllocatorPartitionKey(), initialized,
                        LedgerIdAllocatorRecord.class, deadline);
                if (version.isPresent()) {
                    writeObserver.afterWrite(WriteKind.ALLOCATOR);
                    return ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID;
                }
            } else {
                LedgerIdAllocatorRecord current = decode(stored.orElseThrow(), LedgerIdAllocatorRecord.class);
                if (current.nextLedgerId() == Long.MAX_VALUE) {
                    throw invariant("managed-ledger virtual ledger ID allocator is exhausted");
                }
                long allocated = current.nextLedgerId();
                LedgerIdAllocatorRecord candidate = new LedgerIdAllocatorRecord(
                        Math.addExact(allocated, 1), Math.addExact(current.allocations(), 1), 0);
                Optional<Long> version = putIfVersion(
                        keyspace.ledgerIdAllocatorKey(), keyspace.ledgerIdAllocatorPartitionKey(),
                        current.metadataVersion(), candidate, LedgerIdAllocatorRecord.class, deadline);
                if (version.isPresent()) {
                    writeObserver.afterWrite(WriteKind.ALLOCATOR);
                    return allocated;
                }
            }
            backoff = deadline.backoff(backoff);
        }
    }

    private Optional<TopicProjectionRecord> readTopic(
            ManagedLedgerProjectionKeyspace keyspace,
            String managedLedgerName,
            Deadline deadline) {
        Optional<PartitionedOxiaClient.VersionedValue> value = get(
                keyspace.topicProjectionKey(managedLedgerName),
                keyspace.topicProjectionPartitionKey(managedLedgerName),
                deadline);
        if (value.isEmpty()) {
            return Optional.empty();
        }
        TopicProjectionRecord record = decode(value.orElseThrow(), TopicProjectionRecord.class);
        if (!record.managedLedgerName().equals(managedLedgerName)
                || !record.managedLedgerNameHash().equals(
                        ManagedLedgerProjectionNames.managedLedgerNameHash(managedLedgerName))) {
            throw invariant("managed-ledger topic hash collision or exact-name mismatch");
        }
        return Optional.of(record);
    }

    private void requireNoOrphanDerivedRecords(
            ManagedLedgerProjectionKeyspace keyspace,
            ProjectionCreateRequest request,
            Deadline deadline) {
        com.nereusstream.api.StreamId streamId = request.emptyStream().streamId();
        PartitionKey partitionKey = keyspace.streamPartitionKey(streamId);
        boolean derivedPresent = get(
                        keyspace.virtualLedgerProjectionKey(streamId), partitionKey, deadline).isPresent()
                || get(keyspace.positionIndexKey(streamId), partitionKey, deadline).isPresent();
        if (!derivedPresent) {
            return;
        }
        Optional<TopicProjectionRecord> raced = readTopic(keyspace, request.managedLedgerName(), deadline);
        if (raced.isEmpty()) {
            throw invariant("derived projection exists without topic authority");
        }
    }

    private Optional<PartitionedOxiaClient.VersionedValue> get(
            String key,
            PartitionKey partitionKey,
            Deadline deadline) {
        return deadline.await(client.get(key, partitionKey));
    }

    private <T> Optional<Long> putIfAbsent(
            String key,
            PartitionKey partitionKey,
            T candidate,
            Class<T> recordClass,
            Deadline deadline) {
        byte[] encoded = encode(candidate, recordClass);
        try {
            return Optional.of(deadline.await(client.putIfAbsent(key, encoded, partitionKey)).version());
        } catch (RuntimeException e) {
            if (isConditionFailure(e)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private <T> Optional<Long> putIfVersion(
            String key,
            PartitionKey partitionKey,
            long expectedVersion,
            T candidate,
            Class<T> recordClass,
            Deadline deadline) {
        byte[] encoded = encode(candidate, recordClass);
        try {
            return Optional.of(deadline.await(
                    client.putIfVersion(key, encoded, expectedVersion, partitionKey)).version());
        } catch (RuntimeException e) {
            if (isConditionFailure(e)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private <T> byte[] encode(T candidate, Class<T> recordClass) {
        byte[] encoded = MetadataRecordCodecFactory.encodeEnvelope(candidate, recordClass);
        if (encoded.length > config.maxValueBytes()) {
            throw new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "encoded F2 metadata exceeds maxValueBytes");
        }
        return encoded;
    }

    private static <T> T decode(PartitionedOxiaClient.VersionedValue stored, Class<T> recordClass) {
        try {
            T decoded = MetadataRecordCodecFactory.decodeEnvelope(stored.value(), recordClass);
            return hydrate(decoded, stored.version(), recordClass);
        } catch (MetadataCodecException | IllegalArgumentException e) {
            throw invariant("invalid durable F2 metadata record: " + recordClass.getSimpleName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T hydrate(T record, long version, Class<T> recordClass) {
        Object hydrated;
        if (record instanceof LedgerIdAllocatorRecord value) {
            hydrated = new LedgerIdAllocatorRecord(value.nextLedgerId(), value.allocations(), version);
        } else if (record instanceof TopicProjectionRecord value) {
            hydrated = withMetadataVersion(value, version);
        } else if (record instanceof VirtualLedgerProjectionRecord value) {
            hydrated = new VirtualLedgerProjectionRecord(
                    value.managedLedgerName(), value.managedLedgerNameHash(), value.identity(), value.startOffset(),
                    value.positionMappingVersion(), version);
        } else if (record instanceof PositionIndexRecord value) {
            hydrated = new PositionIndexRecord(
                    value.managedLedgerName(), value.managedLedgerNameHash(), value.identity(),
                    value.positionMappingVersion(), value.formula(), version);
        } else {
            throw new IllegalArgumentException("record does not carry F2 metadataVersion: " + recordClass.getName());
        }
        return (T) hydrated;
    }

    private static TopicProjectionRecord topicCandidate(ProjectionCreateRequest request, long ledgerId) {
        return new TopicProjectionRecord(
                request.managedLedgerName(),
                ManagedLedgerProjectionNames.managedLedgerNameHash(request.managedLedgerName()),
                request.storageClassBindingGeneration(),
                request.incarnation(),
                request.emptyStream().streamName().value(),
                request.emptyStream().streamId().value(),
                ManagedLedgerProjectionNames.STORAGE_CLASS,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                ledgerId,
                ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1,
                ManagedLedgerFacadeState.OPEN.name(),
                request.initialProperties(),
                request.emptyStream().createdAtMillis(),
                0,
                0);
    }

    private static TopicProjectionRecord withProperties(
            TopicProjectionRecord current,
            Map<String, String> properties) {
        return copyTopic(current, current.facadeState(), properties, current.stateVersion());
    }

    private static TopicProjectionRecord withFacadeState(
            TopicProjectionRecord current,
            ManagedLedgerFacadeState target) {
        ManagedLedgerFacadeState source = current.parsedFacadeState();
        if (source == target) {
            return current;
        }
        boolean allowed = (source == ManagedLedgerFacadeState.OPEN && target == ManagedLedgerFacadeState.SEALED)
                || (source == ManagedLedgerFacadeState.OPEN && target == ManagedLedgerFacadeState.DELETING)
                || (source == ManagedLedgerFacadeState.SEALED && target == ManagedLedgerFacadeState.DELETING)
                || (source == ManagedLedgerFacadeState.DELETING && target == ManagedLedgerFacadeState.DELETED);
        if (!allowed) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "illegal managed-ledger facade state transition: " + source + " -> " + target);
        }
        return copyTopic(current, target.name(), current.properties(), Math.addExact(current.stateVersion(), 1));
    }

    private static TopicProjectionRecord withMetadataVersion(TopicProjectionRecord current, long version) {
        return new TopicProjectionRecord(
                current.managedLedgerName(), current.managedLedgerNameHash(),
                current.storageClassBindingGeneration(), current.incarnation(),
                current.streamName(), current.streamId(), current.storageClass(), current.storageProfile(),
                current.virtualLedgerId(), current.positionMappingVersion(), current.payloadMapping(),
                current.facadeState(), current.properties(), current.createdAtMillis(), current.stateVersion(), version);
    }

    private static TopicProjectionRecord copyTopic(
            TopicProjectionRecord current,
            String facadeState,
            Map<String, String> properties,
            long stateVersion) {
        return new TopicProjectionRecord(
                current.managedLedgerName(), current.managedLedgerNameHash(),
                current.storageClassBindingGeneration(), current.incarnation(),
                current.streamName(), current.streamId(), current.storageClass(), current.storageProfile(),
                current.virtualLedgerId(), current.positionMappingVersion(), current.payloadMapping(),
                facadeState, properties, current.createdAtMillis(), stateVersion, 0);
    }

    private static boolean equalIgnoringMetadataVersion(Object expected, Object actual) {
        if (expected instanceof VirtualLedgerProjectionRecord left
                && actual instanceof VirtualLedgerProjectionRecord right) {
            return new VirtualLedgerProjectionRecord(
                    left.managedLedgerName(), left.managedLedgerNameHash(), left.identity(), left.startOffset(),
                    left.positionMappingVersion(), 0).equals(new VirtualLedgerProjectionRecord(
                    right.managedLedgerName(), right.managedLedgerNameHash(), right.identity(), right.startOffset(),
                    right.positionMappingVersion(), 0));
        }
        if (expected instanceof PositionIndexRecord left && actual instanceof PositionIndexRecord right) {
            return new PositionIndexRecord(
                    left.managedLedgerName(), left.managedLedgerNameHash(), left.identity(),
                    left.positionMappingVersion(), left.formula(), 0).equals(new PositionIndexRecord(
                    right.managedLedgerName(), right.managedLedgerNameHash(), right.identity(),
                    right.positionMappingVersion(), right.formula(), 0));
        }
        return false;
    }

    private static boolean matchesCreation(TopicProjectionRecord record, ProjectionCreateRequest request) {
        return record.managedLedgerName().equals(request.managedLedgerName())
                && record.storageClassBindingGeneration() == request.storageClassBindingGeneration()
                && record.incarnation() == request.incarnation()
                && record.streamName().equals(request.emptyStream().streamName().value())
                && record.streamId().equals(request.emptyStream().streamId().value())
                && record.parsedFacadeState() == ManagedLedgerFacadeState.OPEN;
    }

    private static void validateRecreationRequest(
            ManagedLedgerProjectionIdentity deleted,
            ProjectionCreateRequest request) {
        if (request.storageClassBindingGeneration() <= deleted.storageClassBindingGeneration()
                || request.incarnation() != Math.addExact(deleted.incarnation(), 1)) {
            throw new IllegalArgumentException(
                    "recreation requires the next incarnation and a newer storage-class binding generation");
        }
    }

    private static void requireIdentity(
            ManagedLedgerProjectionIdentity expected,
            TopicProjectionRecord actual) {
        if (!expected.equals(actual.projectionIdentity())) {
            throw new ManagedLedgerProjectionIdentityMismatchException(expected, actual.projectionIdentity());
        }
    }

    private static com.nereusstream.api.StreamId authoritativeStreamId(TopicProjectionRecord authoritative) {
        return new com.nereusstream.api.StreamId(authoritative.streamId());
    }

    private static void requireVersion(long version) {
        if (version < 0) {
            throw new IllegalArgumentException("expected metadata version must be non-negative");
        }
    }

    private <T> CompletableFuture<T> submit(DeadlineOperation<T> operation) {
        if (closed.get()) {
            return NereusException.failedFuture(ErrorCode.STORAGE_CLOSED, false, "projection metadata store is closed");
        }
        if (!admission.tryAcquire()) {
            return NereusException.failedFuture(
                    ErrorCode.BACKPRESSURE_REJECTED, true, "projection metadata operation bound is exhausted");
        }
        Deadline deadline = Deadline.start(config.operationTimeout());
        try {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    if (closed.get()) {
                        throw new NereusException(
                                ErrorCode.STORAGE_CLOSED, false, "projection metadata store is closed");
                    }
                    return operation.call(deadline);
                } catch (Throwable error) {
                    throw normalize(error);
                } finally {
                    admission.release();
                }
            }, operationExecutor);
        } catch (RejectedExecutionException e) {
            admission.release();
            return NereusException.failedFuture(
                    closed.get() ? ErrorCode.STORAGE_CLOSED : ErrorCode.BACKPRESSURE_REJECTED,
                    !closed.get(),
                    closed.get() ? "projection metadata store is closing" : "projection metadata queue rejected work",
                    e);
        }
    }

    private static NereusException normalize(Throwable error) {
        Throwable cause = unwrap(error);
        if (cause instanceof NereusException nereus) {
            return nereus;
        }
        if (cause instanceof MetadataCodecException) {
            return invariant("invalid durable F2 metadata", cause);
        }
        if (isConditionCause(cause)) {
            return conditionFailed("projection metadata condition failed", cause);
        }
        return new NereusException(
                ErrorCode.METADATA_UNAVAILABLE, true, "projection metadata operation failed", cause);
    }

    private static boolean isConditionFailure(Throwable error) {
        return isConditionCause(unwrap(error));
    }

    private static boolean isConditionCause(Throwable cause) {
        return cause instanceof ProjectionMetadataConditionFailedException
                || cause instanceof KeyAlreadyExistsException
                || cause instanceof UnexpectedVersionIdException;
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static NereusException conditionFailed(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException conditionFailed(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message, cause);
    }

    private static ThreadFactory namedThreadFactory() {
        AtomicInteger ids = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, "nereus-f2-metadata-" + ids.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    @FunctionalInterface
    private interface DeadlineOperation<T> {
        T call(Deadline deadline) throws Exception;
    }

    private static final class Deadline {
        private final long deadlineNanos;

        private Deadline(long deadlineNanos) {
            this.deadlineNanos = deadlineNanos;
        }

        static Deadline start(java.time.Duration timeout) {
            long timeoutNanos;
            try {
                timeoutNanos = timeout.toNanos();
            } catch (ArithmeticException ignored) {
                timeoutNanos = Long.MAX_VALUE;
            }
            long now = System.nanoTime();
            long deadline;
            try {
                deadline = Math.addExact(now, timeoutNanos);
            } catch (ArithmeticException ignored) {
                deadline = Long.MAX_VALUE;
            }
            return new Deadline(deadline);
        }

        <T> T await(CompletableFuture<T> future) {
            long remaining = remainingNanos();
            try {
                return future.get(remaining, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                throw timeout(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NereusException(
                        ErrorCode.CANCELLED, true, "projection metadata operation was interrupted", e);
            } catch (ExecutionException e) {
                throw new CompletionException(e.getCause());
            }
        }

        long backoff(long requestedNanos) {
            long remaining = remainingNanos();
            LockSupport.parkNanos(Math.min(requestedNanos, remaining));
            remainingNanos();
            return Math.min(MAX_BACKOFF_NANOS, requestedNanos * 2);
        }

        private long remainingNanos() {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                throw timeout(null);
            }
            return remaining;
        }

        private static NereusException timeout(Throwable cause) {
            return new NereusException(
                    ErrorCode.TIMEOUT, true, "projection metadata operation deadline expired", cause);
        }
    }
}
