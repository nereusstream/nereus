/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.metadata.oxia;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.AppendSessionRequest;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StableStreamHeadSnapshot;
import com.nereusstream.api.StreamCommitAnchor;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.metadata.oxia.codec.MetadataCodecException;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.CommittedAppendRecord;
import com.nereusstream.metadata.oxia.records.CommittedSliceRecord;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.ObjectReferenceRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexTargetRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamHeadRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.StreamNameRecord;
import com.nereusstream.metadata.oxia.records.StreamSliceManifestRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import com.nereusstream.metadata.oxia.records.VisibleSliceReferenceRecord;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/** Production Phase 1 metadata adapter backed by the public Oxia Java client API. */
public final class OxiaJavaClientMetadataStore implements OxiaMetadataStore {
    private static final int MAX_CAS_RETRIES = 64;

    private final OxiaClientConfiguration configuration;
    private final SharedOxiaClientRuntime runtime;
    private final boolean ownsRuntime;
    private final PartitionedOxiaClient client;
    private final F4MetadataStoreSupport f4Support;
    private final BookKeeperStableAppendProtectionValidator bookKeeperProtectionValidator;
    private final Clock clock;
    private final ExecutorService operationExecutor;
    private final CopyOnWriteArrayList<WatchRegistration> watches = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public static OxiaJavaClientMetadataStore connect(
            OxiaClientConfiguration configuration,
            Clock clock) {
        Objects.requireNonNull(configuration, "configuration");
        SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(configuration, clock);
        return new OxiaJavaClientMetadataStore(configuration, runtime, clock, true, null);
    }

    public static OxiaJavaClientMetadataStore usingSharedRuntime(
            OxiaClientConfiguration configuration,
            SharedOxiaClientRuntime runtime,
            Clock clock) {
        return new OxiaJavaClientMetadataStore(configuration, runtime, clock, false, null);
    }

    /** Installs exact BookKeeper proof validation over the same shared Oxia runtime. */
    public static OxiaJavaClientMetadataStore usingSharedRuntime(
            OxiaClientConfiguration configuration,
            SharedOxiaClientRuntime runtime,
            Clock clock,
            BookKeeperMetadataStoreConfig bookKeeperMetadataConfiguration) {
        return new OxiaJavaClientMetadataStore(
                configuration,
                runtime,
                clock,
                false,
                Objects.requireNonNull(bookKeeperMetadataConfiguration, "bookKeeperMetadataConfiguration"));
    }

    OxiaJavaClientMetadataStore(
            OxiaClientConfiguration configuration,
            SyncOxiaClient oxiaClient,
            Clock clock) {
        this(configuration, SharedOxiaClientRuntime.usingClient(configuration, oxiaClient, clock), clock, true, null);
    }

    private OxiaJavaClientMetadataStore(
            OxiaClientConfiguration configuration,
            SharedOxiaClientRuntime runtime,
            Clock clock,
            boolean ownsRuntime,
            BookKeeperMetadataStoreConfig bookKeeperMetadataConfiguration) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        runtime.requireCompatible(configuration);
        this.ownsRuntime = ownsRuntime;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.client = runtime.client();
        this.f4Support = new F4MetadataStoreSupport(client, clock);
        this.bookKeeperProtectionValidator = bookKeeperMetadataConfiguration == null
                ? null
                : new BookKeeperStableAppendProtectionValidator(
                        new OxiaJavaBookKeeperMetadataStore(client, clock, bookKeeperMetadataConfiguration),
                        bookKeeperMetadataConfiguration);
        this.operationExecutor = boundedExecutor(
                4, configuration.maxPendingOperations(), "nereus-oxia-operation");
    }

    @Override
    public CompletableFuture<StreamMetadataRecord> createOrGetStream(
            String cluster,
            StreamName streamName,
            StreamCreateOptions options) {
        return complete(() -> {
            Objects.requireNonNull(streamName, "streamName");
            Objects.requireNonNull(options, "options");
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            StreamId streamId = DeterministicIds.streamIdFor(streamName);
            Optional<StreamHeadRecord> existing = getHead(keyspace, streamId);
            if (existing.isPresent()) {
                validateStreamIdentity(existing.orElseThrow(), streamName, streamId);
                return existing.orElseThrow().toMetadataRecord();
            }
            long now = clock.millis();
            StreamHeadRecord candidate = new StreamHeadRecord(
                    streamId.value(),
                    streamName.value(),
                    DeterministicIds.streamNameHash(streamName),
                    StreamState.ACTIVE.name(),
                    options.profile().canonical().name(),
                    options.attributes(),
                    now,
                    0,
                    0,
                    0,
                    0,
                    0,
                    "",
                    AppendSessionSnapshotRecord.EMPTY,
                    0);
            StreamHeadRecord created;
            try {
                created = putIfAbsent(
                        keyspace.streamHeadKey(streamId),
                        keyspace.streamPartitionKey(streamId),
                        candidate,
                        StreamHeadRecord.class);
            } catch (BackendConditionException e) {
                requireCause(e, KeyAlreadyExistsException.class);
                created = getHead(keyspace, streamId).orElseThrow(() -> invariant("stream head disappeared"));
                validateStreamIdentity(created, streamName, streamId);
            }
            putStreamNameAuditBestEffort(keyspace, streamName, streamId, now);
            return created.toMetadataRecord();
        });
    }

    @Override
    public CompletableFuture<StreamMetadataRecord> getStream(String cluster, StreamId streamId) {
        return complete(() -> headOrThrow(new OxiaKeyspace(cluster), streamId).toMetadataRecord());
    }

    @Override
    public CompletableFuture<StreamMetadataSnapshot> getStreamSnapshot(String cluster, StreamId streamId) {
        return complete(() -> snapshot(headOrThrow(new OxiaKeyspace(cluster), streamId)));
    }

    @Override
    public CompletableFuture<StableStreamHeadSnapshot> getStableStreamHeadSnapshot(
            String cluster, StreamId streamId) {
        return complete(() -> StableStreamHeadSnapshots.from(
                headOrThrow(new OxiaKeyspace(cluster), streamId)));
    }

    @Override
    public CompletableFuture<Boolean> isCommitReachable(
            String cluster,
            StreamCommitAnchor descendant,
            String ancestorCommitId,
            long ancestorCommitVersion) {
        return complete(() -> isCommitReachableSync(
                new OxiaKeyspace(cluster), descendant, ancestorCommitId, ancestorCommitVersion));
    }

    @Override
    public CompletableFuture<Void> revalidateAppendSession(
            String cluster,
            AppendSession session) {
        return complete(() -> {
            AppendSession expected = Objects.requireNonNull(session, "session");
            StreamHeadRecord head = headOrThrow(
                    new OxiaKeyspace(cluster), expected.streamId());
            requireActive(head);
            AppendSessionSnapshotRecord current = head.appendSession();
            long now = clock.millis();
            if (current.isEmpty() || current.expiresAtMillis() <= now) {
                throw failure(
                        ErrorCode.APPEND_SESSION_EXPIRED,
                        true,
                        "append session expired before guarded object upload");
            }
            if (!current.writerId().equals(expected.writerId())
                    || current.epoch() != expected.epoch()
                    || !current.fencingToken().equals(expected.fencingToken())
                    || current.leaseVersion() < expected.leaseVersion()) {
                throw failure(
                        ErrorCode.FENCED_APPEND,
                        false,
                        "append session changed before guarded object upload");
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<AppendSessionRecord> acquireAppendSession(
            String cluster,
            StreamId streamId,
            AppendSessionOptions options) {
        return complete(() -> {
            Objects.requireNonNull(options, "options");
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
                StreamHeadRecord head = headOrThrow(keyspace, streamId);
                requireActive(head);
                AppendAuthoritySessionTransitions.requireLegacyMode(head);
                long now = clock.millis();
                AppendSessionSnapshotRecord current = head.appendSession();
                boolean empty = current.isEmpty();
                boolean expired = !empty && current.expiresAtMillis() <= now;
                boolean sameWriter = !empty && current.writerId().equals(options.writerId());
                if (!empty && !expired && !sameWriter) {
                    throw failure(ErrorCode.FENCED_APPEND, true, "append session is owned by another writer");
                }
                if (!empty && expired && !sameWriter && !options.allowStealExpiredSession()) {
                    throw failure(
                            ErrorCode.APPEND_SESSION_EXPIRED, true, "expired session steal is not allowed");
                }
                long epoch = (!empty && sameWriter && !expired) ? current.epoch() : current.epoch() + 1;
                String token = (!empty && sameWriter && !expired)
                        ? current.fencingToken()
                        : newFencingToken(streamId, options.writerId(), epoch);
                AppendSessionSnapshotRecord session = new AppendSessionSnapshotRecord(
                        options.writerId(),
                        epoch,
                        token,
                        current.leaseVersion() + 1,
                        leaseExpiration(now, options.ttl()));
                Optional<StreamHeadRecord> updated = casHead(keyspace, streamId, head, withSession(head, session));
                if (updated.isPresent()) {
                    return AppendSessionRecord.fromHead(streamId.value(), session);
                }
            }
            throw condition("append session CAS retry budget exhausted");
        });
    }

    @Override
    public CompletableFuture<AppendSessionRecord> acquireAppendSession(
            String cluster,
            StreamId streamId,
            AppendSessionRequest request) {
        Objects.requireNonNull(request, "request");
        if (request.authority().isEmpty()) {
            return acquireAppendSession(cluster, streamId, request.options());
        }
        return complete(() -> {
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
                StreamHeadRecord head = headOrThrow(keyspace, streamId);
                requireActive(head);
                long now = clock.millis();
                AppendSessionSnapshotRecord session = AppendAuthoritySessionTransitions.acquire(
                        head,
                        request.options(),
                        request.authority().orElseThrow(),
                        now,
                        leaseExpiration(now, request.options().ttl()),
                        epoch -> newFencingToken(streamId, request.options().writerId(), epoch));
                if (casHead(keyspace, streamId, head, withSession(head, session)).isPresent()) {
                    return AppendSessionRecord.fromHead(streamId.value(), session);
                }
            }
            throw condition("authority-bound append session CAS retry budget exhausted");
        });
    }

    @Override
    public CompletableFuture<AppendSessionRecord> renewAppendSession(
            String cluster,
            StreamId streamId,
            String writerId,
            long epoch,
            String fencingToken,
            Duration ttl) {
        return complete(() -> {
            Objects.requireNonNull(writerId, "writerId");
            Objects.requireNonNull(fencingToken, "fencingToken");
            Objects.requireNonNull(ttl, "ttl");
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
                StreamHeadRecord head = headOrThrow(keyspace, streamId);
                requireActive(head);
                AppendSessionSnapshotRecord current = head.appendSession();
                long now = clock.millis();
                if (current.isEmpty() || current.expiresAtMillis() <= now) {
                    throw failure(ErrorCode.APPEND_SESSION_EXPIRED, true, "append session expired");
                }
                if (!current.writerId().equals(writerId)
                        || current.epoch() != epoch
                        || !current.fencingToken().equals(fencingToken)) {
                    throw failure(ErrorCode.FENCED_APPEND, true, "append session token does not match");
                }
                AppendSessionSnapshotRecord session =
                        AppendAuthoritySessionTransitions.preserveAuthorityOnRenewal(
                                current, leaseExpiration(now, ttl));
                if (casHead(keyspace, streamId, head, withSession(head, session)).isPresent()) {
                    return AppendSessionRecord.fromHead(streamId.value(), session);
                }
            }
            throw condition("append session renewal CAS retry budget exhausted");
        });
    }

    @Override
    public CompletableFuture<Void> putObjectManifest(String cluster, ObjectManifestRecord manifest) {
        return complete(() -> {
            Phase1ObjectManifestValidator.validateNewUpload(manifest);
            ObjectId objectId = new ObjectId(manifest.objectId());
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            try {
                putIfAbsent(
                        keyspace.objectManifestKey(objectId),
                        keyspace.objectPartitionKey(objectId),
                        manifest,
                        ObjectManifestRecord.class);
            } catch (BackendConditionException e) {
                requireCause(e, KeyAlreadyExistsException.class);
                ObjectManifestRecord existing = getObjectManifestSync(keyspace, objectId)
                        .orElseThrow(() -> invariant("object manifest disappeared"));
                if ("DELETED".equals(existing.state())
                        || !Phase1ObjectManifestValidator.sameImmutableIdentity(existing, manifest)) {
                    throw invariant("object manifest conflict");
                }
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Optional<ObjectManifestRecord>> getObjectManifest(
            String cluster,
            ObjectId objectId) {
        return complete(() -> getObjectManifestSync(new OxiaKeyspace(cluster), objectId));
    }

    @Override
    public CompletableFuture<Optional<ObjectReferenceRecord>> getObjectReferences(
            String cluster,
            ObjectId objectId) {
        return complete(() -> getObjectReferencesSync(new OxiaKeyspace(cluster), objectId));
    }

    @Override
    public CompletableFuture<ObjectReferenceRecord> repairObjectReferences(
            String cluster,
            ObjectId objectId) {
        return complete(() -> repairObjectReferencesSync(new OxiaKeyspace(cluster), objectId));
    }

    @Override
    public CompletableFuture<CommitSliceResult> commitStreamSlice(
            String cluster,
            CommitSliceRequest request) {
        return completeAppend(() -> commitStreamSliceSync(new OxiaKeyspace(cluster), request));
    }

    @Override
    public CompletableFuture<PreparedStableAppend> prepareStableAppend(
            String cluster,
            CommitAppendRequest request) {
        return completeAppend(() -> prepareStableAppendSync(new OxiaKeyspace(cluster), request));
    }

    @Override
    public CompletableFuture<StableAppendResult> commitPreparedStableAppend(
            String cluster,
            PreparedStableAppend prepared,
            PhysicalReferenceProof protectionProof) {
        return completeAppend(() -> commitPreparedStableAppendSync(
                new OxiaKeyspace(cluster),
                prepared,
                protectionProof));
    }

    @Override
    public CompletableFuture<MaterializedGenerationZero> materializeGenerationZero(
            String cluster,
            ReachableCommittedAppend reachableAppend) {
        return completeAppend(() -> materializeGenerationZeroSync(new OxiaKeyspace(cluster), reachableAppend));
    }

    @Override
    public CompletableFuture<Void> revalidateMaterializedGenerationZero(
            String cluster,
            MaterializedGenerationZero materialized) {
        return completeAppend(() -> {
            revalidateMaterializedGenerationZeroSync(new OxiaKeyspace(cluster), materialized);
            return null;
        });
    }

    @Override
    public CompletableFuture<AppendReplaySearchResult> searchAppendReplay(
            String cluster,
            CommitAppendRequest request,
            Optional<AppendReplayCursor> continuation,
            int maxCommitsToScan) {
        return completeAppend(() -> searchAppendReplaySync(
                new OxiaKeyspace(cluster), request, continuation, maxCommitsToScan));
    }

    @Override
    public CompletableFuture<StreamMetadataSnapshot> transitionStreamState(
            String cluster,
            StreamStateTransitionRequest request) {
        return complete(() -> transitionStreamStateSync(new OxiaKeyspace(cluster), request));
    }

    @Override
    public CompletableFuture<DerivedIndexRepairResult> repairDerivedStreamIndexes(
            String cluster,
            StreamId streamId,
            long targetOffset,
            Optional<DerivedIndexRepairCursor> continuation,
            int maxCommitsToScan) {
        return complete(() -> repairDerivedStreamIndexesSync(
                new OxiaKeyspace(cluster),
                streamId,
                targetOffset,
                continuation,
                maxCommitsToScan));
    }

    @Override
    public CompletableFuture<AppendRecoveryTailPage> readAppendRecoveryTail(
            String cluster,
            StreamId streamId,
            AppendRecoveryAnchor anchor,
            Optional<AppendRecoveryTailCursor> continuation,
            int maxCommitsToScan) {
        return complete(() -> readAppendRecoveryTailSync(
                new OxiaKeyspace(cluster),
                streamId,
                anchor,
                continuation,
                maxCommitsToScan));
    }

    @Override
    public CompletableFuture<List<OffsetIndexEntry>> scanOffsetIndex(
            String cluster,
            StreamId streamId,
            long startOffset,
            int limit) {
        return complete(() -> {
            if (startOffset < 0 || limit <= 0) {
                throw new IllegalArgumentException("startOffset must be non-negative and limit positive");
            }
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            PartitionKey partitionKey = keyspace.streamPartitionKey(streamId);
            return join(client.rangeScan(
                            keyspace.offsetIndexScanFromExclusive(streamId, startOffset),
                            keyspace.offsetIndexScanToExclusive(streamId),
                            limit,
                            partitionKey))
                    .stream()
                    .map(value -> decodeOffsetIndexEntry(keyspace, streamId, value))
                    .filter(record -> record.range().endOffset() > startOffset)
                    .toList();
        });
    }

    @Override
    public CompletableFuture<CommittedEndOffsetRecord> getCommittedEndOffset(
            String cluster,
            StreamId streamId) {
        return complete(() -> {
            StreamHeadRecord head = headOrThrow(new OxiaKeyspace(cluster), streamId);
            return new CommittedEndOffsetRecord(
                    streamId.value(),
                    head.committedEndOffset(),
                    head.cumulativeSize(),
                    head.commitVersion(),
                    head.metadataVersion());
        });
    }

    @Override
    public CompletableFuture<TrimRecord> updateTrim(
            String cluster,
            StreamId streamId,
            long beforeOffset,
            String reason) {
        return complete(() -> {
            if (reason == null || beforeOffset < 0) {
                throw new NereusException(ErrorCode.INVALID_ARGUMENT, false, "trim reason/range is invalid");
            }
            OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
            for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
                StreamHeadRecord head = headOrThrow(keyspace, streamId);
                if (!StreamState.ACTIVE.name().equals(head.state())
                        && !StreamState.SEALED.name().equals(head.state())) {
                    throw failure(ErrorCode.STREAM_NOT_ACTIVE, false, "stream state does not allow trim");
                }
                if (beforeOffset < head.trimOffset() || beforeOffset > head.committedEndOffset()) {
                    throw failure(ErrorCode.INVALID_ARGUMENT, false, "trim offset is outside committed range");
                }
                StreamHeadRecord candidate = withTrim(head, beforeOffset);
                Optional<StreamHeadRecord> updated = casHead(keyspace, streamId, head, candidate);
                if (updated.isPresent()) {
                    return new TrimRecord(
                            streamId.value(),
                            beforeOffset,
                            reason,
                            clock.millis(),
                            updated.orElseThrow().metadataVersion());
                }
            }
            throw condition("trim CAS retry budget exhausted");
        });
    }

    @Override
    public CompletableFuture<TrimRecord> getTrim(String cluster, StreamId streamId) {
        return complete(() -> {
            StreamHeadRecord head = headOrThrow(new OxiaKeyspace(cluster), streamId);
            return new TrimRecord(
                    streamId.value(), head.trimOffset(), "", clock.millis(), head.metadataVersion());
        });
    }

    @Override
    public WatchRegistration watchStream(
            String cluster,
            StreamId streamId,
            MetadataWatcher watcher) {
        Objects.requireNonNull(watcher, "watcher");
        ensureOpen();
        OxiaKeyspace keyspace = new OxiaKeyspace(cluster);
        AtomicBoolean active = new AtomicBoolean(true);
        WatchRegistration backend = client.watchPrefix(
                keyspace.streamHeadKey(streamId),
                keyspace.streamPartitionKey(streamId),
                () -> deliverWatch(keyspace, streamId, watcher, active));
        WatchRegistration registration = () -> {
            if (active.compareAndSet(true, false)) {
                backend.close();
            }
        };
        watches.add(registration);
        return () -> {
            registration.close();
            watches.remove(registration);
        };
    }

    private void deliverWatch(
            OxiaKeyspace keyspace,
            StreamId streamId,
            MetadataWatcher watcher,
            AtomicBoolean active) {
        if (!active.get() || closed.get()) {
            return;
        }
        try {
            StreamHeadRecord head = headOrThrow(keyspace, streamId);
            watcher.onOffsetIndexUpdated(streamId, head.committedEndOffset(), head.metadataVersion());
            watcher.onTrimUpdated(streamId, head.trimOffset(), head.metadataVersion());
            if (!head.appendSession().isEmpty()) {
                watcher.onAppendSessionChanged(
                        streamId,
                        head.appendSession().epoch(),
                        head.appendSession().leaseVersion());
            }
        } catch (RuntimeException ignored) {
            // Watch delivery is an invalidation hint and never changes mutation results.
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        f4Support.close();
        watches.forEach(WatchRegistration::close);
        watches.clear();
        operationExecutor.shutdown();
        if (ownsRuntime) {
            runtime.close();
        }
    }

    private PreparedStableAppend prepareStableAppendSync(
            OxiaKeyspace keyspace,
            CommitAppendRequest request) {
        Objects.requireNonNull(request, "request");
        String commitId = request.commitId();
        ReachableCommittedAppend markerReplay = findGenericMarkerReplay(keyspace, request);
        if (markerReplay != null) {
            return preparedFromDurable(
                    keyspace,
                    request,
                    requireDurableTargetCommit(keyspace, request.streamId(), commitId),
                    true);
        }
        StreamHeadRecord initialHead = headOrThrow(keyspace, request.streamId());
        if (initialHead.committedEndOffset() > request.expectedStartOffset()) {
            AppendReplaySearchResult replay = searchAppendReplaySync(keyspace, request, Optional.empty(),
                    configuration.maxCommitChainScan());
            if (replay.status() == AppendReplayStatus.FOUND) {
                return preparedFromDurable(
                        keyspace,
                        request,
                        requireDurableTargetCommit(keyspace, request.streamId(), commitId),
                        true);
            }
            if (replay.status() == AppendReplayStatus.CONTINUE) {
                throw appendFailure(ErrorCode.METADATA_CONDITION_FAILED, true,
                        AppendOutcome.MAY_HAVE_COMMITTED, "append replay requires paged recovery");
            }
        }
        validateCommitPreconditions(initialHead, request);
        StreamCommitTargetRecord candidate = buildTargetCommit(request, initialHead);
        String key = keyspace.streamCommitKey(request.streamId(), commitId);
        DurableRecord<StreamCommitTargetRecord> durable;
        try {
            durable = putIfAbsentDurable(
                    key,
                    keyspace.streamPartitionKey(request.streamId()),
                    candidate,
                    StreamCommitTargetRecord.class);
        } catch (BackendConditionException e) {
            requireCause(e, KeyAlreadyExistsException.class);
            durable = requireDurableTargetCommit(keyspace, request.streamId(), commitId);
        }
        validateReplay(request, durable.value(), AppendOutcome.KNOWN_NOT_COMMITTED);
        StreamHeadRecord currentHead = headOrThrow(keyspace, request.streamId());
        if (!sameCommitAnchor(initialHead, currentHead)) {
            AppendReplaySearchResult replay = searchAppendReplaySync(
                    keyspace, request, Optional.empty(), configuration.maxCommitChainScan());
            if (replay.status() == AppendReplayStatus.FOUND) {
                return preparedFromDurable(keyspace, request, durable, true);
            }
            if (replay.status() == AppendReplayStatus.CONTINUE) {
                throw appendFailure(ErrorCode.METADATA_CONDITION_FAILED, true,
                        AppendOutcome.MAY_HAVE_COMMITTED, "append replay requires paged recovery");
            }
        }
        validateCommitPreconditions(currentHead, request);
        validateCommitAgainstHead(
                request,
                durable.value(),
                currentHead,
                AppendOutcome.KNOWN_NOT_COMMITTED);
        return preparedFromDurable(keyspace, request, durable, false);
    }

    private StableAppendResult commitPreparedStableAppendSync(
            OxiaKeyspace keyspace,
            PreparedStableAppend prepared,
            PhysicalReferenceProof protectionProof) {
        PreparedStableAppend exact = Objects.requireNonNull(prepared, "prepared");
        CommitAppendRequest request = exact.request();
        DurableRecord<StreamCommitTargetRecord> initialIntent = requireExactPreparedIntent(
                keyspace,
                exact);
        validateStableAppendProtection(
                keyspace,
                exact,
                protectionProof);
        ReachableCommittedAppend markerReplay = findGenericMarkerReplay(keyspace, request);
        if (markerReplay != null) {
            return new StableAppendResult(markerReplay, false);
        }
        StreamHeadRecord initialHead = headOrThrow(keyspace, request.streamId());
        if (initialHead.committedEndOffset() > request.expectedStartOffset()) {
            AppendReplaySearchResult replay = searchAppendReplaySync(
                    keyspace,
                    request,
                    Optional.empty(),
                    configuration.maxCommitChainScan());
            if (replay.status() == AppendReplayStatus.FOUND) {
                return new StableAppendResult(replay.committedAppend().orElseThrow(), false);
            }
            if (replay.status() == AppendReplayStatus.CONTINUE) {
                throw appendFailure(ErrorCode.METADATA_CONDITION_FAILED, true,
                        AppendOutcome.MAY_HAVE_COMMITTED, "append replay requires paged recovery");
            }
        }
        validateCommitPreconditions(initialHead, request);
        validateCommitAgainstHead(
                request,
                initialIntent.value(),
                initialHead,
                AppendOutcome.KNOWN_NOT_COMMITTED);
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            DurableRecord<StreamCommitTargetRecord> intent = requireExactPreparedIntent(
                    keyspace,
                    exact);
            validateStableAppendProtection(
                    keyspace,
                    exact,
                    protectionProof);
            StreamHeadRecord head = headOrThrow(keyspace, request.streamId());
            if (!sameCommitAnchor(initialHead, head)) {
                AppendReplaySearchResult replay = searchAppendReplaySync(
                        keyspace, request, Optional.empty(), configuration.maxCommitChainScan());
                if (replay.status() == AppendReplayStatus.FOUND) {
                    return new StableAppendResult(replay.committedAppend().orElseThrow(), false);
                }
                if (replay.status() == AppendReplayStatus.CONTINUE) {
                    throw appendFailure(ErrorCode.METADATA_CONDITION_FAILED, true,
                            AppendOutcome.MAY_HAVE_COMMITTED, "append replay requires paged recovery");
                }
            }
            validateCommitPreconditions(head, request);
            validateCommitAgainstHead(request, intent.value(), head, AppendOutcome.KNOWN_NOT_COMMITTED);
            Optional<StreamHeadRecord> updated = casHeadForCommit(
                    keyspace,
                    request.streamId(),
                    head,
                    withCommit(head, intent.value()));
            if (updated.isPresent()) {
                StreamHeadRecord anchored = updated.orElseThrow();
                return new StableAppendResult(
                        reachable(intent.value(), request.projectionRef(), anchored),
                        true);
            }
        }
        throw appendFailure(ErrorCode.METADATA_CONDITION_FAILED, true, AppendOutcome.MAY_HAVE_COMMITTED,
                "generic stream-head CAS retry budget exhausted");
    }

    private MaterializedGenerationZero materializeGenerationZeroSync(
            OxiaKeyspace keyspace, ReachableCommittedAppend reachableAppend) {
        Objects.requireNonNull(reachableAppend, "reachableAppend");
        CommittedAppend append = reachableAppend.committedAppend();
        Object stored = getCommitValue(keyspace, append.streamId(), append.commitId())
                .orElseThrow(() -> invariant("reachable generic commit is missing"));
        if (!(stored instanceof StreamCommitTargetRecord commit)) {
            throw invariant("reachable generic commit has an unexpected record type");
        }
        if (!toCommitted(commit, append.projectionRef()).equals(append)) {
            throw invariant("reachable generic commit proof no longer matches durable bytes");
        }
        requireReachable(keyspace, append);
        OffsetIndexTargetRecord index = new OffsetIndexTargetRecord(
                commit.streamId(), commit.offsetStart(), commit.offsetEnd(), commit.generation(),
                commit.cumulativeSize(), commit.readTarget(), commit.payloadFormat(), commit.recordCount(),
                commit.entryCount(), commit.logicalBytes(), commit.schemaRefs(), commit.projectionRef(),
                commit.minEventTimeMillis(), commit.maxEventTimeMillis(), commit.commitVersion(), false, 0);
        String indexKey = keyspace.offsetIndexKey(append.streamId(), append.range().endOffset(), 0);
        DurableRecord<OffsetIndexTargetRecord> durableIndex = putOrCompareDurable(
                indexKey,
                keyspace.streamPartitionKey(append.streamId()),
                index,
                OffsetIndexTargetRecord.class);
        CommittedAppendRecord marker = new CommittedAppendRecord(
                commit.streamId(), commit.commitId(), commit.offsetStart(), commit.offsetEnd(), commit.generation(),
                commit.commitVersion(), commit.readTarget().identityChecksumValue(), 0);
        putOrCompare(keyspace.committedAppendKey(append.streamId(), append.commitId()),
                keyspace.streamPartitionKey(append.streamId()), marker, CommittedAppendRecord.class);
        return new MaterializedGenerationZero(
                append,
                indexKey,
                durableIndex.metadataVersion(),
                durableIndex.durableValueSha256());
    }

    private void revalidateMaterializedGenerationZeroSync(
            OxiaKeyspace keyspace,
            MaterializedGenerationZero materialized) {
        MaterializedGenerationZero exact = Objects.requireNonNull(materialized, "materialized");
        CommittedAppend append = exact.committedAppend();
        String expectedIndexKey = keyspace.offsetIndexKey(
                append.streamId(),
                append.range().endOffset(),
                0);
        if (!exact.indexKey().equals(expectedIndexKey)) {
            throw invariant("generation-zero proof contains a non-canonical index key");
        }
        DurableRecord<StreamCommitTargetRecord> durableCommit = requireDurableTargetCommit(
                keyspace,
                append.streamId(),
                append.commitId());
        StreamCommitTargetRecord commit = durableCommit.value();
        if (!toCommitted(commit, append.projectionRef()).equals(append)) {
            throw invariant("generation-zero proof no longer matches its commit intent");
        }
        requireReachable(keyspace, append);
        DurableRecord<OffsetIndexTargetRecord> durableIndex = getDurableRecord(
                        expectedIndexKey,
                        keyspace.streamPartitionKey(append.streamId()),
                        OffsetIndexTargetRecord.class)
                .orElseThrow(() -> invariant("generation-zero index disappeared during protection"));
        if (durableIndex.metadataVersion() != exact.indexMetadataVersion()
                || !durableIndex.durableValueSha256().equals(exact.indexRecordSha256())
                || !hydrate(durableIndex.value(), 0, OffsetIndexTargetRecord.class)
                        .equals(generationZeroIndex(commit))) {
            throw invariant("generation-zero index changed during protection");
        }
    }

    private PreparedStableAppend preparedFromDurable(
            OxiaKeyspace keyspace,
            CommitAppendRequest request,
            DurableRecord<StreamCommitTargetRecord> durable,
            boolean replayWasReachable) {
        String expectedKey = keyspace.streamCommitKey(request.streamId(), request.commitId());
        if (!durable.key().equals(expectedKey)) {
            throw invariant("prepared generic commit key is non-canonical");
        }
        validateReplay(request, durable.value(), AppendOutcome.KNOWN_NOT_COMMITTED);
        return new PreparedStableAppend(
                request,
                request.commitId(),
                durable.key(),
                durable.metadataVersion(),
                durable.durableValueSha256(),
                com.nereusstream.api.ReadTargetIdentities.sha256(request.readTarget()),
                replayWasReachable);
    }

    private DurableRecord<StreamCommitTargetRecord> requireDurableTargetCommit(
            OxiaKeyspace keyspace,
            StreamId streamId,
            String commitId) {
        String key = keyspace.streamCommitKey(streamId, commitId);
        Optional<DurableRecord<StreamCommitTargetRecord>> target = getDurableRecord(
                key,
                keyspace.streamPartitionKey(streamId),
                StreamCommitTargetRecord.class);
        if (target.isPresent()) {
            return target.orElseThrow();
        }
        Optional<PartitionedOxiaClient.VersionedValue> raw = join(
                client.get(key, keyspace.streamPartitionKey(streamId)));
        if (raw.isPresent()) {
            throw invariant("generic commit ID conflicts with a legacy record");
        }
        throw invariant("generic commit intent is absent");
    }

    private DurableRecord<StreamCommitTargetRecord> requireExactPreparedIntent(
            OxiaKeyspace keyspace,
            PreparedStableAppend prepared) {
        if (!prepared.commitId().equals(prepared.request().commitId())
                || !prepared.commitKey().equals(keyspace.streamCommitKey(
                        prepared.request().streamId(),
                        prepared.commitId()))) {
            throw invariant("prepared stable append identity is non-canonical");
        }
        DurableRecord<StreamCommitTargetRecord> durable = requireDurableTargetCommit(
                keyspace,
                prepared.request().streamId(),
                prepared.commitId());
        validateReplay(
                prepared.request(),
                durable.value(),
                AppendOutcome.KNOWN_NOT_COMMITTED);
        if (durable.metadataVersion() != prepared.commitMetadataVersion()
                || !durable.durableValueSha256().equals(prepared.commitRecordSha256())) {
            throw invariant("prepared stable append intent changed before head commit");
        }
        return durable;
    }

    private void validateStableAppendProtection(
            OxiaKeyspace keyspace,
            PreparedStableAppend prepared,
            PhysicalReferenceProof protectionProof) {
        PhysicalReferenceProof genericProof = Objects.requireNonNull(
                protectionProof,
                "protectionProof");
        if (genericProof instanceof BookKeeperPhysicalReferenceProof proof) {
            validateBookKeeperStableAppendProtection(keyspace, prepared, proof);
            return;
        }
        if (!(genericProof instanceof ObjectPhysicalReferenceProof proof)) {
            throw invariant("primary physical-reference proof type is not installed");
        }
        if (proof.purpose() != PhysicalReferencePurpose.REACHABLE_APPEND
                || proof.targetType() != prepared.request().readTarget().type()
                || !proof.targetIdentitySha256().equals(prepared.primaryTargetIdentitySha256())) {
            throw invariant("stable append physical-reference proof is non-canonical");
        }
        ObjectProtectionIdentity identity = proof.protectionIdentity();
        long rootMetadataVersion = proof.rootMetadataVersion();
        long rootLifecycleEpoch = proof.rootLifecycleEpoch();
        long protectionMetadataVersion = proof.protectionMetadataVersion();
        Checksum protectionSha = F4ValueValidation.sha256(
                proof.protectionRecordSha256(),
                "protectionRecordSha256");
        if (rootMetadataVersion < 0 || rootLifecycleEpoch <= 0 || protectionMetadataVersion < 0) {
            throw new IllegalArgumentException("stable append protection versions are invalid");
        }
        String expectedReferenceId = reachableAppendReferenceId(prepared);
        if (!identity.object().equals(prepared.objectKeyHash())
                || identity.type() != ObjectProtectionType.REACHABLE_APPEND
                || !identity.referenceId().equals(expectedReferenceId)) {
            throw invariant("stable append protection identity is non-canonical");
        }
        F4Keyspace f4Keys = new F4Keyspace(keyspace.cluster());
        F4MetadataStoreSupport.Decoded<PhysicalObjectRootRecord> root = join(f4Support.get(
                        f4Keys.physicalRootKey(prepared.objectKeyHash()),
                        f4Keys.physicalObjectPartitionKey(prepared.objectKeyHash()),
                        PhysicalObjectRootRecord.class))
                .orElseThrow(() -> invariant("physical Object WAL root is absent before head commit"));
        PhysicalObjectRootRecord rootValue = root.value();
        if (root.version() != rootMetadataVersion
                || rootValue.lifecycle() != PhysicalObjectLifecycle.ACTIVE
                || rootValue.lifecycleEpoch() != rootLifecycleEpoch
                || !rootValue.objectKeyHash().equals(prepared.objectKeyHash().value())) {
            throw invariant("physical Object WAL root changed before head commit");
        }
        requireRootMatchesPreparedTarget(keyspace, prepared, rootValue);
        F4MetadataStoreSupport.Decoded<ObjectProtectionRecord> protection = join(f4Support.get(
                        f4Keys.protectionKey(
                                identity.object(),
                                identity.type(),
                                identity.referenceId()),
                        f4Keys.physicalObjectPartitionKey(identity.object()),
                        ObjectProtectionRecord.class))
                .orElseThrow(() -> invariant("REACHABLE_APPEND protection is absent before head commit"));
        ObjectProtectionRecord protectionValue = protection.value();
        if (protection.version() != protectionMetadataVersion
                || !protection.durableSha256().equals(protectionSha)
                || protectionValue.protectionTypeId() != ObjectProtectionType.REACHABLE_APPEND.wireId()
                || !protectionValue.objectKeyHash().equals(prepared.objectKeyHash().value())
                || !protectionValue.referenceId().equals(expectedReferenceId)
                || !protectionValue.ownerKey().equals(prepared.commitKey())
                || protectionValue.ownerMetadataVersion() != prepared.commitMetadataVersion()
                || !protectionValue.ownerIdentitySha256().equals(prepared.commitRecordSha256().value())
                || protectionValue.rootLifecycleEpoch() != rootLifecycleEpoch
                || protectionValue.expiresAtMillis() != 0) {
            throw invariant("REACHABLE_APPEND protection changed before head commit");
        }
    }

    private void validateBookKeeperStableAppendProtection(
            OxiaKeyspace keyspace,
            PreparedStableAppend prepared,
            BookKeeperPhysicalReferenceProof proof) {
        if (bookKeeperProtectionValidator == null) {
            throw invariant("BookKeeper physical-reference proof validation is not installed");
        }
        join(bookKeeperProtectionValidator.validate(keyspace.cluster(), prepared, proof));
    }

    private void requireRootMatchesPreparedTarget(
            OxiaKeyspace keyspace,
            PreparedStableAppend prepared,
            PhysicalObjectRootRecord root) {
        if (!(prepared.request().readTarget() instanceof ObjectSliceReadTarget target)) {
            throw invariant("prepared stable append target is not an object slice");
        }
        ObjectManifestRecord manifest = getObjectManifestSync(keyspace, target.objectId())
                .orElseThrow(() -> invariant("Object WAL manifest is absent before head commit"));
        Phase1ObjectManifestValidator.validateStoredManifest(manifest);
        long targetEnd;
        try {
            targetEnd = Math.addExact(target.objectOffset(), target.objectLength());
        } catch (ArithmeticException overflow) {
            throw invariant("Object WAL target range overflows", overflow);
        }
        String expectedContentSha = "SHA256".equals(manifest.objectChecksumType())
                ? manifest.objectChecksumValue()
                : "";
        if (!root.objectKey().equals(target.objectKey().value())
                || !root.objectId().equals(target.objectId().value())
                || root.objectKindId() != 1
                || root.objectLength() != manifest.objectLength()
                || targetEnd > root.objectLength()
                || !root.storageChecksumType().equals(manifest.storageChecksumType())
                || !root.storageChecksumValue().equals(manifest.storageChecksumValue())
                || !root.contentSha256().equals(expectedContentSha)) {
            throw invariant("physical Object WAL root conflicts with manifest/target identity");
        }
    }

    private static String reachableAppendReferenceId(PreparedStableAppend prepared) {
        return "ra1-" + DeterministicIds.stableHashComponent(
                prepared.request().streamId().value()
                        + prepared.commitId()
                        + prepared.objectKeyHash().value());
    }

    private static OffsetIndexTargetRecord generationZeroIndex(StreamCommitTargetRecord commit) {
        return new OffsetIndexTargetRecord(
                commit.streamId(),
                commit.offsetStart(),
                commit.offsetEnd(),
                commit.generation(),
                commit.cumulativeSize(),
                commit.readTarget(),
                commit.payloadFormat(),
                commit.recordCount(),
                commit.entryCount(),
                commit.logicalBytes(),
                commit.schemaRefs(),
                commit.projectionRef(),
                commit.minEventTimeMillis(),
                commit.maxEventTimeMillis(),
                commit.commitVersion(),
                false,
                0);
    }

    private void requireReachable(OxiaKeyspace keyspace, CommittedAppend expected) {
        StreamHeadRecord head = headOrThrow(keyspace, expected.streamId());
        String commitId = head.lastCommitId();
        ChainExpectation expectation = expectationFromHead(head);
        int scanned = 0;
        while (!commitId.isEmpty() && scanned++ < configuration.maxCommitChainScan()) {
            Object durable = getCommitValue(keyspace, expected.streamId(), commitId)
                    .orElseThrow(() -> invariant("reachability proof found a broken chain"));
            AnyCommit view = anyCommit(durable);
            validateAnyCommit(expected.streamId(), commitId, view, expectation);
            if (commitId.equals(expected.commitId())) return;
            if (view.offsetStart() <= expected.range().startOffset()) break;
            commitId = view.previousCommitId();
            expectation = new ChainExpectation(
                    view.offsetStart(), view.cumulativeSize() - view.logicalBytes(), view.commitVersion() - 1);
        }
        throw invariant("commit is not reachable from the current append-only head");
    }

    private <T> void putOrCompare(String key, PartitionKey partitionKey, T candidate, Class<T> type) {
        try {
            putIfAbsent(key, partitionKey, candidate, type);
        } catch (BackendConditionException e) {
            requireCause(e, KeyAlreadyExistsException.class);
            T existing = getRecord(key, partitionKey, type)
                    .orElseThrow(() -> invariant("derived record disappeared after put conflict"));
            if (!hydrate(existing, 0, type).equals(hydrate(candidate, 0, type))) {
                throw invariant("derived generic record conflicts with durable bytes");
            }
        }
    }

    private AppendReplaySearchResult searchAppendReplaySync(
            OxiaKeyspace keyspace, CommitAppendRequest request,
            Optional<AppendReplayCursor> continuation, int maxCommitsToScan) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(continuation, "continuation");
        if (maxCommitsToScan <= 0) throw new IllegalArgumentException("maxCommitsToScan must be positive");
        StreamHeadRecord currentHead = headOrThrow(keyspace, request.streamId());
        String observedId = currentHead.lastCommitId();
        long observedEnd = currentHead.committedEndOffset();
        long observedSize = currentHead.cumulativeSize();
        long observedVersion = currentHead.commitVersion();
        String nextId = observedId;
        ChainExpectation expectation = new ChainExpectation(observedEnd, observedSize, observedVersion);
        if (continuation.isPresent()) {
            AppendReplayCursor cursor = continuation.orElseThrow();
            if (!cursor.streamId().equals(request.streamId()) || !cursor.commitId().equals(request.commitId())
                    || cursor.expectedStartOffset() != request.expectedStartOffset()
                    || cursor.observedHeadCommitVersion() > currentHead.commitVersion()) {
                throw invariant("append replay continuation does not match request/head");
            }
            Object anchorValue = getCommitValue(keyspace, request.streamId(), cursor.observedHeadCommitId())
                    .orElseThrow(() -> invariant("append replay continuation anchor is missing"));
            AnyCommit anchor = anyCommit(anchorValue);
            if (anchor.offsetEnd() != cursor.observedHeadOffsetEnd()
                    || anchor.cumulativeSize() != cursor.observedHeadCumulativeSize()
                    || anchor.commitVersion() != cursor.observedHeadCommitVersion()) {
                throw invariant("append replay continuation anchor changed");
            }
            observedId = cursor.observedHeadCommitId(); observedEnd = cursor.observedHeadOffsetEnd();
            observedSize = cursor.observedHeadCumulativeSize(); observedVersion = cursor.observedHeadCommitVersion();
            nextId = cursor.nextCommitId();
            expectation = new ChainExpectation(cursor.nextOffsetEnd(), cursor.nextCumulativeSize(), cursor.nextCommitVersion());
        }
        int scanned = 0;
        while (!nextId.isEmpty() && scanned < maxCommitsToScan) {
            Object durable = getCommitValue(keyspace, request.streamId(), nextId)
                    .orElseThrow(() -> invariant("broken mixed-version commit chain"));
            AnyCommit view = anyCommit(durable);
            validateAnyCommit(request.streamId(), nextId, view, expectation);
            scanned++;
            if (nextId.equals(request.commitId())) {
                if (!(durable instanceof StreamCommitTargetRecord target)) {
                    throw invariant("generic commit ID resolved to legacy bytes");
                }
                validateReplay(request, target, AppendOutcome.KNOWN_COMMITTED);
                CommittedAppend append = toCommitted(target, request.projectionRef());
                return new AppendReplaySearchResult(AppendReplayStatus.FOUND,
                        Optional.of(ReachableCommittedAppend.verified(
                                append, observedId, observedEnd, observedSize, observedVersion)),
                        Optional.empty(), scanned);
            }
            if (view.offsetStart() <= request.expectedStartOffset()) {
                return new AppendReplaySearchResult(
                        AppendReplayStatus.PROVEN_NOT_COMMITTED, Optional.empty(), Optional.empty(), scanned);
            }
            expectation = new ChainExpectation(view.offsetStart(),
                    view.cumulativeSize() - view.logicalBytes(), view.commitVersion() - 1);
            nextId = view.previousCommitId();
        }
        if (nextId.isEmpty()) {
            return new AppendReplaySearchResult(
                    AppendReplayStatus.PROVEN_NOT_COMMITTED, Optional.empty(), Optional.empty(), scanned);
        }
        AppendReplayCursor cursor = new AppendReplayCursor(
                request.streamId(), request.commitId(), request.expectedStartOffset(), observedId, observedEnd,
                observedSize, observedVersion, nextId, expectation.offsetEnd(), expectation.cumulativeSize(),
                expectation.commitVersion());
        return new AppendReplaySearchResult(
                AppendReplayStatus.CONTINUE, Optional.empty(), Optional.of(cursor), scanned);
    }

    private AppendRecoveryTailPage readAppendRecoveryTailSync(
            OxiaKeyspace keyspace,
            StreamId streamId,
            AppendRecoveryAnchor anchor,
            Optional<AppendRecoveryTailCursor> continuation,
            int maxCommitsToScan) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(anchor, "anchor");
        Objects.requireNonNull(continuation, "continuation");
        if (!anchor.streamId().equals(streamId)) {
            throw new IllegalArgumentException("append recovery anchor belongs to another stream");
        }
        F4MetadataStoreSupport.requirePageLimit(maxCommitsToScan);

        StreamHeadRecord currentHead = headOrThrow(keyspace, streamId);
        if (anchor.commitVersion() > currentHead.commitVersion()
                || anchor.offsetEnd() > currentHead.committedEndOffset()
                || anchor.cumulativeSize() > currentHead.cumulativeSize()) {
            throw invariant("append recovery anchor is ahead of the current stream head");
        }
        AppendRecoveryHead observedHead = recoveryHead(streamId, currentHead);
        String nextCommitId = currentHead.lastCommitId();
        ChainExpectation expectation = expectationFromHead(currentHead);
        if (continuation.isPresent()) {
            AppendRecoveryTailCursor cursor = continuation.orElseThrow();
            validateRecoveryTailCursor(keyspace, streamId, anchor, currentHead, cursor);
            observedHead = cursor.observedHead();
            nextCommitId = cursor.nextCommitId();
            expectation = new ChainExpectation(
                    cursor.nextOffsetEnd(),
                    cursor.nextCumulativeSize(),
                    cursor.nextCommitVersion());
        }

        List<AppendRecoveryCommit> commits = new ArrayList<>(maxCommitsToScan);
        while (commits.size() < maxCommitsToScan) {
            if (nextCommitId.equals(anchor.lastCommitId())) {
                requireRecoveryBridge(anchor, expectation);
                return new AppendRecoveryTailPage(
                        anchor,
                        observedHead,
                        commits,
                        true,
                        Optional.empty());
            }
            if (nextCommitId.isEmpty()) {
                if (!anchor.isGenesis()) {
                    throw invariant("live commit tail ended before the recovery anchor");
                }
                requireRecoveryBridge(anchor, expectation);
                return new AppendRecoveryTailPage(
                        anchor,
                        observedHead,
                        commits,
                        true,
                        Optional.empty());
            }
            PartitionedOxiaClient.VersionedValue raw = join(client.get(
                            keyspace.streamCommitKey(streamId, nextCommitId),
                            keyspace.streamPartitionKey(streamId)))
                    .orElseThrow(() -> invariant("anchor-aware walk found a missing live commit"));
            AppendRecoveryCommit commit = recoveryCommit(keyspace, raw, streamId, nextCommitId);
            StreamCommitTargetRecord value = commit.canonicalCommit();
            AnyCommit view = anyCommit(value);
            validateAnyCommit(streamId, nextCommitId, view, expectation);
            ChainExpectation previous = predecessorExpectation(view);
            commits.add(commit);
            nextCommitId = view.previousCommitId();
            expectation = previous;
        }

        if (nextCommitId.equals(anchor.lastCommitId())) {
            requireRecoveryBridge(anchor, expectation);
            return new AppendRecoveryTailPage(
                    anchor,
                    observedHead,
                    commits,
                    true,
                    Optional.empty());
        }
        AppendRecoveryTailCursor cursor = new AppendRecoveryTailCursor(
                streamId,
                anchor,
                observedHead,
                nextCommitId,
                expectation.offsetEnd(),
                expectation.cumulativeSize(),
                expectation.commitVersion());
        return new AppendRecoveryTailPage(
                anchor,
                observedHead,
                commits,
                false,
                Optional.of(cursor));
    }

    private AppendRecoveryCommit recoveryCommit(
            OxiaKeyspace keyspace,
            PartitionedOxiaClient.VersionedValue raw,
            StreamId streamId,
            String commitId) {
        String expectedKey = keyspace.streamCommitKey(streamId, commitId);
        if (!raw.key().equals(expectedKey)) {
            throw invariant("append recovery commit key escaped its stream scope");
        }
        String type = MetadataRecordCodecFactory.recordType(raw.value());
        StreamCommitTargetRecord canonical;
        AppendRecoveryCommitEncoding encoding;
        if (type.equals(StreamCommitTargetRecord.class.getSimpleName())) {
            StreamCommitTargetRecord decoded = decode(raw, StreamCommitTargetRecord.class);
            canonical = hydrate(decoded, 0, StreamCommitTargetRecord.class);
            encoding = AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1;
        } else if (type.equals(StreamCommitRecord.class.getSimpleName())) {
            StreamCommitRecord decoded = decode(raw, StreamCommitRecord.class);
            canonical = canonicalRecoveryCommit(decoded);
            encoding = AppendRecoveryCommitEncoding.LEGACY_STREAM_COMMIT_V1;
        } else {
            throw invariant("append recovery commit key contains an unsupported record type");
        }
        byte[] canonicalBytes = MetadataRecordCodecFactory.encodeEnvelope(
                canonical, StreamCommitTargetRecord.class);
        return new AppendRecoveryCommit(
                raw.key(),
                encoding,
                canonical,
                raw.version(),
                sha256(raw.value()),
                ByteBuffer.wrap(canonicalBytes),
                sha256(canonicalBytes));
    }

    private static StreamCommitTargetRecord canonicalRecoveryCommit(StreamCommitRecord value) {
        EntryIndexReferenceRecord rawIndex = value.entryIndexRef();
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.valueOf(rawIndex.location()),
                rawIndex.objectId().isEmpty()
                        ? Optional.empty()
                        : Optional.of(new ObjectId(rawIndex.objectId())),
                rawIndex.objectKey().isEmpty()
                        ? Optional.empty()
                        : Optional.of(new ObjectKey(rawIndex.objectKey())),
                rawIndex.inlineData().length == 0
                        ? Optional.empty()
                        : Optional.of(rawIndex.inlineData()),
                rawIndex.offset(),
                rawIndex.length(),
                new Checksum(
                        ChecksumType.valueOf(rawIndex.checksumType()),
                        rawIndex.checksumValue()));
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                new ObjectId(value.objectId()),
                new ObjectKey(value.objectKey()),
                ObjectType.valueOf(value.objectType()),
                value.physicalFormat(),
                value.logicalFormat(),
                value.sliceId(),
                value.objectOffset(),
                value.objectLength(),
                new Checksum(
                        ChecksumType.valueOf(value.sliceChecksumType()),
                        value.sliceChecksumValue()),
                index);
        ReadTargetRecord readTarget = ReadTargetCodecRegistry.phase15().encode(target);
        return new StreamCommitTargetRecord(
                value.streamId(),
                value.commitId(),
                value.previousCommitId(),
                value.offsetStart(),
                value.offsetEnd(),
                0,
                value.cumulativeSize(),
                value.commitVersion(),
                value.writerId(),
                value.writerRunIdHash(),
                value.writerEpoch(),
                value.fencingTokenHash(),
                readTarget,
                value.payloadFormat(),
                value.recordCount(),
                value.entryCount(),
                value.logicalBytes(),
                value.schemaRefs(),
                value.projectionRef(),
                value.minEventTimeMillis(),
                value.maxEventTimeMillis(),
                value.preparedAtMillis(),
                0);
    }

    private void validateRecoveryTailCursor(
            OxiaKeyspace keyspace,
            StreamId streamId,
            AppendRecoveryAnchor anchor,
            StreamHeadRecord currentHead,
            AppendRecoveryTailCursor cursor) {
        if (!cursor.streamId().equals(streamId)
                || !cursor.anchor().equals(anchor)
                || cursor.observedHead().commitVersion() > currentHead.commitVersion()) {
            throw invariant("append recovery continuation does not match its stream/root/head");
        }
        AppendRecoveryHead observed = cursor.observedHead();
        if (observed.lastCommitId().isEmpty()) {
            throw invariant("a non-terminal append recovery cursor cannot have an empty observed head");
        }
        Object value = getCommitValue(keyspace, streamId, observed.lastCommitId())
                .orElseThrow(() -> invariant("append recovery continuation head is missing"));
        AnyCommit commit = anyCommit(value);
        if (commit.offsetEnd() != observed.offsetEnd()
                || commit.cumulativeSize() != observed.cumulativeSize()
                || commit.commitVersion() != observed.commitVersion()) {
            throw invariant("append recovery continuation head changed");
        }
    }

    private static AppendRecoveryHead recoveryHead(StreamId streamId, StreamHeadRecord head) {
        return new AppendRecoveryHead(
                streamId,
                head.lastCommitId(),
                head.committedEndOffset(),
                head.cumulativeSize(),
                head.commitVersion(),
                head.metadataVersion());
    }

    private static ChainExpectation predecessorExpectation(AnyCommit commit) {
        long previousSize;
        long previousVersion;
        try {
            previousSize = Math.subtractExact(commit.cumulativeSize(), commit.logicalBytes());
            previousVersion = Math.subtractExact(commit.commitVersion(), 1);
        } catch (ArithmeticException failure) {
            throw invariant("append recovery predecessor scalars overflow", failure);
        }
        if (previousSize < 0
                || (commit.previousCommitId().isEmpty()
                        && (commit.offsetStart() != 0 || previousSize != 0 || previousVersion != 0))
                || (!commit.previousCommitId().isEmpty()
                        && (commit.offsetStart() == 0 || previousVersion <= 0))) {
            throw invariant("append recovery predecessor scalars are inconsistent");
        }
        return new ChainExpectation(commit.offsetStart(), previousSize, previousVersion);
    }

    private static void requireRecoveryBridge(
            AppendRecoveryAnchor anchor,
            ChainExpectation expectation) {
        if (expectation.offsetEnd() != anchor.offsetEnd()
                || expectation.cumulativeSize() != anchor.cumulativeSize()
                || expectation.commitVersion() != anchor.commitVersion()) {
            throw invariant("live commit tail does not bridge the requested recovery anchor");
        }
    }

    private StreamMetadataSnapshot transitionStreamStateSync(
            OxiaKeyspace keyspace, StreamStateTransitionRequest request) {
        StreamHeadRecord head = headOrThrow(keyspace, request.streamId());
        if (head.metadataVersion() != request.expectedMetadataVersion()
                || !head.state().equals(request.expectedState().name())) {
            throw condition("stream state transition precondition changed");
        }
        StreamHeadRecord candidate = new StreamHeadRecord(
                head.streamId(), head.streamName(), head.streamNameHash(), request.targetState().name(), head.profile(),
                head.attributes(), head.createdAtMillis(), head.policyVersion(), head.committedEndOffset(),
                head.cumulativeSize(), head.commitVersion(), head.trimOffset(), head.lastCommitId(),
                head.appendSession(), 0);
        try {
            StreamHeadRecord updated = putIfVersion(keyspace.streamHeadKey(request.streamId()),
                    keyspace.streamPartitionKey(request.streamId()), candidate, head.metadataVersion(), StreamHeadRecord.class);
            return snapshot(updated);
        } catch (BackendConditionException e) {
            requireCause(e, UnexpectedVersionIdException.class);
            throw condition("stream state transition CAS conflict");
        }
    }

    private CommitSliceResult commitStreamSliceSync(OxiaKeyspace keyspace, CommitSliceRequest request) {
        Objects.requireNonNull(request, "request");
        String commitId = request.commitId();
        StreamCommitRecord replay = findMarkerReplay(keyspace, request);
        if (replay != null) {
            validateReplay(request, replay, AppendOutcome.KNOWN_COMMITTED);
            materializeForAppend(keyspace, replay);
            return commitResult(replay, request.projectionRef());
        }

        StreamHeadRecord initialHead = headOrThrow(keyspace, request.streamId());
        if (initialHead.committedEndOffset() > request.expectedStartOffset()) {
            replay = appendSearchResult(searchReplay(keyspace, request, commitId), "commit replay search");
            if (replay != null) {
                validateReplay(request, replay, AppendOutcome.KNOWN_COMMITTED);
                materializeForAppend(keyspace, replay);
                return commitResult(replay, request.projectionRef());
            }
        }
        validateCommitPreconditions(initialHead, request);
        ObjectManifestRecord manifest = getObjectManifestSync(keyspace, request.objectId())
                .orElseThrow(() -> appendFailure(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "object manifest is missing"));
        try {
            Phase1ObjectManifestValidator.validateCommitCandidate(manifest, request, false);
        } catch (NereusException e) {
            throw new NereusException(
                    e.code(), e.retriable(), e.getMessage(), e, AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        StreamCommitRecord commit = buildCommit(request, initialHead, manifest, commitId);
        String commitKey = keyspace.streamCommitKey(request.streamId(), commitId);
        try {
            commit = putIfAbsent(
                    commitKey,
                    keyspace.streamPartitionKey(request.streamId()),
                    commit,
                    StreamCommitRecord.class);
        } catch (BackendConditionException e) {
            requireCause(e, KeyAlreadyExistsException.class);
            StreamCommitRecord existing = getCommit(keyspace, request.streamId(), commitId)
                    .orElseThrow(() -> invariant("commit intent disappeared after put conflict"));
            validateReplay(request, existing, AppendOutcome.KNOWN_NOT_COMMITTED);
            commit = existing;
        }

        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            StreamHeadRecord head = headOrThrow(keyspace, request.streamId());
            if (!sameCommitAnchor(initialHead, head)) {
                replay = appendSearchResult(searchReplay(keyspace, request, commitId), "head CAS replay search");
                if (replay != null) {
                    validateReplay(request, replay, AppendOutcome.KNOWN_COMMITTED);
                    materializeForAppend(keyspace, replay);
                    return commitResult(replay, request.projectionRef());
                }
            }
            validateCommitPreconditions(head, request);
            validateCommitAgainstHead(request, commit, head, AppendOutcome.KNOWN_NOT_COMMITTED);
            StreamHeadRecord candidate = withCommit(head, commit);
            Optional<StreamHeadRecord> committed = casHeadForCommit(keyspace, request.streamId(), head, candidate);
            if (committed.isEmpty()) {
                continue;
            }
            try {
                materializeForAppend(keyspace, commit);
            } catch (NereusException e) {
                throw new NereusException(
                        e.code(), e.retriable(), e.getMessage(), e, AppendOutcome.KNOWN_COMMITTED);
            }
            updateObjectAuditBestEffort(keyspace, manifest, commit);
            return commitResult(commit, request.projectionRef());
        }
        throw appendFailure(
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                AppendOutcome.KNOWN_NOT_COMMITTED,
                "stream-head CAS retry budget exhausted");
    }

    private DerivedIndexRepairResult repairDerivedStreamIndexesSync(
            OxiaKeyspace keyspace,
            StreamId streamId,
            long targetOffset,
            Optional<DerivedIndexRepairCursor> continuation,
            int maxCommitsToScan) {
        Objects.requireNonNull(continuation, "continuation");
        if (targetOffset < 0 || maxCommitsToScan <= 0) {
            throw new IllegalArgumentException("targetOffset must be non-negative and maxCommitsToScan positive");
        }
        StreamHeadRecord head = headOrThrow(keyspace, streamId);
        ChainExpectation headExpectation = expectationFromHead(head);
        continuation.ifPresent(cursor -> validateContinuation(keyspace, streamId, targetOffset, head, cursor));
        if (targetOffset >= head.committedEndOffset()) {
            return new DerivedIndexRepairResult(
                    streamId, targetOffset, targetOffset, 0, 0, true, false, Optional.empty(), head.commitVersion());
        }
        String observedHeadId = head.lastCommitId();
        long observedVersion = head.commitVersion();
        String commitId = head.lastCommitId();
        ChainExpectation expectation = headExpectation;
        if (continuation.isPresent()) {
            DerivedIndexRepairCursor cursor = continuation.orElseThrow();
            observedHeadId = cursor.observedHeadCommitId();
            observedVersion = cursor.observedCommitVersion();
            commitId = cursor.nextCommitId();
            expectation = new ChainExpectation(
                    cursor.nextOffsetEnd(), cursor.nextCumulativeSize(), cursor.nextCommitVersion());
        }
        int scanned = 0;
        int repaired = 0;
        long repairedFrom = Long.MAX_VALUE;
        long repairedTo = 0;
        boolean covered = false;
        while (!commitId.isEmpty() && scanned < maxCommitsToScan) {
            Object commit = getCommitValue(keyspace, streamId, commitId)
                    .orElseThrow(() -> invariant("broken commit chain"));
            AnyCommit view = anyCommit(commit);
            validateAnyCommit(streamId, commitId, view, expectation);
            ChainExpectation previous = new ChainExpectation(
                    view.offsetStart(), view.cumulativeSize() - view.logicalBytes(), view.commitVersion() - 1);
            scanned++;
            boolean changed = commit instanceof StreamCommitTargetRecord target
                    ? materializeTargetDerived(keyspace, target)
                    : materializeDerived(keyspace, (StreamCommitRecord) commit);
            if (changed) {
                repaired++;
                repairedFrom = Math.min(repairedFrom, view.offsetStart());
                repairedTo = Math.max(repairedTo, view.offsetEnd());
            }
            if (view.offsetStart() <= targetOffset && targetOffset < view.offsetEnd()) {
                covered = true;
                break;
            }
            commitId = view.previousCommitId();
            expectation = previous;
        }
        boolean exhausted = !covered && !commitId.isEmpty();
        if (!covered && !exhausted) {
            throw invariant("commit chain ended before covering the repair target");
        }
        Optional<DerivedIndexRepairCursor> next = exhausted
                ? Optional.of(new DerivedIndexRepairCursor(
                        streamId,
                        targetOffset,
                        observedHeadId,
                        observedVersion,
                        commitId,
                        expectation.offsetEnd(),
                        expectation.cumulativeSize(),
                        expectation.commitVersion()))
                : Optional.empty();
        return new DerivedIndexRepairResult(
                streamId,
                repairedFrom == Long.MAX_VALUE ? targetOffset : repairedFrom,
                repairedFrom == Long.MAX_VALUE ? targetOffset : repairedTo,
                scanned,
                repaired,
                covered,
                exhausted,
                next,
                observedVersion);
    }

    private ObjectReferenceRecord repairObjectReferencesSync(OxiaKeyspace keyspace, ObjectId objectId) {
        ObjectManifestRecord manifest = getObjectManifestSync(keyspace, objectId)
                .orElseThrow(() -> invariant("object manifest is missing"));
        Phase1ObjectManifestValidator.validateStoredManifest(manifest);
        List<VisibleSliceReferenceRecord> visible = new ArrayList<>();
        for (StreamSliceManifestRecord slice : manifest.slices()) {
            VisibleSliceReferenceRecord reference = findReachableManifestCommit(keyspace, manifest, slice);
            if (reference != null) {
                visible.add(reference);
            } else if ("VISIBLE".equals(slice.state())) {
                throw invariant("visible manifest slice has no matching reachable commit");
            }
        }
        visible = visible.stream()
                .sorted(Comparator.comparing(VisibleSliceReferenceRecord::streamId)
                        .thenComparingLong(VisibleSliceReferenceRecord::offsetStart)
                        .thenComparing(VisibleSliceReferenceRecord::sliceId))
                .toList();
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            Optional<ObjectReferenceRecord> existing = getObjectReferencesSync(keyspace, objectId);
            if (existing.isPresent() && !visible.containsAll(existing.orElseThrow().visibleSlices())) {
                throw invariant("object-reference repair would remove an existing visible reference");
            }
            ObjectReferenceRecord candidate = new ObjectReferenceRecord(
                    objectId.value(), visible, clock.millis(), 0);
            try {
                if (existing.isEmpty()) {
                    return putIfAbsent(
                            keyspace.objectReferencesKey(objectId),
                            keyspace.objectPartitionKey(objectId),
                            candidate,
                            ObjectReferenceRecord.class);
                }
                return putIfVersion(
                        keyspace.objectReferencesKey(objectId),
                        keyspace.objectPartitionKey(objectId),
                        candidate,
                        existing.orElseThrow().metadataVersion(),
                        ObjectReferenceRecord.class);
            } catch (BackendConditionException e) {
                if (!(e.getCause() instanceof KeyAlreadyExistsException)
                        && !(e.getCause() instanceof UnexpectedVersionIdException)) {
                    throw e;
                }
            }
        }
        throw condition("object-reference repair CAS retry budget exhausted");
    }

    private StreamCommitRecord buildCommit(
            CommitSliceRequest request,
            StreamHeadRecord head,
            ObjectManifestRecord manifest,
            String commitId) {
        try {
            return new StreamCommitRecord(
                    request.streamId().value(),
                    commitId,
                    head.lastCommitId(),
                    request.expectedStartOffset(),
                    Math.addExact(request.expectedStartOffset(), request.recordCount()),
                    0,
                    Math.addExact(head.cumulativeSize(), request.logicalBytes()),
                    Math.addExact(head.commitVersion(), 1),
                    request.writerId(),
                    request.writerRunIdHash(),
                    request.epoch(),
                    request.fencingTokenHash(),
                    request.objectId().value(),
                    request.objectKey().value(),
                    request.sliceId(),
                    manifest.objectType(),
                    "WAL_OBJECT_V1",
                    "OPAQUE_SLICE",
                    request.payloadFormat().name(),
                    request.objectChecksum().type().name(),
                    request.objectChecksum().value(),
                    request.objectOffset(),
                    request.objectLength(),
                    request.recordCount(),
                    request.entryCount(),
                    request.logicalBytes(),
                    request.schemaRefs(),
                    EntryIndexReferenceRecord.fromApi(request.entryIndexRef()),
                    request.projectionIdentity(),
                    request.sliceChecksum().type().name(),
                    request.sliceChecksum().value(),
                    request.minEventTimeMillis(),
                    request.maxEventTimeMillis(),
                    clock.millis(),
                    0);
        } catch (ArithmeticException e) {
            throw new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "commit head-derived fields overflow",
                    e,
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
    }

    private Optional<StreamHeadRecord> casHeadForCommit(
            OxiaKeyspace keyspace,
            StreamId streamId,
            StreamHeadRecord expected,
            StreamHeadRecord candidate) {
        try {
            return casHead(keyspace, streamId, expected, candidate);
        } catch (NereusException e) {
            throw new NereusException(
                    e.code(), e.retriable(), e.getMessage(), e, AppendOutcome.MAY_HAVE_COMMITTED);
        } catch (RuntimeException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof UnexpectedVersionIdException) {
                return Optional.empty();
            }
            throw new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    "stream-head CAS result is unknown",
                    cause,
                    AppendOutcome.MAY_HAVE_COMMITTED);
        }
    }

    private void validateCommitPreconditions(StreamHeadRecord head, CommitSliceRequest request) {
        if (!StreamState.ACTIVE.name().equals(head.state())) {
            throw appendFailure(
                    ErrorCode.STREAM_NOT_ACTIVE,
                    false,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "stream is not active");
        }
        AppendSessionSnapshotRecord session = head.appendSession();
        if (session.isEmpty() || session.expiresAtMillis() <= clock.millis()) {
            throw appendFailure(
                    ErrorCode.APPEND_SESSION_EXPIRED,
                    true,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "append session expired");
        }
        if (!session.writerId().equals(request.writerId())
                || session.epoch() != request.epoch()
                || !session.fencingToken().equals(request.fencingToken())) {
            throw appendFailure(
                    ErrorCode.FENCED_APPEND,
                    true,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "append session token does not match");
        }
        if (head.committedEndOffset() != request.expectedStartOffset()) {
            throw appendFailure(
                    ErrorCode.OFFSET_CONFLICT,
                    true,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "expected start offset does not match committed end");
        }
    }

    private void validateCommitPreconditions(StreamHeadRecord head, CommitAppendRequest request) {
        if (!StreamState.ACTIVE.name().equals(head.state())) {
            throw appendFailure(ErrorCode.STREAM_NOT_ACTIVE, false, AppendOutcome.KNOWN_NOT_COMMITTED,
                    "stream is not active");
        }
        AppendSessionSnapshotRecord session = head.appendSession();
        if (session.isEmpty() || session.expiresAtMillis() <= clock.millis()) {
            throw appendFailure(ErrorCode.APPEND_SESSION_EXPIRED, true, AppendOutcome.KNOWN_NOT_COMMITTED,
                    "append session expired");
        }
        if (!session.writerId().equals(request.writerId()) || session.epoch() != request.epoch()
                || !session.fencingToken().equals(request.fencingToken())) {
            throw appendFailure(ErrorCode.FENCED_APPEND, true, AppendOutcome.KNOWN_NOT_COMMITTED,
                    "append session token does not match");
        }
        if (head.committedEndOffset() != request.expectedStartOffset()) {
            throw appendFailure(ErrorCode.OFFSET_CONFLICT, true, AppendOutcome.KNOWN_NOT_COMMITTED,
                    "expected start offset does not match committed end");
        }
    }

    private StreamCommitTargetRecord buildTargetCommit(CommitAppendRequest request, StreamHeadRecord head) {
        try {
            return new StreamCommitTargetRecord(
                    request.streamId().value(), request.commitId(), head.lastCommitId(),
                    request.expectedStartOffset(), Math.addExact(request.expectedStartOffset(), request.recordCount()),
                    0, Math.addExact(head.cumulativeSize(), request.logicalBytes()),
                    Math.addExact(head.commitVersion(), 1), request.writerId(), request.writerRunIdHash(),
                    request.epoch(), request.fencingTokenHash(), request.readTargetRecord(), request.payloadFormat().name(),
                    request.recordCount(), request.entryCount(), request.logicalBytes(), request.schemaRefs(),
                    request.projectionIdentity(), request.minEventTimeMillis(), request.maxEventTimeMillis(),
                    clock.millis(), 0);
        } catch (ArithmeticException e) {
            throw new NereusException(ErrorCode.INVALID_ARGUMENT, false, "generic commit fields overflow", e,
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
    }

    private void validateReplay(
            CommitAppendRequest request, StreamCommitTargetRecord replay, AppendOutcome outcome) {
        AppendReplayRecords.requireMatches(request, replay, outcome);
    }

    private void validateCommitAgainstHead(
            CommitAppendRequest request, StreamCommitTargetRecord commit,
            StreamHeadRecord head, AppendOutcome outcome) {
        long end;
        long cumulative;
        long version;
        try {
            end = Math.addExact(request.expectedStartOffset(), request.recordCount());
            cumulative = Math.addExact(head.cumulativeSize(), request.logicalBytes());
            version = Math.addExact(head.commitVersion(), 1);
        } catch (ArithmeticException e) {
            throw new NereusException(ErrorCode.INVALID_ARGUMENT, false, "commit fields overflow", e, outcome);
        }
        if (!commit.previousCommitId().equals(head.lastCommitId()) || commit.offsetStart() != request.expectedStartOffset()
                || commit.offsetEnd() != end || commit.cumulativeSize() != cumulative || commit.commitVersion() != version) {
            throw appendFailure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, outcome,
                    "stored generic commit does not match current head snapshot");
        }
    }

    private ReachableCommittedAppend findGenericMarkerReplay(
            OxiaKeyspace keyspace, CommitAppendRequest request) {
        Optional<CommittedAppendRecord> marker = getRecord(
                keyspace.committedAppendKey(request.streamId(), request.commitId()),
                keyspace.streamPartitionKey(request.streamId()), CommittedAppendRecord.class);
        if (marker.isEmpty()) return null;
        CommittedAppendRecord value = marker.orElseThrow();
        Object durable = getCommitValue(keyspace, request.streamId(), request.commitId())
                .orElseThrow(() -> invariant("generic replay marker has no commit"));
        if (!(durable instanceof StreamCommitTargetRecord commit)) {
            throw invariant("generic replay marker points to legacy commit bytes");
        }
        validateReplay(request, commit, AppendOutcome.KNOWN_COMMITTED);
        if (!value.streamId().equals(commit.streamId()) || !value.commitId().equals(commit.commitId())
                || value.offsetStart() != commit.offsetStart() || value.offsetEnd() != commit.offsetEnd()
                || value.commitVersion() != commit.commitVersion()
                || !value.readTargetIdentitySha256().equals(commit.readTarget().identityChecksumValue())) {
            throw invariant("generic replay marker conflicts with commit");
        }
        StreamHeadRecord head = headOrThrow(keyspace, request.streamId());
        if (head.committedEndOffset() < commit.offsetEnd() || head.commitVersion() < commit.commitVersion()) {
            throw invariant("generic replay marker has no reachable head proof");
        }
        return reachable(commit, request.projectionRef(), head);
    }

    private ReachableCommittedAppend reachable(
            StreamCommitTargetRecord commit, Optional<ProjectionRef> projectionRef, StreamHeadRecord head) {
        return ReachableCommittedAppend.verified(toCommitted(commit, projectionRef), head.lastCommitId(),
                head.committedEndOffset(), head.cumulativeSize(), head.commitVersion());
    }

    private CommittedAppend toCommitted(
            StreamCommitTargetRecord commit, Optional<ProjectionRef> projectionRef) {
        return AppendReplayRecords.hydrate(commit, projectionRef);
    }

    private Optional<Object> getCommitValue(OxiaKeyspace keyspace, StreamId streamId, String commitId) {
        return join(client.get(keyspace.streamCommitKey(streamId, commitId), keyspace.streamPartitionKey(streamId)))
                .map(value -> {
                    String type = MetadataRecordCodecFactory.recordType(value.value());
                    return switch (type) {
                        case "StreamCommitRecord" -> decode(value, StreamCommitRecord.class);
                        case "StreamCommitTargetRecord" -> decode(value, StreamCommitTargetRecord.class);
                        default -> throw invariant("unexpected record type at commit-log key: " + type);
                    };
                });
    }

    private boolean isCommitReachableSync(
            OxiaKeyspace keyspace,
            StreamCommitAnchor descendant,
            String ancestorCommitId,
            long ancestorCommitVersion) {
        Objects.requireNonNull(descendant, "descendant");
        Objects.requireNonNull(ancestorCommitId, "ancestorCommitId");
        if (ancestorCommitId.isBlank() || ancestorCommitVersion <= 0) {
            throw new IllegalArgumentException("ancestor commit ID/version must be nonblank and positive");
        }
        if (ancestorCommitVersion > descendant.commitVersion() || descendant.isGenesis()) {
            return false;
        }

        StreamId streamId = descendant.streamId();
        String current = descendant.lastCommitId();
        ChainExpectation expectation = new ChainExpectation(
                descendant.committedEndOffset(),
                descendant.cumulativeSize(),
                descendant.commitVersion());
        int scanned = 0;
        while (!current.isEmpty() && scanned < configuration.maxCommitChainScan()) {
            Object durable = getCommitValue(keyspace, streamId, current)
                    .orElseThrow(() -> invariant("commit reachability found a missing descendant commit"));
            AnyCommit view = anyCommit(durable);
            validateAnyCommit(streamId, current, view, expectation);
            scanned++;
            if (current.equals(ancestorCommitId)) {
                return view.commitVersion() == ancestorCommitVersion;
            }
            if (view.commitVersion() <= ancestorCommitVersion) {
                return false;
            }
            expectation = predecessorExpectation(view);
            current = view.previousCommitId();
        }
        if (current.isEmpty()) {
            if (!isGenesis(expectation)) {
                throw invariant("commit reachability did not terminate at canonical genesis");
            }
            return false;
        }
        throw failure(
                ErrorCode.METADATA_UNAVAILABLE,
                true,
                "commit reachability exhausted the configured commit-chain scan budget");
    }

    private static AnyCommit anyCommit(Object durable) {
        if (durable instanceof StreamCommitRecord value) {
            return new AnyCommit(value.streamId(), value.commitId(), value.previousCommitId(), value.offsetStart(),
                    value.offsetEnd(), value.cumulativeSize(), value.commitVersion(), value.logicalBytes());
        }
        StreamCommitTargetRecord value = (StreamCommitTargetRecord) durable;
        return new AnyCommit(value.streamId(), value.commitId(), value.previousCommitId(), value.offsetStart(),
                value.offsetEnd(), value.cumulativeSize(), value.commitVersion(), value.logicalBytes());
    }

    private static void validateAnyCommit(
            StreamId streamId, String expectedId, AnyCommit commit, ChainExpectation expectation) {
        if (!commit.streamId().equals(streamId.value()) || !commit.commitId().equals(expectedId)
                || commit.offsetEnd() != expectation.offsetEnd()
                || commit.cumulativeSize() != expectation.cumulativeSize()
                || commit.commitVersion() != expectation.commitVersion()
                || commit.offsetStart() < 0 || commit.offsetStart() >= commit.offsetEnd()
                || commit.cumulativeSize() < commit.logicalBytes()) {
            throw invariant("mixed-version commit chain record is inconsistent");
        }
    }

    private void validateReplay(
            CommitSliceRequest request,
            StreamCommitRecord replay,
            AppendOutcome outcome) {
        if (!replay.commitId().equals(request.commitId())
                || !replay.streamId().equals(request.streamId().value())
                || !replay.writerId().equals(request.writerId())
                || !replay.writerRunIdHash().equals(request.writerRunIdHash())
                || replay.writerEpoch() != request.epoch()
                || !replay.fencingTokenHash().equals(request.fencingTokenHash())
                || replay.offsetStart() != request.expectedStartOffset()
                || replay.offsetEnd() != expectedEnd(request)
                || replay.generation() != 0
                || replay.recordCount() != request.recordCount()
                || replay.entryCount() != request.entryCount()
                || replay.logicalBytes() != request.logicalBytes()
                || !replay.objectId().equals(request.objectId().value())
                || !replay.objectKey().equals(request.objectKey().value())
                || !replay.objectType().equals(ObjectType.MULTI_STREAM_WAL_OBJECT.name())
                || !replay.physicalFormat().equals("WAL_OBJECT_V1")
                || !replay.logicalFormat().equals("OPAQUE_SLICE")
                || !replay.objectChecksumType().equals(request.objectChecksum().type().name())
                || !replay.objectChecksumValue().equals(request.objectChecksum().value())
                || replay.objectOffset() != request.objectOffset()
                || replay.objectLength() != request.objectLength()
                || !replay.sliceId().equals(request.sliceId())
                || !replay.payloadFormat().equals(request.payloadFormat().name())
                || !replay.schemaRefs().equals(request.schemaRefs())
                || !replay.entryIndexRef().equals(EntryIndexReferenceRecord.fromApi(request.entryIndexRef()))
                || !replay.projectionRef().equals(request.projectionIdentity())
                || !replay.sliceChecksumType().equals(request.sliceChecksum().type().name())
                || !replay.sliceChecksumValue().equals(request.sliceChecksum().value())
                || replay.minEventTimeMillis() != request.minEventTimeMillis()
                || replay.maxEventTimeMillis() != request.maxEventTimeMillis()) {
            throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION, false, outcome, "replayed commit does not match request");
        }
    }

    private void validateCommitAgainstHead(
            CommitSliceRequest request,
            StreamCommitRecord commit,
            StreamHeadRecord head,
            AppendOutcome outcome) {
        long end;
        long cumulative;
        long version;
        try {
            end = Math.addExact(request.expectedStartOffset(), request.recordCount());
            cumulative = Math.addExact(head.cumulativeSize(), request.logicalBytes());
            version = Math.addExact(head.commitVersion(), 1);
        } catch (ArithmeticException e) {
            throw new NereusException(ErrorCode.INVALID_ARGUMENT, false, "commit fields overflow", e, outcome);
        }
        if (!commit.previousCommitId().equals(head.lastCommitId())
                || commit.offsetStart() != request.expectedStartOffset()
                || commit.offsetEnd() != end
                || commit.cumulativeSize() != cumulative
                || commit.commitVersion() != version) {
            throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    outcome,
                    "stored commit does not match current head snapshot");
        }
    }

    private StreamCommitRecord findMarkerReplay(OxiaKeyspace keyspace, CommitSliceRequest request) {
        Optional<CommittedSliceRecord> marker = getRecord(
                keyspace.committedSliceKey(request.streamId(), request.objectId(), request.sliceId()),
                keyspace.streamPartitionKey(request.streamId()),
                CommittedSliceRecord.class);
        if (marker.isEmpty()) {
            return null;
        }
        CommittedSliceRecord value = marker.orElseThrow();
        if (!value.streamId().equals(request.streamId().value())
                || !value.objectId().equals(request.objectId().value())
                || !value.sliceId().equals(request.sliceId())
                || value.offsetStart() != request.expectedStartOffset()
                || value.offsetEnd() != expectedEnd(request)) {
            throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    "committed-slice marker does not match request");
        }
        StreamCommitRecord commit = getCommit(keyspace, request.streamId(), request.commitId())
                .orElseThrow(() -> appendFailure(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        AppendOutcome.MAY_HAVE_COMMITTED,
                        "committed marker has no commit record"));
        StreamHeadRecord head = headOrThrow(keyspace, request.streamId());
        if (!commitMatchesMarker(commit, value)
                || head.commitVersion() < value.commitVersion()
                || head.committedEndOffset() < value.offsetEnd()) {
            throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    "committed marker has no reachable head proof");
        }
        return commit;
    }

    private SearchResult searchReplay(
            OxiaKeyspace keyspace,
            CommitSliceRequest request,
            String commitId) {
        return searchReachable(
                keyspace,
                request.streamId(),
                commit -> commit.commitId().equals(commitId),
                commit -> commit.offsetStart() <= request.expectedStartOffset());
    }

    private SearchResult searchReachable(
            OxiaKeyspace keyspace,
            StreamId streamId,
            Predicate<StreamCommitRecord> predicate,
            Predicate<StreamCommitRecord> provesNotFound) {
        Optional<StreamHeadRecord> optionalHead = getHead(keyspace, streamId);
        if (optionalHead.isEmpty()) {
            return new SearchResult(SearchStatus.NOT_FOUND, null, 0);
        }
        StreamHeadRecord head = optionalHead.orElseThrow();
        ChainExpectation expectation;
        try {
            expectation = expectationFromHead(head);
        } catch (NereusException e) {
            return new SearchResult(SearchStatus.BROKEN, null, 0);
        }
        String current = head.lastCommitId();
        int scanned = 0;
        while (!current.isEmpty() && scanned < configuration.maxCommitChainScan()) {
            Optional<StreamCommitRecord> optionalCommit = getCommit(keyspace, streamId, current);
            if (optionalCommit.isEmpty()) {
                return new SearchResult(SearchStatus.BROKEN, null, scanned);
            }
            StreamCommitRecord commit = optionalCommit.orElseThrow();
            try {
                expectation = validateReachableCommit(streamId, current, commit, expectation);
            } catch (NereusException e) {
                return new SearchResult(SearchStatus.BROKEN, null, scanned);
            }
            scanned++;
            if (predicate.test(commit)) {
                return new SearchResult(SearchStatus.FOUND, commit, scanned);
            }
            if (provesNotFound.test(commit)) {
                return new SearchResult(SearchStatus.NOT_FOUND, null, scanned);
            }
            current = commit.previousCommitId();
        }
        if (current.isEmpty() && !isGenesis(expectation)) {
            return new SearchResult(SearchStatus.BROKEN, null, scanned);
        }
        return new SearchResult(
                current.isEmpty() ? SearchStatus.NOT_FOUND : SearchStatus.EXHAUSTED,
                null,
                scanned);
    }

    private StreamCommitRecord appendSearchResult(SearchResult result, String operation) {
        return switch (result.status()) {
            case FOUND -> result.commit();
            case NOT_FOUND -> null;
            case EXHAUSTED -> throw appendFailure(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    operation + " exhausted the commit-chain scan budget");
            case BROKEN -> throw appendFailure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    operation + " found a broken commit chain");
        };
    }

    private ChainExpectation expectationFromHead(StreamHeadRecord head) {
        boolean empty = head.lastCommitId().isEmpty();
        if (empty != (head.commitVersion() == 0)
                || empty != (head.committedEndOffset() == 0)
                || (empty && head.cumulativeSize() != 0)) {
            throw invariant("stream head commit anchor is invalid");
        }
        return new ChainExpectation(head.committedEndOffset(), head.cumulativeSize(), head.commitVersion());
    }

    private ChainExpectation validateReachableCommit(
            StreamId streamId,
            String expectedId,
            StreamCommitRecord commit,
            ChainExpectation expectation) {
        long calculatedEnd;
        try {
            calculatedEnd = Math.addExact(commit.offsetStart(), commit.recordCount());
        } catch (ArithmeticException e) {
            throw invariant("commit logical range overflows");
        }
        if (!commit.streamId().equals(streamId.value())
                || !commit.commitId().equals(expectedId)
                || commit.generation() != 0
                || commit.offsetEnd() != calculatedEnd
                || commit.offsetEnd() != expectation.offsetEnd()
                || commit.cumulativeSize() != expectation.cumulativeSize()
                || commit.commitVersion() != expectation.commitVersion()
                || !ObjectType.MULTI_STREAM_WAL_OBJECT.name().equals(commit.objectType())
                || !"WAL_OBJECT_V1".equals(commit.physicalFormat())
                || !"OPAQUE_SLICE".equals(commit.logicalFormat())) {
            throw invariant("commit chain record is inconsistent");
        }
        long previousSize = commit.cumulativeSize() - commit.logicalBytes();
        long previousVersion = commit.commitVersion() - 1;
        if (previousSize < 0
                || (commit.previousCommitId().isEmpty()
                        && (commit.offsetStart() != 0 || previousSize != 0 || previousVersion != 0))
                || (!commit.previousCommitId().isEmpty()
                        && (commit.offsetStart() == 0 || previousVersion <= 0))) {
            throw invariant("commit chain predecessor is inconsistent");
        }
        return new ChainExpectation(commit.offsetStart(), previousSize, previousVersion);
    }

    private void validateContinuation(
            OxiaKeyspace keyspace,
            StreamId streamId,
            long targetOffset,
            StreamHeadRecord currentHead,
            DerivedIndexRepairCursor cursor) {
        if (!cursor.streamId().equals(streamId)
                || cursor.targetOffset() != targetOffset
                || cursor.observedCommitVersion() > currentHead.commitVersion()) {
            throw invariant("repair continuation does not match current stream head");
        }
        Object observedValue = getCommitValue(keyspace, streamId, cursor.observedHeadCommitId())
                .orElseThrow(() -> invariant("repair continuation observed head is missing"));
        AnyCommit observed = anyCommit(observedValue);
        if (observed.commitVersion() != cursor.observedCommitVersion()
                || cursor.nextOffsetEnd() > observed.offsetStart()
                || cursor.nextCumulativeSize() > observed.cumulativeSize()
                || cursor.nextCommitVersion() >= observed.commitVersion()) {
            throw invariant("repair continuation position is invalid");
        }
        validateAnyCommit(streamId, cursor.observedHeadCommitId(), observed,
                new ChainExpectation(observed.offsetEnd(), observed.cumulativeSize(), observed.commitVersion()));
    }

    private boolean materializeDerived(OxiaKeyspace keyspace, StreamCommitRecord commit) {
        boolean repaired = false;
        StreamId streamId = new StreamId(commit.streamId());
        ObjectId objectId = new ObjectId(commit.objectId());
        OffsetIndexRecord index = toOffsetIndex(commit);
        String indexKey = keyspace.offsetIndexKey(streamId, commit.offsetEnd(), commit.generation());
        try {
            putIfAbsent(indexKey, keyspace.streamPartitionKey(streamId), index, OffsetIndexRecord.class);
            repaired = true;
        } catch (BackendConditionException e) {
            requireCause(e, KeyAlreadyExistsException.class);
            OffsetIndexRecord existing = getRecord(
                            indexKey, keyspace.streamPartitionKey(streamId), OffsetIndexRecord.class)
                    .orElseThrow(() -> invariant("offset index disappeared after put conflict"));
            if (!hydrate(existing, 0, OffsetIndexRecord.class).equals(hydrate(index, 0, OffsetIndexRecord.class))) {
                throw invariant("offset index conflict");
            }
        }
        CommittedSliceRecord marker = new CommittedSliceRecord(
                commit.streamId(), commit.objectId(), commit.sliceId(), commit.offsetStart(), commit.offsetEnd(),
                commit.generation(), commit.commitVersion(), 0);
        String markerKey = keyspace.committedSliceKey(streamId, objectId, commit.sliceId());
        try {
            putIfAbsent(markerKey, keyspace.streamPartitionKey(streamId), marker, CommittedSliceRecord.class);
            repaired = true;
        } catch (BackendConditionException e) {
            requireCause(e, KeyAlreadyExistsException.class);
            CommittedSliceRecord existing = getRecord(
                            markerKey, keyspace.streamPartitionKey(streamId), CommittedSliceRecord.class)
                    .orElseThrow(() -> invariant("committed marker disappeared after put conflict"));
            if (!hydrate(existing, 0, CommittedSliceRecord.class)
                    .equals(hydrate(marker, 0, CommittedSliceRecord.class))) {
                throw invariant("committed slice conflict");
            }
        }
        return repaired;
    }

    private boolean materializeTargetDerived(OxiaKeyspace keyspace, StreamCommitTargetRecord commit) {
        StreamId streamId = new StreamId(commit.streamId());
        boolean repaired = false;
        OffsetIndexTargetRecord index = new OffsetIndexTargetRecord(
                commit.streamId(), commit.offsetStart(), commit.offsetEnd(), commit.generation(),
                commit.cumulativeSize(), commit.readTarget(), commit.payloadFormat(), commit.recordCount(),
                commit.entryCount(), commit.logicalBytes(), commit.schemaRefs(), commit.projectionRef(),
                commit.minEventTimeMillis(), commit.maxEventTimeMillis(), commit.commitVersion(), false, 0);
        String indexKey = keyspace.offsetIndexKey(streamId, commit.offsetEnd(), commit.generation());
        if (getRecord(indexKey, keyspace.streamPartitionKey(streamId), OffsetIndexTargetRecord.class).isEmpty()) {
            putOrCompare(indexKey, keyspace.streamPartitionKey(streamId), index, OffsetIndexTargetRecord.class);
            repaired = true;
        } else {
            putOrCompare(indexKey, keyspace.streamPartitionKey(streamId), index, OffsetIndexTargetRecord.class);
        }
        CommittedAppendRecord marker = new CommittedAppendRecord(
                commit.streamId(), commit.commitId(), commit.offsetStart(), commit.offsetEnd(), commit.generation(),
                commit.commitVersion(), commit.readTarget().identityChecksumValue(), 0);
        String markerKey = keyspace.committedAppendKey(streamId, commit.commitId());
        if (getRecord(markerKey, keyspace.streamPartitionKey(streamId), CommittedAppendRecord.class).isEmpty()) {
            putOrCompare(markerKey, keyspace.streamPartitionKey(streamId), marker, CommittedAppendRecord.class);
            repaired = true;
        } else {
            putOrCompare(markerKey, keyspace.streamPartitionKey(streamId), marker, CommittedAppendRecord.class);
        }
        return repaired;
    }

    private void materializeForAppend(OxiaKeyspace keyspace, StreamCommitRecord commit) {
        try {
            materializeDerived(keyspace, commit);
        } catch (Throwable error) {
            throw normalize(error, Optional.of(AppendOutcome.KNOWN_COMMITTED));
        }
    }

    private OffsetIndexRecord toOffsetIndex(StreamCommitRecord commit) {
        return new OffsetIndexRecord(
                commit.streamId(), commit.offsetStart(), commit.offsetEnd(), commit.generation(),
                commit.cumulativeSize(), commit.objectId(), commit.objectKey(), commit.sliceId(), commit.objectType(),
                commit.physicalFormat(), commit.logicalFormat(), commit.payloadFormat(), commit.objectOffset(),
                commit.objectLength(), commit.recordCount(), commit.entryCount(), commit.logicalBytes(),
                commit.schemaRefs(), commit.entryIndexRef(), commit.projectionRef(), commit.sliceChecksumType(),
                commit.sliceChecksumValue(), commit.minEventTimeMillis(), commit.maxEventTimeMillis(),
                commit.commitVersion(), false, 0);
    }

    private VisibleSliceReferenceRecord findReachableManifestCommit(
            OxiaKeyspace keyspace,
            ObjectManifestRecord manifest,
            StreamSliceManifestRecord slice) {
        StreamId streamId = new StreamId(slice.streamId());
        StreamHeadRecord head = headOrThrow(keyspace, streamId);
        String commitId = head.lastCommitId();
        ChainExpectation expectation = expectationFromHead(head);
        int scanned = 0;
        while (!commitId.isEmpty() && scanned++ < configuration.maxCommitChainScan()) {
            Object durable = getCommitValue(keyspace, streamId, commitId)
                    .orElseThrow(() -> invariant("object repair found a broken mixed commit chain"));
            AnyCommit view = anyCommit(durable);
            validateAnyCommit(streamId, commitId, view, expectation);
            boolean matches = false;
            if (durable instanceof StreamCommitRecord legacy) {
                if (legacy.objectId().equals(manifest.objectId()) && legacy.sliceId().equals(slice.sliceId())) {
                    if (!commitMatchesManifest(manifest, slice, legacy)) {
                        throw invariant("reachable commit conflicts with object manifest slice");
                    }
                    materializeDerived(keyspace, legacy);
                    matches = true;
                }
            } else {
                StreamCommitTargetRecord generic = (StreamCommitTargetRecord) durable;
                com.nereusstream.api.target.ReadTarget decoded =
                        ReadTargetCodecRegistry.phase15().decode(generic.readTarget());
                if (decoded instanceof ObjectSliceReadTarget target
                        && target.objectId().value().equals(manifest.objectId())
                        && target.sliceId().equals(slice.sliceId())) {
                    if (!target.objectKey().value().equals(manifest.objectKey())
                            || !target.objectType().name().equals(manifest.objectType())
                            || !generic.writerId().equals(manifest.writerId())
                            || !generic.writerRunIdHash().equals(manifest.writerRunIdHash())
                            || generic.writerEpoch() != manifest.writerEpoch()
                            || target.objectOffset() != slice.objectOffset()
                            || target.objectLength() != slice.objectLength()
                            || !target.sliceChecksum().type().name().equals(slice.sliceChecksumType())
                            || !target.sliceChecksum().value().equals(slice.sliceChecksumValue())) {
                        throw invariant("generic object target conflicts with manifest slice");
                    }
                    materializeTargetDerived(keyspace, generic);
                    matches = true;
                }
            }
            if (matches) return new VisibleSliceReferenceRecord(
                    view.streamId(), slice.sliceId(), view.offsetStart(), view.offsetEnd(), 0, view.commitVersion());
            commitId = view.previousCommitId();
            expectation = new ChainExpectation(
                    view.offsetStart(), view.cumulativeSize() - view.logicalBytes(), view.commitVersion() - 1);
        }
        if (!commitId.isEmpty()) throw failure(
                ErrorCode.METADATA_UNAVAILABLE, true, "object repair exhausted commit-chain scan budget");
        return null;
    }

    private void updateObjectAuditBestEffort(
            OxiaKeyspace keyspace,
            ObjectManifestRecord original,
            StreamCommitRecord commit) {
        try {
            ObjectId objectId = new ObjectId(original.objectId());
            for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
                ObjectManifestRecord current = getObjectManifestSync(keyspace, objectId).orElseThrow();
                List<StreamSliceManifestRecord> slices = current.slices().stream()
                        .map(slice -> slice.streamId().equals(commit.streamId())
                                        && slice.sliceId().equals(commit.sliceId())
                                ? slice.withState("VISIBLE")
                                : slice)
                        .toList();
                String state = slices.stream().allMatch(slice -> "VISIBLE".equals(slice.state()))
                        ? "VISIBLE"
                        : "PARTIALLY_VISIBLE";
                try {
                    putIfVersion(
                            keyspace.objectManifestKey(objectId),
                            keyspace.objectPartitionKey(objectId),
                            current.withStateAndSlices(state, slices, 0),
                            current.metadataVersion(),
                            ObjectManifestRecord.class);
                    break;
                } catch (BackendConditionException e) {
                    requireCause(e, UnexpectedVersionIdException.class);
                }
            }
            repairObjectReferencesSync(keyspace, objectId);
        } catch (RuntimeException ignored) {
            // Object audit is repairable and cannot weaken a committed append result.
        }
    }

    private Optional<StreamCommitRecord> getCommit(
            OxiaKeyspace keyspace,
            StreamId streamId,
            String commitId) {
        Optional<StreamCommitRecord> result = getRecord(
                keyspace.streamCommitKey(streamId, commitId),
                keyspace.streamPartitionKey(streamId),
                StreamCommitRecord.class);
        result.ifPresent(commit -> {
            if (!commit.streamId().equals(streamId.value()) || !commit.commitId().equals(commitId)) {
                throw invariant("stream commit key/value identity mismatch");
            }
        });
        return result;
    }

    private static CommitSliceResult commitResult(
            StreamCommitRecord commit,
            Optional<ProjectionRef> projection) {
        return new CommitSliceResult(
                new StreamId(commit.streamId()),
                new OffsetRange(commit.offsetStart(), commit.offsetEnd()),
                commit.offsetEnd(),
                commit.generation(),
                commit.commitVersion(),
                projection);
    }

    private static boolean sameCommitAnchor(StreamHeadRecord left, StreamHeadRecord right) {
        return left.committedEndOffset() == right.committedEndOffset()
                && left.cumulativeSize() == right.cumulativeSize()
                && left.commitVersion() == right.commitVersion()
                && left.lastCommitId().equals(right.lastCommitId());
    }

    private static boolean commitMatchesMarker(StreamCommitRecord commit, CommittedSliceRecord marker) {
        return commit.streamId().equals(marker.streamId())
                && commit.objectId().equals(marker.objectId())
                && commit.sliceId().equals(marker.sliceId())
                && commit.offsetStart() == marker.offsetStart()
                && commit.offsetEnd() == marker.offsetEnd()
                && commit.generation() == marker.generation()
                && commit.commitVersion() == marker.commitVersion();
    }

    private static boolean commitMatchesManifest(
            ObjectManifestRecord manifest,
            StreamSliceManifestRecord slice,
            StreamCommitRecord commit) {
        return commit.streamId().equals(slice.streamId())
                && commit.objectId().equals(manifest.objectId())
                && commit.objectKey().equals(manifest.objectKey())
                && commit.objectType().equals(manifest.objectType())
                && commit.objectChecksumType().equals(manifest.objectChecksumType())
                && commit.objectChecksumValue().equals(manifest.objectChecksumValue())
                && commit.writerId().equals(manifest.writerId())
                && commit.writerRunIdHash().equals(manifest.writerRunIdHash())
                && commit.writerEpoch() == manifest.writerEpoch()
                && commit.sliceId().equals(slice.sliceId())
                && commit.writerEpoch() == slice.writerEpoch()
                && commit.objectOffset() == slice.objectOffset()
                && commit.objectLength() == slice.objectLength()
                && commit.recordCount() == slice.recordCount()
                && commit.entryCount() == slice.entryCount()
                && commit.logicalBytes() == slice.logicalBytes()
                && commit.schemaRefs().equals(slice.schemaRefs())
                && commit.entryIndexRef().equals(slice.entryIndexRef())
                && commit.sliceChecksumType().equals(slice.sliceChecksumType())
                && commit.sliceChecksumValue().equals(slice.sliceChecksumValue())
                && commit.payloadFormat().equals(slice.payloadFormat());
    }

    private static boolean isGenesis(ChainExpectation expectation) {
        return expectation.offsetEnd() == 0
                && expectation.cumulativeSize() == 0
                && expectation.commitVersion() == 0;
    }

    private static long expectedEnd(CommitSliceRequest request) {
        try {
            return Math.addExact(request.expectedStartOffset(), request.recordCount());
        } catch (ArithmeticException e) {
            throw appendFailure(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "commit offset range overflows");
        }
    }

    private static NereusException appendFailure(
            ErrorCode code,
            boolean retriable,
            AppendOutcome outcome,
            String message) {
        return new NereusException(code, retriable, message, outcome);
    }

    private Optional<ObjectManifestRecord> getObjectManifestSync(OxiaKeyspace keyspace, ObjectId objectId) {
        Optional<ObjectManifestRecord> result = getRecord(
                keyspace.objectManifestKey(objectId),
                keyspace.objectPartitionKey(objectId),
                ObjectManifestRecord.class);
        result.ifPresent(record -> {
            if (!record.objectId().equals(objectId.value())) {
                throw invariant("object manifest key/value identity mismatch");
            }
        });
        return result;
    }

    private Optional<StreamHeadRecord> getHead(OxiaKeyspace keyspace, StreamId streamId) {
        Optional<StreamHeadRecord> result = getRecord(
                keyspace.streamHeadKey(streamId),
                keyspace.streamPartitionKey(streamId),
                StreamHeadRecord.class);
        result.ifPresent(head -> {
            StreamName streamName;
            try {
                streamName = new StreamName(head.streamName());
            } catch (RuntimeException e) {
                throw invariant("stream head contains an invalid stream name");
            }
            if (!head.streamId().equals(streamId.value())
                    || !DeterministicIds.streamIdFor(streamName).equals(streamId)
                    || !head.streamNameHash().equals(DeterministicIds.streamNameHash(streamName))) {
                throw invariant("stream head key/value identity mismatch");
            }
        });
        return result;
    }

    private Optional<ObjectReferenceRecord> getObjectReferencesSync(
            OxiaKeyspace keyspace,
            ObjectId objectId) {
        Optional<ObjectReferenceRecord> result = getRecord(
                keyspace.objectReferencesKey(objectId),
                keyspace.objectPartitionKey(objectId),
                ObjectReferenceRecord.class);
        result.ifPresent(record -> {
            if (!record.objectId().equals(objectId.value())) {
                throw invariant("object reference key/value identity mismatch");
            }
        });
        return result;
    }

    private OffsetIndexEntry decodeOffsetIndexEntry(
            OxiaKeyspace keyspace,
            StreamId streamId,
            PartitionedOxiaClient.VersionedValue value) {
        String type = MetadataRecordCodecFactory.recordType(value.value());
        OffsetIndexEntry entry;
        if (type.equals("OffsetIndexRecord")) {
            OffsetIndexRecord record = decode(value, OffsetIndexRecord.class);
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
            entry = new OffsetIndexEntry(streamId, new OffsetRange(record.offsetStart(), record.offsetEnd()),
                    record.generation(), record.cumulativeSize(), target, PayloadFormat.valueOf(record.payloadFormat()),
                    record.recordCount(), record.entryCount(), record.logicalBytes(), record.schemaRefs(),
                    ProjectionIdentity.decode(record.projectionRef()), record.commitVersion(), record.tombstoned(),
                    record.metadataVersion());
        } else if (type.equals("OffsetIndexTargetRecord")) {
            OffsetIndexTargetRecord record = decode(value, OffsetIndexTargetRecord.class);
            entry = new OffsetIndexEntry(streamId, new OffsetRange(record.offsetStart(), record.offsetEnd()),
                    record.generation(), record.cumulativeSize(),
                    ReadTargetCodecRegistry.phase15().decode(record.readTarget()),
                    PayloadFormat.valueOf(record.payloadFormat()), record.recordCount(), record.entryCount(),
                    record.logicalBytes(), record.schemaRefs(), ProjectionIdentity.decode(record.projectionRef()),
                    record.commitVersion(), record.tombstoned(), record.metadataVersion());
        } else {
            throw invariant("unexpected record type at offset-index key: " + type);
        }
        String expectedKey = keyspace.offsetIndexKey(
                streamId, entry.range().endOffset(), entry.generation());
        if (!entry.streamId().equals(streamId) || !value.key().equals(expectedKey)) {
            throw invariant("offset index key/value identity mismatch");
        }
        return entry;
    }

    private StreamMetadataSnapshot snapshot(StreamHeadRecord head) {
        long observedAtMillis = clock.millis();
        return new StreamMetadataSnapshot(
                head.toMetadataRecord(),
                new CommittedEndOffsetRecord(
                        head.streamId(),
                        head.committedEndOffset(),
                        head.cumulativeSize(),
                        head.commitVersion(),
                        head.metadataVersion()),
                new TrimRecord(
                        head.streamId(),
                        head.trimOffset(),
                        "",
                        observedAtMillis,
                        head.metadataVersion()));
    }

    private StreamHeadRecord headOrThrow(OxiaKeyspace keyspace, StreamId streamId) {
        return getHead(keyspace, streamId)
                .orElseThrow(() -> failure(ErrorCode.STREAM_NOT_FOUND, false, "stream does not exist"));
    }

    private <T> Optional<T> getRecord(
            String key,
            PartitionKey partitionKey,
            Class<T> recordClass) {
        return join(client.get(key, partitionKey)).map(value -> decode(value, recordClass));
    }

    private <T> Optional<DurableRecord<T>> getDurableRecord(
            String key,
            PartitionKey partitionKey,
            Class<T> recordClass) {
        return join(client.get(key, partitionKey)).map(value -> new DurableRecord<>(
                value.key(),
                decode(value, recordClass),
                value.version(),
                sha256(value.value())));
    }

    private <T> T putIfAbsent(
            String key,
            PartitionKey partitionKey,
            T record,
            Class<T> recordClass) {
        PartitionedOxiaClient.WriteResult result = join(client.putIfAbsent(
                key, encode(record, recordClass), partitionKey));
        return hydrate(record, result.version(), recordClass);
    }

    private <T> DurableRecord<T> putIfAbsentDurable(
            String key,
            PartitionKey partitionKey,
            T record,
            Class<T> recordClass) {
        byte[] bytes = encode(record, recordClass);
        PartitionedOxiaClient.WriteResult result = join(client.putIfAbsent(key, bytes, partitionKey));
        return new DurableRecord<>(
                key,
                hydrate(record, result.version(), recordClass),
                result.version(),
                sha256(bytes));
    }

    private <T> DurableRecord<T> putOrCompareDurable(
            String key,
            PartitionKey partitionKey,
            T candidate,
            Class<T> type) {
        try {
            return putIfAbsentDurable(key, partitionKey, candidate, type);
        } catch (BackendConditionException e) {
            requireCause(e, KeyAlreadyExistsException.class);
            DurableRecord<T> existing = getDurableRecord(key, partitionKey, type)
                    .orElseThrow(() -> invariant("derived record disappeared after put conflict"));
            if (!hydrate(existing.value(), 0, type).equals(hydrate(candidate, 0, type))) {
                throw invariant("derived generic record conflicts with durable bytes");
            }
            return existing;
        }
    }

    private <T> T putIfVersion(
            String key,
            PartitionKey partitionKey,
            T record,
            long expectedVersion,
            Class<T> recordClass) {
        PartitionedOxiaClient.WriteResult result = join(client.putIfVersion(
                key, encode(record, recordClass), expectedVersion, partitionKey));
        return hydrate(record, result.version(), recordClass);
    }

    private Optional<StreamHeadRecord> casHead(
            OxiaKeyspace keyspace,
            StreamId streamId,
            StreamHeadRecord expected,
            StreamHeadRecord candidate) {
        try {
            return Optional.of(putIfVersion(
                    keyspace.streamHeadKey(streamId),
                    keyspace.streamPartitionKey(streamId),
                    candidate,
                    expected.metadataVersion(),
                    StreamHeadRecord.class));
        } catch (BackendConditionException e) {
            requireCause(e, UnexpectedVersionIdException.class);
            return Optional.empty();
        }
    }

    private <T> T decode(PartitionedOxiaClient.VersionedValue value, Class<T> recordClass) {
        T decoded = MetadataRecordCodecFactory.decodeEnvelope(value.value(), recordClass);
        return hydrate(decoded, value.version(), recordClass);
    }

    private <T> byte[] encode(T record, Class<T> recordClass) {
        return MetadataRecordCodecFactory.encodeEnvelope(hydrate(record, 0, recordClass), recordClass);
    }

    @SuppressWarnings("unchecked")
    private static <T> T hydrate(T record, long version, Class<T> recordClass) {
        Object hydrated;
        if (record instanceof StreamHeadRecord value) {
            hydrated = new StreamHeadRecord(
                    value.streamId(), value.streamName(), value.streamNameHash(), value.state(), value.profile(),
                    value.attributes(), value.createdAtMillis(), value.policyVersion(), value.committedEndOffset(),
                    value.cumulativeSize(), value.commitVersion(), value.trimOffset(), value.lastCommitId(),
                    value.appendSession(), version);
        } else if (record instanceof StreamCommitRecord value) {
            hydrated = copyCommit(value, version);
        } else if (record instanceof StreamCommitTargetRecord value) {
            hydrated = new StreamCommitTargetRecord(
                    value.streamId(), value.commitId(), value.previousCommitId(), value.offsetStart(), value.offsetEnd(),
                    value.generation(), value.cumulativeSize(), value.commitVersion(), value.writerId(),
                    value.writerRunIdHash(), value.writerEpoch(), value.fencingTokenHash(), value.readTarget(),
                    value.payloadFormat(), value.recordCount(), value.entryCount(), value.logicalBytes(),
                    value.schemaRefs(), value.projectionRef(), value.minEventTimeMillis(), value.maxEventTimeMillis(),
                    value.preparedAtMillis(), version);
        } else if (record instanceof ObjectManifestRecord value) {
            hydrated = new ObjectManifestRecord(
                    value.objectId(), value.objectKey(), value.objectType(), value.state(), value.formatMajorVersion(),
                    value.formatMinorVersion(), value.writerVersion(), value.writerId(), value.writerRunIdHash(),
                    value.writerEpoch(), value.createdAtMillis(), value.uploadedAtMillis(), value.objectLength(),
                    value.objectChecksumType(), value.objectChecksumValue(), value.storageChecksumType(),
                    value.storageChecksumValue(), value.slices(), value.orphanExpiresAtMillis(), version);
        } else if (record instanceof ObjectReferenceRecord value) {
            hydrated = new ObjectReferenceRecord(value.objectId(), value.visibleSlices(), value.updatedAtMillis(), version);
        } else if (record instanceof OffsetIndexRecord value) {
            hydrated = copyOffsetIndex(value, version);
        } else if (record instanceof OffsetIndexTargetRecord value) {
            hydrated = new OffsetIndexTargetRecord(
                    value.streamId(), value.offsetStart(), value.offsetEnd(), value.generation(), value.cumulativeSize(),
                    value.readTarget(), value.payloadFormat(), value.recordCount(), value.entryCount(), value.logicalBytes(),
                    value.schemaRefs(), value.projectionRef(), value.minEventTimeMillis(), value.maxEventTimeMillis(),
                    value.commitVersion(), value.tombstoned(), version);
        } else if (record instanceof CommittedSliceRecord value) {
            hydrated = new CommittedSliceRecord(
                    value.streamId(), value.objectId(), value.sliceId(), value.offsetStart(), value.offsetEnd(),
                    value.generation(), value.commitVersion(), version);
        } else if (record instanceof CommittedAppendRecord value) {
            hydrated = new CommittedAppendRecord(
                    value.streamId(), value.commitId(), value.offsetStart(), value.offsetEnd(), value.generation(),
                    value.commitVersion(), value.readTargetIdentitySha256(), version);
        } else if (record instanceof StreamNameRecord value) {
            hydrated = new StreamNameRecord(
                    value.streamName(), value.streamId(), value.streamNameHash(), value.createdAtMillis(), version);
        } else {
            throw new IllegalArgumentException("record type does not carry metadataVersion: " + recordClass.getName());
        }
        return (T) hydrated;
    }

    private void putStreamNameAuditBestEffort(
            OxiaKeyspace keyspace,
            StreamName streamName,
            StreamId streamId,
            long createdAtMillis) {
        try {
            putIfAbsent(
                    keyspace.streamNameKey(streamName),
                    keyspace.streamPartitionKey(streamId),
                    new StreamNameRecord(
                            streamName.value(), streamId.value(), DeterministicIds.streamNameHash(streamName),
                            createdAtMillis, 0),
                    StreamNameRecord.class);
        } catch (RuntimeException ignored) {
            // The deterministic stream head remains authoritative.
        }
    }

    private <T> CompletableFuture<T> complete(Callable<T> operation) {
        if (closed.get()) {
            return NereusException.failedFuture(ErrorCode.STORAGE_CLOSED, false, "metadata store is closed");
        }
        try {
            return CompletableFuture.supplyAsync(() -> {
                ensureOpen();
                try {
                    return operation.call();
                } catch (Throwable error) {
                    throw normalize(error, Optional.empty());
                }
            }, operationExecutor);
        } catch (RejectedExecutionException e) {
            if (!closed.get()) {
                return NereusException.failedFuture(
                        ErrorCode.BACKPRESSURE_REJECTED, true, "metadata operation queue is full");
            }
            return NereusException.failedFuture(
                    ErrorCode.STORAGE_CLOSED, false, "metadata store is closing");
        }
    }

    private <T> CompletableFuture<T> completeAppend(Callable<T> operation) {
        if (closed.get()) {
            return NereusException.failedAppendFuture(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "metadata store is closed");
        }
        try {
            return CompletableFuture.supplyAsync(() -> {
                ensureOpen();
                try {
                    return operation.call();
                } catch (Throwable error) {
                    throw normalize(error, Optional.of(AppendOutcome.KNOWN_NOT_COMMITTED));
                }
            }, operationExecutor);
        } catch (RejectedExecutionException e) {
            if (!closed.get()) {
                return NereusException.failedAppendFuture(
                        ErrorCode.BACKPRESSURE_REJECTED,
                        true,
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "metadata operation queue is full");
            }
            return NereusException.failedAppendFuture(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "metadata store is closing");
        }
    }

    private static NereusException normalize(Throwable error, Optional<AppendOutcome> fallback) {
        Throwable cause = unwrap(error);
        if (cause instanceof NereusException nereus) {
            if (fallback.isPresent() && nereus.appendOutcome().isEmpty()) {
                return new NereusException(
                        nereus.code(), nereus.retriable(), nereus.getMessage(), nereus, fallback.orElseThrow());
            }
            return nereus;
        }
        ErrorCode code;
        boolean retriable;
        if (cause instanceof UnexpectedVersionIdException || cause instanceof KeyAlreadyExistsException) {
            code = ErrorCode.METADATA_CONDITION_FAILED;
            retriable = true;
        } else if (cause instanceof MetadataCodecException) {
            code = ErrorCode.METADATA_INVARIANT_VIOLATION;
            retriable = false;
        } else if (cause instanceof IllegalArgumentException) {
            code = ErrorCode.INVALID_ARGUMENT;
            retriable = false;
        } else {
            code = ErrorCode.METADATA_UNAVAILABLE;
            retriable = true;
        }
        String message = "Oxia metadata operation failed: " + cause.getClass().getSimpleName();
        return fallback.<NereusException>map(outcome -> new NereusException(
                        code, retriable, message, cause, outcome))
                .orElseGet(() -> new NereusException(code, retriable, message, cause));
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw failure(ErrorCode.STORAGE_CLOSED, false, "metadata store is closed");
        }
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof BackendConditionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> T join(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof UnexpectedVersionIdException || cause instanceof KeyAlreadyExistsException) {
                throw new BackendConditionException(cause);
            }
            throw e;
        }
    }

    private static void requireCause(BackendConditionException error, Class<? extends Throwable> expected) {
        if (!expected.isInstance(error.getCause())) {
            throw error;
        }
    }

    private static void validateStreamIdentity(StreamHeadRecord head, StreamName name, StreamId streamId) {
        if (!head.streamId().equals(streamId.value()) || !head.streamName().equals(name.value())) {
            throw invariant("deterministic stream id collision");
        }
    }

    private static void requireActive(StreamHeadRecord head) {
        if (!StreamState.ACTIVE.name().equals(head.state())) {
            throw failure(ErrorCode.STREAM_NOT_ACTIVE, false, "stream is not active");
        }
    }

    private static NereusException failure(ErrorCode code, boolean retriable, String message) {
        return new NereusException(code, retriable, message);
    }

    private static NereusException invariant(String message) {
        return failure(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static NereusException condition(String message) {
        return failure(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static long leaseExpiration(long now, Duration ttl) {
        try {
            long millis = ttl.toMillis();
            if (millis <= 0) {
                throw new ArithmeticException("ttl below millisecond resolution");
            }
            return Math.addExact(now, millis);
        } catch (ArithmeticException e) {
            throw failure(ErrorCode.INVALID_ARGUMENT, false, "append session ttl is outside millisecond range");
        }
    }

    private String newFencingToken(StreamId streamId, String writerId, long epoch) {
        return DeterministicIds.stableHashComponent(
                streamId.value() + "\0" + writerId + "\0" + epoch + "\0" + clock.millis()
                        + "\0" + java.util.UUID.randomUUID());
    }

    private static StreamHeadRecord withSession(
            StreamHeadRecord head,
            AppendSessionSnapshotRecord session) {
        return copyHead(head, head.committedEndOffset(), head.cumulativeSize(), head.commitVersion(),
                head.trimOffset(), head.lastCommitId(), session);
    }

    private static StreamHeadRecord withTrim(StreamHeadRecord head, long trimOffset) {
        return copyHead(head, head.committedEndOffset(), head.cumulativeSize(), head.commitVersion(),
                trimOffset, head.lastCommitId(), head.appendSession());
    }

    private static StreamHeadRecord withCommit(StreamHeadRecord head, StreamCommitRecord commit) {
        return copyHead(head, commit.offsetEnd(), commit.cumulativeSize(), commit.commitVersion(),
                head.trimOffset(), commit.commitId(), head.appendSession());
    }

    private static StreamHeadRecord withCommit(StreamHeadRecord head, StreamCommitTargetRecord commit) {
        return copyHead(head, commit.offsetEnd(), commit.cumulativeSize(), commit.commitVersion(),
                head.trimOffset(), commit.commitId(), head.appendSession());
    }

    private static StreamHeadRecord copyHead(
            StreamHeadRecord head,
            long committedEndOffset,
            long cumulativeSize,
            long commitVersion,
            long trimOffset,
            String lastCommitId,
            AppendSessionSnapshotRecord session) {
        return new StreamHeadRecord(
                head.streamId(), head.streamName(), head.streamNameHash(), head.state(), head.profile(),
                head.attributes(), head.createdAtMillis(), head.policyVersion(), committedEndOffset,
                cumulativeSize, commitVersion, trimOffset, lastCommitId, session, 0);
    }

    private static StreamCommitRecord copyCommit(StreamCommitRecord value, long version) {
        return new StreamCommitRecord(
                value.streamId(), value.commitId(), value.previousCommitId(), value.offsetStart(), value.offsetEnd(),
                value.generation(), value.cumulativeSize(), value.commitVersion(), value.writerId(),
                value.writerRunIdHash(), value.writerEpoch(), value.fencingTokenHash(), value.objectId(),
                value.objectKey(), value.sliceId(), value.objectType(), value.physicalFormat(), value.logicalFormat(),
                value.payloadFormat(), value.objectChecksumType(), value.objectChecksumValue(), value.objectOffset(),
                value.objectLength(), value.recordCount(), value.entryCount(), value.logicalBytes(), value.schemaRefs(),
                value.entryIndexRef(), value.projectionRef(), value.sliceChecksumType(), value.sliceChecksumValue(),
                value.minEventTimeMillis(), value.maxEventTimeMillis(), value.preparedAtMillis(), version);
    }

    private static OffsetIndexRecord copyOffsetIndex(OffsetIndexRecord value, long version) {
        return new OffsetIndexRecord(
                value.streamId(), value.offsetStart(), value.offsetEnd(), value.generation(), value.cumulativeSize(),
                value.objectId(), value.objectKey(), value.sliceId(), value.objectType(), value.physicalFormat(),
                value.logicalFormat(), value.payloadFormat(), value.objectOffset(), value.objectLength(),
                value.recordCount(), value.entryCount(), value.logicalBytes(), value.schemaRefs(), value.entryIndexRef(),
                value.projectionRef(), value.sliceChecksumType(), value.sliceChecksumValue(), value.minEventTimeMillis(),
                value.maxEventTimeMillis(), value.commitVersion(), value.tombstoned(), version);
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicLong ids = new AtomicLong();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + ids.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static ExecutorService boundedExecutor(int threads, int queueCapacity, String prefix) {
        return new ThreadPoolExecutor(
                threads,
                threads,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                daemonFactory(prefix),
                new ThreadPoolExecutor.AbortPolicy());
    }

    private record ChainExpectation(long offsetEnd, long cumulativeSize, long commitVersion) {
    }

    private record AnyCommit(
            String streamId,
            String commitId,
            String previousCommitId,
            long offsetStart,
            long offsetEnd,
            long cumulativeSize,
            long commitVersion,
            long logicalBytes) {
    }

    private record DurableRecord<T>(
            String key,
            T value,
            long metadataVersion,
            Checksum durableValueSha256) {
        private DurableRecord {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            if (metadataVersion < 0) {
                throw new IllegalArgumentException("metadataVersion must be non-negative");
            }
            F4ValueValidation.sha256(durableValueSha256, "durableValueSha256");
        }
    }

    private enum SearchStatus {
        FOUND,
        NOT_FOUND,
        EXHAUSTED,
        BROKEN
    }

    private record SearchResult(SearchStatus status, StreamCommitRecord commit, int scanned) {
    }

    private static final class BackendConditionException extends RuntimeException {
        private BackendConditionException(Throwable cause) {
            super(cause);
        }
    }
}
