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

package com.nereusstream.core;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendRecoveryOptions;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.DeleteOptions;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ResolveOptions;
import com.nereusstream.api.ResolveResult;
import com.nereusstream.api.SealOptions;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.api.TrimOptions;
import com.nereusstream.core.append.AppendAdmissionGuard;
import com.nereusstream.core.append.AppendCoordinator;
import com.nereusstream.core.append.AppendSessionManager;
import com.nereusstream.core.append.DefaultGenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.append.GenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.append.RequiredObjectGenerationCompletion;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.lifecycle.StreamLifecycleCoordinator;
import com.nereusstream.core.profile.Phase15StorageProfileResolver;
import com.nereusstream.core.profile.StorageProfileResolver;
import com.nereusstream.core.read.ReadCoordinator;
import com.nereusstream.core.read.ReadMetricsObserver;
import com.nereusstream.core.read.ReadResolver;
import com.nereusstream.core.read.Phase4ReadComponents;
import com.nereusstream.core.recovery.AppendRecoverySearcher;
import com.nereusstream.core.recovery.MetadataAppendRecoverySearcher;
import com.nereusstream.core.trim.TrimCoordinator;
import com.nereusstream.core.trim.TrimMetricsObserver;
import com.nereusstream.core.wal.PrimaryWalRegistry;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.objectstore.wal.WalObjectReader;
import com.nereusstream.objectstore.wal.WalObjectWriter;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/** Phase 1 stream-storage facade. M4 implements the strict Object WAL append path. */
public final class DefaultStreamStorage implements StreamStorage {
    private final StreamStorageConfig config;
    private final OxiaMetadataStore metadataStore;
    private final AppendSessionManager appendSessionManager;
    private final AppendCoordinator appendCoordinator;
    private final ReadCoordinator readCoordinator;
    private final TrimCoordinator trimCoordinator;
    private final StreamLifecycleCoordinator lifecycleCoordinator;
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Complete generation-zero-only provider-neutral composition used by additional primary-WAL modules. */
    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            PrimaryWalRegistry primaryWalRegistry,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            AppendRecoverySearcher recoverySearcher,
            StorageProfileResolver profileResolver,
            AppendAdmissionGuard appendAdmissionGuard,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver,
            TrimMetricsObserver trimMetricsObserver) {
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        PrimaryWalRegistry registry = Objects.requireNonNull(primaryWalRegistry, "primaryWalRegistry");
        Objects.requireNonNull(physicalReferences, "physicalReferences");
        Objects.requireNonNull(recoverySearcher, "recoverySearcher");
        Objects.requireNonNull(profileResolver, "profileResolver");
        Objects.requireNonNull(appendAdmissionGuard, "appendAdmissionGuard");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        Objects.requireNonNull(readMetricsObserver, "readMetricsObserver");
        Objects.requireNonNull(trimMetricsObserver, "trimMetricsObserver");
        this.appendSessionManager = new AppendSessionManager(config, metadataStore, clock);
        this.appendCoordinator = new AppendCoordinator(
                config,
                metadataStore,
                registry,
                appendSessionManager,
                physicalReferences,
                recoverySearcher,
                profileResolver,
                appendAdmissionGuard,
                clock,
                callbackExecutor);
        ReadResolver readResolver = new ReadResolver(
                config,
                metadataStore,
                profileResolver,
                registry::hasReader,
                clock,
                readMetricsObserver,
                callbackExecutor);
        this.readCoordinator = new ReadCoordinator(
                config,
                readResolver,
                registry.readerRegistry(),
                readMetricsObserver,
                callbackExecutor);
        this.trimCoordinator = new TrimCoordinator(
                config, metadataStore, readCoordinator::invalidate, trimMetricsObserver, callbackExecutor);
        this.lifecycleCoordinator = new StreamLifecycleCoordinator(config, metadataStore, appendCoordinator);
    }

    /** Complete provider-neutral generation-aware seam used by primary WALs whose higher generations are Objects. */
    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            PrimaryWalRegistry primaryWalRegistry,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            AppendRecoverySearcher recoverySearcher,
            StorageProfileResolver profileResolver,
            AppendAdmissionGuard appendAdmissionGuard,
            Phase4ReadComponents readComponents,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver,
            TrimMetricsObserver trimMetricsObserver) {
        this(
                config,
                metadataStore,
                primaryWalRegistry,
                physicalReferences,
                recoverySearcher,
                profileResolver,
                appendAdmissionGuard,
                null,
                readComponents,
                clock,
                callbackExecutor,
                readMetricsObserver,
                trimMetricsObserver);
    }

    /** Generation-aware composition whose producer may wait for an exact higher Object generation. */
    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            PrimaryWalRegistry primaryWalRegistry,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            AppendRecoverySearcher recoverySearcher,
            StorageProfileResolver profileResolver,
            AppendAdmissionGuard appendAdmissionGuard,
            RequiredObjectGenerationCompletion requiredObjectGeneration,
            Phase4ReadComponents readComponents,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver,
            TrimMetricsObserver trimMetricsObserver) {
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        PrimaryWalRegistry registry = Objects.requireNonNull(primaryWalRegistry, "primaryWalRegistry");
        Objects.requireNonNull(physicalReferences, "physicalReferences");
        Objects.requireNonNull(recoverySearcher, "recoverySearcher");
        Objects.requireNonNull(profileResolver, "profileResolver");
        Objects.requireNonNull(appendAdmissionGuard, "appendAdmissionGuard");
        Phase4ReadComponents components = Objects.requireNonNull(readComponents, "readComponents");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        Objects.requireNonNull(readMetricsObserver, "readMetricsObserver");
        Objects.requireNonNull(trimMetricsObserver, "trimMetricsObserver");
        this.appendSessionManager = new AppendSessionManager(config, metadataStore, clock);
        this.appendCoordinator = new AppendCoordinator(
                config,
                metadataStore,
                registry,
                appendSessionManager,
                physicalReferences,
                recoverySearcher,
                profileResolver,
                appendAdmissionGuard,
                requiredObjectGeneration,
                clock,
                callbackExecutor);
        ReadResolver readResolver = new ReadResolver(
                config,
                metadataStore,
                profileResolver,
                registry::hasReader,
                clock,
                readMetricsObserver,
                callbackExecutor);
        this.readCoordinator = new ReadCoordinator(
                config,
                readResolver,
                components.resolver(),
                components.readers(),
                components.failureHandler(),
                components.retryPolicy(),
                readMetricsObserver,
                callbackExecutor);
        this.trimCoordinator = new TrimCoordinator(
                config,
                metadataStore,
                readCoordinator::invalidate,
                trimMetricsObserver,
                callbackExecutor);
        this.lifecycleCoordinator = new StreamLifecycleCoordinator(config, metadataStore, appendCoordinator);
    }

    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            WalObjectReader walObjectReader,
            Clock clock,
            Executor callbackExecutor) {
        this(
                config,
                metadataStore,
                walObjectWriter,
                walObjectReader,
                inferredPhysicalReferences(config, metadataStore, clock),
                clock,
                callbackExecutor,
                ReadMetricsObserver.noop(),
                TrimMetricsObserver.noop());
    }

    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            WalObjectReader walObjectReader,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver) {
        this(
                config,
                metadataStore,
                walObjectWriter,
                walObjectReader,
                inferredPhysicalReferences(config, metadataStore, clock),
                clock,
                callbackExecutor,
                readMetricsObserver,
                TrimMetricsObserver.noop());
    }

    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            WalObjectReader walObjectReader,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            Clock clock,
            Executor callbackExecutor) {
        this(
                config,
                metadataStore,
                walObjectWriter,
                walObjectReader,
                physicalReferences,
                clock,
                callbackExecutor,
                ReadMetricsObserver.noop(),
                TrimMetricsObserver.noop());
    }

    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            WalObjectReader walObjectReader,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver,
            TrimMetricsObserver trimMetricsObserver) {
        this(
                config,
                metadataStore,
                walObjectWriter,
                walObjectReader,
                inferredPhysicalReferences(config, metadataStore, clock),
                clock,
                callbackExecutor,
                readMetricsObserver,
                trimMetricsObserver);
    }

    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            WalObjectReader walObjectReader,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver,
            TrimMetricsObserver trimMetricsObserver) {
        this(
                config,
                metadataStore,
                walObjectWriter,
                walObjectReader,
                physicalReferences,
                new MetadataAppendRecoverySearcher(config.cluster(), metadataStore),
                clock,
                callbackExecutor,
                readMetricsObserver,
                trimMetricsObserver);
    }

    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            WalObjectReader walObjectReader,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            AppendRecoverySearcher recoverySearcher,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver,
            TrimMetricsObserver trimMetricsObserver) {
        this(
                config,
                metadataStore,
                walObjectWriter,
                walObjectReader,
                physicalReferences,
                recoverySearcher,
                new Phase15StorageProfileResolver(),
                clock,
                callbackExecutor,
                readMetricsObserver,
                trimMetricsObserver);
    }

    /**
     * Explicit profile-resolver seam used by the Phase 4 runtime after registration, activation, and lag admission
     * have been composed ahead of primary IO.
     */
    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            WalObjectReader walObjectReader,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            AppendRecoverySearcher recoverySearcher,
            StorageProfileResolver profileResolver,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver,
            TrimMetricsObserver trimMetricsObserver) {
        this(
                config,
                metadataStore,
                walObjectWriter,
                walObjectReader,
                physicalReferences,
                recoverySearcher,
                profileResolver,
                AppendAdmissionGuard.noOp(),
                clock,
                callbackExecutor,
                readMetricsObserver,
                trimMetricsObserver);
    }

    /**
     * Complete Phase 4 append seam. The admission guard executes on the per-stream lane immediately before primary
     * WAL preparation.
     */
    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            WalObjectReader walObjectReader,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            AppendRecoverySearcher recoverySearcher,
            StorageProfileResolver profileResolver,
            AppendAdmissionGuard appendAdmissionGuard,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver,
            TrimMetricsObserver trimMetricsObserver) {
        this(
                config,
                metadataStore,
                walObjectWriter,
                walObjectReader,
                physicalReferences,
                recoverySearcher,
                profileResolver,
                appendAdmissionGuard,
                Optional.empty(),
                clock,
                callbackExecutor,
                readMetricsObserver,
                trimMetricsObserver);
    }

    /**
     * Complete Phase 4 storage seam. Append enablement and generation-aware read/repair are installed as one unit.
     */
    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            WalObjectReader walObjectReader,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            AppendRecoverySearcher recoverySearcher,
            StorageProfileResolver profileResolver,
            AppendAdmissionGuard appendAdmissionGuard,
            Phase4ReadComponents readComponents,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver,
            TrimMetricsObserver trimMetricsObserver) {
        this(
                config,
                metadataStore,
                walObjectWriter,
                walObjectReader,
                physicalReferences,
                recoverySearcher,
                profileResolver,
                appendAdmissionGuard,
                Optional.of(Objects.requireNonNull(
                        readComponents, "readComponents")),
                clock,
                callbackExecutor,
                readMetricsObserver,
                trimMetricsObserver);
    }

    private DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            WalObjectReader walObjectReader,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            AppendRecoverySearcher recoverySearcher,
            StorageProfileResolver profileResolver,
            AppendAdmissionGuard appendAdmissionGuard,
            Optional<Phase4ReadComponents> readComponents,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver,
            TrimMetricsObserver trimMetricsObserver) {
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        Objects.requireNonNull(walObjectWriter, "walObjectWriter");
        Objects.requireNonNull(walObjectReader, "walObjectReader");
        Objects.requireNonNull(physicalReferences, "physicalReferences");
        Objects.requireNonNull(recoverySearcher, "recoverySearcher");
        Objects.requireNonNull(profileResolver, "profileResolver");
        Objects.requireNonNull(appendAdmissionGuard, "appendAdmissionGuard");
        Objects.requireNonNull(readComponents, "readComponents");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        Objects.requireNonNull(readMetricsObserver, "readMetricsObserver");
        Objects.requireNonNull(trimMetricsObserver, "trimMetricsObserver");
        this.appendSessionManager = new AppendSessionManager(config, metadataStore, clock);
        this.appendCoordinator = new AppendCoordinator(
                config,
                metadataStore,
                walObjectWriter,
                appendSessionManager,
                physicalReferences,
                recoverySearcher,
                profileResolver,
                appendAdmissionGuard,
                clock,
                callbackExecutor);
        ReadResolver readResolver = new ReadResolver(
                config,
                metadataStore,
                profileResolver,
                type -> type == com.nereusstream.api.target.ReadTargetType.OBJECT_SLICE,
                clock,
                readMetricsObserver,
                callbackExecutor);
        this.readCoordinator = readComponents
                .map(components -> new ReadCoordinator(
                        config,
                        readResolver,
                        components.resolver(),
                        components.readers(),
                        components.failureHandler(),
                        components.retryPolicy(),
                        readMetricsObserver,
                        callbackExecutor))
                .orElseGet(() -> new ReadCoordinator(
                        config,
                        readResolver,
                        walObjectReader,
                        readMetricsObserver,
                        callbackExecutor));
        this.trimCoordinator = new TrimCoordinator(
                config,
                metadataStore,
                readCoordinator::invalidate,
                trimMetricsObserver,
                callbackExecutor);
        this.lifecycleCoordinator = new StreamLifecycleCoordinator(config, metadataStore, appendCoordinator);
    }

    @Override
    public CompletableFuture<StreamMetadata> createOrGetStream(
            StreamName streamName,
            StreamCreateOptions options) {
        Objects.requireNonNull(streamName, "streamName");
        Objects.requireNonNull(options, "options");
        CompletableFuture<StreamMetadata> rejection = rejectIfClosed();
        if (rejection != null) {
            return rejection;
        }
        return metadataStore.createOrGetStream(config.cluster(), streamName, options)
                .thenCompose(record -> loadStreamMetadata(new StreamId(record.streamId())));
    }

    @Override
    public CompletableFuture<AppendSession> acquireAppendSession(
            StreamId streamId,
            AppendSessionOptions options) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(options, "options");
        CompletableFuture<AppendSession> rejection = rejectIfClosed();
        return rejection != null ? rejection : appendSessionManager.acquire(streamId, options);
    }

    @Override
    public CompletableFuture<AppendResult> append(
            StreamId streamId,
            AppendBatch batch,
            AppendOptions options) {
        return appendCoordinator.append(streamId, batch, options);
    }

    @Override
    public CompletableFuture<AppendResult> recoverAppend(
            StreamId streamId, AppendAttemptId attemptId, AppendRecoveryOptions options) {
        return appendCoordinator.recoverAppend(streamId, attemptId, options);
    }

    @Override
    public CompletableFuture<ReadResult> read(
            StreamId streamId,
            long startOffset,
            ReadOptions options) {
        return readCoordinator.read(streamId, startOffset, options);
    }

    @Override
    public CompletableFuture<ResolveResult> resolve(
            StreamId streamId,
            long startOffset,
            ResolveOptions options) {
        return readCoordinator.resolve(streamId, startOffset, options);
    }

    @Override
    public CompletableFuture<Void> trim(
            StreamId streamId,
            long beforeOffset,
            TrimOptions options) {
        return trimCoordinator.trim(streamId, beforeOffset, options);
    }

    @Override
    public CompletableFuture<StreamMetadata> getStreamMetadata(StreamId streamId) {
        Objects.requireNonNull(streamId, "streamId");
        CompletableFuture<StreamMetadata> rejection = rejectIfClosed();
        return rejection != null ? rejection : loadStreamMetadata(streamId);
    }

    @Override
    public CompletableFuture<StreamMetadata> seal(StreamId streamId, SealOptions options) {
        return lifecycleCoordinator.seal(streamId, options);
    }

    @Override
    public CompletableFuture<StreamMetadata> delete(StreamId streamId, DeleteOptions options) {
        return lifecycleCoordinator.delete(streamId, options);
    }

    private CompletableFuture<StreamMetadata> loadStreamMetadata(StreamId streamId) {
        return metadataStore.getStreamSnapshot(config.cluster(), streamId).thenApply(snapshot -> {
            StreamMetadataRecord stream = snapshot.metadata();
            StreamState state;
            StorageProfile profile;
            try {
                state = StreamState.valueOf(stream.state());
                profile = StorageProfile.valueOf(stream.profile()).canonical();
            } catch (IllegalArgumentException e) {
                throw new NereusException(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "stream metadata contains an unknown state or profile",
                        e);
            }
            return new StreamMetadata(
                    streamId,
                    new StreamName(stream.streamName()),
                    state,
                    profile,
                    stream.attributes(),
                    stream.createdAtMillis(),
                    snapshot.metadataVersion(),
                    snapshot.committedEnd().committedEndOffset(),
                    snapshot.committedEnd().cumulativeSize(),
                    snapshot.trim().trimOffset());
        });
    }

    private <T> CompletableFuture<T> rejectIfClosed() {
        return closed.get()
                ? NereusException.failedFuture(ErrorCode.STORAGE_CLOSED, false, "stream storage is closed")
                : null;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            long deadlineNanos = shutdownDeadline(config.shutdownGrace());
            appendCoordinator.beginClose();
            lifecycleCoordinator.close();
            trimCoordinator.beginClose();
            readCoordinator.close();
            trimCoordinator.awaitClose(remainingShutdownGrace(deadlineNanos));
            appendCoordinator.awaitClose(remainingShutdownGrace(deadlineNanos));
        }
    }

    private static long shutdownDeadline(Duration grace) {
        long graceNanos;
        try {
            graceNanos = grace.toNanos();
        } catch (ArithmeticException e) {
            graceNanos = Long.MAX_VALUE;
        }
        long now = System.nanoTime();
        return graceNanos >= Long.MAX_VALUE - now ? Long.MAX_VALUE : now + graceNanos;
    }

    private static Duration remainingShutdownGrace(long deadlineNanos) {
        return Duration.ofNanos(Math.max(0, deadlineNanos - System.nanoTime()));
    }

    private static GenerationZeroPhysicalReferencePublisher inferredPhysicalReferences(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            Clock clock) {
        if (!(metadataStore instanceof PhysicalObjectMetadataStore physicalStore)) {
            throw new IllegalArgumentException(
                    "GenerationZeroPhysicalReferencePublisher is required for this metadata-store composition");
        }
        DefaultObjectProtectionManager protections = new DefaultObjectProtectionManager(
                config.cluster(),
                physicalStore,
                Duration.ofMinutes(10),
                Duration.ZERO,
                Duration.ofHours(24),
                clock);
        return new DefaultGenerationZeroPhysicalReferencePublisher(
                config.cluster(),
                metadataStore,
                physicalStore,
                protections);
    }
}
