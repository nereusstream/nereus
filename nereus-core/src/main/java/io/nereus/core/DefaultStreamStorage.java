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

package io.nereus.core;

import io.nereus.api.AppendBatch;
import io.nereus.api.AppendOptions;
import io.nereus.api.AppendResult;
import io.nereus.api.AppendSession;
import io.nereus.api.AppendSessionOptions;
import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;
import io.nereus.api.ReadOptions;
import io.nereus.api.ReadResult;
import io.nereus.api.ResolveOptions;
import io.nereus.api.ResolveResult;
import io.nereus.api.StorageProfile;
import io.nereus.api.StreamCreateOptions;
import io.nereus.api.StreamId;
import io.nereus.api.StreamMetadata;
import io.nereus.api.StreamName;
import io.nereus.api.StreamState;
import io.nereus.api.StreamStorage;
import io.nereus.api.TrimOptions;
import io.nereus.core.append.AppendCoordinator;
import io.nereus.core.append.AppendSessionManager;
import io.nereus.core.read.ReadCoordinator;
import io.nereus.core.read.ReadMetricsObserver;
import io.nereus.core.read.ReadResolver;
import io.nereus.metadata.oxia.OxiaMetadataStore;
import io.nereus.metadata.oxia.records.CommittedEndOffsetRecord;
import io.nereus.metadata.oxia.records.StreamMetadataRecord;
import io.nereus.metadata.oxia.records.TrimRecord;
import io.nereus.objectstore.wal.WalObjectReader;
import io.nereus.objectstore.wal.WalObjectWriter;
import java.time.Clock;
import java.util.Objects;
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
    private final AtomicBoolean closed = new AtomicBoolean();

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
                clock,
                callbackExecutor,
                ReadMetricsObserver.noop());
    }

    public DefaultStreamStorage(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            WalObjectWriter walObjectWriter,
            WalObjectReader walObjectReader,
            Clock clock,
            Executor callbackExecutor,
            ReadMetricsObserver readMetricsObserver) {
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        Objects.requireNonNull(walObjectWriter, "walObjectWriter");
        Objects.requireNonNull(walObjectReader, "walObjectReader");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        Objects.requireNonNull(readMetricsObserver, "readMetricsObserver");
        this.appendSessionManager = new AppendSessionManager(config, metadataStore, clock);
        this.appendCoordinator = new AppendCoordinator(
                config,
                metadataStore,
                walObjectWriter,
                appendSessionManager,
                clock,
                callbackExecutor);
        ReadResolver readResolver = new ReadResolver(
                config,
                metadataStore,
                clock,
                readMetricsObserver,
                callbackExecutor);
        this.readCoordinator = new ReadCoordinator(
                config,
                readResolver,
                walObjectReader,
                readMetricsObserver,
                callbackExecutor);
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
        return milestoneFailure("trim", "M6");
    }

    @Override
    public CompletableFuture<StreamMetadata> getStreamMetadata(StreamId streamId) {
        Objects.requireNonNull(streamId, "streamId");
        CompletableFuture<StreamMetadata> rejection = rejectIfClosed();
        return rejection != null ? rejection : loadStreamMetadata(streamId);
    }

    private CompletableFuture<StreamMetadata> loadStreamMetadata(StreamId streamId) {
        CompletableFuture<StreamMetadataRecord> metadata = metadataStore.getStream(config.cluster(), streamId);
        CompletableFuture<CommittedEndOffsetRecord> committed =
                metadataStore.getCommittedEndOffset(config.cluster(), streamId);
        CompletableFuture<TrimRecord> trim = metadataStore.getTrim(config.cluster(), streamId);
        return CompletableFuture.allOf(metadata, committed, trim).thenApply(ignored -> {
            StreamMetadataRecord stream = metadata.join();
            CommittedEndOffsetRecord end = committed.join();
            TrimRecord lowWatermark = trim.join();
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
            long metadataVersion = Math.max(
                    stream.metadataVersion(),
                    Math.max(end.metadataVersion(), lowWatermark.metadataVersion()));
            return new StreamMetadata(
                    streamId,
                    new StreamName(stream.streamName()),
                    state,
                    profile,
                    stream.attributes(),
                    stream.createdAtMillis(),
                    metadataVersion,
                    end.committedEndOffset(),
                    end.cumulativeSize(),
                    lowWatermark.trimOffset());
        });
    }

    private <T> CompletableFuture<T> rejectIfClosed() {
        return closed.get()
                ? NereusException.failedFuture(ErrorCode.STORAGE_CLOSED, false, "stream storage is closed")
                : null;
    }

    private <T> CompletableFuture<T> milestoneFailure(String operation, String milestone) {
        CompletableFuture<T> rejection = rejectIfClosed();
        if (rejection != null) {
            return rejection;
        }
        return NereusException.failedFuture(
                ErrorCode.UNSUPPORTED_FORMAT,
                false,
                operation + " is not implemented until " + milestone);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            readCoordinator.close();
            appendCoordinator.close();
        }
    }
}
