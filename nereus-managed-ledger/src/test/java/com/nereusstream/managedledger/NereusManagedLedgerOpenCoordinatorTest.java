/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendRecoveryOptions;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.DeleteOptions;
import com.nereusstream.api.ErrorCode;
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
import com.nereusstream.managedledger.config.ManagedLedgerConfigValidator;
import com.nereusstream.managedledger.config.ManagedLedgerOpenConfigView;
import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.managedledger.integration.NereusCreationPermit;
import com.nereusstream.metadata.oxia.FakeManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.junit.jupiter.api.Test;

class NereusManagedLedgerOpenCoordinatorTest {
    private static final String CLUSTER = "cluster/a";
    private static final String NAME = "tenant/ns/persistent/topic";
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(100), ZoneOffset.UTC);

    @Test
    void firstCreatePublishesOnlyAfterPermitRevalidationAndExistingOpenUsesGetOnlyL0() {
        FakeStreamStorage streamStorage = new FakeStreamStorage();
        FakeManagedLedgerProjectionMetadataStore projections = new FakeManagedLedgerProjectionMetadataStore();
        MutableGuard guard = new MutableGuard(3);
        try (NereusManagedLedgerRuntime runtime = ManagedLedgerRuntimeTestSupport.runtime(streamStorage, projections)) {
            NereusManagedLedgerOpenCoordinator coordinator =
                    new NereusManagedLedgerOpenCoordinator(runtime, guard);

            NereusLedgerOpenResult first = coordinator.open(NAME, openConfig(true)).join();
            int creates = streamStorage.createCalls.get();
            NereusLedgerOpenResult reopened = coordinator.open(NAME, openConfig(true)).join();

            assertThat(first).isEqualTo(reopened);
            assertThat(first.streamMetadata().state()).isEqualTo(StreamState.ACTIVE);
            assertThat(first.projection().storageClassBindingGeneration()).isEqualTo(3);
            assertThat(guard.acquires).hasValue(2);
            assertThat(guard.validations).hasValue(1);
            assertThat(streamStorage.createCalls).hasValue(creates);
            assertThat(streamStorage.getCalls.get()).isPositive();
        }
    }

    @Test
    void openDoesNotReturnBeforeExactMaterializationRegistration() {
        FakeStreamStorage streamStorage = new FakeStreamStorage();
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        CompletableFuture<Void> registration = new CompletableFuture<>();
        CompletableFuture<Void> registrationInvoked =
                new CompletableFuture<>();
        AtomicReference<String> registeredName = new AtomicReference<>();
        AtomicReference<ManagedLedgerProjectionIdentity> registeredIdentity =
                new AtomicReference<>();
        try (NereusManagedLedgerRuntime runtime =
                ManagedLedgerRuntimeTestSupport.runtime(
                        streamStorage,
                        projections,
                        (name, identity) -> {
                            registeredName.set(name);
                            registeredIdentity.set(identity);
                            registrationInvoked.complete(null);
                            return registration;
                        })) {
            NereusManagedLedgerOpenCoordinator coordinator =
                    new NereusManagedLedgerOpenCoordinator(
                            runtime, new MutableGuard(3));

            CompletableFuture<NereusLedgerOpenResult> opened =
                    coordinator.open(NAME, openConfig(true));

            registrationInvoked.orTimeout(5, TimeUnit.SECONDS).join();
            assertThat(opened).isNotDone();
            assertThat(registeredName).hasValue(NAME);
            assertThat(registeredIdentity.get())
                    .isEqualTo(projections
                            .getProjection(CLUSTER, NAME)
                            .join()
                            .orElseThrow()
                            .projectionIdentity());

            registration.complete(null);
            assertThat(opened.join().topicProjection().managedLedgerName())
                    .isEqualTo(NAME);
        }
    }

    @Test
    void missingWithoutCreatePerformsNoL0WriteAndInspectionDoesNotAcquirePermit() {
        FakeStreamStorage streamStorage = new FakeStreamStorage();
        FakeManagedLedgerProjectionMetadataStore projections = new FakeManagedLedgerProjectionMetadataStore();
        MutableGuard guard = new MutableGuard(3);
        try (NereusManagedLedgerRuntime runtime = ManagedLedgerRuntimeTestSupport.runtime(streamStorage, projections)) {
            NereusManagedLedgerOpenCoordinator coordinator =
                    new NereusManagedLedgerOpenCoordinator(runtime, guard);

            assertNereusFailure(() -> coordinator.open(NAME, openConfig(false)).join(), ErrorCode.STREAM_NOT_FOUND);
            assertThat(coordinator.inspectStorageState(NAME).join())
                    .isEqualTo(NereusStorageStateSnapshot.missing());
            assertThat(streamStorage.createCalls).hasValue(0);
            assertThat(guard.acquires).hasValue(1);
            assertThat(guard.validations).hasValue(0);
        }
    }

    @Test
    void bindingMismatchFailsBeforeL0ReadAndMissingPublishedHeadIsCorruption() {
        FakeStreamStorage streamStorage = new FakeStreamStorage();
        FakeManagedLedgerProjectionMetadataStore projections = new FakeManagedLedgerProjectionMetadataStore();
        MutableGuard firstGuard = new MutableGuard(3);
        try (NereusManagedLedgerRuntime runtime = ManagedLedgerRuntimeTestSupport.runtime(streamStorage, projections)) {
            NereusManagedLedgerOpenCoordinator first =
                    new NereusManagedLedgerOpenCoordinator(runtime, firstGuard);
            NereusLedgerOpenResult opened = first.open(NAME, openConfig(true)).join();
            int gets = streamStorage.getCalls.get();

            NereusManagedLedgerOpenCoordinator stale =
                    new NereusManagedLedgerOpenCoordinator(runtime, new MutableGuard(4));
            assertNereusFailure(
                    () -> stale.open(NAME, openConfig(true)).join(),
                    ErrorCode.METADATA_INVARIANT_VIOLATION);
            assertThat(streamStorage.getCalls).hasValue(gets);

            streamStorage.remove(opened.streamMetadata().streamId());
            assertNereusFailure(
                    () -> first.inspectStorageState(NAME).join(),
                    ErrorCode.METADATA_INVARIANT_VIOLATION);
            assertThat(streamStorage.createCalls).hasValue(1);
        }
    }

    @Test
    void sealedMirrorReconcilesAndDeletedTopicRecreatesNextIncarnation() {
        FakeStreamStorage streamStorage = new FakeStreamStorage();
        FakeManagedLedgerProjectionMetadataStore projections = new FakeManagedLedgerProjectionMetadataStore();
        MutableGuard guard = new MutableGuard(3);
        try (NereusManagedLedgerRuntime runtime = ManagedLedgerRuntimeTestSupport.runtime(streamStorage, projections)) {
            NereusManagedLedgerOpenCoordinator coordinator =
                    new NereusManagedLedgerOpenCoordinator(runtime, guard);
            NereusLedgerOpenResult open = coordinator.open(NAME, openConfig(true)).join();
            streamStorage.seal(open.streamMetadata().streamId(), new SealOptions(
                    java.time.Duration.ofSeconds(1), "test")).join();

            NereusLedgerOpenResult sealed = coordinator.open(NAME, openConfig(true)).join();
            assertThat(sealed.streamMetadata().state()).isEqualTo(StreamState.SEALED);
            assertThat(sealed.topicProjection().parsedFacadeState()).isEqualTo(ManagedLedgerFacadeState.SEALED);

            StreamMetadata deletedMetadata = streamStorage.delete(
                    sealed.streamMetadata().streamId(),
                    new DeleteOptions(java.time.Duration.ofSeconds(1), "test")).join();
            TopicProjectionRecord deleting = projections.mirrorFacadeState(
                    CLUSTER, NAME, sealed.topicProjection().projectionIdentity(),
                    sealed.topicProjection().metadataVersion(), ManagedLedgerFacadeState.DELETING).join();
            projections.mirrorFacadeState(
                    CLUSTER, NAME, deleting.projectionIdentity(), deleting.metadataVersion(),
                    ManagedLedgerFacadeState.DELETED).join();
            assertThat(deletedMetadata.state()).isEqualTo(StreamState.DELETED);

            guard.bindingGeneration = 4;
            NereusLedgerOpenResult recreated = coordinator.open(NAME, openConfig(true)).join();
            assertThat(recreated.projection().incarnation()).isEqualTo(2);
            assertThat(recreated.projection().storageClassBindingGeneration()).isEqualTo(4);
            assertThat(recreated.projection().streamId()).isNotEqualTo(open.projection().streamId());
            assertThat(recreated.projection().virtualLedgerId()).isNotEqualTo(open.projection().virtualLedgerId());
        }
    }

    @Test
    void invalidNonemptyCandidateIsNotAdoptedWithoutPublishedAuthority() {
        FakeStreamStorage streamStorage = new FakeStreamStorage();
        streamStorage.install(new StreamMetadata(
                ManagedLedgerProjectionNames.streamId(NAME, 1),
                ManagedLedgerProjectionNames.streamName(NAME, 1),
                StreamState.ACTIVE,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                payloadAttributes(),
                CLOCK.millis(),
                2,
                1,
                7,
                0));
        FakeManagedLedgerProjectionMetadataStore projections = new FakeManagedLedgerProjectionMetadataStore();
        MutableGuard guard = new MutableGuard(3);
        try (NereusManagedLedgerRuntime runtime = ManagedLedgerRuntimeTestSupport.runtime(streamStorage, projections)) {
            NereusManagedLedgerOpenCoordinator coordinator =
                    new NereusManagedLedgerOpenCoordinator(runtime, guard);

            assertNereusFailure(
                    () -> coordinator.open(NAME, openConfig(true)).join(),
                    ErrorCode.METADATA_INVARIANT_VIOLATION);
            assertThat(projections.getProjection(CLUSTER, NAME).join()).isEmpty();
            assertThat(guard.validations).hasValue(0);
        }
    }

    private static ManagedLedgerOpenConfigView openConfig(boolean createIfMissing) {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setStorageClassName("nereus");
        config.setCreateIfMissing(createIfMissing);
        config.setProperties(Map.of("owner", "test"));
        return ManagedLedgerConfigValidator.captureForOpen(config);
    }

    private static Map<String, String> payloadAttributes() {
        return Map.of(
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1);
    }

    private static void assertNereusFailure(Runnable operation, ErrorCode code) {
        assertThatThrownBy(operation::run).satisfies(error -> assertThat(rootCause(error))
                .isInstanceOfSatisfying(NereusException.class,
                        nereus -> assertThat(nereus.code()).isEqualTo(code)));
    }

    private static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class MutableGuard implements NereusCreationGuard {
        private final AtomicInteger acquires = new AtomicInteger();
        private final AtomicInteger validations = new AtomicInteger();
        private volatile long bindingGeneration;

        private MutableGuard(long bindingGeneration) {
            this.bindingGeneration = bindingGeneration;
        }

        @Override
        public CompletableFuture<NereusCreationPermit> acquire(String persistenceName) {
            acquires.incrementAndGet();
            long generation = bindingGeneration;
            return CompletableFuture.completedFuture(new NereusCreationPermit() {
                @Override
                public String persistenceName() {
                    return persistenceName;
                }

                @Override
                public long bindingGeneration() {
                    return generation;
                }

                @Override
                public CompletableFuture<Void> validateBeforeProjectionPublish() {
                    validations.incrementAndGet();
                    return generation == bindingGeneration
                            ? CompletableFuture.completedFuture(null)
                            : CompletableFuture.failedFuture(new NereusException(
                                    ErrorCode.METADATA_CONDITION_FAILED, true, "binding changed"));
                }
            });
        }
    }

    private static final class FakeStreamStorage implements StreamStorage {
        private final Map<StreamId, StreamMetadata> streams = new HashMap<>();
        private final AtomicInteger createCalls = new AtomicInteger();
        private final AtomicInteger getCalls = new AtomicInteger();

        @Override
        public synchronized CompletableFuture<StreamMetadata> createOrGetStream(
                StreamName streamName,
                StreamCreateOptions options) {
            createCalls.incrementAndGet();
            StreamId streamId = com.nereusstream.api.keys.DeterministicIds.streamIdFor(streamName);
            StreamMetadata metadata = streams.computeIfAbsent(streamId, ignored -> new StreamMetadata(
                    streamId,
                    streamName,
                    StreamState.ACTIVE,
                    options.profile(),
                    options.attributes(),
                    CLOCK.millis(),
                    0,
                    0,
                    0,
                    0));
            return CompletableFuture.completedFuture(metadata);
        }

        @Override
        public synchronized CompletableFuture<StreamMetadata> getStreamMetadata(StreamId streamId) {
            getCalls.incrementAndGet();
            StreamMetadata metadata = streams.get(streamId);
            return metadata == null
                    ? CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.STREAM_NOT_FOUND, false, "missing"))
                    : CompletableFuture.completedFuture(metadata);
        }

        @Override
        public synchronized CompletableFuture<StreamMetadata> seal(StreamId streamId, SealOptions options) {
            StreamMetadata current = streams.get(streamId);
            if (current == null) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.STREAM_NOT_FOUND, false, "missing"));
            }
            StreamMetadata sealed = copy(current, StreamState.SEALED);
            streams.put(streamId, sealed);
            return CompletableFuture.completedFuture(sealed);
        }

        @Override
        public synchronized CompletableFuture<StreamMetadata> delete(StreamId streamId, DeleteOptions options) {
            StreamMetadata current = streams.get(streamId);
            if (current == null) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.STREAM_NOT_FOUND, false, "missing"));
            }
            StreamMetadata deleted = copy(current, StreamState.DELETED);
            streams.put(streamId, deleted);
            return CompletableFuture.completedFuture(deleted);
        }

        synchronized void install(StreamMetadata metadata) {
            streams.put(metadata.streamId(), metadata);
        }

        synchronized void remove(StreamId streamId) {
            streams.remove(streamId);
        }

        @Override
        public CompletableFuture<AppendSession> acquireAppendSession(
                StreamId streamId, AppendSessionOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<AppendResult> append(
                StreamId streamId, AppendBatch batch, AppendOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<AppendResult> recoverAppend(
                StreamId streamId, AppendAttemptId attemptId, AppendRecoveryOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<ReadResult> read(StreamId streamId, long startOffset, ReadOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<ResolveResult> resolve(
                StreamId streamId, long startOffset, ResolveOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<Void> trim(StreamId streamId, long beforeOffset, TrimOptions options) {
            return unsupported();
        }

        @Override
        public void close() {
        }

        private static StreamMetadata copy(StreamMetadata current, StreamState state) {
            return new StreamMetadata(
                    current.streamId(), current.streamName(), state, current.profile(), current.attributes(),
                    current.createdAtMillis(), current.metadataVersion() + 1,
                    current.committedEndOffset(), current.cumulativeSize(), current.trimOffset());
        }

        private static <T> CompletableFuture<T> unsupported() {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }
}
