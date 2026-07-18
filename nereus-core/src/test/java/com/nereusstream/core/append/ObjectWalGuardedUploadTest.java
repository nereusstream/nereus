/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectAttemptGuard;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import com.nereusstream.objectstore.ReplayableObjectUpload;
import com.nereusstream.objectstore.wal.CompressionType;
import com.nereusstream.objectstore.wal.DefaultWalObjectWriter;
import com.nereusstream.objectstore.wal.PreparedWalObject;
import com.nereusstream.objectstore.wal.WalStreamSliceInput;
import com.nereusstream.objectstore.wal.WalWriteOptions;
import com.nereusstream.objectstore.wal.WalWriteRequest;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ObjectWalGuardedUploadTest {
    private static final String CLUSTER = "guarded-object-wal";
    private static final String WRITER_ID = "guarded-writer";
    private static final Duration UPLOAD_TIMEOUT = Duration.ofSeconds(5);

    @Test
    void providerRetryRevalidatesSessionAndRejectsDeletedRootBeforeSecondTransmission() {
        try (Fixture fixture = new Fixture()) {
            AppendSession session = fixture.acquireSession(Duration.ofMinutes(5));
            PreparedWalObject prepared = fixture.prepare(session);
            PhysicalObjectIdentity object = identity(prepared);
            fixture.objectStore.retryAfterFirstTransmission(() -> fixture.transitionToDeleted(object, false));

            assertThatThrownBy(() -> fixture.writer
                            .upload(prepared, fixture.guard(session, object))
                            .join())
                    .satisfies(failure -> assertNereusFailure(failure, ErrorCode.FENCED_APPEND, false));

            assertThat(fixture.objectStore.authorizationAttempts()).containsExactly(1, 2);
            assertThat(fixture.objectStore.providerTransmissions()).isEqualTo(1);
            assertThat(fixture.physical.getRoot(CLUSTER, object.objectKeyHash()).join())
                    .get()
                    .extracting(VersionedPhysicalObjectRoot::value)
                    .extracting(PhysicalObjectRootRecord::lifecycle)
                    .isEqualTo(PhysicalObjectLifecycle.DELETED);
        }
    }

    @Test
    void expiredSessionRejectsFirstAttemptAfterRootTombstoneAndFreshAppendUsesFreshKey() {
        try (Fixture fixture = new Fixture()) {
            AppendSession expiredSession = fixture.acquireSession(Duration.ofMillis(100));
            PreparedWalObject stale = fixture.prepare(expiredSession);
            PhysicalObjectIdentity staleObject = identity(stale);
            fixture.transitionToDeleted(staleObject, true);
            assertThat(fixture.physical.getRoot(CLUSTER, staleObject.objectKeyHash()).join()).isEmpty();

            fixture.clock.advance(Duration.ofMillis(101));
            fixture.objectStore.singleAttempt();
            assertThatThrownBy(() -> fixture.writer
                            .upload(stale, fixture.guard(expiredSession, staleObject))
                            .join())
                    .satisfies(failure -> assertNereusFailure(
                            failure, ErrorCode.APPEND_SESSION_EXPIRED, true));
            assertThat(fixture.objectStore.authorizationAttempts()).containsExactly(1);
            assertThat(fixture.objectStore.providerTransmissions()).isZero();

            AppendSession freshSession = fixture.acquireSession(Duration.ofMinutes(5));
            PreparedWalObject fresh = fixture.prepare(freshSession);
            PhysicalObjectIdentity freshObject = identity(fresh);
            assertThat(freshSession.epoch()).isGreaterThan(expiredSession.epoch());
            assertThat(freshSession.fencingToken()).isNotEqualTo(expiredSession.fencingToken());
            assertThat(fresh.result().objectKey()).isNotEqualTo(stale.result().objectKey());

            fixture.writer.upload(fresh, fixture.guard(freshSession, freshObject)).join();

            assertThat(fixture.objectStore.authorizationAttempts()).containsExactly(1, 1);
            assertThat(fixture.objectStore.providerTransmissions()).isEqualTo(1);
        }
    }

    private static PhysicalObjectIdentity identity(PreparedWalObject prepared) {
        return PhysicalObjectIdentity.create(
                prepared.result().objectKey(),
                Optional.of(prepared.result().objectId()),
                PhysicalObjectKind.OBJECT_WAL,
                prepared.result().objectLength(),
                prepared.result().storageChecksum(),
                Optional.empty(),
                Optional.empty());
    }

    private static void assertNereusFailure(
            Throwable failure,
            ErrorCode expectedCode,
            boolean expectedRetriable) {
        Throwable exact = unwrap(failure);
        assertThat(exact).isInstanceOf(NereusException.class);
        NereusException nereus = (NereusException) exact;
        assertThat(nereus.code()).isEqualTo(expectedCode);
        assertThat(nereus.retriable()).isEqualTo(expectedRetriable);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class Fixture implements AutoCloseable {
        private final MutableClock clock = new MutableClock(1_000);
        private final FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        private final FakePhysicalObjectMetadataStore physical = new FakePhysicalObjectMetadataStore();
        private final ObjectProtectionManager protections = new DefaultObjectProtectionManager(
                CLUSTER,
                physical,
                Duration.ofMinutes(10),
                Duration.ZERO,
                Duration.ofHours(1),
                clock);
        private final DefaultGenerationZeroPhysicalReferencePublisher publisher =
                new DefaultGenerationZeroPhysicalReferencePublisher(
                        CLUSTER, metadata, physical, protections);
        private final GuardedRetryObjectStore objectStore = new GuardedRetryObjectStore();
        private final DefaultWalObjectWriter writer = new DefaultWalObjectWriter(
                objectStore,
                "f4-guarded-upload-test",
                Clock.fixed(Instant.parse("2026-07-19T00:00:00Z"), ZoneOffset.UTC));
        private final StreamId streamId;

        private Fixture() {
            streamId = new StreamId(metadata.createOrGetStream(
                            CLUSTER,
                            new StreamName("tenant/namespace/guarded-object-wal"),
                            new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                    .join()
                    .streamId());
        }

        private AppendSession acquireSession(Duration ttl) {
            AppendSessionRecord record = metadata.acquireAppendSession(
                            CLUSTER,
                            streamId,
                            new AppendSessionOptions(WRITER_ID, ttl, false))
                    .join();
            return new AppendSession(
                    streamId,
                    record.writerId(),
                    record.epoch(),
                    record.fencingToken(),
                    record.leaseVersion(),
                    record.expiresAtMillis());
        }

        private PreparedWalObject prepare(AppendSession session) {
            long eventTime = clock.millis();
            AppendBatch batch = new AppendBatch(
                    PayloadFormat.OPAQUE_RECORD_BATCH,
                    List.of(new AppendEntry(
                            "value".getBytes(StandardCharsets.UTF_8),
                            1,
                            eventTime,
                            Map.of())),
                    1,
                    1,
                    eventTime,
                    eventTime,
                    List.of(),
                    Map.of(),
                    Optional.empty());
            return writer.prepare(new WalWriteRequest(
                    CLUSTER,
                    WRITER_ID,
                    "guardedrunhash",
                    session.epoch(),
                    List.of(new WalStreamSliceInput(streamId, batch)),
                    new WalWriteOptions(
                            CompressionType.NONE,
                            1 << 20,
                            1 << 20,
                            UPLOAD_TIMEOUT,
                            true)));
        }

        private PutObjectAttemptGuard guard(
                AppendSession session,
                PhysicalObjectIdentity object) {
            return (key, ignoredAttempt) -> {
                if (!key.equals(object.objectKey())) {
                    return CompletableFuture.failedFuture(new AssertionError(
                            "provider guard received a different Object-WAL key"));
                }
                return publisher.authorizeUpload(session, object, UPLOAD_TIMEOUT);
            };
        }

        private void transitionToDeleted(PhysicalObjectIdentity object, boolean retireRoot) {
            VersionedPhysicalObjectRoot active = physical.createRoot(
                            CLUSTER, activeRoot(object, clock.millis()))
                    .join();
            VersionedPhysicalObjectRoot marked = physical.compareAndSetRoot(
                            CLUSTER,
                            gcRoot(active.value(), PhysicalObjectLifecycle.MARKED, 2, false),
                            active.metadataVersion())
                    .join();
            VersionedPhysicalObjectRoot deleting = physical.compareAndSetRoot(
                            CLUSTER,
                            gcRoot(marked.value(), PhysicalObjectLifecycle.DELETING, 3, false),
                            marked.metadataVersion())
                    .join();
            VersionedPhysicalObjectRoot deleted = physical.compareAndSetRoot(
                            CLUSTER,
                            gcRoot(deleting.value(), PhysicalObjectLifecycle.DELETED, 4, false),
                            deleting.metadataVersion())
                    .join();
            if (!retireRoot) {
                return;
            }
            VersionedPhysicalObjectRoot observed = physical.compareAndSetRoot(
                            CLUSTER,
                            gcRoot(deleted.value(), PhysicalObjectLifecycle.DELETED, 4, true),
                            deleted.metadataVersion())
                    .join();
            physical.deleteRoot(
                            CLUSTER,
                            object.objectKeyHash(),
                            observed.metadataVersion(),
                            observed.durableValueSha256())
                    .join();
        }

        @Override
        public void close() {
            protections.close();
            physical.close();
            metadata.close();
            objectStore.close();
        }
    }

    private static PhysicalObjectRootRecord activeRoot(
            PhysicalObjectIdentity object,
            long now) {
        return new PhysicalObjectRootRecord(
                1,
                object.objectKeyHash().value(),
                object.objectKey().value(),
                object.objectId().orElseThrow().value(),
                object.kind().wireId(),
                object.objectLength(),
                object.storageChecksum().type().name(),
                object.storageChecksum().value(),
                "",
                "",
                PhysicalObjectLifecycle.ACTIVE,
                1,
                now,
                now,
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

    private static PhysicalObjectRootRecord gcRoot(
            PhysicalObjectRootRecord identity,
            PhysicalObjectLifecycle lifecycle,
            long lifecycleEpoch,
            boolean observedAbsent) {
        long cut = Math.addExact(identity.createdAtMillis(), 100);
        return new PhysicalObjectRootRecord(
                identity.schemaVersion(),
                identity.objectKeyHash(),
                identity.objectKey(),
                identity.objectId(),
                identity.objectKindId(),
                identity.objectLength(),
                identity.storageChecksumType(),
                identity.storageChecksumValue(),
                identity.contentSha256(),
                identity.etag(),
                lifecycle,
                lifecycleEpoch,
                identity.createdAtMillis(),
                identity.orphanNotBeforeMillis(),
                "a".repeat(52),
                "b".repeat(64),
                cut,
                cut,
                lifecycle == PhysicalObjectLifecycle.MARKED ? 0 : cut,
                lifecycle == PhysicalObjectLifecycle.DELETED ? cut : 0,
                observedAbsent ? Math.addExact(cut, 100) : 0,
                observedAbsent ? "c".repeat(64) : "",
                "",
                0);
    }

    private static final class GuardedRetryObjectStore implements ObjectStore {
        private final List<Integer> authorizationAttempts = new ArrayList<>();
        private int providerTransmissions;
        private boolean retry;
        private Runnable afterFirstTransmission = () -> { };

        private void retryAfterFirstTransmission(Runnable cut) {
            retry = true;
            afterFirstTransmission = cut;
        }

        private void singleAttempt() {
            retry = false;
            afterFirstTransmission = () -> { };
        }

        private List<Integer> authorizationAttempts() {
            return List.copyOf(authorizationAttempts);
        }

        private int providerTransmissions() {
            return providerTransmissions;
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key,
                ReplayableObjectUpload source,
                PutObjectOptions options,
                PutObjectAttemptGuard attemptGuard) {
            authorizationAttempts.add(1);
            return attemptGuard.authorize(key, 1).thenCompose(ignored -> {
                providerTransmissions++;
                afterFirstTransmission.run();
                if (!retry) {
                    return CompletableFuture.completedFuture(result(key, source, options));
                }
                authorizationAttempts.add(2);
                return attemptGuard.authorize(key, 2).thenApply(second -> {
                    providerTransmissions++;
                    return result(key, source, options);
                });
            });
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key,
                ReplayableObjectUpload source,
                PutObjectOptions options) {
            return CompletableFuture.failedFuture(new AssertionError(
                    "DefaultWalObjectWriter bypassed the provider-attempt guard"));
        }

        private static PutObjectResult result(
                ObjectKey key,
                ReplayableObjectUpload source,
                PutObjectOptions options) {
            return new PutObjectResult(
                    key,
                    source.contentLength(),
                    options.expectedChecksum(),
                    "guarded-etag");
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(
                ObjectKey key,
                long offset,
                long length,
                RangeReadOptions options) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("readRange"));
        }

        @Override
        public CompletableFuture<HeadObjectResult> headObject(
                ObjectKey key,
                HeadObjectOptions options) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("headObject"));
        }

        @Override
        public void close() {
        }
    }

    private static final class MutableClock extends Clock {
        private final AtomicLong millis;

        private MutableClock(long initialMillis) {
            millis = new AtomicLong(initialMillis);
        }

        private void advance(Duration duration) {
            millis.addAndGet(duration.toMillis());
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public long millis() {
            return millis.get();
        }
    }
}
