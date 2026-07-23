/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.kafka.metadata.KafkaBindingRequest;
import com.nereusstream.kafka.metadata.KafkaPartitionLifecycleCoordinator;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.recovery.KafkaCheckpointRecoveryCoordinator;
import com.nereusstream.kafka.recovery.KafkaCheckpointRecoveryRequest;
import com.nereusstream.kafka.recovery.KafkaCheckpointRecoveryResult;
import com.nereusstream.kafka.testing.TestStreamStorage;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.testing.FakeKafkaPartitionMetadataStore;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointCodecV1;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointObject;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointReader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSectionType;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointVerifier;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointWriteRequest;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointWriter;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaCheckpointPublicationRecoveryIntegrationTest {
    private static final String NEREUS_CLUSTER = "nereus";
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);

    @TempDir Path temporaryDirectory;

    @Test
    void publishesUnderProtectionAndRecoversWithAReleasedReaderPin() throws Exception {
        try (Fixture fixture = fixture()) {
            VersionedKafkaPartitionBinding captured = fixture.activeBinding();
            KafkaCheckpointSourceState source = source(fixture.identity, 0, 42, 12, 'a');
            KafkaCheckpointObject object = fixture.publish(captured, source);

            VersionedKafkaPartitionBinding published = fixture.currentBinding();
            assertThat(published.value().checkpointReferences())
                    .singleElement()
                    .satisfies(reference -> assertThat(reference.objectId()).isEqualTo(object.objectId().value()));
            assertThat(fixture.protections(object)).containsExactly(ObjectProtectionType.KAFKA_CHECKPOINT_ROOT);

            KafkaCheckpointRecoveryResult recovered = fixture.recover(published, source);

            assertThat(recovered.checkpoint()).isPresent();
            assertThat(recovered.checkpoint().orElseThrow().header()).isEqualTo(object.header());
            assertThat(recovered.checkpoint().orElseThrow().sections()).isEqualTo(object.sections());
            assertThat(fixture.readerLeaseCount(object)).isZero();
        }
    }

    @Test
    void idempotentRetryConvergesAndMissingNewestFallsBackToOlderCheckpoint() throws Exception {
        try (Fixture fixture = fixture()) {
            KafkaCheckpointSourceState oldSource = source(fixture.identity, 0, 42, 12, 'a');
            KafkaCheckpointObject oldObject = fixture.publish(fixture.activeBinding(), oldSource);
            fixture.publish(fixture.currentBinding(), oldSource);

            assertThat(fixture.currentBinding().value().checkpointReferences()).hasSize(1);

            KafkaCheckpointSourceState newSource = source(fixture.identity, 0, 50, 13, 'c');
            KafkaCheckpointObject newObject = fixture.publish(fixture.currentBinding(), newSource);
            assertThat(fixture.currentBinding().value().checkpointReferences()).hasSize(2);
            fixture.objects.deleteObject(
                    newObject.objectKey(),
                    new DeleteObjectOptions(
                            newObject.objectLength(), newObject.storageCrc32c(),
                            newObject.etag(), Duration.ofSeconds(5)))
                    .join();

            KafkaCheckpointRecoveryResult recovered = fixture.recover(fixture.currentBinding(), newSource);

            assertThat(recovered.checkpoint()).isPresent();
            assertThat(recovered.checkpoint().orElseThrow().header().checkpointOffset()).isEqualTo(42);
            assertThat(recovered.checkpoint().orElseThrow().reference().objectId())
                    .isEqualTo(oldObject.objectId().value());
            assertThat(fixture.unusable).containsExactly(newObject.objectId().value());
            assertThat(fixture.readerLeaseCount(newObject)).isZero();
            assertThat(fixture.readerLeaseCount(oldObject)).isZero();
        }
    }

    @Test
    void untrimmedPartitionMayFullReplayButTrimmedPartitionRequiresAUsableCheckpoint() throws Exception {
        try (Fixture fixture = fixture()) {
            VersionedKafkaPartitionBinding binding = fixture.activeBinding();

            KafkaCheckpointRecoveryResult fullReplay = fixture.recover(
                    binding, source(fixture.identity, 0, 42, 12, 'a'));
            assertThat(fullReplay.checkpoint()).isEmpty();

            assertThatThrownBy(() -> fixture.recover(
                            binding, source(fixture.identity, 5, 42, 12, 'a')))
                    .satisfies(failure -> assertThat(unwrap(failure))
                            .isInstanceOfSatisfying(NereusException.class, nereus ->
                                    assertThat(nereus.code())
                                            .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION)));
        }
    }

    @Test
    void timeoutDoesNotSilentlyFallBackToAnOlderCheckpoint() throws Exception {
        try (Fixture fixture = fixture()) {
            KafkaCheckpointSourceState oldSource = source(fixture.identity, 0, 42, 12, 'a');
            fixture.publish(fixture.activeBinding(), oldSource);
            KafkaCheckpointSourceState newSource = source(fixture.identity, 0, 50, 13, 'c');
            fixture.publish(fixture.currentBinding(), newSource);

            KafkaCheckpointRecoveryCoordinator expired = fixture.recovery(new ExpiringClock());
            KafkaCheckpointRecoveryRequest request = new KafkaCheckpointRecoveryRequest(
                    fixture.identity, fixture.currentBinding(), newSource, fixture.validator,
                    Duration.ofMillis(1));

            assertThatThrownBy(() -> expired.recover(request).join())
                    .satisfies(failure -> assertThat(unwrap(failure))
                            .isInstanceOfSatisfying(NereusException.class, nereus ->
                                    assertThat(nereus.code()).isEqualTo(ErrorCode.TIMEOUT)));
            assertThat(fixture.unusable).isEmpty();
        }
    }

    private Fixture fixture() throws Exception {
        return new Fixture(temporaryDirectory);
    }

    private static KafkaCheckpointSourceState source(
            KafkaPartitionIdentity identity, long trim, long end, long commitVersion, char hash) {
        return new KafkaCheckpointSourceState(
                new AppendAuthority(
                        "kafka-partition-leader-v1", identity.durableId().canonicalIdentity(),
                        7, "broker-1", 1),
                "writer-1", 1, "fencing-token-1", 1,
                trim, end, commitVersion, "commit-" + commitVersion,
                sha256(hash), false, end);
    }

    private static List<KafkaCheckpointSection> sections(long offset) {
        return java.util.Arrays.stream(KafkaCheckpointSectionType.values())
                .map(type -> KafkaCheckpointSection.required(
                        type, ByteBuffer.allocate(12).putInt(type.wireId()).putLong(offset).array()))
                .toList();
    }

    private static Checksum sha256(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private static KafkaPartitionIdentity identity() {
        ByteBuffer bytes = ByteBuffer.allocate(16)
                .putLong(0x1234_5678_9abc_def0L)
                .putLong(99);
        return new KafkaPartitionIdentity(
                "kraft",
                Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.array()),
                3,
                "orders");
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof java.util.concurrent.CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class ExpiringClock extends Clock {
        private int reads;

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            if (!ZoneOffset.UTC.equals(zone)) throw new IllegalArgumentException("UTC only");
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public long millis() {
            return reads++ == 0 ? 10_000 : 10_002;
        }
    }

    private static final class MutableValidator implements KafkaCheckpointSourceValidator {
        private final AtomicReference<KafkaCheckpointSourceState> current = new AtomicReference<>();

        @Override
        public CompletableFuture<KafkaCheckpointSourceState> loadCurrent() {
            return CompletableFuture.completedFuture(current.get());
        }

        @Override
        public CompletableFuture<Boolean> isSourceCommitReachable(
                KafkaCheckpointHeader captured, KafkaCheckpointSourceState current) {
            return CompletableFuture.completedFuture(
                    captured.sourceCommitVersion() <= current.commitVersion());
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final KafkaPartitionIdentity identity = identity();
        private final FakeKafkaPartitionMetadataStore bindings =
                new FakeKafkaPartitionMetadataStore(NEREUS_CLUSTER, "kraft");
        private final FakePhysicalObjectMetadataStore physical = new FakePhysicalObjectMetadataStore();
        private final LocalFileObjectStore objects;
        private final StagingFileManager staging;
        private final DefaultObjectProtectionManager protections;
        private final DefaultObjectReadPinManager readPins;
        private final KafkaCheckpointWriter writer;
        private final KafkaCheckpointReader reader;
        private final KafkaCheckpointVerifier verifier = new KafkaCheckpointVerifier();
        private final MutableValidator validator = new MutableValidator();
        private final List<String> unusable = new ArrayList<>();

        private Fixture(Path directory) throws Exception {
            Path objectDirectory = Files.createDirectory(directory.resolve("objects"));
            Path stagingDirectory = Files.createDirectory(directory.resolve("staging"));
            Files.setPosixFilePermissions(stagingDirectory, PosixFilePermissions.fromString("rwx------"));
            objects = new LocalFileObjectStore(objectDirectory);
            staging = new StagingFileManager(
                    stagingDirectory, 32L << 20, StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                    Duration.ofHours(1), Runnable::run);
            KafkaCheckpointCodecV1 codec = new KafkaCheckpointCodecV1();
            reader = new KafkaCheckpointReader(objects, codec);
            writer = new KafkaCheckpointWriter(objects, staging, Runnable::run, codec, reader, verifier);
            protections = new DefaultObjectProtectionManager(
                    NEREUS_CLUSTER, physical, Duration.ofMinutes(10), Duration.ofSeconds(1),
                    Duration.ofMinutes(1), CLOCK);
            readPins = new DefaultObjectReadPinManager(
                    NEREUS_CLUSTER, "p".repeat(26), physical, Duration.ofSeconds(10),
                    Duration.ofSeconds(1), Duration.ofMinutes(1), CLOCK);
        }

        private VersionedKafkaPartitionBinding activeBinding() {
            KafkaPartitionLifecycleCoordinator lifecycle = new KafkaPartitionLifecycleCoordinator(
                    bindings, new TestStreamStorage(), bindings.keyspace(), CLOCK);
            return lifecycle.ensureBinding(new KafkaBindingRequest(
                            identity, StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                            20, "broker-run", 1, Duration.ofSeconds(30)))
                    .join()
                    .durableRoot();
        }

        private VersionedKafkaPartitionBinding currentBinding() {
            return bindings.get(identity.durableId()).join().orElseThrow();
        }

        private KafkaCheckpointObject publish(
                VersionedKafkaPartitionBinding binding, KafkaCheckpointSourceState source) {
            validator.current.set(source);
            KafkaCheckpointHeader header = new KafkaCheckpointHeader(
                    0, identity.kafkaClusterId(), identity.topicId(), identity.partition(),
                    binding.value().incarnation(), new StreamId(binding.value().streamId()),
                    binding.value().payloadMappingId(), 7,
                    source.endOffset(), source.trimOffset(), source.endOffset(), source.commitVersion(),
                    source.lastCommitId(), source.headSha256());
            KafkaCheckpointWriteRequest objectRequest = new KafkaCheckpointWriteRequest(
                    NEREUS_CLUSTER, header, sections(source.endOffset()), sha256('b'),
                    Duration.ofSeconds(5));
            KafkaCheckpointPublicationCoordinator publication = new KafkaCheckpointPublicationCoordinator(
                    bindings, writer, verifier, protections, CLOCK);
            return publication.publish(new KafkaCheckpointPublicationRequest(
                            identity, binding, source, objectRequest, validator,
                            Duration.ofMinutes(5), "test-build"))
                    .join();
        }

        private KafkaCheckpointRecoveryResult recover(
                VersionedKafkaPartitionBinding binding, KafkaCheckpointSourceState source) {
            validator.current.set(source);
            return recovery(CLOCK).recover(new KafkaCheckpointRecoveryRequest(
                            identity, binding, source, validator, Duration.ofSeconds(5)))
                    .join();
        }

        private KafkaCheckpointRecoveryCoordinator recovery(Clock clock) {
            return new KafkaCheckpointRecoveryCoordinator(
                    NEREUS_CLUSTER, bindings, physical, readPins, reader, verifier, clock,
                    (reference, failure) -> unusable.add(reference.objectId()));
        }

        private List<ObjectProtectionType> protections(KafkaCheckpointObject object) {
            return physical.scanProtections(
                            NEREUS_CLUSTER, ObjectKeyHash.from(object.objectKey()), Optional.empty(), 100)
                    .join()
                    .values()
                    .stream()
                    .map(value -> ObjectProtectionType.fromWireId(value.value().protectionTypeId()))
                    .toList();
        }

        private int readerLeaseCount(KafkaCheckpointObject object) {
            return physical.scanReaderLeases(
                            NEREUS_CLUSTER, ObjectKeyHash.from(object.objectKey()), Optional.empty(), 100)
                    .join()
                    .values()
                    .size();
        }

        @Override
        public void close() {
            readPins.close();
            protections.close();
            staging.close();
            objects.close();
        }
    }
}
