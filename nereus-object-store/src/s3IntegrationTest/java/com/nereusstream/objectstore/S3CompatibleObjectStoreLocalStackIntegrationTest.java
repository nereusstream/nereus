/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import com.nereusstream.objectstore.compacted.KafkaCompactionDispositionV2;
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedFormatSpecV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectRow;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectWriteRequest;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedReader;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedWriter;
import com.nereusstream.objectstore.compacted.ParquetRangedCompactedObjectReader;
import com.nereusstream.objectstore.compacted.ParquetRangedCompactedObjectWriter;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectRow;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerificationRequest;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerifier;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectWriteRequest;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectWriteResult;
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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

class S3CompatibleObjectStoreLocalStackIntegrationTest {
    private static final DockerImageName IMAGE =
            DockerImageName.parse("localstack/localstack:4.14.0");

    @TempDir
    Path temporaryDirectory;

    @Test
    void nkc1RoundTripThroughRealS3Provider() throws Exception {
        try (LocalStackContainer localstack = new LocalStackContainer(IMAGE)
                .withServices(LocalStackContainer.Service.S3)) {
            localstack.start();
            try (S3AsyncClient admin = client(localstack)) {
                admin.createBucket(CreateBucketRequest.builder().bucket("nereus-nkc1-test").build()).join();
            }
            S3CompatibleObjectStoreProvider provider = new S3CompatibleObjectStoreProvider();
            ObjectStore store = provider.create(
                    config(localstack, "nereus-nkc1-test"),
                    ref -> Optional.of(("access".equals(ref)
                            ? localstack.getAccessKey()
                            : localstack.getSecretKey()).toCharArray()));
            Path stagingDirectory = Files.createDirectory(temporaryDirectory.resolve("nkc1-staging"));
            Files.setPosixFilePermissions(stagingDirectory, PosixFilePermissions.fromString("rwx------"));
            try (StagingFileManager staging = new StagingFileManager(
                    stagingDirectory, 32L << 20, StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                    Duration.ofHours(1), Runnable::run)) {
                KafkaCheckpointCodecV1 codec = new KafkaCheckpointCodecV1();
                KafkaCheckpointReader reader = new KafkaCheckpointReader(store, codec);
                KafkaCheckpointVerifier verifier = new KafkaCheckpointVerifier();
                KafkaCheckpointWriter writer = new KafkaCheckpointWriter(
                        store, staging, Runnable::run, codec, reader, verifier);
                KafkaCheckpointHeader header = new KafkaCheckpointHeader(
                        0, "kraft", "EjRWeJq83vAAAAAAAAAAAQ", 1, 1,
                        new StreamId("s-s3-nkc1"), 1, 9, 20, 0, 20, 5,
                        "commit-5", sha256('7'));
                List<KafkaCheckpointSection> sections = java.util.Arrays.stream(
                                KafkaCheckpointSectionType.values())
                        .map(type -> KafkaCheckpointSection.required(
                                type, new byte[] {(byte) type.wireId()}))
                        .toList();
                KafkaCheckpointWriteRequest request = new KafkaCheckpointWriteRequest(
                        "test-cluster", header, sections, sha256('8'), Duration.ofSeconds(20));

                KafkaCheckpointObject object = writer.write(request).join();
                KafkaCheckpointObject reopened = reader.openAndVerify(
                        object.objectKey(), object.objectLength(), object.storageCrc32c(),
                        object.objectSha256(), Duration.ofSeconds(20)).join();

                verifier.verifyExpected(reopened, "test-cluster", header, request.contentPolicySha256());
                assertThat(reopened.sections()).isEqualTo(sections);
            } finally {
                store.close();
                provider.close();
            }
        }
    }

    @Test
    void ncp2AndNtc2RoundTripThroughRealS3Provider() throws Exception {
        try (LocalStackContainer localstack = new LocalStackContainer(IMAGE)
                .withServices(LocalStackContainer.Service.S3)) {
            localstack.start();
            try (S3AsyncClient admin = client(localstack)) {
                admin.createBucket(CreateBucketRequest.builder().bucket("nereus-v2-test").build()).join();
            }
            S3CompatibleObjectStoreProvider provider = new S3CompatibleObjectStoreProvider();
            ObjectStore store = provider.create(
                    config(localstack, "nereus-v2-test"),
                    ref -> Optional.of(("access".equals(ref)
                            ? localstack.getAccessKey()
                            : localstack.getSecretKey()).toCharArray()));
            Path stagingDirectory = Files.createDirectory(temporaryDirectory.resolve("v2-staging"));
            Files.setPosixFilePermissions(stagingDirectory, PosixFilePermissions.fromString("rwx------"));
            try (StagingFileManager staging = new StagingFileManager(
                            stagingDirectory,
                            64L << 20,
                            StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                            Duration.ofHours(1),
                            Runnable::run)) {
                RangedCompactedObjectWriteRequest ncp2 = new RangedCompactedObjectWriteRequest(
                        "test-cluster", new StreamId("s-s3-ncp2"), new OffsetRange(0, 3), "a".repeat(26),
                        sha256('1'), sha256('2'), PayloadFormat.KAFKA_RECORD_BATCH,
                        CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT, 3, 1, 3, 3,
                        1, "UNCOMPRESSED", "s3-integration");
                byte[] committedPayload = new byte[] {1, 2, 3};
                ParquetRangedCompactedObjectWriter ncp2Writer =
                        new ParquetRangedCompactedObjectWriter(staging, Runnable::run);
                ParquetRangedCompactedObjectReader ncp2Reader =
                        new ParquetRangedCompactedObjectReader(store, Runnable::run);
                ParquetKafkaTopicCompactedReader ntc2Reader =
                        new ParquetKafkaTopicCompactedReader(store, Runnable::run);
                RangedCompactedObjectVerifier verifier =
                        new RangedCompactedObjectVerifier(store, ncp2Reader, ntc2Reader);
                try (RangedCompactedObjectWriteResult written = ncp2Writer.write(
                                ncp2,
                                publisher(List.of(new RangedCompactedObjectRow(
                                        0, 3, 0, ByteBuffer.wrap(committedPayload),
                                        Crc32cChecksums.intValue(Crc32cChecksums.checksum(committedPayload)),
                                        OptionalLong.empty()))))
                        .join()) {
                    upload(store, written);
                    verifier.verifyExact(
                                    RangedCompactedObjectVerificationRequest.from(
                                            ncp2, written, Duration.ofSeconds(20)),
                                    ncp2)
                            .join();
                }

                KafkaTopicCompactedObjectWriteRequest ntc2 = new KafkaTopicCompactedObjectWriteRequest(
                        "test-cluster", new StreamId("s-s3-ntc2"), new OffsetRange(10, 20), "b".repeat(26),
                        sha256('3'), sha256('4'), 1, 1, 1, 1,
                        1, "UNCOMPRESSED", "s3-integration",
                        new KafkaTopicCompactedFormatSpecV2(
                                "latest", 1, CompactedObjectFormatV2.KAFKA_KEY_CODEC,
                                CompactedObjectFormatV2.KAFKA_REWRITE_CODEC, sha256('5'), 1, 1));
                byte[] survivor = new byte[] {9};
                ParquetKafkaTopicCompactedWriter ntc2Writer =
                        new ParquetKafkaTopicCompactedWriter(staging, Runnable::run);
                try (RangedCompactedObjectWriteResult written = ntc2Writer.write(
                                ntc2,
                                publisher(List.of(new KafkaTopicCompactedObjectRow(
                                        15, 1, KafkaCompactionDispositionV2.RETAIN_UNKEYED,
                                        KafkaCompactionKeyEncodingV2.nullKey(15), ByteBuffer.wrap(survivor),
                                        Crc32cChecksums.intValue(Crc32cChecksums.checksum(survivor)),
                                        15, 0, sha256('6'), OptionalLong.empty()))))
                        .join()) {
                    upload(store, written);
                    verifier.verifyExact(
                                    RangedCompactedObjectVerificationRequest.from(
                                            ntc2, written, Duration.ofSeconds(20)),
                                    ntc2)
                            .join();
                }
            } finally {
                store.close();
                provider.close();
            }
        }
    }

    @Test
    void conditionalPutRangeChecksumZeroLengthAndRestart() throws Exception {
        try (LocalStackContainer localstack = new LocalStackContainer(IMAGE)
                .withServices(LocalStackContainer.Service.S3)) {
            localstack.start();
            S3AsyncClient admin = client(localstack);
            try {
                admin.createBucket(CreateBucketRequest.builder().bucket("nereus-test").build()).join();
            } finally {
                admin.close();
            }
            S3CompatibleObjectStoreProvider missingBucketProvider = new S3CompatibleObjectStoreProvider();
            try {
                assertThatThrownBy(() -> missingBucketProvider.create(
                                config(localstack, "missing-bucket"),
                                ref -> Optional.of("test".toCharArray())))
                        .isInstanceOfSatisfying(NereusException.class,
                                error -> assertThat(error.code()).isEqualTo(ErrorCode.OBJECT_NOT_FOUND));
            } finally {
                missingBucketProvider.close();
            }
            ObjectStoreConfiguration config = config(localstack);
            char[] accessChars = localstack.getAccessKey().toCharArray();
            char[] secretChars = localstack.getSecretKey().toCharArray();
            ObjectStoreSecretResolver resolver = ref -> Optional.of(
                    "access".equals(ref) ? accessChars : secretChars);
            ObjectKey key = new ObjectKey("wal/object-1");
            byte[] payload = "0123456789".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            var checksum = Crc32cChecksums.checksum(payload);
            PutObjectOptions put = new PutObjectOptions(
                    "application/octet-stream", checksum, true,
                    Map.of("owner", "nereus"), Duration.ofSeconds(10));

            S3CompatibleObjectStoreProvider firstProvider = new S3CompatibleObjectStoreProvider();
            ObjectStore first = firstProvider.create(config, resolver);
            assertThat(accessChars).containsOnly('\0');
            assertThat(secretChars).containsOnly('\0');
            assertThatThrownBy(() -> firstProvider.create(config, ref -> Optional.empty()))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(first.putObject(key, ByteBuffer.wrap(payload), put).join().checksum()).isEqualTo(checksum);
            ObjectKey secondKey = new ObjectKey("wal/object-2");
            assertThat(first.putObject(secondKey, ByteBuffer.wrap(payload), put).join().checksum()).isEqualTo(checksum);
            assertNereus(() -> first.putObject(key, ByteBuffer.wrap(payload), put).join(),
                    ErrorCode.OBJECT_UPLOAD_FAILED);
            HeadObjectResult firstHead = first.headObject(
                    key, new HeadObjectOptions(Duration.ofSeconds(10))).join();
            assertThat(firstHead)
                    .satisfies(head -> {
                        assertThat(head.objectLength()).isEqualTo(payload.length);
                        assertThat(head.checksum()).isEqualTo(checksum);
                        assertThat(head.metadata()).containsEntry("owner", "nereus");
                    });
            byte[] range = "2345".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            assertThat(first.readRange(key, 2, 4, new RangeReadOptions(
                    Optional.of(Crc32cChecksums.checksum(range)), Duration.ofSeconds(10))).join().payload())
                    .isEqualByComparingTo(ByteBuffer.wrap(range));
            assertThat(first.readRange(key, payload.length, 0,
                    new RangeReadOptions(Optional.empty(), Duration.ofSeconds(10))).join().length()).isZero();
            assertNereus(() -> first.readRange(key, 0, 4, new RangeReadOptions(
                            Optional.of(Crc32cChecksums.checksum(new byte[] {1})), Duration.ofSeconds(10))).join(),
                    ErrorCode.OBJECT_CHECKSUM_MISMATCH);
            List<String> listed = new ArrayList<>();
            Optional<String> continuation = Optional.empty();
            do {
                ListObjectsResult page = first.listObjects(
                        new ObjectKeyPrefix("wal/"),
                        continuation,
                        new ListObjectsOptions(1, Duration.ofSeconds(10))).join();
                listed.addAll(page.objects().stream().map(value -> value.key().value()).toList());
                continuation = page.continuationToken();
            } while (continuation.isPresent());
            assertThat(listed).containsExactlyInAnyOrderElementsOf(List.of("wal/object-1", "wal/object-2"));
            HeadObjectResult secondHead = first.headObject(
                    secondKey, new HeadObjectOptions(Duration.ofSeconds(10))).join();
            DeleteObjectOptions delete = new DeleteObjectOptions(
                    secondHead.objectLength(),
                    secondHead.checksum(),
                    secondHead.etag(),
                    Duration.ofSeconds(10));
            assertThat(first.deleteObject(secondKey, delete).join().status())
                    .isEqualTo(DeleteObjectResult.Status.DELETED);
            assertThat(first.deleteObject(secondKey, delete).join().status())
                    .isEqualTo(DeleteObjectResult.Status.ALREADY_ABSENT);
            first.close();
            firstProvider.close();

            S3CompatibleObjectStoreProvider secondProvider = new S3CompatibleObjectStoreProvider();
            ObjectStore reopened = secondProvider.create(config, ref -> Optional.of(
                    "access".equals(ref)
                            ? localstack.getAccessKey().toCharArray()
                            : localstack.getSecretKey().toCharArray()));
            assertThat(reopened.headObject(key, new HeadObjectOptions(Duration.ofSeconds(10))).join().checksum())
                    .isEqualTo(checksum);
            reopened.close();
            secondProvider.close();
        }
    }

    private static S3AsyncClient client(LocalStackContainer localstack) {
        return S3AsyncClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
                .region(Region.of(localstack.getRegion()))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();
    }

    private static ObjectStoreConfiguration config(LocalStackContainer localstack) {
        return config(localstack, "nereus-test");
    }

    private static ObjectStoreConfiguration config(LocalStackContainer localstack, String bucket) {
        return new ObjectStoreConfiguration(
                S3CompatibleObjectStoreProvider.class.getName(),
                localstack.getEndpointOverride(LocalStackContainer.Service.S3),
                localstack.getRegion(), bucket, "objects", true,
                Duration.ofSeconds(10), 4,
                Optional.of("access"), Optional.of("secret"), Optional.empty());
    }

    private static void assertNereus(Runnable operation, ErrorCode code) {
        assertThatThrownBy(operation::run).satisfies(error -> {
            Throwable current = error;
            while (current instanceof CompletionException && current.getCause() != null) {
                current = current.getCause();
            }
            assertThat(current).isInstanceOfSatisfying(NereusException.class,
                    nereus -> assertThat(nereus.code()).isEqualTo(code));
        });
    }

    private static void upload(ObjectStore store, RangedCompactedObjectWriteResult result) {
        store.putObject(
                        result.objectKey(),
                        result.stagingFile(),
                        new PutObjectOptions(
                                "application/vnd.apache.parquet", result.storageCrc32c(), true,
                                Map.of(), Duration.ofSeconds(20)))
                .join();
    }

    private static <T> Flow.Publisher<T> publisher(List<T> values) {
        List<T> exact = List.copyOf(values);
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private int index;
            private boolean done;

            @Override
            public void request(long count) {
                if (done) {
                    return;
                }
                while (count-- > 0 && index < exact.size()) {
                    subscriber.onNext(exact.get(index++));
                }
                if (index == exact.size()) {
                    done = true;
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
                done = true;
            }
        });
    }

    private static Checksum sha256(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }
}
