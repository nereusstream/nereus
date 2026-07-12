/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

class S3CompatibleObjectStoreLocalStackIntegrationTest {
    private static final DockerImageName IMAGE =
            DockerImageName.parse("localstack/localstack:4.14.0");

    @Test
    void conditionalPutRangeChecksumZeroLengthAndRestart() {
        try (LocalStackContainer localstack = new LocalStackContainer(IMAGE)
                .withServices(LocalStackContainer.Service.S3)) {
            localstack.start();
            AmazonS3 admin = client(localstack);
            try {
                admin.createBucket("nereus-test");
            } finally {
                admin.shutdown();
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
            assertThat(first.putObject(key, ByteBuffer.wrap(payload), put).join().checksum()).isEqualTo(checksum);
            assertNereus(() -> first.putObject(key, ByteBuffer.wrap(payload), put).join(),
                    ErrorCode.OBJECT_UPLOAD_FAILED);
            assertThat(first.headObject(key, new HeadObjectOptions(Duration.ofSeconds(10))).join())
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

    private static AmazonS3 client(LocalStackContainer localstack) {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
                        localstack.getRegion()))
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();
    }

    private static ObjectStoreConfiguration config(LocalStackContainer localstack) {
        return new ObjectStoreConfiguration(
                S3CompatibleObjectStoreProvider.class.getName(),
                localstack.getEndpointOverride(LocalStackContainer.Service.S3),
                localstack.getRegion(), "nereus-test", "objects", true,
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
}
