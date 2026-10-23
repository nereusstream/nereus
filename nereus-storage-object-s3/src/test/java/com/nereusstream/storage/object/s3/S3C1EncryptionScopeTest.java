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

package com.nereusstream.storage.object.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/** SDK reply fixtures only; actual namespace/credential/Cell qualification remains the Native caller's duty. */
class S3C1EncryptionScopeTest {
    private static final byte[] BYTES = new byte[] {1, 2, 3};
    private static final ObjectIdentity IDENTITY =
            new ObjectIdentity("cell/part", 3, Sha256Digest.hash(CanonicalBytes.copyOf(BYTES)));

    @Test
    void admittedAes256IsRequestedExplicitlyAndCheckedOnOriginalPutReply() throws Exception {
        var observed = new AtomicReference<PutObjectRequest>();
        var sdk = sdk((name, args) -> {
            if (name.equals("putObject")) {
                observed.set((PutObjectRequest) args[0]);
                assertThat(((RequestBody) args[1])
                                .contentStreamProvider()
                                .newStream()
                                .readAllBytes())
                        .containsExactly(BYTES);
                return PutObjectResponse.builder()
                        .serverSideEncryption("AES256")
                        .versionId("declared-v1")
                        .checksumSHA256(Base64.getEncoder()
                                .encodeToString(IDENTITY.bodySha256().bytes().toByteArray()))
                        .build();
            }
            throw new UnsupportedOperationException(name);
        });
        try (var transport = S3C1ObjectProviderTransport.admitVersionMatchDeleteV1(
                sdk, "declared-bucket", "declared-provider", false, 1000, "S3_MANAGED_AES256")) {
            var created = transport.putIfAbsentWithEvidence(IDENTITY, new ByteArrayInputStream(BYTES));
            assertThat(created.immutableVersionToken()).contains(CanonicalBytes.copyOf("declared-v1".getBytes()));
            assertThat(observed.get().serverSideEncryptionAsString()).isEqualTo("AES256");
        }
    }

    @Test
    void foreignEncryptionOnSuccessfulPutRetainsOriginalUnknownInsteadOfClaimingCreated() throws Exception {
        var sdk = sdk((name, args) -> {
            if (name.equals("putObject")) {
                ((RequestBody) args[1]).contentStreamProvider().newStream().readAllBytes();
                return PutObjectResponse.builder()
                        .serverSideEncryption("aws:kms")
                        .ssekmsKeyId("arn:aws:kms:declared-foreign")
                        .versionId("declared-v1")
                        .build();
            }
            throw new UnsupportedOperationException(name);
        });
        try (var transport = S3C1ObjectProviderTransport.admitVersionMatchDeleteV1(
                sdk, "declared-bucket", "declared-provider", false, 1000, "NONE")) {
            var provider = provider(transport);
            assertThatThrownBy(() -> provider.conditionalCreate(new RepeatableObjectBody() {
                        public ObjectIdentity identity() {
                            return IDENTITY;
                        }

                        public InputStream openStream() {
                            return new ByteArrayInputStream(BYTES);
                        }
                    }))
                    .isInstanceOf(S3C1ProviderException.class)
                    .hasMessageContaining("PUT encryption response differs");
            assertThatThrownBy(provider::close).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void foreignGetEncryptionRetainsOriginalFailedResponseCloseAndNeverIssuesAnotherGet() throws Exception {
        var rejectClose = new AtomicBoolean(true);
        var reads = new AtomicInteger();
        var sdk = sdk((name, args) -> {
            if (name.equals("getObject")) {
                reads.incrementAndGet();
                var body = new ByteArrayInputStream(BYTES) {
                    public void close() throws IOException {
                        if (rejectClose.get()) {
                            throw new IOException("declared original close failed");
                        }
                        super.close();
                    }
                };
                return new ResponseInputStream<>(
                        GetObjectResponse.builder()
                                .contentLength(3L)
                                .serverSideEncryption("aws:kms")
                                .ssekmsKeyId("arn:aws:kms:declared-foreign")
                                .versionId("declared-v1")
                                .build(),
                        AbortableInputStream.create(body));
            }
            throw new UnsupportedOperationException(name);
        });
        try (var transport = S3C1ObjectProviderTransport.admitVersionMatchDeleteV1(
                sdk, "declared-bucket", "declared-provider", false, 1000, "NONE")) {
            var provider = provider(transport);
            C1ObjectProviderSession.VerifiedReadCleanupException retained = null;
            try {
                provider.consumeVerifiedObject(IDENTITY, Optional.empty(), InputStream::readAllBytes);
            } catch (C1ObjectProviderSession.VerifiedReadCleanupException failure) {
                retained = failure;
            }
            assertThat(retained).isNotNull().hasCauseInstanceOf(S3C1ProviderException.class);
            assertThatThrownBy(provider::close).isInstanceOf(IllegalStateException.class);
            rejectClose.set(false);
            retained.reconcileCleanup();
            provider.close();
            assertThat(reads.get()).isEqualTo(1);
        }
    }

    private static C1ObjectProviderSession provider(S3C1ObjectProviderTransport transport) {
        return new C1ObjectProviderSession(
                transport,
                new CellProviderScopeId(Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1}))),
                "cell",
                1024,
                1024);
    }

    @FunctionalInterface
    private interface Reply {
        Object reply(String name, Object[] args) throws Exception;
    }

    private static S3Client sdk(Reply reply) {
        return (S3Client) Proxy.newProxyInstance(
                S3Client.class.getClassLoader(), new Class<?>[] {S3Client.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getBucketVersioning")) {
                        return GetBucketVersioningResponse.builder()
                                .status(BucketVersioningStatus.ENABLED)
                                .build();
                    }
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    return reply.reply(method.getName(), args);
                });
    }
}
