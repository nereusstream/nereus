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

package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.metadata.oxia.v2.retention.OxiaCellProviderScopeRegistryV2;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport;
import com.nereusstream.storage.object.s3.S3M5PhysicalNamespaceAuthorityV2;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Bounded S3 production adapter for immutable NPD1/NPO1 Objects. */
public final class S3PulsarOffloadObjectStoreV1 implements PulsarOffloadObjectStoreV1 {
    private static final String SHA256_METADATA = "nereus-sha256";
    private static final long FIVE_MIB = 5L * 1_024 * 1_024;
    private static final long FIVE_GIB = 5L * 1_024 * 1_024 * 1_024;
    private static final Capabilities CAPABILITIES = new Capabilities(
            PulsarOffloadLimitCandidateV1.FOUR_GIB, FIVE_MIB, FIVE_GIB, 10_000, true, true, true, true, true);

    private final S3Client client;
    private final String bucket;
    private final ExecutorService executor;
    private final boolean closeClient;
    private final MaterializationConfiguration materializationConfiguration;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object lifetimeLock = new Object();
    private final CompletableFuture<Void> closeCompletion = new CompletableFuture<>();
    private int activeOperations;
    private boolean shutdownComplete;
    private boolean closeStarted;
    private Throwable shutdownFailure;

    public S3PulsarOffloadObjectStoreV1(S3Client client, String bucket, ExecutorService executor, boolean closeClient) {
        this(client, bucket, executor, closeClient, null);
    }

    /** Explicit stable references attached to the actual configured credential provider; no secret is persisted. */
    public record MaterializationConfiguration(
            M5CellProviderProfileV2.References references,
            IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider,
            java.util.Optional<M5CellProviderProfileV2.GcGracePolicyV1> gcGracePolicy) {
        public MaterializationConfiguration(
                M5CellProviderProfileV2.References references,
                IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider) {
            this(references, credentialsProvider, java.util.Optional.empty());
        }

        public MaterializationConfiguration {
            Objects.requireNonNull(references, "references");
            Objects.requireNonNull(credentialsProvider, "credentialsProvider");
            gcGracePolicy = Objects.requireNonNull(gcGracePolicy, "gcGracePolicy");
        }
    }

    public S3PulsarOffloadObjectStoreV1(
            S3Client client,
            String bucket,
            ExecutorService executor,
            boolean closeClient,
            MaterializationConfiguration materializationConfiguration) {
        this.client = Objects.requireNonNull(client, "client");
        this.bucket = Objects.requireNonNull(bucket, "bucket");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.closeClient = closeClient;
        this.materializationConfiguration = materializationConfiguration;
        if (bucket.isBlank()) {
            throw new IllegalArgumentException("S3 bucket is blank");
        }
        if (materializationConfiguration != null
                && client.serviceClientConfiguration().credentialsProvider()
                        != materializationConfiguration.credentialsProvider()) {
            throw new IllegalArgumentException("Native materialization credential reference belongs to another SDK");
        }
    }

    @Override
    public Capabilities capabilities() {
        return CAPABILITIES;
    }

    /** Actual SDK provider configuration for low-frequency Native read qualification. */
    Map<String, String> readAdmissionConfiguration() {
        var configuration = client.serviceClientConfiguration();
        return Map.of(
                "endpoint",
                        configuration.endpointOverride().map(Object::toString).orElse("AWS_DEFAULT"),
                "region", configuration.region().id(),
                "responseChecksum", configuration.responseChecksumValidation().name(),
                "requestChecksum", configuration.requestChecksumCalculation().name());
    }

    /** Checks the permanent marker through this store's actual client and bucket on its owned SDK executor. */
    public CompletionStage<PhysicalNamespaceAuthorityBindingV2> observePhysicalNamespace(
            Namespace expected, MetadataNamespaceIdentityV2 metadataNamespace) {
        return submit(() -> {
            var backend = S3M5PhysicalNamespaceAuthorityV2.connect(client, bucket, expected);
            return backend.bind(metadataNamespace).toCompletableFuture().join();
        });
    }

    /** One original SDK connection hold; all Task/Cell/writer admission is still the native caller's prerequisite. */
    final class MaterializationConnection implements AutoCloseable {
        private final S3M5PhysicalNamespaceAuthorityV2 physical;
        private final PhysicalNamespaceAuthorityBindingV2 binding;
        private final S3C1ObjectProviderTransport transport;
        private final C1ObjectProviderSession provider;
        private final Runnable ownerFinished;
        private final M5CellProviderProfileV2 profile;
        private OxiaCellProviderScopeRegistryV2.Attempt scopeAttempt;
        private OxiaCellProviderScopeRegistryV2.Admission scopeAdmission;
        private CompletableFuture<Void> scopeWork;
        private CompletableFuture<Void> currentCheck;
        private PulsarNativeBindingV1 originalBinding;
        private boolean released;

        private MaterializationConnection(
                S3M5PhysicalNamespaceAuthorityV2 physical,
                PhysicalNamespaceAuthorityBindingV2 binding,
                S3C1ObjectProviderTransport transport,
                C1ObjectProviderSession provider,
                Runnable ownerFinished,
                M5CellProviderProfileV2 profile) {
            this.physical = physical;
            this.binding = binding;
            this.transport = transport;
            this.provider = provider;
            this.ownerFinished = ownerFinished;
            this.profile = profile;
        }

        synchronized C1ObjectProviderSession provider() {
            if (released) {
                throw new IllegalStateException("Original native materialization connection was released");
            }
            return provider;
        }

        PhysicalNamespaceAuthorityBindingV2 binding() {
            return binding;
        }

        S3M5PhysicalNamespaceAuthorityV2 physical() {
            return physical;
        }

        void requireObjectDeletePolicyAllows(PhysicalResourceIdV2.ObjectVersion resource) throws java.io.IOException {
            var admitted = profile();
            qualifiedProvider();
            if (!resource.namespace().equals(binding.physicalNamespace())
                    || resource.identityKind() != PhysicalResourceIdV2.ObjectIdentityKind.IMMUTABLE_VERSION
                    || !resource.objectKey().value().startsWith(admitted.exclusiveNamespacePrefix() + "/")) {
                throw new IllegalArgumentException("Provider deletion policy belongs to another admitted scope");
            }
            transport.requireObjectDeletePolicyAllows(
                    resource.objectKey().value(), resource.immutableIdentity().bytes());
        }

        synchronized M5CellProviderProfileV2 profile() {
            if (released || profile == null) {
                throw new IllegalStateException("Original connection has no complete configured Provider profile");
            }
            return profile;
        }

        synchronized CompletableFuture<Void> admitProfile(PulsarNativeBindingV1 actualBinding) {
            var expected = profile();
            if (!actualBinding.metadataNamespace().equals(binding.metadataNamespace())
                    || !actualBinding
                            .authority()
                            .aggregate()
                            .binding()
                            .cellIdentity()
                            .equals(expected.protocolCell())
                    || originalBinding != null && originalBinding != actualBinding) {
                throw new IllegalArgumentException("Native Provider profile belongs to another actual Cell/Binding");
            }
            if (scopeWork == null) {
                originalBinding = actualBinding;
                scopeWork = actualBinding
                        .providerScopes(physical)
                        .thenCompose(registry -> {
                            synchronized (this) {
                                scopeAttempt = registry.prepare(expected);
                                return scopeAttempt.admit();
                            }
                        })
                        .thenCompose(admitted -> admitted.requireCurrent().thenRun(() -> {
                            synchronized (this) {
                                scopeAdmission = admitted;
                            }
                        }))
                        .thenCompose(ignored -> actualBinding.revalidate())
                        .toCompletableFuture();
            }
            return scopeWork.copy();
        }

        synchronized CompletableFuture<Void> reopenProfile(PulsarNativeBindingV1 actualBinding) {
            var expected = profile();
            if (!actualBinding.metadataNamespace().equals(binding.metadataNamespace())
                    || !actualBinding
                            .authority()
                            .aggregate()
                            .binding()
                            .cellIdentity()
                            .equals(expected.protocolCell())
                    || originalBinding != null && originalBinding != actualBinding) {
                throw new IllegalArgumentException("Native Provider reopen belongs to another Cell/Binding");
            }
            if (scopeWork == null) {
                originalBinding = actualBinding;
                scopeWork = actualBinding
                        .providerScopes(physical)
                        .thenCompose(registry -> registry.reopen(expected))
                        .thenAccept(admitted -> scopeAdmission = admitted);
            }
            return scopeWork.copy();
        }

        synchronized CompletableFuture<com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1>
                captureProfileAuthority() {
            qualifiedProvider();
            return scopeAdmission.captureAuthority();
        }

        synchronized C1ObjectProviderSession qualifiedProvider() {
            profile();
            if (scopeAdmission == null
                    || scopeWork == null
                    || !scopeWork.isDone()
                    || scopeWork.isCompletedExceptionally()) {
                throw new IllegalStateException("Original Provider profile is not actually admitted");
            }
            return provider();
        }

        synchronized CompletableFuture<Void> requireProfileCurrent() {
            qualifiedProvider();
            if (currentCheck == null || currentCheck.isDone()) {
                currentCheck = scopeAdmission
                        .requireCurrent()
                        .thenCompose(unused -> S3PulsarOffloadObjectStoreV1.this.<Void>submit(() -> {
                            requireConfiguredEncryption();
                            return null;
                        }))
                        .toCompletableFuture();
            }
            return currentCheck.copy();
        }

        /** Historical profile confirmation settles this original loan only; no Task is admitted here. */
        synchronized CompletableFuture<Void> reconcileScopeForClose() {
            if (scopeWork != null && !scopeWork.isDone()) {
                return scopeWork.handle((ignored, failure) -> null).thenCompose(ignored -> reconcileScopeForClose());
            }
            if (currentCheck != null && !currentCheck.isDone()) {
                return currentCheck.handle((ignored, failure) -> null).thenCompose(ignored -> reconcileScopeForClose());
            }
            if (scopeAttempt == null || scopeAttempt.isSettledForClose()) {
                return CompletableFuture.completedFuture(null);
            }
            return scopeAttempt.reconcile().handle((admitted, failure) -> {
                if (!scopeAttempt.isSettledForClose()) {
                    throw new java.util.concurrent.CompletionException(
                            "Original Provider scope metadata remains unresolved", failure);
                }
                return null;
            });
        }

        @Override
        public synchronized void close() {
            if (!released) {
                if (scopeWork != null && !scopeWork.isDone()
                        || currentCheck != null && !currentCheck.isDone()
                        || scopeAttempt != null && !scopeAttempt.isSettledForClose()) {
                    throw new IllegalStateException("Original Provider scope metadata remains unresolved");
                }
                provider.close(); // Pending GET/PUT/UNKNOWN/response cleanup retains this original SDK hold.
                transport.close(); // This transport borrows the original client.
                released = true;
                operationFinished();
                ownerFinished.run();
            }
        }
    }

    MaterializationConnection borrowMaterializationConnection(
            Namespace expected,
            MetadataNamespaceIdentityV2 metadataNamespace,
            CellProviderScopeId scope,
            String prefix,
            long maximumObjectBytes,
            Runnable ownerFinished) {
        return borrowMaterializationConnection(
                expected, metadataNamespace, scope, null, prefix, maximumObjectBytes, ownerFinished);
    }

    MaterializationConnection borrowConfiguredMaterializationConnection(
            Namespace expected,
            MetadataNamespaceIdentityV2 metadataNamespace,
            ProtocolCellIdentity cell,
            String prefix,
            long maximumObjectBytes,
            Runnable ownerFinished) {
        if (materializationConfiguration == null) {
            throw new IllegalStateException("Native materialization requires its complete configured Provider profile");
        }
        return borrowMaterializationConnection(
                expected, metadataNamespace, null, cell, prefix, maximumObjectBytes, ownerFinished);
    }

    private MaterializationConnection borrowMaterializationConnection(
            Namespace expected,
            MetadataNamespaceIdentityV2 metadataNamespace,
            CellProviderScopeId suppliedScope,
            ProtocolCellIdentity cell,
            String prefix,
            long maximumObjectBytes,
            Runnable ownerFinished) {
        synchronized (lifetimeLock) {
            if (closed.get()) {
                throw new IllegalStateException("Original S3 native connection is closed");
            }
            activeOperations++;
        }
        boolean delivered = false;
        try {
            var physical = S3M5PhysicalNamespaceAuthorityV2.connect(client, bucket, expected);
            var binding = physical.bind(metadataNamespace).toCompletableFuture().join();
            var transport = S3C1ObjectProviderTransport.admitVersionMatchDeleteV1(
                    client,
                    bucket,
                    "native-pulsar/" + expected.serviceIdentity().value() + "/" + bucket,
                    false,
                    1000,
                    cell == null
                            ? null
                            : materializationConfiguration.references().allowedEncryptionScope());
            M5CellProviderProfileV2 profile = null;
            var scope = suppliedScope;
            if (cell != null) {
                requireConfiguredEncryption();
                var caps = transport.capabilities();
                var delete = transport.deleteCapabilities();
                delete.requireVersionMatchDeleteV1();
                var configuration = new java.util.TreeMap<>(readAdmissionConfiguration());
                configuration.remove("endpoint"); // Actual physical namespace identity replaces endpoint aliases.
                configuration.put("adapter", S3C1ObjectProviderTransport.ADAPTER_VERSION);
                configuration.put("deleteMechanism", delete.mechanism());
                profile = new M5CellProviderProfileV2(
                        binding,
                        cell,
                        prefix,
                        materializationConfiguration.references(),
                        S3C1ObjectProviderTransport.ADAPTER_VERSION,
                        PulsarNativeM4DrainAdmissionV1.digest(configuration),
                        caps.maximumObjectBytes(),
                        caps.maximumRangeBytes(),
                        caps.maximumListPageKeys(),
                        delete.maximumVersionTokenBytes(),
                        materializationConfiguration.gcGracePolicy());
                scope = profile.scopeId();
            }
            var provider = new C1ObjectProviderSession(transport, scope, prefix, maximumObjectBytes, 65_536);
            var connection =
                    new MaterializationConnection(physical, binding, transport, provider, ownerFinished, profile);
            synchronized (lifetimeLock) {
                if (closed.get()) {
                    throw new IllegalStateException("Original S3 connection closed during native admission");
                }
                delivered = true;
            }
            return connection;
        } catch (IOException failure) {
            throw new IllegalStateException("Original S3 native materialization admission failed", failure);
        } finally {
            if (!delivered) {
                operationFinished();
            }
        }
    }

    private void requireConfiguredEncryption() {
        if (materializationConfiguration == null
                || client.serviceClientConfiguration().credentialsProvider()
                        != materializationConfiguration.credentialsProvider()) {
            throw new IllegalStateException("Original configured Provider credential context differs");
        }
        String actual;
        try {
            var configuration = client.getBucketEncryption(
                            GetBucketEncryptionRequest.builder().bucket(bucket).build())
                    .serverSideEncryptionConfiguration();
            if (configuration == null
                    || configuration.rules().size() != 1
                    || configuration.rules().get(0).applyServerSideEncryptionByDefault() == null) {
                throw new IllegalStateException("Actual bucket encryption scope is unavailable");
            }
            var encryption = configuration.rules().get(0).applyServerSideEncryptionByDefault();
            actual = switch (encryption.sseAlgorithmAsString()) {
                case "AES256" -> "S3_MANAGED_AES256";
                case "aws:kms" -> "AWS_KMS:" + encryption.kmsMasterKeyID();
                default -> throw new IllegalStateException("Actual bucket encryption algorithm is not admitted");
            };
            if (actual.startsWith("AWS_KMS:")
                    && (encryption.kmsMasterKeyID() == null
                            || !encryption.kmsMasterKeyID().startsWith("arn:"))) {
                throw new IllegalStateException("Actual bucket KMS identity is not a stable full ARN");
            }
        } catch (S3Exception failure) {
            if (failure.statusCode() != 404
                    || failure.awsErrorDetails() == null
                    || !"ServerSideEncryptionConfigurationNotFoundError"
                            .equals(failure.awsErrorDetails().errorCode())) {
                throw failure;
            }
            actual = "NONE";
        }
        if (!materializationConfiguration.references().allowedEncryptionScope().equals(actual)) {
            throw new IllegalStateException("Actual bucket encryption differs from the allowed KMS scope");
        }
    }

    private void requireObjectEncryption(String algorithm, String kmsKey) throws ObjectStoreException {
        if (materializationConfiguration == null) {
            return;
        }
        String scope = materializationConfiguration.references().allowedEncryptionScope();
        boolean matches =
                switch (scope) {
                    case "NONE" -> algorithm == null && kmsKey == null;
                    case "S3_MANAGED_AES256" -> "AES256".equals(algorithm) && kmsKey == null;
                    default -> "aws:kms".equals(algorithm) && scope.equals("AWS_KMS:" + kmsKey);
                };
        if (!matches) {
            throw failure(FailureKind.INTEGRITY, "S3 Object encryption differs from the admitted scope");
        }
    }

    @Override
    public CompletionStage<ImmutableObject> createImmutable(String key, Body body) {
        Objects.requireNonNull(body, "body");
        return submit(() -> {
            validateKey(key);
            try (InputStream input = body.inputStreamFactory().open()) {
                var request = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .ifNoneMatch("*")
                        .metadata(Map.of(SHA256_METADATA, body.sha256()));
                if (materializationConfiguration != null) {
                    String scope = materializationConfiguration.references().allowedEncryptionScope();
                    if ("S3_MANAGED_AES256".equals(scope)) {
                        request.serverSideEncryption("AES256");
                    } else if (scope.startsWith("AWS_KMS:")) {
                        request.serverSideEncryption("aws:kms").ssekmsKeyId(scope.substring("AWS_KMS:".length()));
                    }
                }
                PutObjectResponse response =
                        client.putObject(request.build(), RequestBody.fromInputStream(input, body.bytes()));
                requireObjectEncryption(response.serverSideEncryptionAsString(), response.ssekmsKeyId());
                return new ImmutableObject(version(response.versionId(), response.eTag()), body.bytes(), body.sha256());
            }
        });
    }

    @Override
    public CompletionStage<ImmutableObject> head(String key) {
        return submit(() -> headSync(key));
    }

    @Override
    public CompletionStage<byte[]> readRange(String key, long offset, int length) {
        return submit(() -> readRangeSync(key, null, offset, length));
    }

    @Override
    public CompletionStage<byte[]> readRange(String key, ImmutableObject expected, long offset, int length) {
        return submit(() -> readRangeSync(key, Objects.requireNonNull(expected, "expected"), offset, length));
    }

    private byte[] readRangeSync(String key, ImmutableObject expected, long offset, int length) throws IOException {
        validateKey(key);
        if (offset < 0 || length <= 0) {
            throw new IllegalArgumentException("S3 range is empty or negative");
        }
        long last = Math.addExact(offset, length - 1L);
        var request = GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=" + offset + "-" + last);
        if (expected != null) {
            if (last >= expected.bytes()) {
                throw new IllegalArgumentException("S3 range exceeds the captured Object extent");
            }
            String identity = expected.immutableVersion();
            if (identity.startsWith("version:")
                    && identity.length() > "version:".length()
                    && !identity.substring("version:".length()).isBlank()
                    && !identity.substring("version:".length()).equals("null")) {
                request.versionId(identity.substring("version:".length()));
            } else if (identity.startsWith("etag:")
                    && identity.length() > "etag:".length()
                    && !identity.substring("etag:".length()).isBlank()) {
                request.ifMatch(identity.substring("etag:".length()));
            } else {
                throw failure(FailureKind.INTEGRITY, "captured S3 Object version identity is unsupported");
            }
        }
        try (var input = client.getObject(request.build())) {
            var response = input.response();
            requireObjectEncryption(response.serverSideEncryptionAsString(), response.ssekmsKeyId());
            if (expected != null) {
                if (!expected.immutableVersion().equals(version(response.versionId(), response.eTag()))
                        || !expected.sha256().equals(response.metadata().get(SHA256_METADATA))
                        || !Long.valueOf(length).equals(response.contentLength())
                        || !("bytes " + offset + "-" + last + "/" + expected.bytes()).equals(response.contentRange())) {
                    throw failure(FailureKind.INTEGRITY, "S3 GET response differs from the captured Object proof");
                }
            }
            byte[] result = input.readNBytes(length);
            if (result.length != length || input.read() != -1) {
                throw failure(FailureKind.SHORT_READ, "S3 range response differs from the exact requested length");
            }
            return result;
        }
    }

    @Override
    public CompletionStage<Void> deleteAndProveAbsent(String key) {
        return submit(() -> {
            validateKey(key);
            client.deleteObject(
                    DeleteObjectRequest.builder().bucket(bucket).key(key).build());
            try {
                headSync(key);
            } catch (ObjectStoreException expected) {
                if (expected.kind() == FailureKind.NOT_FOUND) {
                    return null;
                }
                throw expected;
            }
            throw failure(FailureKind.CONFLICT, "S3 Object remains visible after delete");
        });
    }

    @Override
    public CompletionStage<Void> cleanupAttemptMultipartResidue(String attemptPrefix) {
        return submit(() -> {
            validateKey(attemptPrefix);
            for (MultipartUpload upload : listMultipartUploads(attemptPrefix)) {
                client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(bucket)
                        .key(upload.key())
                        .uploadId(upload.uploadId())
                        .build());
            }
            if (!listMultipartUploads(attemptPrefix).isEmpty()) {
                throw failure(FailureKind.CONFLICT, "S3 multipart uploads remain after deterministic cleanup");
            }
            return null;
        });
    }

    @Override
    public CompletionStage<Void> close() {
        synchronized (lifetimeLock) {
            if (closed.get()) {
                return closeCompletion.copy();
            }
            closed.set(true);
        }
        Throwable failure = null;
        try {
            executor.shutdown();
        } catch (Throwable caught) {
            failure = caught;
        }
        boolean finish;
        synchronized (lifetimeLock) {
            shutdownFailure = failure;
            shutdownComplete = true;
            finish = readyToClose();
        }
        if (finish) {
            finishClose();
        }
        return closeCompletion.copy();
    }

    private boolean readyToClose() {
        if (closed.get() && shutdownComplete && activeOperations == 0 && !closeStarted) {
            closeStarted = true;
            return true;
        }
        return false;
    }

    private void operationFinished() {
        boolean finish;
        synchronized (lifetimeLock) {
            activeOperations--;
            finish = readyToClose();
        }
        if (finish) {
            finishClose();
        }
    }

    private void finishClose() {
        Throwable failure;
        synchronized (lifetimeLock) {
            failure = shutdownFailure;
        }
        try {
            if (closeClient) {
                client.close();
            }
        } catch (Throwable caught) {
            if (failure == null) {
                failure = caught;
            } else if (failure != caught) {
                failure.addSuppressed(caught);
            }
        }
        if (failure == null) {
            closeCompletion.complete(null);
        } else {
            closeCompletion.completeExceptionally(failure);
        }
    }

    private ImmutableObject headSync(String key) throws ObjectStoreException {
        validateKey(key);
        HeadObjectResponse response;
        try {
            response = client.headObject(
                    HeadObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (Throwable failure) {
            Throwable mapped = map(failure);
            if (mapped instanceof ObjectStoreException objectStoreFailure) {
                throw objectStoreFailure;
            }
            if (mapped instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            throw new IllegalStateException("unexpected checked S3 HEAD failure", mapped);
        }
        requireObjectEncryption(response.serverSideEncryptionAsString(), response.ssekmsKeyId());
        String sha256 = response.metadata().get(SHA256_METADATA);
        if (sha256 == null || !sha256.matches("[0-9a-f]{64}") || response.contentLength() <= 0) {
            throw failure(FailureKind.INTEGRITY, "S3 HEAD lacks the exact Nereus length or SHA-256 proof");
        }
        return new ImmutableObject(version(response.versionId(), response.eTag()), response.contentLength(), sha256);
    }

    private List<MultipartUpload> listMultipartUploads(String prefix) {
        List<MultipartUpload> result = new ArrayList<>();
        String keyMarker = null;
        String uploadIdMarker = null;
        do {
            ListMultipartUploadsResponse response = client.listMultipartUploads(ListMultipartUploadsRequest.builder()
                    .bucket(bucket)
                    .prefix(prefix)
                    .keyMarker(keyMarker)
                    .uploadIdMarker(uploadIdMarker)
                    .build());
            result.addAll(response.uploads());
            if (!response.isTruncated()) {
                break;
            }
            keyMarker = response.nextKeyMarker();
            uploadIdMarker = response.nextUploadIdMarker();
        } while (true);
        return result;
    }

    private <T> CompletionStage<T> submit(ThrowingSupplier<T> operation) {
        var result = new CompletableFuture<T>();
        synchronized (lifetimeLock) {
            if (closed.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("S3 Pulsar Object store is closed"));
            }
            activeOperations++;
        }
        try {
            executor.execute(() -> execute(operation, result));
        } catch (RejectedExecutionException failure) {
            operationFinished();
            result.completeExceptionally(failure);
        }
        return result.copy();
    }

    private <T> void execute(ThrowingSupplier<T> operation, CompletableFuture<T> result) {
        T value = null;
        Throwable failure = null;
        try {
            if (closed.get()) {
                throw new IllegalStateException("S3 Pulsar Object store is closed");
            }
            value = operation.get();
        } catch (Throwable caught) {
            failure = map(caught);
        } finally {
            operationFinished();
        }
        if (failure == null) {
            result.complete(value);
        } else {
            result.completeExceptionally(failure);
        }
    }

    private static Throwable map(Throwable failure) {
        if (failure instanceof ObjectStoreException || failure instanceof IllegalArgumentException) {
            return failure;
        }
        if (failure instanceof CancellationException) {
            return failure(FailureKind.CANCELLED, "S3 operation was cancelled", failure);
        }
        if (failure instanceof NoSuchKeyException) {
            return failure(FailureKind.NOT_FOUND, "S3 Object is absent", failure);
        }
        if (failure instanceof ApiCallTimeoutException || failure instanceof ApiCallAttemptTimeoutException) {
            return failure(FailureKind.TIMEOUT, "S3 operation timed out", failure);
        }
        if (failure instanceof S3Exception s3Failure) {
            int status = s3Failure.statusCode();
            if (status == 404) {
                return failure(FailureKind.NOT_FOUND, "S3 Object is absent", failure);
            }
            if (status == 408 || status == 504) {
                return failure(FailureKind.TIMEOUT, "S3 operation timed out", failure);
            }
            if (status == 409 || status == 412) {
                return failure(FailureKind.CONFLICT, "S3 conditional or concurrent operation conflicted", failure);
            }
            if (status >= 500) {
                return failure(FailureKind.UNAVAILABLE, "S3 service is unavailable", failure);
            }
        }
        if (failure instanceof SdkClientException || failure instanceof IOException) {
            return failure(FailureKind.UNAVAILABLE, "S3 client operation failed", failure);
        }
        return failure;
    }

    private static String version(String versionId, String eTag) throws ObjectStoreException {
        if (versionId != null && !versionId.isBlank() && !"null".equals(versionId)) {
            return "version:" + versionId;
        }
        if (eTag == null || eTag.isBlank()) {
            throw failure(FailureKind.INTEGRITY, "S3 response lacks an immutable version identity");
        }
        return "etag:" + eTag.toLowerCase(Locale.ROOT);
    }

    private static void validateKey(String key) {
        Objects.requireNonNull(key, "key");
        if (key.isBlank() || key.startsWith("/") || key.contains("//")) {
            throw new IllegalArgumentException("S3 Object key is not canonical");
        }
    }

    private static ObjectStoreException failure(FailureKind kind, String message) {
        return new ObjectStoreException(kind, message);
    }

    private static ObjectStoreException failure(FailureKind kind, String message, Throwable cause) {
        return new ObjectStoreException(kind, message, cause);
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
