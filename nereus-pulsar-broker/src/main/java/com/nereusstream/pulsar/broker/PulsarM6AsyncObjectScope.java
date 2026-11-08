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

package com.nereusstream.pulsar.broker;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1;
import com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1.AttemptKeyManager;
import com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1.PreparedAttemptKey;
import com.nereusstream.pulsar.offload.PulsarOffloadBlockPolicyV1.BlockClass;
import com.nereusstream.pulsar.offload.PulsarOffloadLimitCandidateV1;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.S3PulsarOffloadObjectStoreV1;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionPolicy;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.AttemptKeyEnvelope;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ProviderKind;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2.References;
import com.nereusstream.storage.object.vault.VaultTransitRunKeyKms;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.function.Function;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Owns the real S3 and Vault clients used by one enrolled native ManagedLedger factory. */
final class PulsarM6AsyncObjectScope {
    private static final Duration PROVIDER_TIMEOUT = Duration.ofSeconds(20);
    private static final int NPO1_KEY_ENVELOPE_VERSION = 1;
    // NPO1 uses lowercase canonical identifiers; the Vault runtime has a distinct wire envelope.
    private static final String NPO1_VAULT_PROVIDER = "hashicorp_vault_transit";
    private static final String NPO1_VAULT_ALGORITHM = "aes256_gcm96";
    private final S3PulsarOffloadObjectStoreV1 objects;
    private final VaultTransitRunKeyKms kms;
    private final NereusPulsarLedgerOffloaderV1 offloader;

    record Configuration(
            URI endpoint,
            String region,
            String bucket,
            Namespace physicalNamespace,
            String keyPrefix,
            BlockClass blocks,
            Path staging,
            References references,
            URI vaultEndpoint,
            String vaultMount,
            String vaultKey,
            String vaultTokenEnvironment) {
        static Configuration read(Function<String, String> value) {
            String mount = value.apply("vaultMount");
            String key = value.apply("vaultKey");
            return new Configuration(
                    URI.create(value.apply("s3Endpoint")),
                    value.apply("s3Region"),
                    value.apply("s3Bucket"),
                    new Namespace(
                            ProviderKind.OBJECT_PROVIDER,
                            CanonicalUtf8.fromString(value.apply("s3ServiceIdentity")),
                            CanonicalUtf8.fromString(value.apply("s3ContainerIdentity"))),
                    value.apply("s3KeyPrefix"),
                    BlockClass.fromId(value.apply("offloadBlockClass")),
                    Path.of(value.apply("offloadStagingDirectory")),
                    new References(
                            value.apply("s3SecurityScope"),
                            value.apply("s3CredentialScope"),
                            value.apply("s3EncryptionScope"),
                            value.apply("s3QuotaScope"),
                            value.apply("s3OperatorOwner"),
                            value.apply("s3DeleteCapability")),
                    URI.create(value.apply("vaultEndpoint")),
                    mount,
                    key,
                    value.apply("vaultTokenEnvironment"));
        }
    }

    private PulsarM6AsyncObjectScope(
            S3PulsarOffloadObjectStoreV1 objects, VaultTransitRunKeyKms kms, NereusPulsarLedgerOffloaderV1 offloader) {
        this.objects = objects;
        this.kms = kms;
        this.offloader = offloader;
    }

    static CompletableFuture<PulsarM6AsyncObjectScope> open(
            Configuration config, MetadataNamespaceIdentityV2 metadata) {
        var credentials = EnvironmentVariableCredentialsProvider.create();
        var sequence = new java.util.concurrent.atomic.AtomicInteger();
        var executor = Executors.newFixedThreadPool(
                2, task -> new Thread(task, "nereus-native-offload-" + sequence.incrementAndGet()));
        S3PulsarOffloadObjectStoreV1 objects = null;
        VaultTransitRunKeyKms vault = null;
        S3Client client = null;
        try {
            client = S3Client.builder()
                    .credentialsProvider(credentials)
                    .region(Region.of(config.region()))
                    .endpointOverride(config.endpoint())
                    .forcePathStyle(true)
                    .overrideConfiguration(c -> c.apiCallTimeout(PROVIDER_TIMEOUT)
                            .retryStrategy(software.amazon.awssdk.retries.StandardRetryStrategy.builder()
                                    .maxAttempts(1)
                                    .build()))
                    .build();
            objects = new S3PulsarOffloadObjectStoreV1(
                    client,
                    config.bucket(),
                    executor,
                    true,
                    new S3PulsarOffloadObjectStoreV1.MaterializationConfiguration(config.references(), credentials));
            var token = System.getenv(config.vaultTokenEnvironment());
            if (token == null || token.isBlank()) {
                throw new IllegalArgumentException("Vault token is absent for the configured Native Object scope");
            }
            vault = new VaultTransitRunKeyKms(
                    HttpClient.newBuilder().connectTimeout(PROVIDER_TIMEOUT).build(),
                    config.vaultEndpoint(),
                    config.vaultMount(),
                    config.vaultKey(),
                    () -> token,
                    PROVIDER_TIMEOUT,
                    new SecureRandom());
            var offloadPolicies = new OffloadPoliciesImpl();
            offloadPolicies.setManagedLedgerOffloadedReadPriority(OffloadedReadPriority.TIERED_STORAGE_FIRST);
            var scope = new PulsarM6AsyncObjectScope(
                    objects,
                    vault,
                    new NereusPulsarLedgerOffloaderV1(
                            objects,
                            new NereusPulsarLedgerOffloaderV1.RuntimePolicy(
                                    "m6-native-retain-bk",
                                    config.keyPrefix(),
                                    RetentionClass.RETAIN_BK,
                                    config.blocks(),
                                    CompressionPolicy.ZSTD_IF_SMALLER,
                                    PulsarOffloadLimitCandidateV1.adr0056EvidenceCandidate()),
                            keyManager(vault),
                            (ledger, attempt, error) -> {},
                            offloadPolicies,
                            executor,
                            config.staging()));
            return objects.observePhysicalNamespace(config.physicalNamespace(), metadata)
                    .thenApply(unused -> scope)
                    .exceptionallyCompose(error -> scope.closeAsync().handle((unused, cleanup) -> {
                        if (cleanup != null) {
                            error.addSuppressed(cleanup);
                        }
                        throw new CompletionException(error);
                    }))
                    .toCompletableFuture();
        } catch (RuntimeException failure) {
            if (vault != null) {
                vault.close();
            }
            if (objects != null) {
                return objects.close()
                        .<PulsarM6AsyncObjectScope>handle((unused, cleanup) -> {
                            if (cleanup != null) {
                                failure.addSuppressed(cleanup);
                            }
                            throw new CompletionException(failure);
                        })
                        .toCompletableFuture();
            }
            executor.shutdown();
            if (client != null) {
                client.close();
            }
            return CompletableFuture.failedFuture(failure);
        }
    }

    NereusPulsarLedgerOffloaderV1 offloader() {
        return offloader;
    }

    CompletableFuture<Void> closeAsync() {
        return offloader.closeAsync().thenCompose(unused -> objects.close()).thenRun(kms::close);
    }

    private static AttemptKeyManager keyManager(VaultTransitRunKeyKms kms) {
        return new AttemptKeyManager() {
            @Override
            public PreparedAttemptKey create(long ledgerId, UUID attempt, Map<String, String> policy) {
                try (var generated = kms.generateAndWrap()) {
                    var envelope = generated.envelope();
                    return new PreparedAttemptKey(
                            generated.plaintextKey().use(bytes -> new SecretKeySpec(bytes, "AES")),
                            new AttemptKeyEnvelope(
                                    NPO1_KEY_ENVELOPE_VERSION,
                                    NPO1_VAULT_PROVIDER,
                                    envelope.wrappingKeyId(),
                                    envelope.wrappingKeyVersion(),
                                    NPO1_VAULT_ALGORITHM,
                                    envelope.wrappedKey().toByteArray()));
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new CompletionException(failure);
                } catch (IOException failure) {
                    throw new CompletionException(failure);
                }
            }

            @Override
            public SecretKey unwrap(
                    long ledgerId, UUID attempt, AttemptKeyEnvelope envelope, Map<String, String> policy) {
                if (!NPO1_VAULT_PROVIDER.equals(envelope.providerId())
                        || !NPO1_VAULT_ALGORITHM.equals(envelope.wrappingAlgorithm())) {
                    throw new IllegalArgumentException("NPO1 attempt does not carry the configured Vault envelope");
                }
                var wrapped = new WrappedRunKeyEnvelope(
                        VaultTransitRunKeyKms.PROVIDER_ID,
                        VaultTransitRunKeyKms.WRAPPING_ALGORITHM_ID,
                        envelope.wrappingKeyId(),
                        envelope.wrappingKeyVersion(),
                        CanonicalBytes.copyOf(envelope.wrappedKey()));
                try (var key = kms.unwrapZeroizable(wrapped)) {
                    return key.use(bytes -> new SecretKeySpec(bytes, "AES"));
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new CompletionException(failure);
                } catch (IOException failure) {
                    throw new CompletionException(failure);
                }
            }
        };
    }
}
