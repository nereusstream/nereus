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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.metadata.oxia.v2.retention.OxiaCellProviderScopeRegistryV2.Admission;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport;
import com.nereusstream.storage.object.s3.S3M5PhysicalNamespaceAuthorityV2;
import com.nereusstream.storage.object.vault.VaultTransitRunKeyKms;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Owns the admitted physical S3 transport and wrapping route for one Object WAL Cell. */
final class PulsarM6ObjectWalProvider implements AutoCloseable {
    private static final int MAX_TOKEN_BYTES = 4096;
    private static final int MAX_LIVE_RUN_KEYS = 128;
    private static final String ACCESS = "aws_access_key_id";
    private static final String SECRET = "aws_secret_access_key";
    private static final String SESSION = "aws_session_token";
    private final S3C1ObjectProviderTransport transport;
    private final VaultTransitRunKeyKms vault;
    private final Admission admission;
    private final SecureRandom random;

    private PulsarM6ObjectWalProvider(
            S3C1ObjectProviderTransport transport,
            VaultTransitRunKeyKms vault,
            Admission admission,
            SecureRandom random) {
        this.transport = transport;
        this.vault = vault;
        this.admission = admission;
        this.random = random;
    }

    record Configuration(
            URI endpoint,
            String bucket,
            String region,
            Path credentials,
            Path capability,
            URI vaultEndpoint,
            String vaultMount,
            String vaultKey,
            Path vaultToken) {
        static Configuration read(Function<String, String> setting) {
            return new Configuration(
                    origin(setting.apply("s3Endpoint")),
                    setting.apply("s3Bucket"),
                    setting.apply("s3Region"),
                    Path.of(setting.apply("s3CredentialsFile")),
                    Path.of(setting.apply("objectWalProviderProfile")),
                    origin(setting.apply("vaultEndpoint")),
                    setting.apply("vaultMount"),
                    setting.apply("vaultKey"),
                    Path.of(setting.apply("vaultTokenFile")));
        }

        String wrappingKeyId() {
            return "vault-transit://" + vaultMount + "/keys/" + vaultKey;
        }

        Sha256Digest commitment() throws IOException {
            try (var bytes = new ByteArrayOutputStream();
                    var data = new DataOutputStream(bytes)) {
                data.writeUTF("NEREUS_PULSAR_OBJECT_WAL_CONFIG_V1");
                data.writeUTF(endpoint.toASCIIString());
                data.writeUTF(bucket);
                data.writeUTF(region);
                data.writeUTF(S3C1ObjectProviderTransport.ADAPTER_VERSION);
                data.writeUTF(vaultEndpoint.toASCIIString());
                data.writeUTF(wrappingKeyId());
                data.flush();
                return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
            }
        }

        private static URI origin(String value) {
            var uri = URI.create(value);
            if (!Set.of("http", "https").contains(uri.getScheme())
                    || uri.getHost() == null
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || !Set.of("", "/").contains(uri.getRawPath())) {
                throw new IllegalArgumentException("Pulsar Object WAL requires an exact HTTP(S) endpoint origin");
            }
            return URI.create(value.endsWith("/") ? value : value + "/");
        }
    }

    static PulsarM6ObjectWalProvider open(
            Configuration config,
            PulsarProtocolCellIdentity cell,
            OxiaPhysicalMetadataNamespaceV2 metadata,
            Duration timeout)
            throws Exception {
        M5CellProviderProfileV2 profile;
        try (var input = Files.newInputStream(config.capability())) {
            profile = M5CellProviderProfileV2.decode(
                    CanonicalBytes.copyOf(input.readNBytes(M5CellProviderProfileV2.MAX_BYTES + 1)));
        }
        if (!profile.protocolCell().equals(cell)
                || !S3C1ObjectProviderTransport.ADAPTER_VERSION.equals(profile.adapterVersion())
                || !profile.providerConfigurationSha256().equals(config.commitment())) {
            throw new IllegalArgumentException("Object WAL capability differs from the admitted Cell or provider");
        }
        var credentials = credentials(config.credentials(), profile.references().credentialScope());
        var client = S3Client.builder()
                .region(Region.of(config.region()))
                .endpointOverride(config.endpoint())
                .forcePathStyle(true)
                .credentialsProvider(credentials)
                .overrideConfiguration(settings -> settings.apiCallTimeout(timeout))
                .build();
        VaultTransitRunKeyKms vault = null;
        try {
            var expected = profile.physicalBinding().physicalNamespace();
            var physical = S3M5PhysicalNamespaceAuthorityV2.connect(client, config.bucket(), expected);
            long wait = timeout.toMillis();
            var admitted = metadata.openCellProviderScopes(physical)
                    .thenCompose(scopes -> scopes.reopen(profile))
                    .toCompletableFuture()
                    .get(wait, TimeUnit.MILLISECONDS);
            metadata.openAuthorityRoute(physical)
                    .thenCompose(route -> route.quota().snapshot())
                    .toCompletableFuture()
                    .get(wait, TimeUnit.MILLISECONDS);
            var transport = S3C1ObjectProviderTransport.admitVersionMatchDeleteV1(
                    client,
                    config.bucket(),
                    expected.serviceIdentity().value(),
                    true,
                    profile.maximumListPageKeys(),
                    profile.references().allowedEncryptionScope());
            if (profile.maximumObjectBytes() > transport.capabilities().maximumObjectBytes()
                    || profile.maximumRangeBytes() > transport.capabilities().maximumRangeBytes()
                    || profile.maximumVersionTokenBytes()
                            > transport.deleteCapabilities().maximumVersionTokenBytes()) {
                throw new IllegalArgumentException("Object WAL Cell exceeds the real Provider limits");
            }
            var random = new SecureRandom();
            String token = token(config.vaultToken());
            vault = new VaultTransitRunKeyKms(
                    HttpClient.newBuilder().connectTimeout(timeout).build(),
                    config.vaultEndpoint(),
                    config.vaultMount(),
                    config.vaultKey(),
                    () -> token,
                    timeout,
                    random);
            if (!vault.keyIdentity().equals(config.wrappingKeyId())) {
                throw new IllegalArgumentException("Object WAL wrapping route differs from the admitted Cell");
            }
            try (var probe = vault.generateAndWrap()) {
                byte[] read = vault.unwrap(probe.envelope());
                try {
                    if (!probe.plaintextKey().use(bytes -> MessageDigest.isEqual(bytes, read))) {
                        throw new IllegalStateException("Object WAL Vault did not unwrap its original run key");
                    }
                } finally {
                    Arrays.fill(read, (byte) 0);
                }
            }
            return new PulsarM6ObjectWalProvider(transport, vault, admitted, random);
        } catch (Exception | Error failure) {
            if (vault != null) {
                vault.close();
            }
            try {
                client.close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static StaticCredentialsProvider credentials(Path path, String scope) {
        var values = ProfileFile.builder()
                .content(path)
                .type(ProfileFile.Type.CREDENTIALS)
                .build()
                .profile(scope)
                .orElseThrow(() -> new IllegalArgumentException("Object WAL credential scope is absent"))
                .properties();
        if (values.keySet().equals(Set.of(ACCESS, SECRET))) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(values.get(ACCESS), values.get(SECRET)));
        }
        if (values.keySet().equals(Set.of(ACCESS, SECRET, SESSION))) {
            return StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(values.get(ACCESS), values.get(SECRET), values.get(SESSION)));
        }
        throw new IllegalArgumentException("Object WAL credentials differ from the admitted scope");
    }

    private static String token(Path file) throws IOException {
        try (var input = Files.newInputStream(file)) {
            var bytes = input.readNBytes(MAX_TOKEN_BYTES + 1);
            try {
                var token = new String(bytes, StandardCharsets.UTF_8).strip();
                if (bytes.length > MAX_TOKEN_BYTES
                        || token.isEmpty()
                        || token.chars().anyMatch(Character::isWhitespace)) {
                    throw new IllegalArgumentException("Invalid Object WAL Vault token");
                }
                return token;
            } finally {
                Arrays.fill(bytes, (byte) 0);
            }
        }
    }

    M5CellProviderProfileV2 profile() {
        return admission.profile();
    }

    String wrappingKeyId() {
        return vault.keyIdentity();
    }

    java.util.concurrent.CompletionStage<Void> requireCurrent() {
        return admission.requireCurrent();
    }

    KmsCellSession openKms() {
        return new KmsCellSession(vault, profile().scopeId(), vault.keyIdentity(), MAX_LIVE_RUN_KEYS, random);
    }

    C1ObjectProviderSession openRun(WalRunRootRecord root, PulsarM6ObjectWalDispatchFence dispatch) {
        var source = root.providerConfiguration();
        if (!root.protocolCellIdentity().equals(profile().protocolCell())
                || !root.providerScopeId().equals(profile().scopeId())
                || !source.exclusiveNamespacePrefix().startsWith(profile().exclusiveNamespacePrefix() + "/object-wal/")
                || source.maxObjectBodyBytes() > profile().maximumObjectBytes()
                || root.nwg1AdmissionCaps().maxDirectoryPrefixBytes()
                        > profile().maximumRangeBytes()) {
            throw new IllegalArgumentException("Object WAL Root exceeds its admitted Cell");
        }
        var io = dispatch == null ? transport : new PulsarM6ObjectWalTransport(transport, dispatch);
        return new C1ObjectProviderSession(
                io,
                profile().scopeId(),
                source.exclusiveNamespacePrefix(),
                source.maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
    }

    @Override
    public void close() {
        try {
            vault.close();
        } finally {
            transport.close();
        }
    }
}
