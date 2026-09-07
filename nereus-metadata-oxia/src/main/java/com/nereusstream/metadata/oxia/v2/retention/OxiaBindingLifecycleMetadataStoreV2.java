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

package com.nereusstream.metadata.oxia.v2.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityControlMetadataStoreV1;
import com.nereusstream.storage.object.retention.M5RetiredBatchHistoryV2;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

/**
 * One admitted Cell root/shard/Binding route for exact M4 control and immutable M5 history.
 *
 * <p>Public keys remain canonical relative keys, including VersionedValue keys. Only this boundary adds the Cell
 * root. The returned synchronous control view belongs on the owner's low-frequency control executor; asynchronous
 * history operations never block a native callback.
 * This route does not grant owner, namespace-quota or writer authority.
 */
public final class OxiaBindingLifecycleMetadataStoreV2 implements ExactMetadataTransactionStoreV1 {
    public static final int MAX_NATIVE_KEY_BYTES = 512;
    private static final Pattern CELL_ROOT = Pattern.compile("/(?:[A-Za-z0-9_-]+)(?:/[A-Za-z0-9_-]+)*");
    private static final int VERSION_TOKEN_MAGIC = 0x4d354f32; // M5O2
    private final Sha256Digest routeDigest;
    private final String cellRoot;
    private final BindingIdentity binding;
    private final M4ReadControlKeysV1 keys;
    private final M5RetiredBatchHistoryV2 history;
    private final String historyPrefix;
    private final Pattern acceptedKey;
    private final Oxia09ExactMetadataTransactionStoreV1 exact;
    private final CanonicalControlMetadataStore rawControl;
    private final CanonicalControlMetadataStore control;

    public OxiaBindingLifecycleMetadataStoreV2(
            AsyncOxiaClient client, String cellRoot, int shardId, BindingIdentity binding) {
        this(new AsyncOxiaConditionalClient(client), cellRoot, shardId, binding);
    }

    public OxiaBindingLifecycleMetadataStoreV2(
            OxiaConditionalClient client, String cellRoot, int shardId, BindingIdentity binding) {
        Objects.requireNonNull(client, "client");
        this.cellRoot = Objects.requireNonNull(cellRoot, "cellRoot");
        this.binding = Objects.requireNonNull(binding, "binding");
        if (cellRoot.length() > MAX_NATIVE_KEY_BYTES
                || !CELL_ROOT.matcher(cellRoot).matches()) {
            throw new IllegalArgumentException("lifecycle Cell root is not canonical ASCII");
        }
        keys = new M4ReadControlKeysV1(shardId, binding);
        history = new M5RetiredBatchHistoryV2(binding);
        routeDigest = Sha256Digest.hash(CanonicalBytes.copyOf((cellRoot + "\0" + keys.selector() + "\0"
                        + binding.incarnationSha256().toHex()
                        + binding.storageEpochSha256().toHex())
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII)));
        String sampleNode = history.key(parseSha("1".repeat(64)));
        historyPrefix = sampleNode.substring(0, sampleNode.length() - 64);
        String m4Prefix = keys.selector().substring(0, keys.selector().length() - "/selector".length());
        acceptedKey = Pattern.compile("(?:" + Pattern.quote(m4Prefix)
                + "(?:/selector|/proof-head|/(?:capabilities|terminals|proofs)/[0-9]{20}"
                + "|/protections/[0-9a-f]{64}-[0-9]{20})|" + Pattern.quote(historyPrefix) + "[0-9a-f]{64})");
        int maximumRelative = Math.max(
                sampleNode.length(),
                keys.protection(parseSha("1".repeat(64)), Long.MAX_VALUE).length());
        if (cellRoot.length() + 1 + maximumRelative > MAX_NATIVE_KEY_BYTES) {
            throw new IllegalArgumentException(
                    "lifecycle Cell root leaves insufficient room for a 512-byte native key");
        }
        exact = new Oxia09ExactMetadataTransactionStoreV1(new RoutedClient(client));
        rawControl = new CanonicalView();
        control = new M5BindingAuthorityControlMetadataStoreV1(rawControl, keys.selector());
    }

    /** Direct projected-selector access; M4ReadControlCoordinatorV1 must receive rawControlMetadata instead. */
    public CanonicalControlMetadataStore controlMetadata() {
        return control;
    }

    /** Validated raw envelope view for coordinators that own their M5 selector projection. */
    public CanonicalControlMetadataStore rawControlMetadata() {
        return rawControl;
    }

    /** Checked native projection for admission diagnostics and exact backend conformance. */
    public String nativeKey(String relativeKey) {
        Objects.requireNonNull(relativeKey, "relativeKey");
        if (relativeKey.length() > MAX_NATIVE_KEY_BYTES
                || !acceptedKey.matcher(relativeKey).matches()) {
            throw new IllegalArgumentException("key is outside the exact lifecycle Binding route");
        }
        return cellRoot + "/" + relativeKey;
    }

    @Override
    public CompletionStage<Optional<VersionedValue>> read(String key) {
        nativeKey(key);
        return exact.read(key)
                .thenApply(observed -> observed.map(value ->
                        VersionedValue.of(key, value.canonicalStoredBytes(), scopedVersion(value.metadataVersion()))));
    }

    @Override
    public CompletionStage<MutationOutcome> compareAndSet(
            Optional<VersionedValue> predecessor, String key, CanonicalBytes candidate) {
        nativeKey(key);
        Objects.requireNonNull(predecessor, "predecessor");
        verifyValue(key, candidate);
        predecessor.ifPresent(value -> {
            if (!key.equals(value.key())) {
                throw new IllegalArgumentException("lifecycle predecessor belongs to another key");
            }
            verifyValue(key, value.canonicalStoredBytes());
        });
        if (key.startsWith(historyPrefix) && predecessor.isPresent()) {
            throw new IllegalArgumentException("retired history nodes are immutable create-only values");
        }
        if (key.equals(keys.selector())) {
            requireSelectorSuccessor(predecessor, candidate);
        }
        var nativePredecessor = predecessor.map(
                value -> VersionedValue.of(key, value.canonicalStoredBytes(), nativeVersion(value.metadataVersion())));
        return exact.compareAndSet(nativePredecessor, key, candidate);
    }

    @Override
    public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
        Objects.requireNonNull(transaction, "transaction");
        return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
    }

    @Override
    public boolean supportsAtomicMultiKeyTransactions() {
        return false;
    }

    private MetadataVersion scopedVersion(MetadataVersion nativeVersion) {
        byte[] original = nativeVersion.value().toByteArray();
        if (original.length != Long.BYTES) {
            throw new IllegalStateException("native lifecycle version is not the locked Oxia version format");
        }
        return new MetadataVersion(CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(44)
                .putInt(VERSION_TOKEN_MAGIC)
                .put(routeDigest.bytes().toByteArray())
                .put(original)
                .array()));
    }

    private MetadataVersion nativeVersion(MetadataVersion scopedVersion) {
        byte[] token = scopedVersion.value().toByteArray();
        if (token.length != 44) {
            throw new IllegalArgumentException("lifecycle version token is not scoped to this route");
        }
        var input = java.nio.ByteBuffer.wrap(token);
        int magic = input.getInt();
        byte[] digest = new byte[Sha256Digest.LENGTH];
        input.get(digest);
        if (magic != VERSION_TOKEN_MAGIC || !routeDigest.equals(Sha256Digest.copyOf(digest))) {
            throw new IllegalArgumentException("lifecycle version token belongs to another route");
        }
        byte[] nativeBytes = new byte[Long.BYTES];
        input.get(nativeBytes);
        return new MetadataVersion(CanonicalBytes.copyOf(nativeBytes));
    }

    private static Sha256Digest parseSha(String hex) {
        return Sha256Digest.copyOf(java.util.HexFormat.of().parseHex(hex));
    }

    private void verifyValue(String key, CanonicalBytes value) {
        Objects.requireNonNull(value, "value");
        if (value.isEmpty() || value.length() > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("lifecycle value exceeds its native admission bound");
        }
        if (key.startsWith(historyPrefix)) {
            history.verifyNode(parseSha(key.substring(historyPrefix.length())), value);
            return;
        }
        BindingIdentity observed;
        String actualKey;
        if (key.equals(keys.selector())) {
            observed = M5BindingAuthorityCodecV1.projectSelector(value).binding();
            actualKey = keys.selector();
        } else if (key.equals(keys.proofHead())) {
            observed = M4ReadControlCodecV1.decodeHead(value).binding();
            actualKey = keys.proofHead();
        } else if (key.contains("/capabilities/")) {
            var record = M4ReadControlCodecV1.decodeCapability(value);
            observed = record.binding();
            actualKey = keys.capability(record.generation());
        } else if (key.contains("/terminals/")) {
            var record = M4ReadControlCodecV1.decodeTerminal(value);
            observed = record.binding();
            actualKey = keys.terminal(record.readAdmissionEpoch());
        } else if (key.contains("/proofs/")) {
            var record = M4ReadControlCodecV1.decodeProof(value);
            observed = record.binding();
            actualKey = keys.proof(record.readAdmissionEpoch());
        } else {
            var record = M4ReadControlCodecV1.decodeProtection(value);
            observed = record.binding();
            actualKey = keys.protection(
                    record.identity().sourceIdentitySha256(), record.identity().protectionGeneration());
        }
        if (!binding.equals(observed) || !key.equals(actualKey)) {
            throw new IllegalArgumentException("lifecycle record Binding, incarnation, storage epoch or key differs");
        }
    }

    private void requireSelectorSuccessor(Optional<VersionedValue> predecessor, CanonicalBytes candidate) {
        if (predecessor.isEmpty()) {
            if (M5BindingAuthorityCodecV1.isAuthorityValue(candidate)) {
                var next = M5BindingAuthorityCodecV1.decodeAuthority(candidate);
                if (next.authorityGeneration() != 1
                        || next.predecessorValueSha256().isPresent()
                        || next.retiredHistory().count() != 0) {
                    throw new IllegalArgumentException("lifecycle creation cannot import a prior selector history");
                }
            }
            return;
        }
        var before = predecessor.orElseThrow();
        boolean wasAuthority = M5BindingAuthorityCodecV1.isAuthorityValue(before.canonicalStoredBytes());
        if (!M5BindingAuthorityCodecV1.isAuthorityValue(candidate)) {
            if (wasAuthority) {
                throw new IllegalArgumentException("lifecycle selector cannot downgrade to legacy bytes");
            }
            return;
        }
        var next = M5BindingAuthorityCodecV1.decodeAuthority(candidate);
        long expectedGeneration = 1;
        if (wasAuthority) {
            var previous = M5BindingAuthorityCodecV1.decodeAuthority(before.canonicalStoredBytes());
            expectedGeneration = Math.addExact(previous.authorityGeneration(), 1);
            if (next.lastActivationOrdinal() < previous.lastActivationOrdinal()
                    || next.retiredHistory().count() < previous.retiredHistory().count()
                    || (next.retiredHistory().count()
                                    == previous.retiredHistory().count()
                            && !next.retiredHistory().equals(previous.retiredHistory()))) {
                throw new IllegalArgumentException(
                        "lifecycle selector history cannot roll back or replace a current root");
            }
        }
        if (next.authorityGeneration() != expectedGeneration
                || !next.predecessorValueSha256().equals(Optional.of(before.canonicalStoredSha256()))) {
            throw new IllegalArgumentException("lifecycle selector must name its exact next revision and predecessor");
        }
    }

    private final class RoutedClient implements OxiaConditionalClient {
        private final OxiaConditionalClient nativeClient;

        private RoutedClient(OxiaConditionalClient nativeClient) {
            this.nativeClient = nativeClient;
        }

        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            String qualified = nativeKey(key);
            return nativeClient
                    .read(qualified)
                    .thenApply(observed -> observed.map(value -> {
                        if (!qualified.equals(value.key())) {
                            throw new IllegalStateException("native lifecycle read returned another key");
                        }
                        verifyValue(key, value.storedBytes());
                        return new AuthorityRecord(key, value.storedBytes(), value.versionId());
                    }));
        }

        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes value) {
            return nativeClient.createIfAbsent(nativeKey(key), value);
        }

        public CompletionStage<Void> compareAndSet(String key, CanonicalBytes value, long version) {
            return nativeClient.compareAndSet(nativeKey(key), value, version);
        }
    }

    private final class CanonicalView implements CanonicalControlMetadataStore {
        public Optional<CanonicalBytes> get(String key) {
            return read(key).toCompletableFuture().join().map(VersionedValue::canonicalStoredBytes);
        }

        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
            return compareAndSet(key, Optional.empty(), value);
        }

        public ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
            nativeKey(key);
            verifyValue(key, value);
            Objects.requireNonNull(expected, "expected");
            return read(key)
                    .thenCompose(before -> {
                        if (!before.map(VersionedValue::canonicalStoredBytes).equals(expected)) {
                            return CompletableFuture.completedFuture(
                                    before.map(VersionedValue::canonicalStoredBytes)
                                                    .equals(Optional.of(value))
                                            ? ControlMutationOutcome.APPLIED
                                            : ControlMutationOutcome.DEFINITIVE_CONFLICT);
                        }
                        return OxiaBindingLifecycleMetadataStoreV2.this
                                .compareAndSet(before, key, value)
                                .thenApply(outcome -> switch (outcome) {
                                    case APPLIED_EXACT -> ControlMutationOutcome.APPLIED;
                                    case PREDECESSOR_UNCHANGED, DEFINITIVE_CONFLICT ->
                                        ControlMutationOutcome.DEFINITIVE_CONFLICT;
                                    case RESPONSE_UNKNOWN -> ControlMutationOutcome.RESPONSE_UNKNOWN;
                                });
                    })
                    .exceptionally(failure -> ControlMutationOutcome.RESPONSE_UNKNOWN)
                    .toCompletableFuture()
                    .join();
        }
    }
}
