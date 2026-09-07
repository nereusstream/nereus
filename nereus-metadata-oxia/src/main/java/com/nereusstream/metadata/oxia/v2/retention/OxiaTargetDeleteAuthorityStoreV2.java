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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteDoneV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

/** Configured physical-namespace authority route with permanent terminal validation and scoped native CAS versions. */
public final class OxiaTargetDeleteAuthorityStoreV2 implements ExactMetadataTransactionStoreV1 {
    public static final int MAX_NATIVE_KEY_BYTES = 512;
    private static final int VERSION_MAGIC = 0x4d354432; // M5D2
    private static final Pattern AUTHORITY_KEY = Pattern.compile("v2/physical-delete-m5-v2/[0-9a-f]{64}/authority-v2");
    private final String root;
    private final PhysicalResourceIdV2.Namespace namespace;
    private final Sha256Digest routeDigest;
    private final ExactMetadataTransactionStoreV1 facts;
    private final Oxia09ExactMetadataTransactionStoreV1 exact;

    /** Native namespace-to-route uniqueness and proof-fact admission must be established by the composition owner. */
    public OxiaTargetDeleteAuthorityStoreV2(
            OxiaConditionalClient client,
            String root,
            PhysicalResourceIdV2.Namespace namespace,
            ExactMetadataTransactionStoreV1 authoritativeFacts) {
        Objects.requireNonNull(client, "client");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.facts = Objects.requireNonNull(authoritativeFacts, "authoritativeFacts");
        this.root = Objects.requireNonNull(root, "root");
        if (!root.matches("/[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*")
                || root.length() + 1 + ("v2/physical-delete-m5-v2/" + "0".repeat(64) + "/authority-v2").length()
                        > MAX_NATIVE_KEY_BYTES) {
            throw new IllegalArgumentException(
                    "physical authority root is noncanonical or exceeds native 512-byte cap");
        }
        var rootBytes = CanonicalUtf8.fromString(root).bytes();
        var service = namespace.serviceIdentity().bytes();
        var container = namespace.containerIdentity().bytes();
        routeDigest = Sha256Digest.hash(CanonicalBytes.copyOf(
                ByteBuffer.allocate(16 + rootBytes.length() + service.length() + container.length())
                        .putInt(namespace.providerKind().ordinal())
                        .putInt(rootBytes.length())
                        .put(rootBytes.toByteArray())
                        .putInt(service.length())
                        .put(service.toByteArray())
                        .putInt(container.length())
                        .put(container.toByteArray())
                        .array()));
        exact = new Oxia09ExactMetadataTransactionStoreV1(new OxiaConditionalClient() {
            public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                return client.read(nativeKey(key))
                        .thenApply(value -> value.map(record -> {
                            if (!record.key().equals(nativeKey(key))) {
                                throw new IllegalArgumentException(
                                        "native physical authority read returned another key");
                            }
                            verify(key, record.storedBytes());
                            return new AuthorityRecord(key, record.storedBytes(), record.versionId());
                        }));
            }

            public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes bytes) {
                return client.createIfAbsent(nativeKey(key), bytes);
            }

            public CompletionStage<Void> compareAndSet(String key, CanonicalBytes bytes, long version) {
                return client.compareAndSet(nativeKey(key), bytes, version);
            }
        });
    }

    public String nativeKey(String key) {
        if (!AUTHORITY_KEY.matcher(Objects.requireNonNull(key, "key")).matches()) {
            throw new IllegalArgumentException("key is outside the physical delete authority family");
        }
        return root + "/" + key;
    }

    @Override
    public CompletionStage<Optional<VersionedValue>> read(String key) {
        Objects.requireNonNull(key, "key");
        if (!key.startsWith("v2/physical-delete-m5-v2/")) {
            return facts.read(key);
        }
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
        verify(key, candidate);
        var current = predecessor.map(value -> {
            if (!value.key().equals(key)) {
                throw new IllegalArgumentException("physical authority predecessor key differs");
            }
            verify(key, value.canonicalStoredBytes());
            return M5TargetDeleteStoredValueV2.decode(value);
        });
        requireTransition(current, candidate);
        return exact.compareAndSet(
                predecessor.map(value ->
                        VersionedValue.of(key, value.canonicalStoredBytes(), nativeVersion(value.metadataVersion()))),
                key,
                candidate);
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

    private void verify(String key, CanonicalBytes bytes) {
        if (bytes.isEmpty() || bytes.length() > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("physical authority native value bound differs");
        }
        var resource = M5TargetDeleteDoneV2.isCompactDone(bytes)
                ? M5TargetDeleteDoneV2.decode(bytes).resource()
                : M5TargetDeleteAuthorityCodecV1.decodeAuthority(bytes).target().resourceId();
        if (!resource.namespace().equals(namespace) || !resource.authorityKey().equals(key)) {
            throw new IllegalArgumentException("physical authority namespace or canonical resource key differs");
        }
    }

    private static void requireTransition(Optional<M5TargetDeleteStoredValueV2> predecessor, CanonicalBytes candidate) {
        boolean compact = M5TargetDeleteDoneV2.isCompactDone(candidate);
        if (predecessor.isEmpty()) {
            if (compact) {
                throw new IllegalArgumentException("compact done cannot be imported as a new authority");
            }
            var initial = M5TargetDeleteAuthorityCodecV1.decodeAuthority(candidate);
            if (initial.state() != TargetDeleteAuthorityStateV1.OPEN_V1
                    || initial.authorityRevision() != 1
                    || initial.predecessorAuthoritySha256().isPresent()) {
                throw new IllegalArgumentException("new native authority requires revision-one OPEN");
            }
            return;
        }
        var current = predecessor.orElseThrow();
        if (current.compactDone().isPresent()) {
            throw new IllegalArgumentException("permanent compact done has no successor");
        }
        var full = current.fullAuthority().orElseThrow();
        if (full.state() == TargetDeleteAuthorityStateV1.DELETE_DONE_V1) {
            if (!compact || !M5TargetDeleteDoneV2.from(full).encode().equals(candidate)) {
                throw new IllegalArgumentException("full done only admits its exact permanent compaction");
            }
            return;
        }
        if (compact) {
            throw new IllegalArgumentException("active authority cannot skip full done before compaction");
        }
        var next = M5TargetDeleteAuthorityCodecV1.decodeAuthority(candidate);
        if (!next.target().equals(full.target())
                || next.authorityRevision() != Math.addExact(full.authorityRevision(), 1)
                || !next.predecessorAuthoritySha256()
                        .equals(Optional.of(current.exactStoredValue().canonicalStoredSha256()))) {
            throw new IllegalArgumentException("physical authority must preserve resource and exact revision chain");
        }
        boolean allowed =
                switch (full.state()) {
                    case OPEN_V1 ->
                        next.state() == TargetDeleteAuthorityStateV1.OPEN_V1
                                || next.state() == TargetDeleteAuthorityStateV1.READ_FENCED_V1;
                    case READ_FENCED_V1 ->
                        next.state() == TargetDeleteAuthorityStateV1.READ_FENCED_V1
                                || next.state() == TargetDeleteAuthorityStateV1.DELETE_INTENT_V1;
                    case DELETE_INTENT_V1 ->
                        next.state() == TargetDeleteAuthorityStateV1.DELETE_INTENT_V1
                                || next.state() == TargetDeleteAuthorityStateV1.DELETE_DONE_V1;
                    case DELETE_DONE_V1 -> false;
                };
        if (!allowed) {
            throw new IllegalArgumentException("native physical authority cannot skip or reopen a deletion phase");
        }
    }

    private MetadataVersion scopedVersion(MetadataVersion version) {
        if (version.value().length() != Long.BYTES) {
            throw new IllegalStateException("native physical authority version format differs");
        }
        return new MetadataVersion(CanonicalBytes.copyOf(ByteBuffer.allocate(44)
                .putInt(VERSION_MAGIC)
                .put(routeDigest.bytes().toByteArray())
                .put(version.value().toByteArray())
                .array()));
    }

    private MetadataVersion nativeVersion(MetadataVersion version) {
        if (version.value().length() != 44) {
            throw new IllegalArgumentException("physical authority version lacks its exact native route");
        }
        var in = ByteBuffer.wrap(version.value().toByteArray());
        int magic = in.getInt();
        var digest = new byte[32];
        in.get(digest);
        if (magic != VERSION_MAGIC || !routeDigest.equals(Sha256Digest.copyOf(digest))) {
            throw new IllegalArgumentException("physical authority version belongs to another namespace or route");
        }
        var nativeBytes = new byte[8];
        in.get(nativeBytes);
        return new MetadataVersion(CanonicalBytes.copyOf(nativeBytes));
    }
}
