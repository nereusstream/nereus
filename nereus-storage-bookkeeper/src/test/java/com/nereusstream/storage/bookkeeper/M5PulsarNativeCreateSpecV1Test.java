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

package com.nereusstream.storage.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class M5PulsarNativeCreateSpecV1Test {
    @Test
    void legacyBytesRemainExactWhileFactoryLifetimesHaveDistinctScopesInSameNamespace() throws Exception {
        var legacy = spec();
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.writeInt(0x4d355043);
        out.writeInt(1);
        out.writeUTF(legacy.nativeInstanceId());
        for (var value : List.of(legacy.namespaceBinding().encode(), legacy.protocolCell(), legacy.allocatorOwner())) {
            out.writeInt(value.length());
            out.write(value.toByteArray());
        }
        out.write(legacy.providerScope().bytes().toByteArray());
        out.write(legacy.configuration().bytes().toByteArray());
        out.writeInt(legacy.ensemble());
        out.writeInt(legacy.writeQuorum());
        out.writeInt(legacy.ackQuorum());
        out.writeInt(1);
        out.writeUTF(legacy.bindings().getFirst().managedLedgerName());
        out.write(legacy.bindings().getFirst().identity().toByteArray());
        var historical = CanonicalBytes.copyOf(bytes.toByteArray());
        assertThat(legacy.encode()).isEqualTo(historical);
        assertThat(M5PulsarNativeCreateSpecV1.decode(historical)).isEqualTo(legacy);
        var first = legacy.forFactory(new UUID(1, 1));
        var next = legacy.forFactory(new UUID(1, 2));
        assertThat(M5PulsarNativeCreateSpecV1.decode(first.encode())).isEqualTo(first);
        assertThat(first.sha256()).isNotEqualTo(legacy.sha256()).isNotEqualTo(next.sha256());
        assertThat(first.namespaceBinding()).isEqualTo(next.namespaceBinding()).isEqualTo(legacy.namespaceBinding());
        assertThat(first.bindings()).isEqualTo(legacy.bindings());
    }

    @Test
    void factoryWireRejectsMissingOrZeroIdentityAndUnsupportedVersion() {
        var encoded = spec().forFactory(new UUID(1, 1)).encode().toByteArray();
        for (int length = 8; length < 24; length++) {
            var truncated = CanonicalBytes.copyOf(Arrays.copyOf(encoded, length));
            assertThatThrownBy(() -> M5PulsarNativeCreateSpecV1.decode(truncated))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        var zero = encoded.clone();
        Arrays.fill(zero, 8, 24, (byte) 0);
        assertThatThrownBy(() -> M5PulsarNativeCreateSpecV1.decode(CanonicalBytes.copyOf(zero)))
                .isInstanceOf(IllegalArgumentException.class);
        var unsupported = encoded.clone();
        ByteBuffer.wrap(unsupported).putInt(4, 4);
        assertThatThrownBy(() -> M5PulsarNativeCreateSpecV1.decode(CanonicalBytes.copyOf(unsupported)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void priorClosedHeaderMatchesSdkFieldsAcrossCustomRowOrderWithoutLosingCreateToken() throws Exception {
        var first = new java.util.LinkedHashMap<String, byte[]>();
        first.put("application", new byte[] {1});
        first.put("component", new byte[] {2});
        var reversed = new java.util.LinkedHashMap<String, byte[]>();
        reversed.put("component", new byte[] {2});
        reversed.put("application", new byte[] {1});
        var original = metadata(first, 10);
        var serde = new org.apache.bookkeeper.meta.LedgerMetadataSerDe();
        var sdk = serde.parseConfig(serde.serialize(metadata(reversed, 10)), 7, java.util.Optional.of(100L));
        assertThat(M5PulsarNativeCreateSpecV1.sameMetadata(original, sdk)).isTrue();
        assertThat(M5PulsarNativeCreateSpecV1.sameMetadata(original, metadata(reversed, 11)))
                .isFalse();
        reversed.put("component", new byte[] {3});
        assertThat(M5PulsarNativeCreateSpecV1.sameMetadata(original, metadata(reversed, 10)))
                .isFalse();
    }

    private static org.apache.bookkeeper.client.api.LedgerMetadata metadata(
            java.util.Map<String, byte[]> rows, long createToken) {
        return metadata(rows, createToken, 1);
    }

    private static org.apache.bookkeeper.client.api.LedgerMetadata metadata(
            java.util.Map<String, byte[]> rows, long createToken, int ensemble) {
        return metadata(rows, createToken, ensemble, true);
    }

    private static org.apache.bookkeeper.client.api.LedgerMetadata metadata(
            java.util.Map<String, byte[]> rows, long createToken, int ensemble, boolean closed) {
        var builder = org.apache.bookkeeper.client.LedgerMetadataBuilder.create()
                .withId(7)
                .withMetadataFormatVersion(3)
                .withEnsembleSize(ensemble)
                .withWriteQuorumSize(ensemble)
                .withAckQuorumSize(ensemble == 1 ? 1 : 2)
                .withDigestType(org.apache.bookkeeper.client.api.DigestType.CRC32C)
                .withPassword(new byte[0])
                .withCreationTime(100)
                .storingCreationTime(false)
                .withCToken(createToken)
                .withCustomMetadata(rows)
                .newEnsembleEntry(
                        0,
                        java.util.stream.IntStream.range(0, ensemble)
                                .mapToObj(index ->
                                        org.apache.bookkeeper.net.BookieId.parse("localhost:" + (3181 + index)))
                                .toList());
        if (closed) {
            builder.withClosedState().withLastEntryId(2).withLength(50);
        }
        return builder.build();
    }

    @Test
    void nativeDeleteTargetKeepsExactScopeVersionAndSealedMetadataAcrossSdkMapOrder() {
        var scope = spec().forFactory(new UUID(2, 1), new M5PulsarNativeCreateSpecV1.BookKeeperGcGraceV1(0, 1, 25));
        var resource = new com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger(
                scope.namespaceBinding().physicalNamespace(), 7);
        var original = taggedData(scope);
        var target = M5BookKeeperNativeDeleteAuthorityV2.pulsarTarget(
                resource,
                scope,
                new org.apache.bookkeeper.versioning.Versioned<>(
                        original, new org.apache.bookkeeper.versioning.LongVersion(4)));
        var reversed = new java.util.LinkedHashMap<String, byte[]>();
        original.getCustomMetadata().entrySet().stream()
                .sorted(java.util.Map.Entry.<String, byte[]>comparingByKey().reversed())
                .forEach(entry -> reversed.put(entry.getKey(), entry.getValue()));
        var reordered = org.apache.bookkeeper.client.LedgerMetadataBuilder.from(original)
                .withCToken(original.getCToken())
                .withCustomMetadata(reversed)
                .build();
        assertThat(M5BookKeeperNativeDeleteAuthorityV2.pulsarTarget(
                        resource,
                        scope,
                        new org.apache.bookkeeper.versioning.Versioned<>(
                                reordered, new org.apache.bookkeeper.versioning.LongVersion(4))))
                .isEqualTo(target);
        assertThat(target.createScope()).isEqualTo(scope.sha256());
        assertThat(target.lastEntryId()).isEqualTo(2);
        assertThat(target.length()).isEqualTo(50);
        assertThat(M5BookKeeperNativeDeleteAuthorityV2.pulsarTarget(
                        resource,
                        scope,
                        new org.apache.bookkeeper.versioning.Versioned<>(
                                original, new org.apache.bookkeeper.versioning.LongVersion(5))))
                .isNotEqualTo(target);
        var changed = org.apache.bookkeeper.client.LedgerMetadataBuilder.from(original)
                .withCToken(11)
                .build();
        assertThat(M5BookKeeperNativeDeleteAuthorityV2.pulsarTarget(
                                resource,
                                scope,
                                new org.apache.bookkeeper.versioning.Versioned<>(
                                        changed, new org.apache.bookkeeper.versioning.LongVersion(4)))
                        .metadataSha256())
                .isNotEqualTo(target.metadataSha256());
    }

    @Test
    void nativeDeleteTargetRejectsCursorUntaggedUnsealedAndAnotherFactory() {
        var scope = spec().forFactory(new UUID(2, 1), new M5PulsarNativeCreateSpecV1.BookKeeperGcGraceV1(0, 1, 25));
        var resource = new com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger(
                scope.namespaceBinding().physicalNamespace(), 7);
        var original = taggedData(scope);
        var cursorRows = new java.util.HashMap<>(original.getCustomMetadata());
        cursorRows.put("pulsar/cursor", new byte[] {1});
        var untaggedRows = new java.util.HashMap<>(original.getCustomMetadata());
        untaggedRows.remove(M5PulsarNativeCreateSpecV1.SCOPE_KEY);
        untaggedRows.remove(M5PulsarNativeCreateSpecV1.BINDING_KEY);
        for (var invalid : List.of(
                org.apache.bookkeeper.client.LedgerMetadataBuilder.from(original)
                        .withCustomMetadata(cursorRows)
                        .build(),
                org.apache.bookkeeper.client.LedgerMetadataBuilder.from(original)
                        .withCustomMetadata(untaggedRows)
                        .build(),
                metadata(original.getCustomMetadata(), 10, 3, false),
                taggedData(scope.forFactory(new UUID(2, 2))))) {
            assertThatThrownBy(() -> M5BookKeeperNativeDeleteAuthorityV2.pulsarTarget(
                            resource,
                            scope,
                            new org.apache.bookkeeper.versioning.Versioned<>(
                                    invalid, new org.apache.bookkeeper.versioning.LongVersion(4))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void nativeGcV3BindsExplicitGraceWithoutUpgradingHistoricalScopes() {
        var old = spec().forFactory(new UUID(3, 1));
        assertThat(old.bookKeeperGcGrace()).isEmpty();
        assertThat(ByteBuffer.wrap(old.encode().toByteArray()).getInt(4)).isEqualTo(2);
        var policy = new M5PulsarNativeCreateSpecV1.BookKeeperGcGraceV1(0, 1, 25);
        var current = spec().forFactory(new UUID(3, 1), policy);
        assertThat(ByteBuffer.wrap(current.encode().toByteArray()).getInt(4)).isEqualTo(3);
        assertThat(current.sha256()).isNotEqualTo(old.sha256());
        assertThat(M5PulsarNativeCreateSpecV1.decode(current.encode())).isEqualTo(current);
        assertThat(current.forFactory(new UUID(3, 2)).bookKeeperGcGrace()).contains(policy);
        assertThat(current.sealedSourceBindingFor(taggedData(current)))
                .contains(current.bindings().getFirst());
        assertThat(old.bindingFor(taggedData(current), true)).isEmpty();
        assertThat(current.bindingFor(taggedData(old), true)).isEmpty();
        assertThat(spec().forFactory(new UUID(3, 1), new M5PulsarNativeCreateSpecV1.BookKeeperGcGraceV1(0, 1, 26))
                        .sha256())
                .isNotEqualTo(current.sha256());
        for (long[] invalid : List.of(
                new long[] {0, 0, 25},
                new long[] {-1, 1, 25},
                new long[] {26, 1, 25},
                new long[] {0, 26, 25},
                new long[] {0, 1, 0},
                new long[] {0, 1, Long.MAX_VALUE})) {
            assertThatThrownBy(() ->
                            new M5PulsarNativeCreateSpecV1.BookKeeperGcGraceV1(invalid[0], invalid[1], invalid[2]))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        var resource = new com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger(
                old.namespaceBinding().physicalNamespace(), 7);
        assertThatThrownBy(() -> M5BookKeeperNativeDeleteAuthorityV2.pulsarTarget(
                        resource,
                        old,
                        new org.apache.bookkeeper.versioning.Versioned<>(
                                taggedData(old), new org.apache.bookkeeper.versioning.LongVersion(4))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static org.apache.bookkeeper.client.api.LedgerMetadata taggedData(M5PulsarNativeCreateSpecV1 scope) {
        var rows = new java.util.LinkedHashMap<String, byte[]>();
        rows.put("application", "pulsar".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        rows.put("component", "managed-ledger".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        rows.put("pulsar/managed-ledger", "native/ledger".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        rows.put(M5PulsarNativeCreateSpecV1.SCOPE_KEY, scope.sha256().bytes().toByteArray());
        rows.put(
                M5PulsarNativeCreateSpecV1.BINDING_KEY,
                scope.bindings().getFirst().identity().toByteArray());
        return metadata(rows, 10, 3);
    }

    private static M5PulsarNativeCreateSpecV1 spec() {
        String instance = "10203040-0000-0000-0000-000000000001";
        var namespace = new PhysicalNamespaceAuthorityBindingV2(
                M5BookKeeperNativeCreateSpecV2.namespace(instance),
                new MetadataNamespaceIdentityV2(new Id128(1, 1), 7));
        var digest = Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1}));
        var identity = new byte[96];
        for (int offset = 0; offset < identity.length; offset += 32) {
            System.arraycopy(digest.bytes().toByteArray(), 0, identity, offset, 32);
        }
        return new M5PulsarNativeCreateSpecV1(
                instance,
                namespace,
                CanonicalBytes.copyOf(new byte[] {2}),
                CanonicalBytes.copyOf(new byte[] {3}),
                digest,
                digest,
                3,
                3,
                2,
                List.of(new M5PulsarNativeCreateSpecV1.Binding("native/ledger", CanonicalBytes.copyOf(identity))));
    }
}
