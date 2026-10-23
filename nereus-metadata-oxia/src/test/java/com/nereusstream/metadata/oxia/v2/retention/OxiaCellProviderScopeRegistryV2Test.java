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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/** Declared values and metadata component only; none of these fixtures qualifies a Native Provider or Task. */
class OxiaCellProviderScopeRegistryV2Test {
    private static final PhysicalNamespaceAuthorityBindingV2 BINDING = new PhysicalNamespaceAuthorityBindingV2(
            new PhysicalResourceIdV2.Namespace(
                    PhysicalResourceIdV2.ProviderKind.OBJECT_PROVIDER,
                    text("declared-provider"),
                    text("declared-container")),
            new MetadataNamespaceIdentityV2(new Id128(1, 2), 0));

    @Test
    void profileWireMatchesIndependentFieldsAndIdentityIncludesAllStableReferences() throws Exception {
        var profile = profile(1, "cell-a", "operator-a");
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(0x4d354350);
            out.writeInt(2);
            write(out, BINDING.encode());
            write(out, ProtocolCellIdentityCodecV1.encode(profile.protocolCell()));
            for (var value : List.of(
                    profile.exclusiveNamespacePrefix(),
                    profile.references().securityScope(),
                    profile.references().credentialScope(),
                    profile.references().allowedEncryptionScope(),
                    profile.references().quotaAdmissionScope(),
                    profile.references().operatorOwner(),
                    profile.references().deleteCapabilityScope(),
                    profile.adapterVersion())) {
                write(out, text(value).bytes());
            }
            out.write(profile.providerConfigurationSha256().bytes().toByteArray());
            out.writeLong(64L * 1024 * 1024);
            out.writeInt(65536);
            out.writeInt(1000);
            out.writeInt(1024);
        }
        var golden = CanonicalBytes.copyOf(bytes.toByteArray());
        assertThat(profile.encode()).isEqualTo(golden);
        assertThat(M5CellProviderProfileV2.decode(golden)).isEqualTo(profile);
        assertThat(profile(1, "cell-a", "operator-b").scopeId()).isNotEqualTo(profile.scopeId());
        assertThat(profile(2, "cell-b", "operator-a").scopeId()).isNotEqualTo(profile.scopeId());
        assertThatThrownBy(() -> M5CellProviderProfileV2.decode(
                        CanonicalBytes.copyOf(Arrays.copyOf(golden.toByteArray(), golden.length() - 1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> M5CellProviderProfileV2.decode(
                        CanonicalBytes.copyOf(Arrays.copyOf(golden.toByteArray(), golden.length() + 1))))
                .hasMessageContaining("canonical");
    }

    @Test
    void sameRootFreezesEachCellAndRejectsEqualOrNestedPrefixesBeforeMutation() {
        var client = new DeterministicOxiaConditionalClient();
        var registry = registry(client);
        var a = profile(1, "shared/a", "operator-a");
        var admission = registry.prepare(a).admit().join();
        assertThat(registry.key()).startsWith(BINDING.authorityRoot() + "/");
        int creates = client.createCount();
        assertThat(registry(client).prepare(a).admit().join().profile()).isEqualTo(a);
        assertThat(client.createCount()).isEqualTo(creates);
        for (String prefix : List.of("shared/a", "shared/a/child", "shared")) {
            var rejected = registry.prepare(profile(2, prefix, "operator-b"));
            assertThatThrownBy(() -> rejected.admit().join())
                    .hasRootCauseMessage("Cell Provider catalogue has duplicate Cell or overlapping prefix");
            assertThat(rejected.isSettledForClose()).isTrue();
        }
        assertThat(registry.prepare(profile(2, "shared/b", "operator-b"))
                        .admit()
                        .join()
                        .profile()
                        .protocolCell())
                .isEqualTo(profile(2, "shared/b", "operator-b").protocolCell());
        admission.requireCurrent().join();
        assertThatThrownBy(() -> registry.prepare(profile(1, "shared/a", "operator-rotated"))
                        .admit()
                        .join())
                .hasRootCauseMessage("Cell Provider profile is already frozen with different fields");
        assertThat(OxiaCellProviderScopeRegistryV2.Catalogue.decode(
                                client.stored(registry.key()).orElseThrow().storedBytes())
                        .profiles())
                .hasSize(2);
    }

    @Test
    void originalUnknownCannotCloseUntilExactSameProfileIsObservedAfterReconnect() {
        var client = new ResponseLossClient();
        var registry = registry(client);
        var requested = profile(1, "cell-a", "operator-a");
        var original = registry.prepare(requested);
        client.loseAppliedReply = true;
        assertThatThrownBy(() -> original.admit().join()).hasRootCauseMessage("declared scope observation unknown");
        assertThat(original.isSettledForClose()).isFalse();
        assertThatThrownBy(() -> original.reconcile().join()).hasRootCauseMessage("declared scope observation unknown");
        assertThat(original.isSettledForClose()).isFalse();
        assertThat(client.delegate.createCount()).isEqualTo(1);
        client.blockReads = false;
        assertThat(original.reconcile().join().profile()).isEqualTo(requested);
        assertThat(original.isSettledForClose()).isTrue();
        assertThat(registry(client).prepare(requested).admit().join().profile()).isEqualTo(requested);
        assertThat(client.delegate.createCount()).isEqualTo(1);
    }

    @Test
    void profileAndCatalogueCapsRejectBeforeUnboundedAllocationOrMetadataDispatch() {
        assertThatThrownBy(() -> profile(1, "cell/../foreign", "operator-a")).hasMessageContaining("non-canonical");
        var profile = profile(1, "cell-a", "operator-a");
        var body = new OxiaCellProviderScopeRegistryV2.Catalogue(1, List.of(profile))
                .encode()
                .toByteArray();
        java.nio.ByteBuffer.wrap(body).putInt(16, OxiaCellProviderScopeRegistryV2.MAX_SCOPES + 1);
        assertThatThrownBy(() -> OxiaCellProviderScopeRegistryV2.Catalogue.decode(CanonicalBytes.copyOf(body)))
                .hasMessageContaining("count exceeds");
        var full = new java.util.ArrayList<M5CellProviderProfileV2>();
        for (int i = 1; i <= OxiaCellProviderScopeRegistryV2.MAX_SCOPES; i++) {
            full.add(profile(i, "cells/c" + i, "operator-a"));
        }
        full.sort(java.util.Comparator.comparing(value -> java.util.HexFormat.of()
                .formatHex(
                        ProtocolCellIdentityCodecV1.encode(value.protocolCell()).toByteArray())));
        var client = new DeterministicOxiaConditionalClient();
        var registry = registry(client);
        client.seed(registry.key(), new OxiaCellProviderScopeRegistryV2.Catalogue(1, full).encode(), 4);
        assertThatThrownBy(() -> registry.prepare(profile(257, "cells/c257", "operator-a"))
                        .admit()
                        .join())
                .hasRootCauseMessage("Cell Provider catalogue revision/count/order differs");
        assertThat(client.casCount()).isZero();
    }

    @Test
    void gcPolicyHasDistinctWireIdentityAndCannotBeAddedOnReadOnlyReopen() {
        var legacy = profile(1, "cell-a", "operator-a");
        var policy = new M5CellProviderProfileV2.GcGracePolicyV1(5, 9, 10);
        var configured = new M5CellProviderProfileV2(
                legacy.physicalBinding(),
                legacy.protocolCell(),
                legacy.exclusiveNamespacePrefix(),
                legacy.references(),
                legacy.adapterVersion(),
                legacy.providerConfigurationSha256(),
                legacy.maximumObjectBytes(),
                legacy.maximumRangeBytes(),
                legacy.maximumListPageKeys(),
                legacy.maximumVersionTokenBytes(),
                Optional.of(policy));
        assertThat(M5CellProviderProfileV2.decode(configured.encode())).isEqualTo(configured);
        assertThat(java.nio.ByteBuffer.wrap(configured.encode().toByteArray()).getInt(4))
                .isEqualTo(3);
        assertThat(legacy.gcGracePolicy()).isEmpty();
        assertThat(configured.scopeId()).isNotEqualTo(legacy.scopeId());
        var client = new DeterministicOxiaConditionalClient();
        var registry = registry(client);
        assertThatThrownBy(() -> registry.reopen(configured).join())
                .hasRootCauseMessage("Original Cell Provider scope is absent or differs");
        assertThat(client.casCount()).isZero();
        registry.prepare(configured).admit().join();
        int mutations = client.casCount();
        var reopened = registry.reopen(configured).join();
        assertThat(reopened.profile()).isEqualTo(configured);
        assertThat(reopened.captureAuthority().join().key()).isEqualTo(registry.key());
        assertThatThrownBy(() -> registry.reopen(legacy).join())
                .hasRootCauseMessage("Original Cell Provider scope is absent or differs");
        assertThat(client.casCount()).isEqualTo(mutations);
        assertThatThrownBy(() -> M5CellProviderProfileV2.decode(CanonicalBytes.copyOf(Arrays.copyOf(
                        configured.encode().toByteArray(), configured.encode().length() - 1))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void gcPolicyRejectsUnknownInvalidOrShortDuration() {
        assertThatThrownBy(() -> new M5CellProviderProfileV2.GcGracePolicyV1(5, 9, 8))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new M5CellProviderProfileV2.GcGracePolicyV1(-1, 0, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new M5CellProviderProfileV2.GcGracePolicyV1(0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new M5CellProviderProfileV2.GcGracePolicyV1(0, 0, Long.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static OxiaCellProviderScopeRegistryV2 registry(OxiaConditionalClient client) {
        return new OxiaCellProviderScopeRegistryV2(new Oxia09ExactMetadataTransactionStoreV1(client), BINDING);
    }

    private static M5CellProviderProfileV2 profile(long cell, String prefix, String operator) {
        return new M5CellProviderProfileV2(
                BINDING,
                new PulsarProtocolCellIdentity(
                        new DeploymentId(new Id128(3, 4)),
                        new ReservationDomainId(new Id128(5, 6)),
                        new PulsarCellId(new Id128(7, cell))),
                prefix,
                new M5CellProviderProfileV2.References(
                        "security-ref", "credential-ref", "NONE", "quota-ref", operator, "delete-ref"),
                "declared-adapter",
                Sha256Digest.hash(text("declared-config").bytes()),
                64L * 1024 * 1024,
                65536,
                1000,
                1024);
    }

    private static CanonicalUtf8 text(String value) {
        return CanonicalUtf8.fromString(value);
    }

    private static void write(DataOutputStream out, CanonicalBytes bytes) throws Exception {
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }

    private static final class ResponseLossClient implements OxiaConditionalClient {
        private final DeterministicOxiaConditionalClient delegate = new DeterministicOxiaConditionalClient();
        private boolean loseAppliedReply;
        private boolean blockReads;

        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            return blockReads
                    ? CompletableFuture.failedFuture(new IllegalStateException("declared scope observation unknown"))
                    : delegate.read(key);
        }

        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes bytes) {
            return delegate.createIfAbsent(key, bytes).thenCompose(ignored -> {
                if (loseAppliedReply) {
                    loseAppliedReply = false;
                    blockReads = true;
                    return CompletableFuture.failedFuture(new IllegalStateException("declared applied reply lost"));
                }
                return CompletableFuture.completedFuture(null);
            });
        }

        public CompletionStage<Void> compareAndSet(String key, CanonicalBytes bytes, long version) {
            return delegate.compareAndSet(key, bytes, version);
        }
    }
}
