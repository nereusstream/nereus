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
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteDoneV2;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class OxiaTargetDeleteAuthorityStoreV2Test {
    @Test
    void exactNativeLifecycleAndDoneCompactionAreOneKeyAndCannotReopen() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client, "/nereus/delete/a");
        var resource = SyntheticDeleteAuthorityFixturesV2.resource(1);
        Optional<ExactMetadataTransactionStoreV1.VersionedValue> current = Optional.empty();
        var phases = SyntheticDeleteAuthorityFixturesV2.phases(resource);
        for (var phase : phases) {
            assertThat(route.compareAndSet(
                                    current,
                                    resource.authorityKey(),
                                    M5TargetDeleteAuthorityCodecV1.encodeAuthority(phase))
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.APPLIED_EXACT);
            current = route.read(resource.authorityKey()).toCompletableFuture().join();
        }
        var compact = M5TargetDeleteDoneV2.from(phases.get(3));
        assertThat(route.compareAndSet(current, resource.authorityKey(), compact.encode())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.APPLIED_EXACT);
        var done = route.read(resource.authorityKey()).toCompletableFuture().join();
        int cas = client.casCount();
        for (var candidate :
                new CanonicalBytes[] {compact.encode(), M5TargetDeleteAuthorityCodecV1.encodeAuthority(phases.get(0))
                }) {
            assertThatThrownBy(() -> route.compareAndSet(done, resource.authorityKey(), candidate))
                    .hasMessageContaining("no successor");
        }
        assertThat(client.casCount()).isEqualTo(cas);
        assertThat(route.compareAndSet(
                                Optional.empty(),
                                resource.authorityKey(),
                                M5TargetDeleteAuthorityCodecV1.encodeAuthority(phases.get(0)))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.DEFINITIVE_CONFLICT);
    }

    @Test
    void compactImportSkippedPhasesAndWrongResourceFailBeforeNativeWrites() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client, "/nereus/delete/a");
        var resource = SyntheticDeleteAuthorityFixturesV2.resource(1);
        var phases = SyntheticDeleteAuthorityFixturesV2.phases(resource);
        var compact = M5TargetDeleteDoneV2.from(phases.get(3));
        assertThatThrownBy(() -> route.compareAndSet(Optional.empty(), resource.authorityKey(), compact.encode()))
                .hasMessageContaining("cannot be imported");
        assertThatThrownBy(() -> route.compareAndSet(
                        Optional.empty(),
                        resource.authorityKey(),
                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(phases.get(3))))
                .hasMessageContaining("revision-one OPEN");
        assertThatThrownBy(() -> route.compareAndSet(
                        Optional.empty(),
                        SyntheticDeleteAuthorityFixturesV2.resource(2).authorityKey(),
                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(phases.get(0))))
                .hasMessageContaining("canonical resource key");
        assertThat(client.createCount()).isZero();
        route.compareAndSet(
                        Optional.empty(),
                        resource.authorityKey(),
                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(phases.get(0)))
                .toCompletableFuture()
                .join();
        var open = route.read(resource.authorityKey()).toCompletableFuture().join();
        assertThatThrownBy(() -> route.compareAndSet(open, resource.authorityKey(), compact.encode()))
                .hasMessageContaining("skip full done");
        assertThatThrownBy(() -> route.compareAndSet(
                        open, resource.authorityKey(), M5TargetDeleteAuthorityCodecV1.encodeAuthority(phases.get(2))))
                .hasMessageContaining("revision chain");
        assertThat(client.casCount()).isZero();
    }

    @Test
    void namespaceRootVersionAndNativeKeyBoundsAreExplicit() {
        var client = new DeterministicOxiaConditionalClient();
        var first = route(client, "/nereus/delete/a");
        var otherRoot = route(client, "/nereus/delete/b");
        var resource = SyntheticDeleteAuthorityFixturesV2.resource(1);
        var phases = SyntheticDeleteAuthorityFixturesV2.phases(resource);
        var open = M5TargetDeleteAuthorityCodecV1.encodeAuthority(phases.get(0));
        first.compareAndSet(Optional.empty(), resource.authorityKey(), open)
                .toCompletableFuture()
                .join();
        var exact = first.read(resource.authorityKey()).toCompletableFuture().join();
        int writes = client.casCount();
        assertThatThrownBy(() -> otherRoot.compareAndSet(
                        exact, resource.authorityKey(), M5TargetDeleteAuthorityCodecV1.encodeAuthority(phases.get(1))))
                .hasMessageContaining("another namespace or route");
        var ns = resource.namespace();
        var foreign = new PhysicalResourceIdV2.Namespace(
                ns.providerKind(), CanonicalUtf8.fromString("foreign"), ns.containerIdentity());
        var foreignRoute = new OxiaTargetDeleteAuthorityStoreV2(
                client, "/nereus/delete/a", foreign, new Oxia09ExactMetadataTransactionStoreV1(client));
        assertThatThrownBy(() -> foreignRoute
                        .read(resource.authorityKey())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("physical authority namespace or canonical resource key differs");
        String maximum = "/" + "x".repeat(512 - 2 - resource.authorityKey().length());
        assertThat(route(client, maximum).nativeKey(resource.authorityKey())).hasSize(512);
        assertThatThrownBy(() -> route(client, maximum + "x")).hasMessageContaining("512-byte");
        assertThat(client.casCount()).isEqualTo(writes);
    }

    @Test
    void changedDoneProofCannotReplaceItsExactFullDonePredecessor() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client, "/nereus/delete/a");
        var phases = SyntheticDeleteAuthorityFixturesV2.phases(SyntheticDeleteAuthorityFixturesV2.resource(1));
        var full = phases.get(3);
        client.seed(route.nativeKey(full.authorityKey()), M5TargetDeleteAuthorityCodecV1.encodeAuthority(full), 3);
        var exact = route.read(full.authorityKey()).toCompletableFuture().join();
        var compact = M5TargetDeleteDoneV2.from(full);
        var changed = new M5TargetDeleteDoneV2(
                compact.resource(),
                compact.authorityRevision(),
                compact.fullDoneAuthoritySha256(),
                compact.closedWriterFenceEpoch(),
                compact.eligibilitySha256(),
                SyntheticDeleteAuthorityFixturesV2.digest("wrong capability"),
                compact.finalDispatchTokenSha256(),
                compact.externalIdentitySha256(),
                compact.done());
        assertThatThrownBy(() -> route.compareAndSet(exact, full.authorityKey(), changed.encode()))
                .hasMessageContaining("exact permanent compaction");
        assertThat(client.casCount()).isZero();
    }

    private static OxiaTargetDeleteAuthorityStoreV2 route(DeterministicOxiaConditionalClient client, String root) {
        return new OxiaTargetDeleteAuthorityStoreV2(
                client,
                root,
                SyntheticDeleteAuthorityFixturesV2.resource(0).namespace(),
                new Oxia09ExactMetadataTransactionStoreV1(client));
    }
}
