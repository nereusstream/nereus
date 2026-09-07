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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient.MutationMode;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.QuiescenceProofHead;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BatchMetadataStateV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredSourceRetirementBatchTombstoneV1;
import com.nereusstream.storage.object.retention.M5RetiredBatchHistoryV2;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class OxiaBindingLifecycleMetadataStoreV2Test {
    private static final String ROOT = "/nereus/cells/a";
    private static final BindingIdentity BINDING =
            new BindingIdentity(new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage"));
    private static final CapabilityBinding CAPABILITY = new CapabilityBinding(1, digest("capability"));
    private static final M4ReadControlKeysV1 KEYS = new M4ReadControlKeysV1(7, BINDING);

    @Test
    void legacyM3AndNewControlViewUseOneNativeSelectorAndPreserveExactVersions() {
        var client = new DeterministicOxiaConditionalClient();
        var legacy = new OxiaCanonicalControlMetadataStore(client, ROOT, 7);
        var route = route(client);
        var before = M4ReadControlCodecV1.encodeSelector(selector(BINDING, 1));
        assertThat(legacy.putIfAbsent(KEYS.selector(), before)).isEqualTo(ControlMutationOutcome.APPLIED);
        var exact = route.read(KEYS.selector()).toCompletableFuture().join().orElseThrow();
        assertThat(exact.key()).isEqualTo(KEYS.selector());
        assertThat(route.controlMetadata().get(KEYS.selector())).contains(before);
        var after = M4ReadControlCodecV1.encodeSelector(selector(BINDING, 2));
        assertThat(route.controlMetadata().compareAndSet(KEYS.selector(), Optional.of(before), after))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(legacy.get(KEYS.selector())).get().satisfies(bytes -> {
            assertThat(M5BindingAuthorityCodecV1.isAuthorityValue(bytes)).isTrue();
            assertThat(M5BindingAuthorityCodecV1.projectSelector(bytes)).isEqualTo(selector(BINDING, 2));
        });
        assertThat(client.stored(route.nativeKey(KEYS.selector())))
                .get()
                .extracting(AuthorityRecord::versionId)
                .isEqualTo(1L);
        assertThat(legacy.compareAndSet(KEYS.selector(), Optional.of(before), before))
                .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(route.rawControlMetadata().get(KEYS.selector())).isEqualTo(legacy.get(KEYS.selector()));
        var coordinatorView = new com.nereusstream.storage.object.retention.M5BindingAuthorityControlMetadataStoreV1(
                route.rawControlMetadata(), KEYS.selector());
        var next = M4ReadControlCodecV1.encodeSelector(selector(BINDING, 3));
        assertThat(coordinatorView.compareAndSet(KEYS.selector(), Optional.of(after), next))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(route.controlMetadata().get(KEYS.selector())).contains(next);
    }

    @Test
    void invalidRoutesFailBeforeAnyNativeIoAndExactMaximumFits() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client);
        var history = new M5RetiredBatchHistoryV2(BINDING);
        String node = history.key(digest("node"));
        for (String key : List.of(
                ROOT + "/" + KEYS.selector(),
                "../" + KEYS.selector(),
                KEYS.selector() + "/extra",
                KEYS.selector().replace("0000000007", "0000000008"),
                new M4ReadControlKeysV1(7, foreign()).selector(),
                node.toUpperCase(java.util.Locale.ROOT),
                node.replace(
                        BINDING.storageEpochSha256().toHex(), digest("other").toHex()),
                "v2/object-wal/shards/0000000007/current")) {
            assertThatThrownBy(() -> route.read(key)).isInstanceOf(IllegalArgumentException.class);
        }
        for (String root : List.of("/a//b", "/a/../b", "/a/", "/单元", "a")) {
            assertThatThrownBy(() -> new OxiaBindingLifecycleMetadataStoreV2(client, root, 7, BINDING))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        String maximumRoot = "/" + "a".repeat(512 - 2 - node.length());
        var maximum = new OxiaBindingLifecycleMetadataStoreV2(client, maximumRoot, 7, BINDING);
        assertThat(maximum.nativeKey(node)).hasSize(512);
        assertThatThrownBy(() -> new OxiaBindingLifecycleMetadataStoreV2(client, maximumRoot + "a", 7, BINDING))
                .hasMessageContaining("512-byte");
        assertThat(client.readCount()).isZero();
        assertThat(client.createCount()).isZero();
        assertThat(client.casCount()).isZero();
    }

    @Test
    void recordsMustMatchTheConfiguredIncarnationAndNativeKey() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client);
        var sameIdOtherEpoch = new BindingIdentity(BINDING.bindingId(), BINDING.incarnationSha256(), digest("other"));
        var wrong = M4ReadControlCodecV1.encodeSelector(selector(sameIdOtherEpoch, 1));
        assertThatThrownBy(() -> route.compareAndSet(Optional.empty(), KEYS.selector(), wrong))
                .hasMessageContaining("incarnation");
        var head = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 1, List.of(), List.of()));
        assertThat(route.compareAndSet(Optional.empty(), KEYS.proofHead(), head)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        assertThatThrownBy(() -> route.compareAndSet(Optional.empty(), KEYS.proof(1), head))
                .isInstanceOf(IllegalArgumentException.class);
        client.seed(route.nativeKey(KEYS.selector()), wrong, 4);
        assertThatThrownBy(
                        () -> route.read(KEYS.selector()).toCompletableFuture().join())
                .hasRootCauseMessage("lifecycle record Binding, incarnation, storage epoch or key differs");
    }

    @Test
    void everyHistoryNodeIsCanonicalBindingBoundAndCreateOnlyBeforeSelection() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client);
        var history = new M5RetiredBatchHistoryV2(BINDING);
        var tombstone = tombstone();
        var insertion = history.insert(
                history.emptyRoot(),
                tombstone,
                history.readProof(history.emptyRoot(), tombstone.batchIdSha256(), ignored -> Optional.empty()));
        for (var node : insertion.nodes()) {
            String key = history.key(node.root().sha256());
            assertThat(route.compareAndSet(Optional.empty(), key, node.bytes())
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(MutationOutcome.APPLIED_EXACT);
            assertThat(route.controlMetadata().get(key)).contains(node.bytes());
        }
        assertThat(route.read(KEYS.selector()).toCompletableFuture().join()).isEmpty();
        var node = insertion.nodes().get(0);
        String key = history.key(node.root().sha256());
        var exact = route.read(key).toCompletableFuture().join().orElseThrow();
        int writes = client.createCount() + client.casCount();
        assertThatThrownBy(() -> route.compareAndSet(Optional.of(exact), key, node.bytes()))
                .hasMessageContaining("create-only");
        assertThatThrownBy(() -> route.compareAndSet(Optional.empty(), history.key(digest("wrong")), node.bytes()))
                .hasMessageContaining("content address");
        var foreignHistory = new M5RetiredBatchHistoryV2(foreign());
        assertThatThrownBy(() -> foreignHistory.verifyNode(node.root().sha256(), node.bytes()))
                .hasMessageContaining("Binding");
        assertThat(client.createCount() + client.casCount()).isEqualTo(writes);
    }

    @Test
    void nativeVersionAbaCannotBeHiddenByIdenticalSelectorBytes() {
        var client = new DeterministicOxiaConditionalClient();
        OxiaConditionalClient aba = new OxiaConditionalClient() {
            public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                return client.read(key);
            }

            public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes value) {
                return client.createIfAbsent(key, value);
            }

            public CompletionStage<Void> compareAndSet(String key, CanonicalBytes value, long version) {
                client.seed(key, client.stored(key).orElseThrow().storedBytes(), version + 1);
                return client.compareAndSet(key, value, version);
            }
        };
        var route = route(aba);
        var before = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.initial(selector(BINDING, 1)));
        client.seed(route.nativeKey(KEYS.selector()), before, 4);
        var exact = route.read(KEYS.selector()).toCompletableFuture().join().orElseThrow();
        var candidate = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.selectorSuccessor(
                M5BindingAuthorityCodecV1.decodeAuthority(before), selector(BINDING, 2)));
        assertThat(route.compareAndSet(Optional.of(exact), KEYS.selector(), candidate)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(client.stored(route.nativeKey(KEYS.selector())))
                .get()
                .extracting(AuthorityRecord::storedBytes)
                .isEqualTo(before);
    }

    @Test
    void responseLossReconcilesAndReadFailureNeverMeansAbsence() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client);
        var initial = M4ReadControlCodecV1.encodeSelector(selector(BINDING, 1));
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);
        assertThat(route.controlMetadata().putIfAbsent(KEYS.selector(), initial))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        var candidate = M4ReadControlCodecV1.encodeSelector(selector(BINDING, 2));
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);
        assertThat(route.controlMetadata().compareAndSet(KEYS.selector(), Optional.of(initial), candidate))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        client.failNextRead();
        assertThatThrownBy(() -> route.controlMetadata().get(KEYS.selector()))
                .hasRootCauseMessage("scripted reread failure");
        assertThat(route.controlMetadata().get(KEYS.selector())).contains(candidate);
    }

    @Test
    void aSecondCellCannotObserveOrConsumeTheFirstCellsVersionedValue() {
        var client = new DeterministicOxiaConditionalClient();
        var first = route(client);
        var second = new OxiaBindingLifecycleMetadataStoreV2(client, "/nereus/cells/b", 7, BINDING);
        var bytes = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.initial(selector(BINDING, 1)));
        assertThat(first.compareAndSet(Optional.empty(), KEYS.selector(), bytes)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        assertThat(second.read(KEYS.selector()).toCompletableFuture().join()).isEmpty();
        var before = first.read(KEYS.selector()).toCompletableFuture().join().orElseThrow();
        var candidate = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.selectorSuccessor(
                M5BindingAuthorityCodecV1.decodeAuthority(bytes), selector(BINDING, 2)));
        client.seed(second.nativeKey(KEYS.selector()), bytes, 0);
        assertThatThrownBy(() -> second.compareAndSet(Optional.of(before), KEYS.selector(), candidate))
                .hasMessageContaining("another route");
        assertThat(client.stored(second.nativeKey(KEYS.selector())))
                .get()
                .extracting(AuthorityRecord::storedBytes)
                .isEqualTo(bytes);
    }

    @Test
    void wrappedSelectorCannotDowngradeReplayOrImportHistory() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client);
        var initial =
                M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.initial(selector(BINDING, 1)));
        route.compareAndSet(Optional.empty(), KEYS.selector(), initial)
                .toCompletableFuture()
                .join();
        var before = route.read(KEYS.selector()).toCompletableFuture().join().orElseThrow();
        assertThatThrownBy(() -> route.compareAndSet(
                        Optional.of(before),
                        KEYS.selector(),
                        M4ReadControlCodecV1.encodeSelector(selector(BINDING, 2))))
                .hasMessageContaining("downgrade");
        assertThatThrownBy(() -> route.compareAndSet(Optional.of(before), KEYS.selector(), initial))
                .hasMessageContaining("next revision");
        var successor = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.selectorSuccessor(
                M5BindingAuthorityCodecV1.decodeAuthority(initial), selector(BINDING, 2)));
        assertThatThrownBy(() -> route.compareAndSet(Optional.empty(), KEYS.selector(), successor))
                .hasMessageContaining("import");
        assertThat(client.casCount()).isZero();
    }

    private static OxiaBindingLifecycleMetadataStoreV2 route(OxiaConditionalClient client) {
        return new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, 7, BINDING);
    }

    private static BindingIdentity foreign() {
        return new BindingIdentity(new TopicBindingId(digest("foreign")), digest("incarnation"), digest("storage"));
    }

    private static BindingReadSelector selector(BindingIdentity binding, long generation) {
        return new BindingReadSelector(
                binding,
                digest("view-" + generation),
                1,
                1,
                generation,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                CAPABILITY,
                List.of(),
                List.of());
    }

    private static RetiredSourceRetirementBatchTombstoneV1 tombstone() {
        return M5RetentionCodecV1.finalizeRetiredBatch(new RetiredSourceRetirementBatchTombstoneV1(
                BatchMetadataStateV1.RETIRED_V1,
                BINDING,
                digest("batch"),
                digest("full"),
                digest("proof"),
                new MetadataVersion(CanonicalBytes.copyOf(new byte[] {1})),
                digest("before"),
                CAPABILITY,
                digest("placeholder")));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }
}
