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

package com.nereusstream.kafka.bookkeeper.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperControlMetadataStoreV2Test {
    @Test
    void nativeReadScopeRetainsRawWireAndRejectsUnboundGenerationMembership() {
        var state = KafkaBookKeeperCompactionTestSupportV2.recoveryState(false);
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(
                KafkaBookKeeperCompactionTestSupportV2.inputWithRecovery(state, 20), state);
        var digest = KafkaBookKeeperCompactionTestSupportV2.digest("scope-codec-model");
        var changed = KafkaBookKeeperCompactionTestSupportV2.digest("scope-codec-model-after");
        var before = new KafkaBookKeeperNativeReadScopeV2.Snapshot(0, digest, digest, digest);
        var after = new KafkaBookKeeperNativeReadScopeV2.Snapshot(1, changed, changed, changed);
        var owner = new KafkaOwnerIdentityV1(1, 0, 0, 1, 0);
        var fact = new AuthorityFactV1(
                "model/owner-admission-v1", new MetadataVersion(CanonicalBytes.copyOf(new byte[] {1})), digest);
        var raw = new KafkaBookKeeperNativeReadScopeV2(
                fixture.descriptor.task().taskIdSha256(),
                fixture.descriptor.descriptorSha256(),
                digest,
                owner,
                fact,
                1,
                digest,
                digest,
                digest,
                digest,
                before,
                after,
                List.of(1L));
        assertThat(ByteBuffer.wrap(raw.encode().toByteArray()).getInt(4)).isEqualTo(1);
        assertThat(KafkaBookKeeperNativeReadScopeV2.decode(raw.encode())).isEqualTo(raw);
        var generation = new KafkaBookKeeperNativeReadScopeV2(
                raw.taskId(),
                raw.descriptorId(),
                raw.nativeScopeSha256(),
                owner,
                fact,
                1,
                digest,
                digest,
                digest,
                digest,
                before,
                after,
                List.of(1L),
                Optional.of(raw.descriptorId()));
        assertThat(ByteBuffer.wrap(generation.encode().toByteArray()).getInt(4)).isEqualTo(2);
        assertThat(KafkaBookKeeperNativeReadScopeV2.decode(generation.encode())).isEqualTo(generation);
        assertThatThrownBy(() -> generation.requireDescriptor(fixture.descriptor))
                .hasMessage("native read scope lacks its exact selected predecessor descriptor");
        assertThatThrownBy(() -> generation.requireDescriptor(fixture.descriptor, Optional.of(fixture.descriptor)))
                .hasMessage("native read scope selected predecessor membership differs");
        var corrupt = generation.encode().toByteArray();
        corrupt[corrupt.length - 1] ^= 1;
        assertThatThrownBy(() -> KafkaBookKeeperNativeReadScopeV2.decode(CanonicalBytes.copyOf(corrupt)))
                .hasMessage("native read scope checksum is corrupt");
    }

    @Test
    void selectedCacheBudgetKeepsActualTaskBoundsAndRequiresIndependentOwnerIdentity() {
        var state = KafkaBookKeeperCompactionTestSupportV2.recoveryState(false);
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(
                KafkaBookKeeperCompactionTestSupportV2.inputWithRecovery(state, 20), state);
        var store = route(fixture);
        var descriptor = fixture.descriptor;
        var reservation = new KafkaCompactionAdmissionV1.Reservation(
                descriptor.task().taskIdSha256(),
                1_048_576,
                128,
                256,
                256,
                4_194_304,
                4_194_304,
                4_194_304,
                256,
                256,
                0,
                0,
                4096,
                0,
                4096,
                64);
        var budget = new KafkaBookKeeperSelectedCacheBudgetV1(
                descriptor.descriptorSha256(),
                new KafkaSealedBookKeeperReaderV2.DecodingBounds(256, 4_194_304),
                reservation);
        budget.requireDescriptor(descriptor);
        assertThat(budget.encode().length()).isEqualTo(KafkaBookKeeperSelectedCacheBudgetV1.ENCODED_BYTES);
        assertThat(store.putIfAbsent(budget.key(), budget.encode())).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(KafkaBookKeeperSelectedCacheBudgetV1.decode(
                        store.get(budget.key()).orElseThrow()))
                .isEqualTo(budget);
        var coldId = KafkaBookKeeperCompactionTestSupportV2.digest("actual-next-owner-cache-invocation");
        var next = budget.forOwnerCache(coldId);
        assertThatThrownBy(() -> budget.forOwnerCache(reservation.compactionTaskIdSha256()))
                .hasMessageContaining("old Task reservation identity");
        assertThat(next.compactionTaskIdSha256()).isEqualTo(coldId);
        assertThat(KafkaCompactionAdmissionV1.totals(List.of(next)))
                .isEqualTo(KafkaCompactionAdmissionV1.totals(List.of(reservation)));
        assertThatThrownBy(() -> new KafkaBookKeeperSelectedCacheBudgetV1(
                        descriptor.descriptorSha256(),
                        new KafkaSealedBookKeeperReaderV2.DecodingBounds(257, 4_194_304),
                        reservation))
                .hasMessageContaining("decoding exceeds");
        assertThatThrownBy(() -> new KafkaBookKeeperSelectedCacheBudgetV1(
                        descriptor.descriptorSha256(),
                        new KafkaSealedBookKeeperReaderV2.DecodingBounds(256, 4_194_305),
                        reservation))
                .hasMessageContaining("decoding exceeds");
        assertThatThrownBy(() -> store.putIfAbsent(
                        KafkaBookKeeperInventoryV2.taskKey(coldId) + "/cache-budget", budget.encode()))
                .hasMessageContaining("Task key");
        for (int length :
                List.of(0, budget.encode().length() - 1, budget.encode().length() + 1)) {
            assertThatThrownBy(() -> KafkaBookKeeperSelectedCacheBudgetV1.decode(CanonicalBytes.copyOf(
                            java.util.Arrays.copyOf(budget.encode().toByteArray(), length))))
                    .hasMessageContaining("length");
        }
        store.putIfAbsent(
                KafkaBookKeeperCompactionPublicationV2.descriptorKey(descriptor.descriptorSha256()),
                descriptor.encode());
        fixture.store.values.remove(
                KafkaBookKeeperInventoryV2.taskKey(descriptor.task().taskIdSha256()));
        assertThatThrownBy(() -> store.get(budget.key())).hasMessageContaining("immutable task");
    }

    @Test
    void typedTaskPartsDescriptorCandidateAndM4ViewComposeThroughOneStore() {
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(false);
        var store = route(fixture);
        var task = fixture.input.layout().task();
        assertThat(new KafkaBookKeeperInventoryV2(store).register(task)).isTrue();
        for (int ordinal = 0; ordinal < task.parts().size(); ordinal++) {
            assertThat(new KafkaBookKeeperInventoryV2(store).readPart(task, ordinal))
                    .isPresent();
        }
        String descriptor = KafkaBookKeeperCompactionPublicationV2.descriptorKey(fixture.descriptor.descriptorSha256());
        assertThat(store.putIfAbsent(descriptor, fixture.descriptor.encode()))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        String candidate = KafkaBookKeeperCompactionPublicationV2.candidateKey(task.taskIdSha256());
        assertThat(store.putIfAbsent(
                        candidate, fixture.descriptor.descriptorSha256().bytes()))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(store.get(candidate))
                .contains(fixture.descriptor.descriptorSha256().bytes());
        assertThat(store.get(store.selectorKey())).isEqualTo(fixture.store.get(store.selectorKey()));
    }

    @Test
    void originalPublicationTargetsRequireExactTaskOutputsAndCanonicalUnchangedBytes() {
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(false);
        var store = route(fixture);
        var descriptor = fixture.descriptor;
        var input = new com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger(
                descriptor.task().namespace(), 9009);
        var resources = KafkaBookKeeperPublicationTicketsV2.targets(
                descriptor,
                java.util.Map.of(descriptor.sourceCut().sources().get(0).sourceIdentitySha256(), List.of(input)));
        var original = KafkaBookKeeperPublicationTargetsV2.of(descriptor, resources);
        assertThat(store.putIfAbsent(original.key(), original.encode())).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(KafkaBookKeeperPublicationTargetsV2.decode(
                        store.get(original.key()).orElseThrow()))
                .isEqualTo(original);
        byte[] corrupt = original.encode().toByteArray();
        corrupt[corrupt.length - 1] ^= 1;
        assertThatThrownBy(() -> store.get(original.key())
                        .map(value -> KafkaBookKeeperPublicationTargetsV2.decode(CanonicalBytes.copyOf(corrupt))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.putIfAbsent(
                        original.key(),
                        KafkaBookKeeperPublicationTargetsV2.of(descriptor, List.of(input))
                                .encode()))
                .hasMessageContaining("omit an output part");
        assertThatThrownBy(() -> store.compareAndSet(original.key(), Optional.of(original.encode()), original.encode()))
                .hasMessageContaining("immutable create-only");
        var reversed = new java.util.ArrayList<>(resources);
        java.util.Collections.reverse(reversed);
        assertThatThrownBy(() -> KafkaBookKeeperPublicationTargetsV2.of(descriptor, reversed))
                .hasMessageContaining("sorted and unique");
        assertThatThrownBy(() -> KafkaBookKeeperPublicationTargetsV2.decode(CanonicalBytes.copyOf(new byte[1_048_577])))
                .hasMessageContaining("length");
    }

    @Test
    void foreignBindingNamespaceAndProviderScopeFailBeforeWrite() {
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(false);
        var task = fixture.input.layout().task();
        var binding = fixture.input.plan().sourceCut().identity().binding();
        var foreign = new BindingIdentity(
                new TopicBindingId(KafkaBookKeeperCompactionTestSupportV2.digest("foreign")),
                binding.incarnationSha256(),
                binding.storageEpochSha256());
        var namespace = new Namespace(
                task.namespace().providerKind(),
                CanonicalUtf8.fromString("foreign"),
                task.namespace().containerIdentity());
        var scope = new CellProviderScopeId(KafkaBookKeeperCompactionTestSupportV2.digest("foreign"));
        for (var store : List.of(
                new KafkaBookKeeperControlMetadataStoreV2(
                        fixture.store,
                        fixture.store,
                        7,
                        foreign,
                        task.namespace(),
                        task.capability().providerScopeId()),
                new KafkaBookKeeperControlMetadataStoreV2(
                        fixture.store,
                        fixture.store,
                        7,
                        binding,
                        namespace,
                        task.capability().providerScopeId()),
                new KafkaBookKeeperControlMetadataStoreV2(
                        fixture.store, fixture.store, 7, binding, task.namespace(), scope))) {
            int before = fixture.store.operations.size();
            assertThatThrownBy(() -> store.putIfAbsent(
                            KafkaBookKeeperInventoryV2.taskKey(task.taskIdSha256()),
                            KafkaBookKeeperInventoryCodecV2.encodeTask(task)))
                    .hasMessageContaining("route");
            assertThat(fixture.store.operations).hasSize(before);
        }
    }

    @Test
    void wrongContentAddressAndMalformedKeyAreRejectedBeforeBackendIo() {
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(false);
        var store = route(fixture);
        String task =
                KafkaBookKeeperInventoryV2.taskKey(fixture.input.layout().task().taskIdSha256());
        int before = fixture.store.operations.size();
        assertThatThrownBy(() -> store.putIfAbsent(
                        KafkaBookKeeperInventoryV2.taskKey(KafkaBookKeeperCompactionTestSupportV2.digest("wrong")),
                        KafkaBookKeeperInventoryCodecV2.encodeTask(
                                fixture.input.layout().task())))
                .hasMessageContaining("content address");
        for (String key : List.of(task + "/part/256", task + "/part/00", task + "/extra")) {
            assertThatThrownBy(() -> store.get(key)).hasMessageContaining("noncanonical");
        }
        assertThat(fixture.store.operations).hasSize(before);
    }

    @Test
    void orphanPartAndWrongPartKeyCannotBeAdopted() {
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(false);
        var store = route(fixture);
        var task = fixture.input.layout().task();
        String key = KafkaBookKeeperInventoryV2.partKey(task.taskIdSha256(), 0);
        var bytes = fixture.store.get(key).orElseThrow();
        assertThatThrownBy(() -> store.putIfAbsent(KafkaBookKeeperInventoryV2.partKey(task.taskIdSha256(), 1), bytes))
                .hasMessageContaining("part key");
        fixture.store.values.remove(KafkaBookKeeperInventoryV2.taskKey(task.taskIdSha256()));
        assertThatThrownBy(() -> store.get(key)).hasMessageContaining("immutable task");
    }

    @Test
    void candidateRequiresItsExactDescriptorAndTaskAndDetectsCorruptReads() {
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(false);
        var store = route(fixture);
        String candidate = KafkaBookKeeperCompactionPublicationV2.candidateKey(
                fixture.descriptor.task().taskIdSha256());
        var digest = fixture.descriptor.descriptorSha256();
        assertThatThrownBy(() -> store.putIfAbsent(candidate, digest.bytes())).hasMessageContaining("descriptor");
        store.putIfAbsent(KafkaBookKeeperCompactionPublicationV2.descriptorKey(digest), fixture.descriptor.encode());
        assertThatThrownBy(() -> store.putIfAbsent(
                        KafkaBookKeeperCompactionPublicationV2.candidateKey(
                                KafkaBookKeeperCompactionTestSupportV2.digest("wrong")),
                        digest.bytes()))
                .hasMessageContaining("task");
        store.putIfAbsent(candidate, digest.bytes());
        fixture.store.values.put(candidate, CanonicalBytes.copyOf(new byte[] {1}));
        assertThatThrownBy(() -> store.get(candidate)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void immutableMutationCannotOverwriteARecordedAllocation() {
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(false);
        var store = route(fixture);
        String key =
                KafkaBookKeeperInventoryV2.partKey(fixture.descriptor.task().taskIdSha256(), 0);
        var value = store.get(key).orElseThrow();
        int before = fixture.store.operations.size();
        assertThatThrownBy(() -> store.compareAndSet(key, Optional.of(value), value))
                .hasMessageContaining("create-only");
        assertThat(fixture.store.operations).hasSize(before);
    }

    private static KafkaBookKeeperControlMetadataStoreV2 route(KafkaSealedBookKeeperDescriptorV2Test.Fixture fixture) {
        var task = fixture.input.layout().task();
        return new KafkaBookKeeperControlMetadataStoreV2(
                fixture.store,
                fixture.store,
                7,
                fixture.input.plan().sourceCut().identity().binding(),
                task.namespace(),
                task.capability().providerScopeId());
    }
}
