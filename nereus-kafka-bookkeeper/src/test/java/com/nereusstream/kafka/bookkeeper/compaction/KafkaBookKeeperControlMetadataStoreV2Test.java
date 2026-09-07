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
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperControlMetadataStoreV2Test {
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
