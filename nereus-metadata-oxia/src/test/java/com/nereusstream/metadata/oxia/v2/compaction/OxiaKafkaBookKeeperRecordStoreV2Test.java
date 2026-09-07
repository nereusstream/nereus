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

package com.nereusstream.metadata.oxia.v2.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient.MutationMode;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class OxiaKafkaBookKeeperRecordStoreV2Test {
    private static final String PREFIX = "v2/kafka-bk-compaction-v2/";
    private static final CanonicalBytes VALUE = CanonicalBytes.copyOf(new byte[] {1, 2, 3});
    private static final String TASK = PREFIX + Sha256Digest.hash(VALUE).toHex() + "/task";

    @Test
    void exactNativeKeyMaximumAndClosedFamiliesRejectBeforeIo() {
        var client = new DeterministicOxiaConditionalClient();
        String descriptor = PREFIX + "views/" + "1".repeat(64) + "/descriptor";
        String root = "/" + "r".repeat(510 - descriptor.length());
        var store = new OxiaKafkaBookKeeperRecordStoreV2(client, root);
        assertThat(store.nativeKey(descriptor)).hasSize(512);
        assertThatThrownBy(() -> new OxiaKafkaBookKeeperRecordStoreV2(client, root + "r"))
                .hasMessageContaining("capacity");
        for (String bad : List.of(
                TASK + "/part/256",
                TASK + "/part/01",
                TASK + "/part/-1",
                root + "/" + TASK,
                TASK + "/unknown",
                "v2/object-wal/shards/0000000007/current")) {
            assertThatThrownBy(() -> store.get(bad)).isInstanceOf(IllegalArgumentException.class);
        }
        for (String bad : List.of("/a//b", "/a/../b", "/a/", "/中文", "a")) {
            assertThatThrownBy(() -> new OxiaKafkaBookKeeperRecordStoreV2(client, bad))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(client.readCount() + client.createCount() + client.casCount())
                .isZero();
    }

    @Test
    void taskAndDescriptorContentAddressesAndCandidateWidthsAreRequiredBeforeCreate() {
        var client = new DeterministicOxiaConditionalClient();
        var store = new OxiaKafkaBookKeeperRecordStoreV2(client, "/cell/a");
        assertThat(store.putIfAbsent(TASK, VALUE)).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(store.get(TASK)).contains(VALUE);
        for (String key : List.of(
                PREFIX + "1".repeat(64) + "/task",
                PREFIX + "views/" + "1".repeat(64) + "/descriptor",
                TASK + "/candidate")) {
            assertThatThrownBy(() -> store.putIfAbsent(key, VALUE)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> store.compareAndSet(TASK, Optional.of(VALUE), VALUE))
                .hasMessageContaining("create-only");
        assertThat(client.createCount()).isEqualTo(1);
        assertThat(client.casCount()).isZero();
    }

    @Test
    void lostCreateResponseReconcilesExactBytesAndConflictNeverOverwrites() {
        var client = new DeterministicOxiaConditionalClient();
        var store = new OxiaKafkaBookKeeperRecordStoreV2(client, "/cell/a");
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);
        assertThat(store.putIfAbsent(TASK, VALUE)).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(store.putIfAbsent(TASK, VALUE)).isEqualTo(ControlMutationOutcome.APPLIED);
        String part = TASK + "/part/0";
        assertThat(store.putIfAbsent(part, VALUE)).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(store.putIfAbsent(part, CanonicalBytes.copyOf(new byte[] {4})))
                .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(store.get(part)).contains(VALUE);
        assertThat(client.casCount()).isZero();
    }

    @Test
    void missingDeliveryAndCorruptNativeValuesNeverBecomeAuthoritativeAbsence() {
        var client = new DeterministicOxiaConditionalClient();
        var store = new OxiaKafkaBookKeeperRecordStoreV2(client, "/cell/a");
        client.failNextRead();
        assertThatThrownBy(() -> store.get(TASK)).hasRootCauseMessage("scripted reread failure");
        client.seed(store.nativeKey(TASK), CanonicalBytes.copyOf(new byte[] {4}), 2);
        assertThatThrownBy(() -> store.get(TASK)).hasMessageContaining("content address");
        assertThat(store.putIfAbsent(TASK, VALUE)).isEqualTo(ControlMutationOutcome.RESPONSE_UNKNOWN);
        assertThat(client.casCount()).isZero();
    }

    @Test
    void sameRelativeRecordInAnotherCellCannotBeObservedOrOverwritten() {
        var client = new DeterministicOxiaConditionalClient();
        var first = new OxiaKafkaBookKeeperRecordStoreV2(client, "/cell/a");
        var second = new OxiaKafkaBookKeeperRecordStoreV2(client, "/cell/b");
        first.putIfAbsent(TASK, VALUE);
        assertThat(second.get(TASK)).isEmpty();
        assertThatThrownBy(() -> second.get(first.nativeKey(TASK))).hasMessageContaining("closed");
        assertThat(first.get(TASK)).contains(VALUE);
    }
}
