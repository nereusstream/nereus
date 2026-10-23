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

package com.nereusstream.kafka.bookkeeper.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2IndexDirectoryEntryV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadRunV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaPackedIndexDirectoryV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperNativeReadSourcesTest {
    private static final TopicBindingId BINDING = new TopicBindingId(digest("binding"));
    private static final KafkaTopicIncarnationIdentity TOPIC =
            new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(1, 2)), new KafkaTopicName("native-source"));
    private static final StorageEpochId STORAGE = new StorageEpochId(digest("storage"));
    private static final CellProviderScopeId SCOPE = new CellProviderScopeId(digest("scope"));
    private static final Sha256Digest CONFIG = digest("config");
    private static final StorageRunId OLD = new StorageRunId(new Id128(3, 4));
    private static final StorageRunId CURRENT = new StorageRunId(new Id128(5, 6));

    @Test
    void selectedViewBindsActualSealedRootAndIndexContent() {
        var old = root(OLD, 41, 0, OptionalLong.of(10), KafkaRunRootStateV1.SEALED);
        var current = root(CURRENT, 42, 10, OptionalLong.empty(), KafkaRunRootStateV1.ACTIVE);
        var first = sources(old, current, 5);
        var changedIndex = sources(old, current, 6);
        var changedLedger = sources(root(OLD, 43, 0, OptionalLong.of(10), KafkaRunRootStateV1.SEALED), current, 5);

        assertThat(first.selectedViewSha256())
                .isEqualTo(sources(old, current, 5).selectedViewSha256());
        assertThat(changedIndex.selectedViewSha256()).isNotEqualTo(first.selectedViewSha256());
        assertThat(changedLedger.selectedViewSha256()).isNotEqualTo(first.selectedViewSha256());
        assertThatThrownBy(() -> new KafkaBookKeeperPartitionV1.NativeReadSources(
                                List.of(old), current, List.of(readRun(old, 44, 5)), CONFIG)
                        .selectedViewSha256())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("read index differs");
    }

    @Test
    void selectedViewRequiresTheCompleteOrderedNativeReadCatalog() {
        var old = root(OLD, 41, 0, OptionalLong.of(10), KafkaRunRootStateV1.SEALED);
        var current = root(CURRENT, 42, 10, OptionalLong.empty(), KafkaRunRootStateV1.ACTIVE);
        var read = readRun(old, 41, 5);

        assertThatThrownBy(
                        () -> new KafkaBookKeeperPartitionV1.NativeReadSources(List.of(old), current, List.of(), CONFIG)
                                .selectedViewSha256())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("complete read index");
        assertThatThrownBy(() -> new KafkaBookKeeperPartitionV1.NativeReadSources(
                                List.of(old), current, List.of(read, read), CONFIG)
                        .selectedViewSha256())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("index membership");
        assertThatThrownBy(() -> new KafkaBookKeeperPartitionV1.NativeReadSources(
                                List.of(old),
                                root(CURRENT, 42, 11, OptionalLong.empty(), KafkaRunRootStateV1.ACTIVE),
                                List.of(read),
                                CONFIG)
                        .selectedViewSha256())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lineage");
    }

    @Test
    void selectedViewRejectsReadIndexFromAnotherProviderScope() {
        var old = root(OLD, 41, 0, OptionalLong.of(10), KafkaRunRootStateV1.SEALED);
        var current = root(CURRENT, 42, 10, OptionalLong.empty(), KafkaRunRootStateV1.ACTIVE);
        var foreign = new CellProviderScopeId(digest("foreign-scope"));
        var binding = new Nbke2RunBindingV1(BINDING, TOPIC, 0, STORAGE, 1, 0, foreign, OLD);
        var handle = new RunLedgerHandleV1(foreign, OLD, old.ledgerIdentity(), CONFIG);
        var read = new KafkaBookKeeperReadRunV1(
                binding,
                handle,
                0,
                10,
                0,
                Optional.empty(),
                Optional.of(new KafkaPackedIndexDirectoryV1(List.of(new Nbke2IndexDirectoryEntryV1(5, 0, 10)))));

        assertThatThrownBy(() -> new KafkaBookKeeperPartitionV1.NativeReadSources(
                                List.of(old), current, List.of(read), CONFIG)
                        .selectedViewSha256())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("read index differs");
    }

    private static KafkaBookKeeperPartitionV1.NativeReadSources sources(
            KafkaRunRootSnapshotV1 old, KafkaRunRootSnapshotV1 current, long indexEntry) {
        return new KafkaBookKeeperPartitionV1.NativeReadSources(
                List.of(old), current, List.of(readRun(old, old.ledgerIdentity().ledgerId(), indexEntry)), CONFIG);
    }

    private static KafkaBookKeeperReadRunV1 readRun(KafkaRunRootSnapshotV1 root, long ledgerId, long indexEntry) {
        var binding = new Nbke2RunBindingV1(BINDING, TOPIC, 0, STORAGE, 1, 0, SCOPE, root.runId());
        var handle = new RunLedgerHandleV1(SCOPE, root.runId(), new BookKeeperLedgerIdentity(ledgerId), CONFIG);
        return new KafkaBookKeeperReadRunV1(
                binding,
                handle,
                0,
                10,
                0,
                Optional.empty(),
                Optional.of(
                        new KafkaPackedIndexDirectoryV1(List.of(new Nbke2IndexDirectoryEntryV1(indexEntry, 0, 10)))));
    }

    private static KafkaRunRootSnapshotV1 root(
            StorageRunId runId, long ledgerId, long start, OptionalLong end, KafkaRunRootStateV1 state) {
        return new KafkaRunRootSnapshotV1(
                BINDING,
                TOPIC,
                0,
                STORAGE,
                1,
                0,
                SCOPE,
                runId,
                new BookKeeperLedgerIdentity(ledgerId),
                start,
                end,
                state,
                runId.equals(OLD) ? Optional.empty() : Optional.of(OLD));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.US_ASCII)));
    }
}
