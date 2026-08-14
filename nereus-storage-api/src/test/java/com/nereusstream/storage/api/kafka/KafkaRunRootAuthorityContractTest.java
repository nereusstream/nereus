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

package com.nereusstream.storage.api.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class KafkaRunRootAuthorityContractTest {
    @Test
    void activeAndSealedRootsHaveDistinctEndOffsetDomains() {
        KafkaRunRootSnapshotV1 active = root(KafkaRunRootStateV1.ACTIVE, OptionalLong.empty(), Optional.empty());
        KafkaRunRootSnapshotV1 sealed = root(KafkaRunRootStateV1.SEALED, OptionalLong.of(101), Optional.empty());

        assertThat(active.kafkaEndOffsetExclusive()).isEmpty();
        assertThat(sealed.kafkaEndOffsetExclusive()).hasValue(101);
    }

    @Test
    void rejectsStateEndMismatchAndBackwardEnd() {
        assertThatThrownBy(() -> root(KafkaRunRootStateV1.ACTIVE, OptionalLong.of(101), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> root(KafkaRunRootStateV1.SEALED, OptionalLong.empty(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> root(KafkaRunRootStateV1.SEALED, OptionalLong.of(99), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsSelfPredecessor() {
        StorageRunId runId = new StorageRunId(new Id128(0, 17));
        assertThatThrownBy(() -> new KafkaRunRootSnapshotV1(
                        new TopicBindingId(digest(1)),
                        new KafkaTopicIncarnationIdentity(
                                new KafkaTopicId(new Id128(0, 2)), new KafkaTopicName("orders")),
                        0,
                        new StorageEpochId(digest(2)),
                        1,
                        3,
                        new CellProviderScopeId(digest(3)),
                        runId,
                        new BookKeeperLedgerIdentity(19),
                        100,
                        OptionalLong.empty(),
                        KafkaRunRootStateV1.ACTIVE,
                        Optional.of(runId)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("own predecessor");
    }

    @Test
    void authoritySurfaceIsClosedToFourLowFrequencyOperations() {
        Set<String> methods = Arrays.stream(KafkaRunRootAuthority.class.getDeclaredMethods())
                .map(method -> method.getName())
                .collect(Collectors.toSet());

        assertThat(methods).containsExactlyInAnyOrder("createRoot", "openRoot", "sealRoot", "createSuccessor");
    }

    private static KafkaRunRootSnapshotV1 root(
            KafkaRunRootStateV1 state, OptionalLong end, Optional<StorageRunId> predecessor) {
        return new KafkaRunRootSnapshotV1(
                new TopicBindingId(digest(1)),
                new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(0, 2)), new KafkaTopicName("orders")),
                0,
                new StorageEpochId(digest(2)),
                1,
                3,
                new CellProviderScopeId(digest(3)),
                new StorageRunId(new Id128(0, 17)),
                new BookKeeperLedgerIdentity(19),
                100,
                end,
                state,
                predecessor);
    }

    private static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }
}
