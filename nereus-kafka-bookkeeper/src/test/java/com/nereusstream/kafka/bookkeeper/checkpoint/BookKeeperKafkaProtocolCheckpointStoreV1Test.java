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

package com.nereusstream.kafka.bookkeeper.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ProtocolCheckpointV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import java.util.List;
import org.junit.jupiter.api.Test;

class BookKeeperKafkaProtocolCheckpointStoreV1Test {
    @Test
    void publishesOneAlignedStateAsAnNbke2ControlEntry() {
        Fixture fixture = fixture();
        KafkaProtocolCheckpointStateV1 state = KafkaProtocolCheckpointStateV1.empty(
                fixture.lifecycle.snapshot().runBinding(), 100);

        KafkaProtocolCheckpointPublicationV1 publication =
                fixture.store.publish(state).toCompletableFuture().join();

        assertThat(publication.physicalIdentity()).isEqualTo(1);
        assertThat(fixture.store.latestPublished()).contains(state);
        assertThat(Nbke2CodecV1.decode(
                        fixture.session.entries.get(1L).toByteArray(),
                        fixture.lifecycle.snapshot().handle().ledgerIdentity().ledgerId(),
                        1))
                .isInstanceOf(Nbke2ProtocolCheckpointV1.class);
    }

    @Test
    void responseLossConvergesByExactEntryIdentityAndBytes() {
        Fixture fixture = fixture();
        fixture.session.nextAppendOverride = ProviderMutationResultV1.outcomeUnknown();

        KafkaProtocolCheckpointPublicationV1 publication = fixture.store
                .publish(KafkaProtocolCheckpointStateV1.empty(
                        fixture.lifecycle.snapshot().runBinding(), 100))
                .toCompletableFuture()
                .join();

        assertThat(publication.physicalIdentity()).isEqualTo(1);
        assertThat(fixture.session.readEntryIds).containsExactly(1L);

        Fixture exceptional = fixture();
        exceptional.session.nextAppendFailure = new IllegalStateException("response lost after acceptance");
        assertThat(exceptional
                        .store
                        .publish(KafkaProtocolCheckpointStateV1.empty(
                                exceptional.lifecycle.snapshot().runBinding(), 100))
                        .toCompletableFuture()
                        .join()
                        .physicalIdentity())
                .isEqualTo(1);
        assertThat(exceptional.session.readEntryIds).containsExactly(1L);
    }

    @Test
    void rejectsUnalignedOrRegressingVectors() {
        Fixture fixture = fixture();
        var binding = fixture.lifecycle.snapshot().runBinding();
        KafkaProtocolCheckpointStateV1 unaligned = new KafkaProtocolCheckpointStateV1(
                new KafkaRecoveryCheckpointVectorV1(binding, 101, 100, 100, 100),
                com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1.empty(),
                com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1.empty(),
                com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1.empty());
        assertThatThrownBy(() -> fixture.store.publish(unaligned)).isInstanceOf(IllegalArgumentException.class);

        fixture.store
                .publish(KafkaProtocolCheckpointStateV1.empty(binding, 101))
                .toCompletableFuture()
                .join();
        assertThatThrownBy(() -> fixture.store.publish(KafkaProtocolCheckpointStateV1.empty(binding, 100)))
                .isInstanceOf(IllegalArgumentException.class);
        KafkaProtocolCheckpointStateV1 substituted = new KafkaProtocolCheckpointStateV1(
                new KafkaRecoveryCheckpointVectorV1(binding, 101, 101, 101, 101),
                com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1.empty(),
                com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1.empty(),
                com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1.empty()
                        .observe(5, 100));
        assertThatThrownBy(() -> fixture.store.publish(substituted)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void excludesASecondPublicationWhileTheFirstControlWriteIsUnresolved() {
        Fixture fixture = fixture();
        fixture.session.delayedEntryId = 1;
        KafkaProtocolCheckpointStateV1 state = KafkaProtocolCheckpointStateV1.empty(
                fixture.lifecycle.snapshot().runBinding(), 100);
        var pending = fixture.store.publish(state);

        assertThatThrownBy(() -> fixture.store.publish(state)).isInstanceOf(IllegalArgumentException.class);

        fixture.session.completeDelayedAppend();
        assertThat(pending.toCompletableFuture().join().physicalIdentity()).isEqualTo(1);
    }

    @Test
    void footerMustBindTheLatestPublishedProtocolCheckpoint() {
        Fixture fixture = fixture();
        fixture.store
                .publish(KafkaProtocolCheckpointStateV1.empty(
                        fixture.lifecycle.snapshot().runBinding(), 100))
                .toCompletableFuture()
                .join();
        fixture.lifecycle.drain().toCompletableFuture().join();

        assertThatThrownBy(() -> fixture.lifecycle.seal(
                        new Nbke2RunFooterV1(fixture.lifecycle.snapshot().runBinding(), 100, 3, -1, -1, 11, List.of())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("latest protocol checkpoint");
        assertThat(fixture.lifecycle
                        .seal(new Nbke2RunFooterV1(
                                fixture.lifecycle.snapshot().runBinding(), 100, 3, -1, 1, 11, List.of()))
                        .toCompletableFuture()
                        .join()
                        .root()
                        .kafkaEndOffsetExclusive())
                .hasValue(100);
    }

    private static Fixture fixture() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperRunLifecycleV1 lifecycle = KafkaBookKeeperRunLifecycleV1.createActive(
                        session,
                        new KafkaRunTestFixtures.FakeRootAuthority(),
                        KafkaRunTestFixtures.binding(6, 11, 5),
                        100)
                .toCompletableFuture()
                .join();
        return new Fixture(session, lifecycle, new BookKeeperKafkaProtocolCheckpointStoreV1(lifecycle));
    }

    private record Fixture(
            KafkaRunTestFixtures.FakeSession session,
            KafkaBookKeeperRunLifecycleV1 lifecycle,
            BookKeeperKafkaProtocolCheckpointStoreV1 store) {}
}
