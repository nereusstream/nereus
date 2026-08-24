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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.kafka.bookkeeper.object.ObjectKafkaTestFixtures;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class ObjectKafkaProtocolCheckpointStoreV1Test {
    private static final String ROOT_PREFIX = "cells/01/shards/0007/runs/0000000000000000001";

    @Test
    void convergesResponseLossAdvancesOrdinalAndTakesOver() {
        var root = ObjectKafkaTestFixtures.digest(12);
        InMemoryNwkcp1Backend backend = new InMemoryNwkcp1Backend(ROOT_PREFIX, root);
        backend.unknownNextCreate = true;
        backend.unknownNextCas = true;
        var context = new KafkaNwkcp1WalRunContextV1(root, ObjectKafkaTestFixtures.runBinding());
        var store = new ObjectKafkaProtocolCheckpointStoreV1(ROOT_PREFIX, context, 9, backend);

        var first = store.publish(ObjectKafkaTestFixtures.checkpoint(100))
                .toCompletableFuture()
                .join();
        assertThat(first.physicalIdentity()).isZero();
        var secondState = ObjectKafkaTestFixtures.checkpoint(101);
        var second = store.publish(secondState).toCompletableFuture().join();
        assertThat(second.physicalIdentity()).isEqualTo(1);
        assertThat(store.publish(secondState).toCompletableFuture().join().physicalIdentity())
                .isEqualTo(1);

        var takenOver = store.takeover(10).toCompletableFuture().join();
        assertThat(takenOver.publisherEpoch()).isEqualTo(10);
        assertThat(takenOver.checkpointOrdinal()).isEqualTo(1);
        var recovery = new KafkaObjectCheckpointRecoveryV1(ROOT_PREFIX, context, backend, Optional.empty())
                .recover()
                .toCompletableFuture()
                .join();
        assertThat(recovery.state()).isEqualTo(secondState);
        assertThat(recovery.source()).isEqualTo(KafkaObjectCheckpointRecoveryV1.Source.NWKCP1);
        assertThat(recovery.terminalHead()).isFalse();
    }

    @Test
    void missingHeadWithoutAuthenticatedReplayFailsClosed() {
        var root = ObjectKafkaTestFixtures.digest(12);
        InMemoryNwkcp1Backend backend = new InMemoryNwkcp1Backend(ROOT_PREFIX, root);
        var context = new KafkaNwkcp1WalRunContextV1(root, ObjectKafkaTestFixtures.runBinding());
        var recovery = new KafkaObjectCheckpointRecoveryV1(ROOT_PREFIX, context, backend, Optional.empty());
        assertThatThrownBy(() -> recovery.recover().toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(KafkaObjectCheckpointException.class)
                .hasRootCauseMessage("authenticated NWG1 suffix replay is not configured");
    }

    @Test
    void definitiveProviderConflictFailsWithoutUnknownOutcomeReread() {
        var root = ObjectKafkaTestFixtures.digest(12);
        InMemoryNwkcp1Backend backend = new InMemoryNwkcp1Backend(ROOT_PREFIX, root);
        backend.conflictNextCreate = true;
        var store = new ObjectKafkaProtocolCheckpointStoreV1(
                ROOT_PREFIX, new KafkaNwkcp1WalRunContextV1(root, ObjectKafkaTestFixtures.runBinding()), 9, backend);

        assertThatThrownBy(() -> store.publish(ObjectKafkaTestFixtures.checkpoint(100))
                        .toCompletableFuture()
                        .join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("NWKCP1 conditional create returned a definitive same-key conflict");
        assertThat(backend.objectReads).isZero();
    }
}
