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

package com.nereusstream.kafka.bookkeeper.run;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperRunFailureCutsV1Test {
    @Test
    void createFailsClosedWhenLedgerOutcomeIsUnknown() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        session.createOverride = ProviderMutationResultV1.outcomeUnknown();

        assertThatThrownBy(() -> KafkaBookKeeperRunLifecycleV1.createActive(
                                session,
                                new KafkaRunTestFixtures.FakeRootAuthority(),
                                KafkaRunTestFixtures.binding(6, 11, 5),
                                100)
                        .toCompletableFuture()
                        .join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("run-ledger creation was not established exactly");
    }

    @Test
    void createFailsClosedWhenHeaderAppendIsNotEstablished() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        session.nextAppendOverride = ProviderMutationResultV1.outcomeUnknown();

        assertThatThrownBy(() -> KafkaBookKeeperRunLifecycleV1.createActive(
                                session,
                                new KafkaRunTestFixtures.FakeRootAuthority(),
                                KafkaRunTestFixtures.binding(6, 11, 5),
                                100)
                        .toCompletableFuture()
                        .join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("ledger append was not established exactly");
    }

    @Test
    void checkpointProofSubstitutionFailsTheRunAndDrain() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperRunLifecycleV1 lifecycle = KafkaBookKeeperRunLifecycleV1Test.create(
                session, new KafkaRunTestFixtures.FakeRootAuthority(), KafkaRunTestFixtures.binding(6, 11, 5));
        session.nextAppendOverride = ProviderMutationResultV1.appliedExact(new AppendQuorumProofV1(
                lifecycle.snapshot().handle(),
                99,
                1,
                Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(new byte[] {1})),
                2));

        assertThatThrownBy(() -> lifecycle
                        .appendProtocolCheckpoint(KafkaRunTestFixtures.checkpoint(
                                lifecycle.snapshot().runBinding(), 100))
                        .toCompletableFuture()
                        .join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("ledger append proof differs from the submitted entry");
        assertThat(lifecycle.snapshot().state()).isEqualTo(KafkaBookKeeperRunStateV1.FAILED);
        assertThatThrownBy(lifecycle::drain).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void sealFailsClosedWhenLedgerCloseIsNotEstablished() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperRunLifecycleV1 lifecycle = KafkaBookKeeperRunLifecycleV1Test.create(
                session, new KafkaRunTestFixtures.FakeRootAuthority(), KafkaRunTestFixtures.binding(6, 11, 5));
        lifecycle.drain().toCompletableFuture().join();
        session.closeOverride = ProviderMutationResultV1.definitelyNotApplied();

        assertThatThrownBy(() -> lifecycle
                        .seal(KafkaRunTestFixtures.footer(lifecycle.snapshot(), 100))
                        .toCompletableFuture()
                        .join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("run-ledger close was not established exactly");
        assertThat(lifecycle.snapshot().state()).isEqualTo(KafkaBookKeeperRunStateV1.FAILED);
    }

    @Test
    void successorRejectsRunReusePartitionChangeAndFenceRegression() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperRunLifecycleV1 lifecycle = KafkaBookKeeperRunLifecycleV1Test.create(
                session, new KafkaRunTestFixtures.FakeRootAuthority(), KafkaRunTestFixtures.binding(6, 11, 5));
        KafkaBookKeeperRunLifecycleV1Test.seal(lifecycle, 100);
        Nbke2RunBindingV1 reused = KafkaRunTestFixtures.binding(6, 11, 5);
        Nbke2RunBindingV1 regressed = KafkaRunTestFixtures.binding(7, 10, 4);

        assertThatThrownBy(() -> lifecycle.createSuccessor(reused)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> lifecycle.createSuccessor(regressed)).isInstanceOf(IllegalArgumentException.class);
    }
}
