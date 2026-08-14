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
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunHeaderV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperRunLifecycleV1Test {
    @Test
    void createsLedgerHeaderAndActiveRootInThatOrder() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRunTestFixtures.FakeRootAuthority roots = new KafkaRunTestFixtures.FakeRootAuthority();
        KafkaBookKeeperRunLifecycleV1 lifecycle = create(session, roots, KafkaRunTestFixtures.binding(6, 11, 5));

        KafkaBookKeeperRunSnapshotV1 snapshot = lifecycle.snapshot();
        assertThat(snapshot.state()).isEqualTo(KafkaBookKeeperRunStateV1.ACTIVE);
        assertThat(snapshot.root().state()).isEqualTo(KafkaRunRootStateV1.ACTIVE);
        assertThat(session.entries).containsOnlyKeys(0L);
        assertThat(roots.roots).containsEntry(snapshot.runBinding().runId(), snapshot.root());
    }

    @Test
    void runHeaderBindsExactLedgerRunScopeEpochAndStart() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRunTestFixtures.FakeRootAuthority roots = new KafkaRunTestFixtures.FakeRootAuthority();
        KafkaBookKeeperRunLifecycleV1 lifecycle = create(session, roots, KafkaRunTestFixtures.binding(6, 11, 5));
        KafkaBookKeeperRunSnapshotV1 snapshot = lifecycle.snapshot();

        Nbke2RunHeaderV1 header = (Nbke2RunHeaderV1) Nbke2CodecV1.decode(
                session.entries.get(0L).toByteArray(),
                snapshot.handle().ledgerIdentity().ledgerId(),
                0);

        assertThat(header.runBinding()).isEqualTo(snapshot.runBinding());
        assertThat(header.kafkaStartOffset()).isEqualTo(100);
        assertThat(header.firstDataEntryId()).isEqualTo(1);
        assertThat(header.ledgerConfigurationDigest()).isEqualTo(session.capability.configurationDigest());
    }

    @Test
    void appendsProtocolCheckpointOnlyAtAGroupBoundary() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperRunLifecycleV1 lifecycle =
                create(session, new KafkaRunTestFixtures.FakeRootAuthority(), KafkaRunTestFixtures.binding(6, 11, 5));

        long entryId = lifecycle
                .appendProtocolCheckpoint(
                        KafkaRunTestFixtures.checkpoint(lifecycle.snapshot().runBinding(), 100))
                .toCompletableFuture()
                .join();

        assertThat(entryId).isEqualTo(1);
        assertThat(lifecycle.snapshot().latestProtocolCheckpointEntryId()).hasValue(1);
        assertThat(lifecycle.snapshot().nextEntryId()).isEqualTo(2);
        assertThat(session.entries).containsKeys(0L, 1L);
    }

    @Test
    void drainWaitsForAnAcceptedCheckpointToReachTerminalReconciliation() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperRunLifecycleV1 lifecycle =
                create(session, new KafkaRunTestFixtures.FakeRootAuthority(), KafkaRunTestFixtures.binding(6, 11, 5));
        session.delayedEntryId = 1;
        CompletionStage<Long> checkpoint = lifecycle.appendProtocolCheckpoint(
                KafkaRunTestFixtures.checkpoint(lifecycle.snapshot().runBinding(), 100));

        CompletionStage<Void> drain = lifecycle.drain();

        assertThat(drain.toCompletableFuture()).isNotDone();
        assertThat(lifecycle.snapshot().state()).isEqualTo(KafkaBookKeeperRunStateV1.DRAINING);
        session.completeDelayedAppend();
        assertThat(checkpoint.toCompletableFuture().join()).isEqualTo(1);
        drain.toCompletableFuture().join();
        assertThat(lifecycle.snapshot().state()).isEqualTo(KafkaBookKeeperRunStateV1.DRAINED);
    }

    @Test
    void drainWaitsForTheExactOpenDataReservation() {
        KafkaBookKeeperRunLifecycleV1 lifecycle = create(
                new KafkaRunTestFixtures.FakeSession(),
                new KafkaRunTestFixtures.FakeRootAuthority(),
                KafkaRunTestFixtures.binding(6, 11, 5));
        KafkaBookKeeperEntryReservationV1 reservation = lifecycle.reserveDataGroup(2);

        CompletionStage<Void> drain = lifecycle.drain();

        assertThat(drain.toCompletableFuture()).isNotDone();
        assertThatThrownBy(() -> lifecycle.reserveDataGroup(1)).isInstanceOf(IllegalStateException.class);
        lifecycle.completeDataGroup(reservation);
        drain.toCompletableFuture().join();
        assertThat(lifecycle.snapshot().state()).isEqualTo(KafkaBookKeeperRunStateV1.DRAINED);
    }

    @Test
    void sealsOnlyAfterDrainFooterQuorumLedgerCloseAndRootCas() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRunTestFixtures.FakeRootAuthority roots = new KafkaRunTestFixtures.FakeRootAuthority();
        KafkaBookKeeperRunLifecycleV1 lifecycle = create(session, roots, KafkaRunTestFixtures.binding(6, 11, 5));

        KafkaBookKeeperRunSnapshotV1 sealed = seal(lifecycle, 100);

        assertThat(sealed.state()).isEqualTo(KafkaBookKeeperRunStateV1.SEALED);
        assertThat(sealed.root().state()).isEqualTo(KafkaRunRootStateV1.SEALED);
        assertThat(sealed.root().kafkaEndOffsetExclusive()).hasValue(100);
        assertThat(session.entries).containsKeys(0L, 1L);
        assertThat(session.closeCalls).isEqualTo(1);
        assertThat(session.drainCalls).isZero();
    }

    @Test
    void createsAContiguousSuccessorWithFreshRunAndPredecessorIdentity() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRunTestFixtures.FakeRootAuthority roots = new KafkaRunTestFixtures.FakeRootAuthority();
        KafkaBookKeeperRunLifecycleV1 lifecycle = create(session, roots, KafkaRunTestFixtures.binding(6, 11, 5));
        KafkaBookKeeperRunSnapshotV1 sealed = seal(lifecycle, 105);
        Nbke2RunBindingV1 successorBinding = KafkaRunTestFixtures.binding(7, 12, 6);

        KafkaBookKeeperRunLifecycleV1 successor = lifecycle
                .createSuccessor(successorBinding)
                .toCompletableFuture()
                .join();

        assertThat(successor.snapshot().root().kafkaStartOffset()).isEqualTo(105);
        assertThat(successor.snapshot().root().predecessorRunId())
                .contains(sealed.runBinding().runId());
        assertThat(successor.snapshot().state()).isEqualTo(KafkaBookKeeperRunStateV1.ACTIVE);
        assertThat(successor.snapshot().handle().ledgerIdentity())
                .isNotEqualTo(sealed.handle().ledgerIdentity());
    }

    @Test
    void retiresOnlyAfterEveryLocalEligibilityConditionIsProven() {
        KafkaBookKeeperRunLifecycleV1 lifecycle = create(
                new KafkaRunTestFixtures.FakeSession(),
                new KafkaRunTestFixtures.FakeRootAuthority(),
                KafkaRunTestFixtures.binding(6, 11, 5));
        seal(lifecycle, 100);

        assertThatThrownBy(() -> lifecycle.retire(new KafkaRunRetirementPermitV1(true, true, 1, true)))
                .isInstanceOf(IllegalArgumentException.class);
        KafkaBookKeeperRunSnapshotV1 retired = lifecycle.retire(new KafkaRunRetirementPermitV1(true, true, 0, true));

        assertThat(retired.state()).isEqualTo(KafkaBookKeeperRunStateV1.RETIRED);
        assertThat(retired.root().state()).isEqualTo(KafkaRunRootStateV1.SEALED);
        assertThatThrownBy(lifecycle::drain).isInstanceOf(IllegalStateException.class);
    }

    static KafkaBookKeeperRunLifecycleV1 create(
            KafkaRunTestFixtures.FakeSession session,
            KafkaRunTestFixtures.FakeRootAuthority roots,
            Nbke2RunBindingV1 binding) {
        return KafkaBookKeeperRunLifecycleV1.createActive(session, roots, binding, 100)
                .toCompletableFuture()
                .join();
    }

    static KafkaBookKeeperRunSnapshotV1 seal(KafkaBookKeeperRunLifecycleV1 lifecycle, long endOffset) {
        lifecycle.drain().toCompletableFuture().join();
        return lifecycle
                .seal(KafkaRunTestFixtures.footer(lifecycle.snapshot(), endOffset))
                .toCompletableFuture()
                .join();
    }
}
