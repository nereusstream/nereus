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

package com.nereusstream.kafka.bookkeeper.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperTakeoverRecoveryV1Test {
    @Test
    void recoversTheCompletePrefixFromTheClosedOwnerRunSet() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);
        KafkaRecoveryTestFixtures.installGroup(session, 1, 100, 1);
        KafkaRecoveryTestFixtures.installGroup(session, 2, 101, 1);

        KafkaBookKeeperRecoveryResultV1 result = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.request(OptionalLong.empty()))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_EXACT);
        assertThat(result.physicalRecoveredEndOffset()).isEqualTo(102);
        assertThat(result.newLeaderLeo()).hasValue(102);
        assertThat(result.recoveredProtocolState()
                        .orElseThrow()
                        .leaderEpochIndex()
                        .startOffsets())
                .containsEntry(5, 100L);
        assertThat(session.readEntryIds).containsExactly(0L, 1L, 2L);
    }

    @Test
    void adoptsCompleteUnacknowledgedTailWithinTheClosedOwnerRunSet() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);
        KafkaRecoveryTestFixtures.installGroup(session, 1, 100, 1);
        KafkaRecoveryTestFixtures.installGroup(session, 2, 101, 1);
        KafkaRecoveryTestFixtures.installGroup(session, 3, 102, 1);

        KafkaBookKeeperRecoveryResultV1 result = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.request(OptionalLong.empty()))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_EXACT);
        assertThat(result.physicalRecoveredEndOffset()).isEqualTo(103);
        assertThat(result.newLeaderLeo()).hasValue(103);
        assertThat(result.recoveredProtocolState().orElseThrow().vector().recoveryCoveredThrough())
                .isEqualTo(103);
    }

    @Test
    void selectedCheckpointSeedsOnlyItsBoundedSuffix() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);
        KafkaRecoveryTestFixtures.installGroup(session, 1, 100, 1);
        KafkaRecoveryTestFixtures.installCheckpoint(session, 2, KafkaRecoveryTestFixtures.checkpointState(101));
        KafkaRecoveryTestFixtures.installGroup(session, 3, 101, 1);

        KafkaBookKeeperRecoveryResultV1 result = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.request(OptionalLong.of(2)))
                .toCompletableFuture()
                .join();

        assertThat(result.recovered()).isTrue();
        assertThat(session.readEntryIds).containsExactly(2L, 3L);
        assertThat(result.progress().entries()).isEqualTo(2);
    }

    @Test
    void corruptHintedCheckpointFallsBackWithoutResettingCumulativeProgress() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);
        KafkaRecoveryTestFixtures.installGroup(session, 1, 100, 1);
        KafkaRecoveryTestFixtures.installCheckpoint(session, 2, KafkaRecoveryTestFixtures.checkpointState(101));
        byte[] corrupt = session.entries.get(2L).toByteArray();
        corrupt[corrupt.length - 1] ^= 1;
        session.entries.put(2L, CanonicalBytes.copyOf(corrupt));
        KafkaRecoveryTestFixtures.installGroup(session, 3, 101, 1);

        KafkaBookKeeperRecoveryResultV1 result = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.request(OptionalLong.of(2)))
                .toCompletableFuture()
                .join();

        assertThat(result.recovered()).isTrue();
        assertThat(session.readEntryIds).containsExactly(2L, 0L, 1L, 3L);
        assertThat(result.progress().entries()).isEqualTo(4);
    }

    @Test
    void failsWhenTheSealedRootPromisesMoreThanThePhysicalPrefix() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);
        KafkaRecoveryTestFixtures.installGroup(session, 1, 100, 1);

        KafkaBookKeeperRecoveryResultV1 result = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.requestSealed(102, OptionalLong.empty()))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.SEALED_END_MISMATCH);
        assertThat(result.physicalRecoveredEndOffset()).isEqualTo(101);
        assertThat(result.newLeaderLeo()).isEmpty();
    }

    @Test
    void rejectsAnOpenOwnerCutBeforeAnyStorageIo() {
        var request = KafkaRecoveryTestFixtures.request(OptionalLong.empty());
        var open = new com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1(
                request.closedOwner().scopeSha256(),
                request.closedOwner().owner(),
                false,
                request.closedOwner().previous(),
                request.closedOwner().runs());
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new KafkaBookKeeperRecoveryRequestV1(
                        request.runBinding(),
                        request.handle(),
                        request.kafkaStartOffset(),
                        request.hintedCheckpointEntryId(),
                        request.envelope(),
                        open,
                        request.selectedRoot(),
                        request.recoveredStateFence(),
                        request.precedingState()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void cumulativeEntryEnvelopeExhaustionFailsClosed() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);
        KafkaRecoveryTestFixtures.installGroup(session, 1, 100, 1);

        KafkaBookKeeperRecoveryResultV1 result = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.request(
                        OptionalLong.empty(), new KafkaBookKeeperRecoveryEnvelopeV1(1, 1_000_000, 1_000_000)))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.ENVELOPE_EXCEEDED);
        assertThat(result.progress().entries()).isEqualTo(1);
        assertThat(session.readEntryIds).containsExactly(0L);
    }

    @Test
    void cumulativeEncodedByteEnvelopeExhaustionFailsClosed() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);

        KafkaBookKeeperRecoveryResultV1 result = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.request(
                        OptionalLong.empty(), new KafkaBookKeeperRecoveryEnvelopeV1(10, 1, 1_000_000)))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.ENVELOPE_EXCEEDED);
        assertThat(result.progress().encodedBytes()).isGreaterThan(1);
    }

    @Test
    void cumulativeElapsedTimeEnvelopeExhaustionFailsClosed() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);

        KafkaBookKeeperRecoveryResultV1 result = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.request(
                        OptionalLong.empty(), new KafkaBookKeeperRecoveryEnvelopeV1(10, 1_000_000, 50)))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.ENVELOPE_EXCEEDED);
        assertThat(result.progress().elapsedNanos()).isEqualTo(100);
    }

    @Test
    void exactOpenAndFenceFailuresRemainDistinctClosedOutcomes() {
        KafkaRunTestFixtures.FakeSession openFailure = new KafkaRunTestFixtures.FakeSession();
        openFailure.openOverride = RunLedgerOpenResultV1.withoutHandle(RunLedgerOpenOutcomeV1.ABSENT);
        KafkaBookKeeperRecoveryResultV1 absent = KafkaRecoveryTestFixtures.engine(openFailure)
                .recover(KafkaRecoveryTestFixtures.request(OptionalLong.empty()))
                .toCompletableFuture()
                .join();

        KafkaRunTestFixtures.FakeSession fenceFailure = new KafkaRunTestFixtures.FakeSession();
        fenceFailure.recoveryOverride = ProviderMutationResultV1.fencedOrConflict();
        KafkaBookKeeperRecoveryResultV1 fenced = KafkaRecoveryTestFixtures.engine(fenceFailure)
                .recover(KafkaRecoveryTestFixtures.request(OptionalLong.empty()))
                .toCompletableFuture()
                .join();

        assertThat(absent.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.OPEN_FAILED);
        assertThat(fenced.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.FENCE_FAILED);
    }

    @Test
    void corruptRunHeaderCannotBeReplacedByTailBytes() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);
        KafkaRecoveryTestFixtures.installGroup(session, 1, 100, 1);
        byte[] corrupt = session.entries.get(0L).toByteArray();
        corrupt[corrupt.length - 1] ^= 1;
        session.entries.put(0L, CanonicalBytes.copyOf(corrupt));

        KafkaBookKeeperRecoveryResultV1 result = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.request(OptionalLong.empty()))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.CORRUPT_HEADER);
        assertThat(session.readEntryIds).containsExactly(0L);
    }

    @Test
    void incompleteAppendGroupNeverAdvancesThePhysicalCandidate() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);
        KafkaRecoveryTestFixtures.installPartialTwoMemberGroup(session, 1, 100);

        KafkaBookKeeperRecoveryResultV1 result = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.request(OptionalLong.empty()))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_WITH_INERT_RESIDUE);
        assertThat(result.physicalRecoveredEndOffset()).isEqualTo(100);
        assertThat(result.conflictEntryId()).hasValue(1);
        assertThat(result.newLeaderLeo()).hasValue(100);
    }

    @Test
    void refusesASealedRootBoundaryInsideACompleteBatch() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);
        KafkaRecoveryTestFixtures.installGroup(session, 1, 100, 3);

        KafkaBookKeeperRecoveryResultV1 result = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.requestSealed(101, OptionalLong.empty()))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.SEALED_END_MISMATCH);
        assertThat(result.newLeaderLeo()).isEmpty();
    }

    @Test
    void gapAfterTheCompletePrefixStopsRecoveryWithoutSalvagingLaterOffsets() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);
        KafkaRecoveryTestFixtures.installGroup(session, 1, 100, 1);
        session.recoveryOverride = ProviderMutationResultV1.appliedExact(
                new RunLedgerRecoveryProofV1(KafkaRecoveryTestFixtures.handle(), 2, true, true));
        session.readOverrides.put(2L, RunLedgerReadResultV1.withoutEntry(RunLedgerReadOutcomeV1.DEFINITIVELY_ABSENT));

        KafkaBookKeeperRecoveryResultV1 result = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.request(OptionalLong.empty()))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_WITH_INERT_RESIDUE);
        assertThat(result.newLeaderLeo()).hasValue(101);
        assertThat(result.conflictEntryId()).hasValue(2);
    }

    @Test
    void recoveredStateInstallsSharedCommitHwWithoutFollowerProgress() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaRecoveryTestFixtures.installHeader(session);
        KafkaRecoveryTestFixtures.installGroup(session, 1, 100, 1);
        KafkaBookKeeperRecoveryResultV1 recovered = KafkaRecoveryTestFixtures.engine(session)
                .recover(KafkaRecoveryTestFixtures.request(OptionalLong.empty()))
                .toCompletableFuture()
                .join();
        var successorBinding = KafkaRunTestFixtures.binding(7, 12, 6);
        RunLedgerHandleV1 successorHandle = new RunLedgerHandleV1(
                successorBinding.providerScopeId(),
                successorBinding.runId(),
                new BookKeeperLedgerIdentity(48),
                KafkaRunTestFixtures.digest(31));

        var snapshot = KafkaCoherentCommitCoordinatorV1.bootstrapRecovered(
                        KafkaRecoveryTestFixtures.recoveredFence(),
                        100,
                        recovered.newLeaderLeo().orElseThrow(),
                        recovered.recoveredProtocolState().orElseThrow(),
                        successorHandle,
                        ignored -> {})
                .capture();

        assertThat(snapshot.root().frontiers().readableEndOffset()).isEqualTo(101);
        assertThat(snapshot.root().frontiers().highWatermark()).isEqualTo(101);
        assertThat(snapshot.root().frontiers().lastStableOffset()).isEqualTo(101);
        assertThat(snapshot.activeTail().startOffset()).isEqualTo(101);
        assertThat(snapshot.speculativeQueue().commits()).isEmpty();
    }
}
