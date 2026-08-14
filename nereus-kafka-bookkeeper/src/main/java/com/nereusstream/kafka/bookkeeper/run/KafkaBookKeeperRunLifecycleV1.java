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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ProtocolCheckpointV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunHeaderV1;
import com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerCloseProofV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootAuthority;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.bookkeeper.ImmutableRetainedStoragePayload;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * One partition/leader-epoch-bound K3 run lifecycle.
 *
 * <p>Normal DATA submission and ordered protocol publication start in K4/K5. K3 owns only ledger/header/root creation,
 * the single entry-ID sequencer, between-group checkpoints, drain, footer/close/root seal, successor, and local retire.
 */
public final class KafkaBookKeeperRunLifecycleV1 {
    private final BookKeeperCellSession session;
    private final KafkaRunRootAuthority rootAuthority;
    private final Nbke2RunBindingV1 runBinding;
    private final RunLedgerHandleV1 handle;
    private final KafkaBookKeeperEntrySequencerV1 entrySequencer;
    private final CompletableFuture<Void> drained = new CompletableFuture<>();

    private KafkaRunRootSnapshotV1 root;
    private KafkaBookKeeperRunStateV1 state = KafkaBookKeeperRunStateV1.ACTIVE;
    private int pendingOperations;
    private OptionalLong latestProtocolCheckpointEntryId = OptionalLong.empty();

    private KafkaBookKeeperRunLifecycleV1(
            BookKeeperCellSession session,
            KafkaRunRootAuthority rootAuthority,
            Nbke2RunBindingV1 runBinding,
            RunLedgerHandleV1 handle,
            KafkaRunRootSnapshotV1 root) {
        this.session = session;
        this.rootAuthority = rootAuthority;
        this.runBinding = runBinding;
        this.handle = handle;
        this.root = root;
        this.entrySequencer = new KafkaBookKeeperEntrySequencerV1(1);
    }

    public static CompletionStage<KafkaBookKeeperRunLifecycleV1> createActive(
            BookKeeperCellSession session,
            KafkaRunRootAuthority rootAuthority,
            Nbke2RunBindingV1 runBinding,
            long kafkaStartOffset) {
        return create(
                session, rootAuthority, runBinding, kafkaStartOffset, Optional.empty(), rootAuthority::createRoot);
    }

    public CompletionStage<KafkaBookKeeperRunLifecycleV1> createSuccessor(Nbke2RunBindingV1 successorBinding) {
        KafkaRunRootSnapshotV1 expectedSealed;
        synchronized (this) {
            requireState(KafkaBookKeeperRunStateV1.SEALED);
            expectedSealed = root;
            validateSuccessorBinding(successorBinding);
        }
        long successorStart = expectedSealed.kafkaEndOffsetExclusive().orElseThrow();
        return create(
                session,
                rootAuthority,
                successorBinding,
                successorStart,
                Optional.of(runBinding.runId()),
                candidate -> rootAuthority.createSuccessor(expectedSealed, candidate));
    }

    public synchronized KafkaBookKeeperEntryReservationV1 reserveDataGroup(int memberCount) {
        requireState(KafkaBookKeeperRunStateV1.ACTIVE);
        return entrySequencer.reserveDataGroup(memberCount);
    }

    public synchronized void completeDataGroup(KafkaBookKeeperEntryReservationV1 reservation) {
        entrySequencer.completeDataGroup(reservation);
        completeDrainIfReady();
    }

    public CompletionStage<Long> appendProtocolCheckpoint(Nbke2ProtocolCheckpointV1 checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint");
        KafkaBookKeeperEntryReservationV1 reservation;
        synchronized (this) {
            requireState(KafkaBookKeeperRunStateV1.ACTIVE);
            if (!checkpoint.runBinding().equals(runBinding)) {
                throw new IllegalArgumentException("checkpoint run binding differs from the active run");
            }
            reservation = entrySequencer.reserveControl();
            pendingOperations++;
        }
        long entryId = reservation.firstEntryId();
        CompletionStage<Long> operation = appendExact(
                        session,
                        handle,
                        entryId,
                        Nbke2CodecV1.encode(handle.ledgerIdentity().ledgerId(), entryId, checkpoint))
                .thenApply(ignored -> entryId);
        return operation.whenComplete((completedEntryId, failure) -> {
            synchronized (KafkaBookKeeperRunLifecycleV1.this) {
                pendingOperations--;
                if (failure == null) {
                    latestProtocolCheckpointEntryId = OptionalLong.of(completedEntryId);
                } else {
                    failRun(failure);
                }
                completeDrainIfReady();
            }
        });
    }

    public synchronized CompletionStage<Void> drain() {
        if (state == KafkaBookKeeperRunStateV1.ACTIVE) {
            state = KafkaBookKeeperRunStateV1.DRAINING;
            completeDrainIfReady();
        } else if (state != KafkaBookKeeperRunStateV1.DRAINING && state != KafkaBookKeeperRunStateV1.DRAINED) {
            throw new IllegalStateException("only an ACTIVE or already draining run can drain");
        }
        return drained;
    }

    public CompletionStage<KafkaBookKeeperRunSnapshotV1> seal(Nbke2RunFooterV1 footer) {
        Objects.requireNonNull(footer, "footer");
        KafkaRunRootSnapshotV1 expectedActive;
        KafkaRunRootSnapshotV1 sealedCandidate;
        long footerEntryId;
        synchronized (this) {
            requireState(KafkaBookKeeperRunStateV1.DRAINED);
            if (!footer.runBinding().equals(runBinding)) {
                throw new IllegalArgumentException("footer run binding differs from the drained run");
            }
            if (latestProtocolCheckpointEntryId.isPresent()
                    && footer.protocolCheckpointEntryId() != latestProtocolCheckpointEntryId.getAsLong()) {
                throw new IllegalArgumentException("footer does not bind the latest protocol checkpoint");
            }
            KafkaBookKeeperEntryReservationV1 reservation = entrySequencer.reserveControl();
            footerEntryId = reservation.firstEntryId();
            if (footer.lastPhysicalEntryIdExclusive() != Math.incrementExact(footerEntryId)) {
                throw new IllegalArgumentException("footer physical end differs from the sequenced footer entry");
            }
            expectedActive = root;
            sealedCandidate = sealedRoot(expectedActive, footer.kafkaEndOffsetExclusive());
            state = KafkaBookKeeperRunStateV1.SEALING;
        }

        byte[] footerBytes = Nbke2CodecV1.encode(handle.ledgerIdentity().ledgerId(), footerEntryId, footer);
        CompletionStage<KafkaBookKeeperRunSnapshotV1> result = appendExact(session, handle, footerEntryId, footerBytes)
                .thenCompose(ignored -> session.closeRunLedger(handle))
                .thenApply(closeResult -> requireExactClose(closeResult, handle, footerEntryId))
                .thenCompose(ignored -> rootAuthority.sealRoot(expectedActive, sealedCandidate))
                .thenApply(sealResult -> requireExactRoot(sealResult, sealedCandidate))
                .thenApply(exactSealedRoot -> {
                    synchronized (KafkaBookKeeperRunLifecycleV1.this) {
                        root = exactSealedRoot;
                        state = KafkaBookKeeperRunStateV1.SEALED;
                        return snapshot();
                    }
                });
        return result.whenComplete((ignored, failure) -> {
            if (failure != null) {
                synchronized (KafkaBookKeeperRunLifecycleV1.this) {
                    failRun(failure);
                }
            }
        });
    }

    public synchronized KafkaBookKeeperRunSnapshotV1 retire(KafkaRunRetirementPermitV1 permit) {
        Objects.requireNonNull(permit, "permit");
        requireState(KafkaBookKeeperRunStateV1.SEALED);
        if (!permit.permitsRetirement()) {
            throw new IllegalArgumentException("retirement proof has an active authority, protection, pin, or hold");
        }
        state = KafkaBookKeeperRunStateV1.RETIRED;
        return snapshot();
    }

    public synchronized KafkaBookKeeperRunSnapshotV1 snapshot() {
        return new KafkaBookKeeperRunSnapshotV1(
                runBinding,
                handle,
                root,
                state,
                entrySequencer.nextEntryId(),
                pendingOperations,
                latestProtocolCheckpointEntryId);
    }

    private static CompletionStage<KafkaBookKeeperRunLifecycleV1> create(
            BookKeeperCellSession session,
            KafkaRunRootAuthority rootAuthority,
            Nbke2RunBindingV1 runBinding,
            long kafkaStartOffset,
            Optional<com.nereusstream.storage.api.bookkeeper.StorageRunId> predecessor,
            Function<KafkaRunRootSnapshotV1, CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>>>
                    rootMutation) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(rootAuthority, "rootAuthority");
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(predecessor, "predecessor");
        Objects.requireNonNull(rootMutation, "rootMutation");
        if (kafkaStartOffset < 0
                || !session.providerScopeId().equals(runBinding.providerScopeId())
                || !session.capabilitySnapshot().providerScopeId().equals(runBinding.providerScopeId())) {
            throw new IllegalArgumentException("run start or provider scope is invalid");
        }
        RunLedgerConfigurationV1 configuration =
                RunLedgerConfigurationV1.from(session.capabilitySnapshot(), runBinding.runId());
        return session.createRunLedger(configuration).thenCompose(createResult -> {
            RunLedgerHandleV1 exactHandle = requireExactHandle(createResult, runBinding, configuration);
            Nbke2RunHeaderV1 header =
                    new Nbke2RunHeaderV1(runBinding, kafkaStartOffset, 1, configuration.configurationDigest());
            byte[] headerBytes =
                    Nbke2CodecV1.encode(exactHandle.ledgerIdentity().ledgerId(), 0, header);
            KafkaRunRootSnapshotV1 candidate = activeRoot(runBinding, exactHandle, kafkaStartOffset, predecessor);
            return appendExact(session, exactHandle, 0, headerBytes)
                    .thenCompose(ignored -> rootMutation.apply(candidate))
                    .thenApply(rootResult -> requireExactRoot(rootResult, candidate))
                    .thenApply(exactRoot -> new KafkaBookKeeperRunLifecycleV1(
                            session, rootAuthority, runBinding, exactHandle, exactRoot));
        });
    }

    private static CompletionStage<Void> appendExact(
            BookKeeperCellSession session, RunLedgerHandleV1 handle, long entryId, byte[] bytes) {
        ImmutableRetainedStoragePayload payload = ImmutableRetainedStoragePayload.copyOf(bytes);
        CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> accepted;
        try {
            accepted = session.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, entryId, payload));
        } catch (RuntimeException failure) {
            payload.release();
            throw failure;
        }
        if (accepted == null) {
            payload.release();
            throw new IllegalStateException("provider returned a null append stage");
        }
        return accepted.handle((result, failure) -> {
            try {
                if (failure != null) {
                    throw new CompletionException(failure);
                }
                requireExactAppend(
                        result,
                        handle,
                        entryId,
                        payload.sha256(),
                        payload.readableBytes(),
                        session.capabilitySnapshot().ackQuorumSize());
                return null;
            } finally {
                payload.release();
            }
        });
    }

    private static RunLedgerHandleV1 requireExactHandle(
            ProviderMutationResultV1<RunLedgerHandleV1> result,
            Nbke2RunBindingV1 binding,
            RunLedgerConfigurationV1 configuration) {
        if (result == null || result.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT) {
            throw new IllegalStateException("run-ledger creation was not established exactly");
        }
        RunLedgerHandleV1 handle = result.exactProof().orElseThrow();
        if (!handle.providerScopeId().equals(binding.providerScopeId())
                || !handle.runId().equals(binding.runId())
                || !handle.configurationDigest().equals(configuration.configurationDigest())) {
            throw new IllegalStateException("created run-ledger handle differs from the requested identity");
        }
        return handle;
    }

    private static void requireExactAppend(
            ProviderMutationResultV1<AppendQuorumProofV1> result,
            RunLedgerHandleV1 handle,
            long entryId,
            Sha256Digest payloadDigest,
            int payloadBytes,
            int requiredAcknowledgedBookies) {
        if (result == null || result.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT) {
            throw new IllegalStateException("ledger append was not established exactly");
        }
        AppendQuorumProofV1 proof = result.exactProof().orElseThrow();
        if (!proof.handle().equals(handle)
                || proof.entryId() != entryId
                || proof.payloadBytes() != payloadBytes
                || !proof.payloadSha256().equals(payloadDigest)
                || proof.acknowledgedBookies() < requiredAcknowledgedBookies) {
            throw new IllegalStateException("ledger append proof differs from the submitted entry");
        }
    }

    private static Void requireExactClose(
            ProviderMutationResultV1<RunLedgerCloseProofV1> result,
            RunLedgerHandleV1 handle,
            long expectedLastAddConfirmed) {
        if (result == null || result.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT) {
            throw new IllegalStateException("run-ledger close was not established exactly");
        }
        RunLedgerCloseProofV1 proof = result.exactProof().orElseThrow();
        if (!proof.handle().equals(handle) || proof.lastAddConfirmed() != expectedLastAddConfirmed) {
            throw new IllegalStateException("run-ledger close proof differs from the sealed footer");
        }
        return null;
    }

    private static KafkaRunRootSnapshotV1 requireExactRoot(
            ProviderMutationResultV1<KafkaRunRootSnapshotV1> result, KafkaRunRootSnapshotV1 expected) {
        if (result == null
                || result.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT
                || !result.exactProof().orElseThrow().equals(expected)) {
            throw new IllegalStateException("run-root mutation was not established exactly");
        }
        return result.exactProof().orElseThrow();
    }

    private static KafkaRunRootSnapshotV1 activeRoot(
            Nbke2RunBindingV1 binding,
            RunLedgerHandleV1 handle,
            long kafkaStartOffset,
            Optional<com.nereusstream.storage.api.bookkeeper.StorageRunId> predecessor) {
        return new KafkaRunRootSnapshotV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                binding.storageEpochId(),
                binding.creatorOwnerEpoch(),
                binding.kafkaLeaderEpoch(),
                binding.providerScopeId(),
                binding.runId(),
                handle.ledgerIdentity(),
                kafkaStartOffset,
                OptionalLong.empty(),
                KafkaRunRootStateV1.ACTIVE,
                predecessor);
    }

    private static KafkaRunRootSnapshotV1 sealedRoot(KafkaRunRootSnapshotV1 active, long endOffsetExclusive) {
        return new KafkaRunRootSnapshotV1(
                active.bindingId(),
                active.topicIncarnation(),
                active.partitionId(),
                active.storageEpochId(),
                active.creatorOwnerEpoch(),
                active.kafkaLeaderEpoch(),
                active.providerScopeId(),
                active.runId(),
                active.ledgerIdentity(),
                active.kafkaStartOffset(),
                OptionalLong.of(endOffsetExclusive),
                KafkaRunRootStateV1.SEALED,
                active.predecessorRunId());
    }

    private synchronized void completeDrainIfReady() {
        if (state == KafkaBookKeeperRunStateV1.DRAINING
                && pendingOperations == 0
                && entrySequencer.openDataGroup().isEmpty()) {
            state = KafkaBookKeeperRunStateV1.DRAINED;
            drained.complete(null);
        }
    }

    private synchronized void failRun(Throwable failure) {
        state = KafkaBookKeeperRunStateV1.FAILED;
        drained.completeExceptionally(failure);
    }

    private void validateSuccessorBinding(Nbke2RunBindingV1 successor) {
        Objects.requireNonNull(successor, "successorBinding");
        if (!runBinding.bindingId().equals(successor.bindingId())
                || !runBinding.topicIncarnation().equals(successor.topicIncarnation())
                || runBinding.partitionId() != successor.partitionId()
                || !runBinding.storageEpochId().equals(successor.storageEpochId())
                || !runBinding.providerScopeId().equals(successor.providerScopeId())
                || runBinding.runId().equals(successor.runId())
                || successor.creatorOwnerEpoch() < runBinding.creatorOwnerEpoch()
                || successor.kafkaLeaderEpoch() < runBinding.kafkaLeaderEpoch()) {
            throw new IllegalArgumentException("successor does not preserve the partition chain and monotonic fences");
        }
    }

    private void requireState(KafkaBookKeeperRunStateV1 expected) {
        if (state != expected) {
            throw new IllegalStateException("run state is " + state + ", expected " + expected);
        }
    }
}
