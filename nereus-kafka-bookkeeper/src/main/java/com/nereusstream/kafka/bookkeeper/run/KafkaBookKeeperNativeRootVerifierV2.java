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
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2FrameV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunHeaderV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootVerifierV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Reads actual NBKE2 header/footer and sealed metadata through the exact guarded BK client. Never fences a writer. */
public final class KafkaBookKeeperNativeRootVerifierV2 implements KafkaRunRootVerifierV2, AutoCloseable {
    private final M5BookKeeperNativeCreateClientV2 client;
    /** The caller owns capacity, independent operation pins, and the accepted session's terminal close. */
    @FunctionalInterface
    public interface ReadOwner {
        CompletionStage<Void> read(
                KafkaRunRootRecordV2 record,
                int maximumHandles,
                Function<RealBookKeeperCellSessionV1, CompletionStage<Void>> verification);

        default CompletionStage<Boolean> permitsRetiredSuccessor(
                KafkaRunRootRecordV2 parent, KafkaRunRootSnapshotV1 successor) {
            return CompletableFuture.completedFuture(false);
        }
    }

    private final ReadOwner readOwner;
    private final Set<RealBookKeeperCellSessionV1> standaloneReads = ConcurrentHashMap.newKeySet();
    private volatile boolean draining;

    public KafkaBookKeeperNativeRootVerifierV2(M5BookKeeperNativeCreateClientV2 client) {
        this(client, null);
    }

    public KafkaBookKeeperNativeRootVerifierV2(M5BookKeeperNativeCreateClientV2 client, ReadOwner readOwner) {
        this.client = Objects.requireNonNull(client, "client");
        this.readOwner = readOwner == null ? this::readStandalone : readOwner;
    }

    @Override
    public Sha256Digest capabilitySha256() {
        return client.capabilitySnapshot().configurationDigest();
    }

    @Override
    public CompletionStage<Void> requireNative(KafkaRunRootRecordV2 record) {
        return verify(record, 1, reads -> requireNative(record, reads));
    }

    @Override
    public CompletionStage<Boolean> permitsRetiredSuccessor(
            KafkaRunRootRecordV2 parent, KafkaRunRootSnapshotV1 successor) {
        if (draining
                || !parent.retired()
                || !parent.resource().namespace().equals(client.spec().namespace())
                || !successor
                        .providerScopeId()
                        .equals(client.capabilitySnapshot().providerScopeId())) {
            return CompletableFuture.completedFuture(false);
        }
        return readOwner.permitsRetiredSuccessor(parent, successor);
    }

    private CompletionStage<Void> requireNative(KafkaRunRootRecordV2 record, RealBookKeeperCellSessionV1 reads) {
        if (record.recoveryCut().isPresent()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("crash run requires its exact closed-owner prefix verification"));
        }
        var root = record.root();
        var configuration = RunLedgerConfigurationV1.from(reads.capabilitySnapshot(), root.runId());
        if (!record.resource().namespace().equals(client.spec().namespace())
                || !root.providerScopeId().equals(reads.providerScopeId())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("root is outside its native BK scope"));
        }
        var handle = new RunLedgerHandleV1(
                root.providerScopeId(), root.runId(), root.ledgerIdentity(), configuration.configurationDigest());
        var binding = new Nbke2RunBindingV1(
                root.bindingId(),
                root.topicIncarnation(),
                root.partitionId(),
                root.storageEpochId(),
                root.creatorOwnerEpoch(),
                root.kafkaLeaderEpoch(),
                root.providerScopeId(),
                root.runId());
        return reads.readNativeRunHeader(handle)
                .thenApply(entry -> Nbke2CodecV1.decode(
                        entry.payload().toByteArray(), handle.ledgerIdentity().ledgerId(), 0))
                .thenCompose(frame -> {
                    if (!(frame instanceof Nbke2RunHeaderV1 header)
                            || !header.runBinding().equals(binding)
                            || header.kafkaStartOffset() != root.kafkaStartOffset()
                            || header.firstDataEntryId() != 1
                            || !header.ledgerConfigurationDigest().equals(configuration.configurationDigest())) {
                        throw new IllegalStateException("actual native run header differs from the root");
                    }
                    if (root.state() == KafkaRunRootStateV1.ACTIVE) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return client.captureExactTarget(handle).thenCompose(captured -> {
                        var seal = captured.exactTarget()
                                .orElseThrow(() ->
                                        new IllegalStateException("SEALED root lacks exact closed native metadata"));
                        return reads.openRunLedger(handle)
                                .thenCompose(open -> {
                                    if (!open.exactHandle().equals(java.util.Optional.of(handle))) {
                                        throw new IllegalStateException("closed native root handle is unavailable");
                                    }
                                    return frame(reads, handle, seal.sealedLastEntryId());
                                })
                                .thenAccept(last -> {
                                    if (!(last instanceof Nbke2RunFooterV1 footer)
                                            || !footer.runBinding().equals(binding)
                                            || footer.kafkaEndOffsetExclusive()
                                                    != root.kafkaEndOffsetExclusive()
                                                            .orElseThrow()
                                            || footer.lastPhysicalEntryIdExclusive()
                                                    != Math.addExact(seal.sealedLastEntryId(), 1)
                                            || footer.sealOwnerEpoch() < root.creatorOwnerEpoch()) {
                                        throw new IllegalStateException(
                                                "actual native run footer differs from the sealed root");
                                    }
                                });
                    });
                });
    }

    @Override
    public CompletionStage<Void> requireRecovered(KafkaRunRootRecordV2 record, KafkaOwnerAdmissionV1 closed) {
        return verify(record, 2, reads -> requireRecovered(record, closed, reads));
    }

    private CompletionStage<Void> requireRecovered(
            KafkaRunRootRecordV2 record, KafkaOwnerAdmissionV1 closed, RealBookKeeperCellSessionV1 reads) {
        var root = record.root();
        var cut = record.recoveryCut().orElseThrow(() -> new IllegalArgumentException("crash recovery cut is absent"));
        if (!closed.closed()
                || !cut.closedOwnerSha256().equals(Sha256Digest.hash(closed.encode()))
                || !closed.runs().contains(record.initialLink())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("crash cut differs from immutable owner closure"));
        }
        var active = new com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1(
                root.bindingId(),
                root.topicIncarnation(),
                root.partitionId(),
                root.storageEpochId(),
                root.creatorOwnerEpoch(),
                root.kafkaLeaderEpoch(),
                root.providerScopeId(),
                root.runId(),
                root.ledgerIdentity(),
                root.kafkaStartOffset(),
                java.util.OptionalLong.empty(),
                KafkaRunRootStateV1.ACTIVE,
                root.predecessorRunId());
        var handle =
                new RunLedgerHandleV1(root.providerScopeId(), root.runId(), root.ledgerIdentity(), capabilitySha256());
        var binding = new Nbke2RunBindingV1(
                root.bindingId(),
                root.topicIncarnation(),
                root.partitionId(),
                root.storageEpochId(),
                root.creatorOwnerEpoch(),
                root.kafkaLeaderEpoch(),
                root.providerScopeId(),
                root.runId());
        var fence = new com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1(
                root.bindingId(),
                root.topicIncarnation(),
                root.partitionId(),
                1,
                root.storageEpochId(),
                root.creatorOwnerEpoch(),
                root.kafkaLeaderEpoch());
        var envelope = com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1.selectedDefault();
        var request = new com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperRecoveryRequestV1(
                binding,
                handle,
                root.kafkaStartOffset(),
                java.util.OptionalLong.empty(),
                envelope,
                closed,
                record,
                fence,
                java.util.Optional.empty());
        // Validate physical commit sets independently. Full producer/transaction replay is composed across all runs by
        // ClosedHistoryRecovery, because a marker may end a transaction begun in a preceding run.
        var nativeAdapter = new com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1();
        var physicalRecovery = new com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperTakeoverRecoveryV1(
                reads,
                batch -> {
                    nativeAdapter.protocolDelta(batch);
                    return com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolBatchDeltaV1.nonIdempotent(
                            batch.endOffsetExclusive() - batch.baseOffset());
                },
                System::nanoTime);
        return requireNative(KafkaRunRootRecordV2.pending(record.resource().namespace(), active), reads)
                .thenCompose(ignored -> client.captureExactTarget(handle))
                .thenCompose(captured -> {
                    var seal = captured.exactTarget()
                            .orElseThrow(() -> new IllegalStateException("crash cut ledger is not natively closed"));
                    if (seal.sealedLastEntryId() != cut.recoveredLastAddConfirmed()) {
                        throw new IllegalStateException("crash cut LAC differs from exact native closed metadata");
                    }
                    return physicalRecovery.recover(request);
                })
                .thenAccept(result -> {
                    if (!result.recovered()
                            || result.newLeaderLeo().orElseThrow()
                                    != root.kafkaEndOffsetExclusive().orElseThrow()
                            || !result.conflictEntryId().equals(cut.inertFromEntryId())) {
                        throw new IllegalStateException(
                                "crash cut differs from the independently verified physical prefix");
                    }
                });
    }

    private CompletionStage<Nbke2FrameV1> frame(
            RealBookKeeperCellSessionV1 reads, RunLedgerHandleV1 handle, long entryId) {
        return reads.readExactEntry(handle, entryId).thenApply(read -> {
            var entry = read.exactEntry().orElseThrow(() -> new IllegalStateException("native root frame is absent"));
            if (!entry.handle().equals(handle) || entry.entryId() != entryId) {
                throw new IllegalStateException("native root read returned another ledger or entry");
            }
            return Nbke2CodecV1.decode(
                    entry.payload().toByteArray(), handle.ledgerIdentity().ledgerId(), entryId);
        });
    }

    private CompletionStage<Void> verify(
            KafkaRunRootRecordV2 record,
            int maximumHandles,
            Function<RealBookKeeperCellSessionV1, CompletionStage<Void>> verification) {
        if (draining) {
            return CompletableFuture.failedFuture(new IllegalStateException("native root verifier is draining"));
        }
        try {
            return Objects.requireNonNull(readOwner.read(record, maximumHandles, verification), "verification stage")
                    .thenApply(ignored -> null);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletionStage<Void> readStandalone(
            KafkaRunRootRecordV2 record,
            int maximumHandles,
            Function<RealBookKeeperCellSessionV1, CompletionStage<Void>> verification) {
        final RealBookKeeperCellSessionV1 reads;
        synchronized (this) {
            if (draining) {
                return CompletableFuture.failedFuture(new IllegalStateException("native root verifier is draining"));
            }
            reads = client.newSession();
            standaloneReads.add(reads);
        }
        CompletionStage<Void> work;
        try {
            work = Objects.requireNonNull(verification.apply(reads), "native verification stage");
        } catch (Throwable failure) {
            work = CompletableFuture.failedFuture(failure);
        }
        var terminal = new CompletableFuture<Void>();
        work.whenComplete((ignored, failure) -> closeStandalone(reads).whenComplete((closed, closeFailure) -> {
            if (closeFailure != null) {
                if (failure != null && failure != closeFailure) {
                    closeFailure.addSuppressed(failure);
                }
                terminal.completeExceptionally(closeFailure);
            } else if (failure != null) {
                terminal.completeExceptionally(failure);
            } else {
                terminal.complete(null);
            }
        }));
        return terminal;
    }

    private CompletionStage<Void> closeStandalone(RealBookKeeperCellSessionV1 reads) {
        try {
            return reads.closeAsync().thenRun(() -> standaloneReads.remove(reads));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public void close() {
        final java.util.List<RealBookKeeperCellSessionV1> reads;
        synchronized (this) {
            draining = true;
            reads = java.util.List.copyOf(standaloneReads);
        }
        CompletableFuture.allOf(reads.stream()
                        .map(session -> closeStandalone(session).toCompletableFuture())
                        .toArray(CompletableFuture[]::new))
                .join();
    }
}
