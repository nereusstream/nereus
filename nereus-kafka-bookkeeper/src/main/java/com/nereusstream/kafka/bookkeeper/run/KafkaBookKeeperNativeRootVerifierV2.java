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
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootVerifierV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Reads actual NBKE2 header/footer and sealed metadata through the exact guarded BK client. Never fences a writer. */
public final class KafkaBookKeeperNativeRootVerifierV2 implements KafkaRunRootVerifierV2, AutoCloseable {
    private final M5BookKeeperNativeCreateClientV2 client;
    private final RealBookKeeperCellSessionV1 reads;

    public KafkaBookKeeperNativeRootVerifierV2(M5BookKeeperNativeCreateClientV2 client) {
        this.client = Objects.requireNonNull(client, "client");
        reads = client.newSession();
    }

    @Override
    public Sha256Digest capabilitySha256() {
        return reads.capabilitySnapshot().configurationDigest();
    }

    @Override
    public CompletionStage<Void> requireNative(KafkaRunRootRecordV2 record) {
        var root = record.root();
        var configuration = RunLedgerConfigurationV1.from(reads.capabilitySnapshot(), root.runId());
        if (!record.resource().namespace().equals(client.spec().namespace())
                || !client.spec().configurations().contains(configuration)
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
        return client.readNativeRunHeader(handle)
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
                                    return frame(handle, seal.sealedLastEntryId());
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

    private CompletionStage<Nbke2FrameV1> frame(RunLedgerHandleV1 handle, long entryId) {
        return reads.readExactEntry(handle, entryId).thenApply(read -> {
            var entry = read.exactEntry().orElseThrow(() -> new IllegalStateException("native root frame is absent"));
            if (!entry.handle().equals(handle) || entry.entryId() != entryId) {
                throw new IllegalStateException("native root read returned another ledger or entry");
            }
            return Nbke2CodecV1.decode(
                    entry.payload().toByteArray(), handle.ledgerIdentity().ledgerId(), entryId);
        });
    }

    @Override
    public void close() {
        reads.closeAsync().toCompletableFuture().join();
    }
}
