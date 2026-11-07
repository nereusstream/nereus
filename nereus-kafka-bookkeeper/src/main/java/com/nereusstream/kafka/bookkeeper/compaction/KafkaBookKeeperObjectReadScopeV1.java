/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ProtocolCheckpointV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Immutable native read scope after Object fallback release and a real retained BK compound checkpoint. */
public record KafkaBookKeeperObjectReadScopeV1(
        BindingIdentity binding,
        Sha256Digest manifest,
        Sha256Digest sourceCut,
        long ownerEpoch,
        long lastFallbackReadEpoch,
        Sha256Digest retirementBatch,
        Sha256Digest releaseProofHead,
        long prefixEnd,
        StorageRunId retainedRun,
        long retainedLedger,
        long checkpointEntry,
        Sha256Digest checkpointFrame,
        Sha256Digest footerFrame) {
    private static final int MAGIC = 0x4b424f53; // KBOS
    public static final int BYTES = Integer.BYTES + 9 * Sha256Digest.LENGTH + Id128.LENGTH + 5 * Long.BYTES;

    public KafkaBookKeeperObjectReadScopeV1 {
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(retainedRun, "retainedRun");
        if (List.of(manifest, sourceCut, retirementBatch, releaseProofHead, checkpointFrame, footerFrame).stream()
                        .anyMatch(Sha256Digest::isZero)
                || ownerEpoch <= 0
                || lastFallbackReadEpoch <= 0
                || prefixEnd <= 0
                || retainedLedger < 0
                || checkpointEntry <= 0) {
            throw new IllegalArgumentException("Kafka Object native read scope lacks its exact retained authority");
        }
    }

    public static String key(Sha256Digest manifest) {
        return "v2/kafka-bk-compaction-v2/views/" + manifest.toHex() + "/native-object-read-scope";
    }

    public String key() {
        return key(manifest);
    }

    public void requireSelection(KafkaObjectMaterializedPrefixV1 prefix, BindingReadSelector selector) {
        var cut = prefix.sourceCut();
        if (!cut.identity().binding().equals(binding)
                || !selector.binding().equals(binding)
                || !selector.selectedViewSha256().equals(manifest)
                || !prefix.manifestSha256().equals(manifest)
                || !M5MaterializationCodecV1.sourceCutSha256(cut).equals(sourceCut)
                || prefix.coverage().exclusiveEnd() != prefixEnd
                || selector.mode() != SelectorMode.PREFERRED_ONLY
                || selector.admissionState() != AdmissionState.ADMITTING
                || !selector.capability().equals(cut.identity().capability())
                || selector.readAdmissionEpoch() <= lastFallbackReadEpoch
                || ownerEpoch > selector.ownerEpoch()
                || ownerEpoch < cut.identity().ownerEpoch()) {
            throw new IllegalStateException("Kafka Object native read scope is outside its exact selected prefix");
        }
    }

    public boolean namesRetainedCarrier(KafkaRunRootSnapshotV1 root) {
        return root.runId().equals(retainedRun)
                && root.ledgerIdentity().ledgerId() == retainedLedger
                && root.kafkaStartOffset() >= prefixEnd
                && root.kafkaEndOffsetExclusive().isPresent()
                && root.creatorOwnerEpoch() == ownerEpoch;
    }

    /** Re-read the retained carrier under the caller's actual native SDK handle and physical reader ticket. */
    public CompletionStage<CarrierProof> readRetainedCarrier(
            BookKeeperCellSession owned, KafkaRunRootSnapshotV1 root, long sealedLastEntry) {
        if (!namesRetainedCarrier(root) || sealedLastEntry <= checkpointEntry) {
            throw new IllegalStateException("Kafka Object proof does not name this sealed checkpoint carrier");
        }
        var handle = new RunLedgerHandleV1(
                root.providerScopeId(),
                root.runId(),
                root.ledgerIdentity(),
                owned.capabilitySnapshot().configurationDigest());
        var expected = new Nbke2RunBindingV1(
                root.bindingId(),
                root.topicIncarnation(),
                root.partitionId(),
                root.storageEpochId(),
                root.creatorOwnerEpoch(),
                root.kafkaLeaderEpoch(),
                root.providerScopeId(),
                root.runId());
        return owned.openRunLedger(handle)
                .thenCompose(open -> {
                    if (open.outcome() != RunLedgerOpenOutcomeV1.OPENED_EXACT
                            || !open.exactHandle().equals(Optional.of(handle))) {
                        throw new IllegalStateException("Kafka Object retained carrier is not physically available");
                    }
                    return readExact(owned, handle, sealedLastEntry);
                })
                .thenCompose(footer -> {
                    var footerHash = Sha256Digest.hash(footer);
                    if (!footerHash.equals(footerFrame)
                            || !(Nbke2CodecV1.decode(footer.toByteArray(), retainedLedger, sealedLastEntry)
                                    instanceof Nbke2RunFooterV1 decoded)
                            || !decoded.runBinding().equals(expected)
                            || decoded.protocolCheckpointEntryId() != checkpointEntry
                            || decoded.kafkaEndOffsetExclusive()
                                    != root.kafkaEndOffsetExclusive().orElseThrow()
                            || decoded.lastPhysicalEntryIdExclusive() != sealedLastEntry + 1) {
                        throw new IllegalStateException("Kafka Object retained carrier has another exact footer");
                    }
                    return readExact(owned, handle, checkpointEntry).thenApply(frame -> {
                        var checkpointHash = Sha256Digest.hash(frame);
                        if (!checkpointHash.equals(checkpointFrame)
                                || !(Nbke2CodecV1.decode(frame.toByteArray(), retainedLedger, checkpointEntry)
                                        instanceof Nbke2ProtocolCheckpointV1 checkpoint)
                                || !checkpoint.runBinding().equals(expected)) {
                            throw new IllegalStateException(
                                    "Kafka Object retained carrier has another exact checkpoint");
                        }
                        var vector = KafkaProtocolCheckpointStateV1.fromNbke2(checkpoint)
                                .vector();
                        if (!vector.isAlignedCompoundCheckpoint()
                                || vector.recoveryCoveredThrough()
                                        != root.kafkaEndOffsetExclusive().orElseThrow()) {
                            throw new IllegalStateException(
                                    "Kafka Object retained checkpoint has another coherent cut");
                        }
                        return new CarrierProof(checkpointHash, footerHash);
                    });
                });
    }

    public record CarrierProof(Sha256Digest checkpointFrame, Sha256Digest footerFrame) {}

    private static CompletionStage<CanonicalBytes> readExact(
            BookKeeperCellSession owned, RunLedgerHandleV1 handle, long entry) {
        return owned.readExactEntry(handle, entry).thenApply(observed -> {
            var value = observed.exactEntry().orElseThrow();
            if (observed.outcome() != RunLedgerReadOutcomeV1.FOUND_EXACT
                    || !value.handle().equals(handle)
                    || value.entryId() != entry) {
                throw new IllegalStateException("Kafka Object retained carrier returned another BK entry");
            }
            return value.payload();
        });
    }

    public CanonicalBytes encode() {
        return CanonicalBytes.copyOf(ByteBuffer.allocate(BYTES)
                .putInt(MAGIC)
                .put(binding.bindingId().digest().bytes().toByteArray())
                .put(binding.incarnationSha256().bytes().toByteArray())
                .put(binding.storageEpochSha256().bytes().toByteArray())
                .put(manifest.bytes().toByteArray())
                .put(sourceCut.bytes().toByteArray())
                .putLong(ownerEpoch)
                .putLong(lastFallbackReadEpoch)
                .put(retirementBatch.bytes().toByteArray())
                .put(releaseProofHead.bytes().toByteArray())
                .putLong(prefixEnd)
                .put(retainedRun.value().bytes().toByteArray())
                .putLong(retainedLedger)
                .putLong(checkpointEntry)
                .put(checkpointFrame.bytes().toByteArray())
                .put(footerFrame.bytes().toByteArray())
                .array());
    }

    public static KafkaBookKeeperObjectReadScopeV1 decode(CanonicalBytes bytes) {
        if (bytes.length() != BYTES) {
            throw new IllegalArgumentException("Kafka Object native read scope has another length");
        }
        var input = ByteBuffer.wrap(bytes.toByteArray());
        if (input.getInt() != MAGIC) {
            throw new IllegalArgumentException("Kafka Object native read scope has another format");
        }
        var binding = new BindingIdentity(new TopicBindingId(readDigest(input)), readDigest(input), readDigest(input));
        var manifest = readDigest(input);
        var cut = readDigest(input);
        long owner = input.getLong();
        long fallbackEpoch = input.getLong();
        var batch = readDigest(input);
        var releaseHead = readDigest(input);
        long end = input.getLong();
        var run = new byte[Id128.LENGTH];
        input.get(run);
        return new KafkaBookKeeperObjectReadScopeV1(
                binding,
                manifest,
                cut,
                owner,
                fallbackEpoch,
                batch,
                releaseHead,
                end,
                new StorageRunId(Id128.fromBytes(run)),
                input.getLong(),
                input.getLong(),
                readDigest(input),
                readDigest(input));
    }

    private static Sha256Digest readDigest(ByteBuffer input) {
        var value = new byte[Sha256Digest.LENGTH];
        input.get(value);
        return Sha256Digest.copyOf(value);
    }
}
