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
import com.nereusstream.kafka.bookkeeper.read.KafkaObjectMaterializedPrefixV1;
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
