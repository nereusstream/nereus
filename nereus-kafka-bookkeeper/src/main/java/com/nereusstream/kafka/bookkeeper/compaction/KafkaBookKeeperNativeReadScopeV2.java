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
package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperNativeFallbackRetirementV2;
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.NativeInputSources;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionStateReferenceV1;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One live actor's confirmed physical source/checkpoint transfer, bound to its observed native Owner.
 * The record is historical evidence; fresh Owner/selector, other references, floors and grace remain required.
 */
public record KafkaBookKeeperNativeReadScopeV2(
        Sha256Digest taskId,
        Sha256Digest descriptorId,
        Sha256Digest nativeScopeSha256,
        KafkaOwnerIdentityV1 owner,
        AuthorityFactV1 ownerFact,
        long closedReadEpoch,
        Sha256Digest selectorCoreSha256,
        Sha256Digest batchId,
        Sha256Digest terminalSha256,
        Sha256Digest proofSha256,
        Snapshot before,
        Snapshot after,
        List<Long> retiredLedgerIds,
        Optional<Sha256Digest> predecessorDescriptorId) {
    private static final int MAGIC = 0x4b424e53; // KBNS
    public static final int MAX_BYTES = 4096;
    private static final int MAX_KEY_BYTES = 512;
    private static final int MAX_VERSION_BYTES = 64;

    public KafkaBookKeeperNativeReadScopeV2(
            Sha256Digest taskId,
            Sha256Digest descriptorId,
            Sha256Digest nativeScopeSha256,
            KafkaOwnerIdentityV1 owner,
            AuthorityFactV1 ownerFact,
            long closedReadEpoch,
            Sha256Digest selectorCoreSha256,
            Sha256Digest batchId,
            Sha256Digest terminalSha256,
            Sha256Digest proofSha256,
            Snapshot before,
            Snapshot after,
            List<Long> retiredLedgerIds) {
        this(
                taskId,
                descriptorId,
                nativeScopeSha256,
                owner,
                ownerFact,
                closedReadEpoch,
                selectorCoreSha256,
                batchId,
                terminalSha256,
                proofSha256,
                before,
                after,
                retiredLedgerIds,
                Optional.empty());
    }

    public record Snapshot(
            long stateVersion, Sha256Digest sourceMap, Sha256Digest protocolRoot, Sha256Digest checkpointRoot) {
        public Snapshot {
            if (stateVersion < 0) {
                throw new IllegalArgumentException("negative native source state version");
            }
            List.of(sourceMap, protocolRoot, checkpointRoot).forEach(KafkaBookKeeperInventoryV2::requireDigest);
        }
    }

    public KafkaBookKeeperNativeReadScopeV2 {
        List.of(taskId, descriptorId, nativeScopeSha256, selectorCoreSha256, batchId, terminalSha256, proofSha256)
                .forEach(KafkaBookKeeperInventoryV2::requireDigest);
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(ownerFact, "ownerFact");
        Objects.requireNonNull(before, "before");
        Objects.requireNonNull(after, "after");
        Objects.requireNonNull(predecessorDescriptorId, "predecessorDescriptorId");
        predecessorDescriptorId.ifPresent(KafkaBookKeeperInventoryV2::requireDigest);
        retiredLedgerIds = List.copyOf(retiredLedgerIds);
        if (closedReadEpoch <= 0
                || after.stateVersion() != Math.incrementExact(before.stateVersion())
                || before.sourceMap().equals(after.sourceMap())
                || before.protocolRoot().equals(after.protocolRoot())) {
            throw new IllegalArgumentException("native read scope lacks an actual source transition");
        }
        if (retiredLedgerIds.isEmpty()
                || retiredLedgerIds.size() > M4ReadControlRecordsV1.MAX_SOURCES_PER_BATCH
                || !retiredLedgerIds.equals(
                        retiredLedgerIds.stream().sorted().distinct().toList())
                || retiredLedgerIds.stream().anyMatch(id -> id < 0)) {
            throw new IllegalArgumentException("native read scope physical members are noncanonical");
        }
        byte[] key = ownerFact.key().getBytes(StandardCharsets.UTF_8);
        if (key.length == 0
                || key.length > MAX_KEY_BYTES
                || !ownerFact.key().endsWith("/owner-admission-v1")
                || ownerFact.metadataVersion().value().isEmpty()
                || ownerFact.metadataVersion().value().length() > MAX_VERSION_BYTES) {
            throw new IllegalArgumentException("native read scope Owner fact is outside its bounded route");
        }
    }

    /** Production consumes the privately owned actual transfer; a data record alone qualifies no native operation. */
    public static KafkaBookKeeperNativeReadScopeV2 verify(
            KafkaBookKeeperNativeFallbackRetirementV2.Result result,
            KafkaOwnerAdmissionV1 admission,
            AuthorityFactV1 fact) {
        var transfer = result.transfer();
        var descriptor = transfer.original().descriptor();
        var scope = Sha256Digest.hash(
                KafkaRunRootRecordV2.Scope.of(transfer.before().sources().activeRoot())
                        .encode());
        var fence = transfer.before().protocol().root().fence();
        if (admission.closed()
                || !admission.scopeSha256().equals(scope)
                || admission.owner().ownerEpoch() != fence.ownerEpoch()
                || admission.owner().kafkaLeaderEpoch() != fence.kafkaLeaderEpoch()
                || admission.owner().ownerEpoch() != result.selector().ownerEpoch()
                || !fact.valueSha256().equals(Sha256Digest.hash(admission.encode()))) {
            throw new IllegalArgumentException("native transferred scope differs from its actual open Owner");
        }
        long end = descriptor.sourceCut().coverage().exclusiveEnd();
        var members = new ArrayList<>(transfer.before().sources().sealedRoots().stream()
                .filter(root -> root.kafkaStartOffset() < end
                        && root.kafkaEndOffsetExclusive().orElseThrow() > root.kafkaStartOffset())
                .map(root -> root.ledgerIdentity().ledgerId())
                .toList());
        var previous = transfer.original().publicationPreferred();
        previous.ifPresent(value -> value.sealedParts()
                .forEach(part -> members.add(part.handle().ledgerIdentity().ledgerId())));
        var value = new KafkaBookKeeperNativeReadScopeV2(
                descriptor.task().taskIdSha256(),
                descriptor.descriptorSha256(),
                scope,
                admission.owner(),
                fact,
                result.terminal().readAdmissionEpoch(),
                M4ReadControlCodecV1.selectorCoreSha256(result.selector()),
                result.batch().batchIdSha256(),
                M4ReadControlCodecV1.terminalSha256(result.terminal()),
                Sha256Digest.hash(M4ReadControlCodecV1.encodeProof(result.proof())),
                snapshot(transfer.before(), scope),
                snapshot(transfer.after(), scope),
                members.stream().sorted().toList(),
                previous.map(KafkaSealedBookKeeperDescriptorV2::descriptorSha256));
        value.requireDescriptor(descriptor, previous);
        return value;
    }

    public void requireDescriptor(KafkaSealedBookKeeperDescriptorV2 descriptor) {
        requireDescriptor(descriptor, Optional.empty());
    }

    public void requireDescriptor(
            KafkaSealedBookKeeperDescriptorV2 descriptor, Optional<KafkaSealedBookKeeperDescriptorV2> previous) {
        if (!taskId.equals(descriptor.task().taskIdSha256()) || !descriptorId.equals(descriptor.descriptorSha256())) {
            throw new IllegalArgumentException("native read scope names another selected descriptor");
        }
        var actual = retiredLedgerIds.stream()
                .map(id -> new PhysicalResourceIdV2.BookKeeperLedger(
                                descriptor.task().namespace(), id)
                        .sha256())
                .sorted(Comparator.comparing(Sha256Digest::toHex))
                .toList();
        if (!predecessorDescriptorId.equals(previous.map(KafkaSealedBookKeeperDescriptorV2::descriptorSha256))) {
            throw new IllegalArgumentException("native read scope lacks its exact selected predecessor descriptor");
        }
        var sources = descriptor.sourceCut().sources();
        var expected = new ArrayList<>(sources.stream()
                .filter(source -> source.kind() == SourceKind.BOOKKEEPER_LEDGER)
                .map(source -> source.ledgerIdentitySha256().orElseThrow())
                .toList());
        if (previous.isPresent()) {
            var prior = previous.orElseThrow();
            var first = sources.get(0);
            var sourceId = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(36)
                    .putInt(0x4d355347)
                    .put(prior.descriptorSha256().bytes().toByteArray())
                    .array()));
            if (first.kind() != SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2
                    || !first.sourceIdentitySha256().equals(sourceId)
                    || !first.formatRootSha256().equals(prior.descriptorSha256())
                    || !first.coverage().equals(prior.sourceCut().coverage())
                    || !descriptor.sourceCut().predecessorViewSha256().equals(prior.descriptorSha256())
                    || !prior.task().namespace().equals(descriptor.task().namespace())
                    || !prior.sourceCut()
                            .identity()
                            .binding()
                            .equals(descriptor.sourceCut().identity().binding())) {
                throw new IllegalArgumentException("native read scope selected predecessor membership differs");
            }
            prior.sealedParts()
                    .forEach(part -> expected.add(new PhysicalResourceIdV2.BookKeeperLedger(
                                    prior.task().namespace(),
                                    part.handle().ledgerIdentity().ledgerId())
                            .sha256()));
        }
        int generations = previous.isPresent() ? 1 : 0;
        if (sources.size() != expectedRawSources(descriptor) + generations
                || !actual.equals(expected.stream()
                        .sorted(Comparator.comparing(Sha256Digest::toHex))
                        .toList())) {
            throw new IllegalArgumentException("native read scope lacks the exact physical source cut");
        }
    }

    private static long expectedRawSources(KafkaSealedBookKeeperDescriptorV2 descriptor) {
        return descriptor.sourceCut().sources().stream()
                .filter(source -> source.kind() == SourceKind.BOOKKEEPER_LEDGER)
                .count();
    }

    public String key() {
        return key(descriptorId, owner.ownerEpoch(), closedReadEpoch);
    }

    public static String key(Sha256Digest descriptor, long ownerEpoch, long readEpoch) {
        KafkaBookKeeperInventoryV2.requireDigest(descriptor);
        if (ownerEpoch <= 0 || readEpoch <= 0) {
            throw new IllegalArgumentException("invalid native read scope epochs");
        }
        return "v2/kafka-bk-compaction-v2/views/" + descriptor.toHex() + "/native-read-scope/" + ownerEpoch + "/"
                + readEpoch;
    }

    public CanonicalBytes encode() {
        var out = ByteBuffer.allocate(MAX_BYTES).putInt(MAGIC).putInt(predecessorDescriptorId.isPresent() ? 2 : 1);
        for (var digest : List.of(taskId, descriptorId, nativeScopeSha256)) {
            putDigest(out, digest);
        }
        out.putLong(owner.ownerEpoch())
                .putInt(owner.kafkaLeaderEpoch())
                .putInt(owner.brokerId())
                .putLong(owner.brokerEpoch())
                .putLong(owner.metadataOffset())
                .putLong(closedReadEpoch);
        putBytes(out, ownerFact.key().getBytes(StandardCharsets.UTF_8));
        putBytes(out, ownerFact.metadataVersion().value().toByteArray());
        for (var digest : List.of(ownerFact.valueSha256(), selectorCoreSha256, batchId, terminalSha256, proofSha256)) {
            putDigest(out, digest);
        }
        putSnapshot(out, before);
        putSnapshot(out, after);
        out.putInt(retiredLedgerIds.size());
        retiredLedgerIds.forEach(out::putLong);
        predecessorDescriptorId.ifPresent(value -> putDigest(out, value));
        var body = CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position()));
        putDigest(out, Sha256Digest.hash(body));
        return CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position()));
    }

    public static KafkaBookKeeperNativeReadScopeV2 decode(CanonicalBytes bytes) {
        if (bytes.length() < 64 || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("native read scope length is invalid");
        }
        byte[] raw = bytes.toByteArray();
        if (!Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(raw, raw.length - 32)))
                .equals(Sha256Digest.copyOf(Arrays.copyOfRange(raw, raw.length - 32, raw.length)))) {
            throw new IllegalArgumentException("native read scope checksum is corrupt");
        }
        try {
            var in = ByteBuffer.wrap(raw, 0, raw.length - 32);
            int magic = in.getInt();
            int wire = in.getInt();
            if (magic != MAGIC || wire != 1 && wire != 2) {
                throw new IllegalArgumentException("unknown native read scope wire");
            }
            var task = digest(in);
            var descriptor = digest(in);
            var scope = digest(in);
            var owner = new KafkaOwnerIdentityV1(in.getLong(), in.getInt(), in.getInt(), in.getLong(), in.getLong());
            long epoch = in.getLong();
            var fact = new AuthorityFactV1(
                    new String(readBytes(in, MAX_KEY_BYTES), StandardCharsets.UTF_8),
                    new MetadataVersion(CanonicalBytes.copyOf(readBytes(in, MAX_VERSION_BYTES))),
                    digest(in));
            var selector = digest(in);
            var batch = digest(in);
            var terminal = digest(in);
            var proof = digest(in);
            var before = readSnapshot(in);
            var after = readSnapshot(in);
            int count = in.getInt();
            if (count < 1 || count > M4ReadControlRecordsV1.MAX_SOURCES_PER_BATCH) {
                throw new IllegalArgumentException("native read scope member count is invalid");
            }
            var members = new ArrayList<Long>(count);
            for (int i = 0; i < count; i++) {
                members.add(in.getLong());
            }
            var value = new KafkaBookKeeperNativeReadScopeV2(
                    task,
                    descriptor,
                    scope,
                    owner,
                    fact,
                    epoch,
                    selector,
                    batch,
                    terminal,
                    proof,
                    before,
                    after,
                    members,
                    wire == 2 ? Optional.of(digest(in)) : Optional.empty());
            if (in.hasRemaining() || !value.encode().equals(bytes)) {
                throw new IllegalArgumentException("native read scope is noncanonical");
            }
            return value;
        } catch (BufferUnderflowException truncated) {
            throw new IllegalArgumentException("native read scope is truncated", truncated);
        }
    }

    static Snapshot snapshot(NativeInputSources sources, Sha256Digest scope) {
        var root = sources.protocol().root();
        var f = root.frontiers();
        var refs = root.references();
        var out = ByteBuffer.allocate(472).putInt(0x4b424e52);
        putDigest(out, scope);
        out.putLong(root.stateVersion())
                .putLong(f.trimStartOffset())
                .putLong(f.allocatedEndOffset())
                .putLong(f.durableEndOffset())
                .putLong(f.readableEndOffset())
                .putLong(f.highWatermark())
                .putLong(f.lastStableOffset())
                .putLong(root.fence().bindingGeneration())
                .putLong(root.fence().ownerEpoch())
                .putInt(root.fence().kafkaLeaderEpoch());
        for (KafkaPartitionStateReferenceV1 ref : List.of(
                refs.runTable(),
                refs.activeTail(),
                refs.sourceMap(),
                refs.committedProducerState(),
                refs.speculativeProducerQueue(),
                refs.transactionIndex(),
                refs.leaderEpochIndex(),
                refs.checkpointVector(),
                refs.sourceProtection())) {
            out.putLong(ref.generation());
            putDigest(out, ref.contentDigest());
        }
        return new Snapshot(
                root.stateVersion(),
                sources.sources().selectedViewSha256(),
                Sha256Digest.hash(CanonicalBytes.copyOf(out.array())),
                refs.checkpointVector().contentDigest());
    }

    private static void putSnapshot(ByteBuffer out, Snapshot value) {
        out.putLong(value.stateVersion());
        for (var digest : List.of(value.sourceMap(), value.protocolRoot(), value.checkpointRoot())) {
            putDigest(out, digest);
        }
    }

    private static Snapshot readSnapshot(ByteBuffer in) {
        return new Snapshot(in.getLong(), digest(in), digest(in), digest(in));
    }

    private static void putDigest(ByteBuffer out, Sha256Digest value) {
        out.put(value.bytes().toByteArray());
    }

    private static Sha256Digest digest(ByteBuffer in) {
        byte[] bytes = new byte[32];
        in.get(bytes);
        return Sha256Digest.copyOf(bytes);
    }

    private static void putBytes(ByteBuffer out, byte[] bytes) {
        out.putInt(bytes.length).put(bytes);
    }

    private static byte[] readBytes(ByteBuffer in, int maximum) {
        int length = in.getInt();
        if (length < 1 || length > maximum) {
            throw new IllegalArgumentException("native read scope field exceeds its bound");
        }
        byte[] bytes = new byte[length];
        in.get(bytes);
        return bytes;
    }
}
