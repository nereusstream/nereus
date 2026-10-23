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
import com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1.NativeRecoveryReadScope;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperNativeReadScopeV2.Snapshot;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One actual cold actor's initial physical read scope; fresh facts and complete other references remain required. */
public record KafkaBookKeeperNativeRecoveryScopeV2(
        Sha256Digest descriptorId,
        Sha256Digest nativeScopeSha256,
        KafkaOwnerIdentityV1 owner,
        KafkaOwnerIdentityV1 predecessor,
        AuthorityFactV1 ownerFact,
        AuthorityFactV1 closedOwnerFact,
        long readEpoch,
        long sourceGeneration,
        SelectorMode mode,
        Sha256Digest selectorCoreSha256,
        Snapshot snapshot,
        List<Long> rawLedgerIds,
        List<Long> selectedLedgerIds) {
    private static final int MAGIC = 0x4b425253; // KBRS
    public static final int MAX_BYTES = 16_384;
    private static final int MAX_KEY_BYTES = 512;
    private static final int MAX_VERSION_BYTES = 64;
    private static final int MAX_RAW_LEDGERS = KafkaBookKeeperClosedHistoryRecoveryV1.MAX_HISTORY_RUNS + 1;

    public KafkaBookKeeperNativeRecoveryScopeV2 {
        List.of(descriptorId, nativeScopeSha256, selectorCoreSha256).forEach(KafkaBookKeeperInventoryV2::requireDigest);
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(predecessor, "predecessor");
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(mode, "mode");
        rawLedgerIds = members(rawLedgerIds, MAX_RAW_LEDGERS);
        selectedLedgerIds = members(selectedLedgerIds, KafkaBookKeeperInventoryV2.MAX_PARTS);
        requireFact(ownerFact, "/owner-admission-v1");
        requireFact(closedOwnerFact, "/closed-owners-v1/" + predecessor.ownerEpoch());
        if (!owner.succeeds(predecessor)
                || readEpoch <= 0
                || sourceGeneration <= 0
                || mode != SelectorMode.PREFERRED_ONLY && mode != SelectorMode.PREFERRED_WITH_FALLBACK
                || rawLedgerIds.stream().anyMatch(selectedLedgerIds::contains)) {
            throw new IllegalArgumentException("cold native read scope has invalid owner/selector/physical membership");
        }
    }

    /** The qualification is privately produced by the installed native cold actor, after its real recovery. */
    public static KafkaBookKeeperNativeRecoveryScopeV2 verify(
            NativeRecoveryReadScope observation,
            KafkaOwnerAdmissionV1 current,
            AuthorityFactV1 currentFact,
            KafkaOwnerAdmissionV1 closed,
            AuthorityFactV1 closedFact) {
        return verify(observation, current, currentFact, closed, closedFact, Optional.empty());
    }

    public static KafkaBookKeeperNativeRecoveryScopeV2 verify(
            NativeRecoveryReadScope observation,
            KafkaOwnerAdmissionV1 current,
            AuthorityFactV1 currentFact,
            KafkaOwnerAdmissionV1 closed,
            AuthorityFactV1 closedFact,
            Optional<KafkaSealedBookKeeperDescriptorV2> predecessorDescriptor) {
        var inputs = observation.inputs();
        var scope = Sha256Digest.hash(
                KafkaRunRootRecordV2.Scope.of(inputs.sources().activeRoot()).encode());
        var selector = observation.selector();
        var fence = inputs.protocol().root().fence();
        if (current.closed()
                || !closed.closed()
                || !closed.equals(observation.predecessor())
                || !current.owner().succeeds(closed.owner())
                || !current.scopeSha256().equals(scope)
                || !closed.scopeSha256().equals(scope)
                || current.owner().ownerEpoch() != fence.ownerEpoch()
                || current.owner().kafkaLeaderEpoch() != fence.kafkaLeaderEpoch()
                || current.owner().ownerEpoch() != selector.ownerEpoch()
                || !currentFact.valueSha256().equals(Sha256Digest.hash(current.encode()))
                || !closedFact.valueSha256().equals(Sha256Digest.hash(closed.encode()))) {
            throw new IllegalArgumentException(
                    "cold read scope differs from its actual current/closed native Owner facts");
        }
        var raw = new ArrayList<Long>();
        inputs.sources()
                .sealedRoots()
                .forEach(root -> raw.add(root.ledgerIdentity().ledgerId()));
        raw.add(inputs.sources().activeRoot().ledgerIdentity().ledgerId());
        var descriptor = observation.descriptor();
        var value = new KafkaBookKeeperNativeRecoveryScopeV2(
                descriptor.descriptorSha256(),
                scope,
                current.owner(),
                closed.owner(),
                currentFact,
                closedFact,
                selector.readAdmissionEpoch(),
                selector.sourceGeneration(),
                selector.mode(),
                M4ReadControlCodecV1.selectorCoreSha256(selector),
                KafkaBookKeeperNativeReadScopeV2.snapshot(inputs, scope),
                raw.stream().sorted().toList(),
                descriptor.sealedParts().stream()
                        .map(part -> part.handle().ledgerIdentity().ledgerId())
                        .sorted()
                        .toList());
        value.requireDescriptor(descriptor, predecessorDescriptor);
        return value;
    }

    public void requireDescriptor(KafkaSealedBookKeeperDescriptorV2 descriptor) {
        requireDescriptor(descriptor, Optional.empty());
    }

    public void requireDescriptor(
            KafkaSealedBookKeeperDescriptorV2 descriptor,
            Optional<KafkaSealedBookKeeperDescriptorV2> predecessorDescriptor) {
        KafkaBookKeeperClosedHistoryRecoveryV1.SelectedPrefix.requirePredecessor(descriptor, predecessorDescriptor);
        var parts = descriptor.sealedParts().stream()
                .map(part -> part.handle().ledgerIdentity().ledgerId())
                .sorted()
                .toList();
        var covered = new ArrayList<>(descriptor.sourceCut().sources().stream()
                .filter(source -> source.kind() == SourceKind.BOOKKEEPER_LEDGER)
                .map(source -> source.ledgerIdentitySha256().orElseThrow())
                .toList());
        predecessorDescriptor.ifPresent(previous -> previous.sealedParts()
                .forEach(part -> covered.add(new PhysicalResourceIdV2.BookKeeperLedger(
                                previous.task().namespace(),
                                part.handle().ledgerIdentity().ledgerId())
                        .sha256())));
        if (!descriptorId.equals(descriptor.descriptorSha256())
                || sourceGeneration < descriptor.sourceGeneration()
                || !selectedLedgerIds.equals(parts)
                || rawLedgerIds.stream()
                        .map(id -> new PhysicalResourceIdV2.BookKeeperLedger(
                                        descriptor.task().namespace(), id)
                                .sha256())
                        .anyMatch(covered::contains)) {
            throw new IllegalArgumentException(
                    "cold read scope lacks exact selected parts or retains a covered raw ledger");
        }
    }

    public String key() {
        return "v2/kafka-bk-compaction-v2/views/" + descriptorId.toHex() + "/native-recovery-scope/"
                + owner.ownerEpoch() + "/" + readEpoch;
    }

    public CanonicalBytes encode() {
        var out = ByteBuffer.allocate(MAX_BYTES).putInt(MAGIC).putInt(1);
        putDigest(out, descriptorId);
        putDigest(out, nativeScopeSha256);
        putOwner(out, owner);
        putOwner(out, predecessor);
        putFact(out, ownerFact);
        putFact(out, closedOwnerFact);
        out.putLong(readEpoch).putLong(sourceGeneration).putInt(mode.ordinal());
        putDigest(out, selectorCoreSha256);
        out.putLong(snapshot.stateVersion());
        putDigest(out, snapshot.sourceMap());
        putDigest(out, snapshot.protocolRoot());
        putDigest(out, snapshot.checkpointRoot());
        for (var ids : List.of(rawLedgerIds, selectedLedgerIds)) {
            out.putInt(ids.size());
            ids.forEach(out::putLong);
        }
        var body = CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position()));
        putDigest(out, Sha256Digest.hash(body));
        return CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position()));
    }

    public static KafkaBookKeeperNativeRecoveryScopeV2 decode(CanonicalBytes bytes) {
        if (bytes.length() < 64 || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("cold native scope length is invalid");
        }
        byte[] raw = bytes.toByteArray();
        if (!Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(raw, raw.length - 32)))
                .equals(Sha256Digest.copyOf(Arrays.copyOfRange(raw, raw.length - 32, raw.length)))) {
            throw new IllegalArgumentException("cold native scope checksum is corrupt");
        }
        try {
            var in = ByteBuffer.wrap(raw, 0, raw.length - 32);
            if (in.getInt() != MAGIC || in.getInt() != 1) {
                throw new IllegalArgumentException("unknown cold native scope wire");
            }
            var descriptor = digest(in);
            var scope = digest(in);
            var owner = readOwner(in);
            var predecessor = readOwner(in);
            var ownerFact = readFact(in);
            var closedFact = readFact(in);
            long epoch = in.getLong();
            long generation = in.getLong();
            int ordinal = in.getInt();
            if (ordinal < 0 || ordinal >= SelectorMode.values().length) {
                throw new IllegalArgumentException("unknown cold native selector mode");
            }
            var selector = digest(in);
            var snapshot = new Snapshot(in.getLong(), digest(in), digest(in), digest(in));
            var value = new KafkaBookKeeperNativeRecoveryScopeV2(
                    descriptor,
                    scope,
                    owner,
                    predecessor,
                    ownerFact,
                    closedFact,
                    epoch,
                    generation,
                    SelectorMode.values()[ordinal],
                    selector,
                    snapshot,
                    readMembers(in, MAX_RAW_LEDGERS),
                    readMembers(in, KafkaBookKeeperInventoryV2.MAX_PARTS));
            if (in.hasRemaining() || !value.encode().equals(bytes)) {
                throw new IllegalArgumentException("cold native scope is noncanonical");
            }
            return value;
        } catch (BufferUnderflowException truncated) {
            throw new IllegalArgumentException("cold native scope is truncated", truncated);
        }
    }

    private static List<Long> members(List<Long> ids, int maximum) {
        ids = List.copyOf(ids);
        if (ids.isEmpty()
                || ids.size() > maximum
                || !ids.equals(ids.stream().sorted().distinct().toList())
                || ids.stream().anyMatch(id -> id < 0)) {
            throw new IllegalArgumentException("cold native scope ledger membership is noncanonical");
        }
        return ids;
    }

    private static List<Long> readMembers(ByteBuffer in, int maximum) {
        int count = in.getInt();
        if (count < 1 || count > maximum || count > in.remaining() / Long.BYTES) {
            throw new IllegalArgumentException("cold native scope ledger count is invalid");
        }
        var ids = new ArrayList<Long>(count);
        for (int index = 0; index < count; index++) {
            ids.add(in.getLong());
        }
        return ids;
    }

    private static void requireFact(AuthorityFactV1 fact, String suffix) {
        Objects.requireNonNull(fact, "fact");
        if (!fact.key().endsWith(suffix)
                || fact.key().getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES
                || fact.metadataVersion().value().length() > MAX_VERSION_BYTES) {
            throw new IllegalArgumentException("cold native scope Owner fact is outside its bounded route");
        }
    }

    private static void putOwner(ByteBuffer out, KafkaOwnerIdentityV1 value) {
        out.putLong(value.ownerEpoch())
                .putInt(value.kafkaLeaderEpoch())
                .putInt(value.brokerId())
                .putLong(value.brokerEpoch())
                .putLong(value.metadataOffset());
    }

    private static KafkaOwnerIdentityV1 readOwner(ByteBuffer in) {
        return new KafkaOwnerIdentityV1(in.getLong(), in.getInt(), in.getInt(), in.getLong(), in.getLong());
    }

    private static void putFact(ByteBuffer out, AuthorityFactV1 fact) {
        putBytes(out, fact.key().getBytes(StandardCharsets.UTF_8));
        putBytes(out, fact.metadataVersion().value().toByteArray());
        putDigest(out, fact.valueSha256());
    }

    private static AuthorityFactV1 readFact(ByteBuffer in) {
        return new AuthorityFactV1(
                new String(readBytes(in, MAX_KEY_BYTES), StandardCharsets.UTF_8),
                new MetadataVersion(CanonicalBytes.copyOf(readBytes(in, MAX_VERSION_BYTES))),
                digest(in));
    }

    private static void putBytes(ByteBuffer out, byte[] bytes) {
        out.putInt(bytes.length).put(bytes);
    }

    private static byte[] readBytes(ByteBuffer in, int maximum) {
        int length = in.getInt();
        if (length <= 0 || length > maximum || length > in.remaining()) {
            throw new IllegalArgumentException("cold native scope fact length is invalid");
        }
        byte[] value = new byte[length];
        in.get(value);
        return value;
    }

    private static void putDigest(ByteBuffer out, Sha256Digest value) {
        out.put(value.bytes().toByteArray());
    }

    private static Sha256Digest digest(ByteBuffer in) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        in.get(bytes);
        return Sha256Digest.copyOf(bytes);
    }
}
