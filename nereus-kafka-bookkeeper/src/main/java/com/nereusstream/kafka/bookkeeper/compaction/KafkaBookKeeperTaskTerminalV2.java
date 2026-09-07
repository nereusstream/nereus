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
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

/** Cancelled selection and a fenced native physical cut; grace, reference rescans and deletion remain separate. */
public record KafkaBookKeeperTaskTerminalV2(
        KafkaBookKeeperInventoryV2.Task task,
        M5BookKeeperNativeCreateSpecV2 nativeCreateScope,
        M5TaskSelectionDecisionV2 selection,
        List<DrainedPart> physicalCut) {
    private static final int MAGIC = 0x4b425454; // KBTT
    public static final int MAX_BYTES = KafkaBookKeeperInventoryV2.MAX_TASK_BYTES;

    /** A missing inventory slot at the fenced cut cannot later acquire a native ledger through this create scope. */
    public record DrainedPart(
            Optional<BookKeeperLedgerIdentity> reservedId, Optional<BookKeeperDeleteTargetV1> sealed) {
        public DrainedPart {
            reservedId = Objects.requireNonNull(reservedId, "reservedId");
            sealed = Objects.requireNonNull(sealed, "sealed");
            if (sealed.isPresent()
                    && !reservedId.equals(
                            Optional.of(sealed.orElseThrow().handle().ledgerIdentity()))) {
                throw new IllegalArgumentException("drained native metadata differs from its inventoried ID");
            }
        }
    }

    public KafkaBookKeeperTaskTerminalV2 {
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(nativeCreateScope, "nativeCreateScope");
        Objects.requireNonNull(selection, "selection");
        physicalCut = List.copyOf(physicalCut);
        requireNativeScope(task, nativeCreateScope);
        if (!selection.taskId().equals(task.taskIdSha256())
                || !selection
                        .binding()
                        .equals(M5MaterializationCodecV1.decodeSourceCut(task.sourceCut())
                                .identity()
                                .binding())
                || selection.outcome() != M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED
                || physicalCut.size() != task.parts().size()) {
            throw new IllegalArgumentException(
                    "BK terminal requires one cancelled task and its complete bounded physical cut");
        }
        var ids = new java.util.HashSet<BookKeeperLedgerIdentity>();
        for (int ordinal = 0; ordinal < physicalCut.size(); ordinal++) {
            var drained = physicalCut.get(ordinal);
            if (drained.reservedId().isPresent()
                    && !ids.add(drained.reservedId().orElseThrow())) {
                throw new IllegalArgumentException("BK terminal repeats a native ledger ID");
            }
            if (drained.sealed().isPresent()) {
                var seal = drained.sealed().orElseThrow();
                var config = task.configuration(ordinal);
                var capability = task.capability();
                var expected = new RunLedgerHandleV1(
                        config.providerScopeId(),
                        config.runId(),
                        drained.reservedId().orElseThrow(),
                        config.configurationDigest());
                if (!seal.handle().equals(expected)
                        || seal.ensembleSize() != config.ensembleSize()
                        || seal.writeQuorumSize() != config.writeQuorumSize()
                        || seal.ackQuorumSize() != config.ackQuorumSize()
                        || seal.digestType() != config.digestType()
                        || !seal.passwordCredentialIdentityVersion().equals(capability.credentialIdentityVersion())
                        || seal.metadataFormatVersion() != 3
                        || seal.metadataSha256().isZero()
                        || !seal.passwordSha256().equals(Sha256Digest.hash(CanonicalBytes.empty()))) {
                    throw new IllegalArgumentException(
                            "BK terminal seal differs from its task's exact native configuration");
                }
            }
        }
    }

    static void requireNativeScope(KafkaBookKeeperInventoryV2.Task task, M5BookKeeperNativeCreateSpecV2 scope) {
        var expected = M5BookKeeperNativeCreateSpecV2.of(
                scope.nativeInstanceId(),
                task.taskIdSha256(),
                IntStream.range(0, task.parts().size())
                        .mapToObj(task::configuration)
                        .toList());
        if (!scope.equals(expected) || !task.namespace().equals(scope.namespace())) {
            throw new IllegalArgumentException("BK task terminal lacks its exact actual native create scope");
        }
    }

    public static String key(Sha256Digest taskId) {
        return KafkaBookKeeperInventoryV2.taskKey(taskId) + "/terminal";
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeInt(2);
            writeBytes(out, KafkaBookKeeperInventoryCodecV2.encodeTask(task));
            writeBytes(out, nativeCreateScope.encode());
            writeBytes(out, selection.encode());
            out.writeInt(physicalCut.size());
            for (var part : physicalCut) {
                out.writeByte(part.reservedId().isEmpty() ? 0 : part.sealed().isEmpty() ? 1 : 2);
                if (part.reservedId().isPresent()) {
                    out.writeLong(part.reservedId().orElseThrow().ledgerId());
                }
                if (part.sealed().isPresent()) {
                    var seal = part.sealed().orElseThrow();
                    out.writeLong(seal.sealedLastEntryId());
                    out.writeLong(seal.sealedLength());
                    out.writeInt(seal.metadataFormatVersion());
                    out.writeLong(seal.metadataCToken());
                    out.write(seal.passwordSha256().bytes().toByteArray());
                    out.write(seal.metadataSha256().bytes().toByteArray());
                }
            }
            out.flush();
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("BK task terminal exceeds its native value cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public static KafkaBookKeeperTaskTerminalV2 decode(CanonicalBytes bytes) {
        if (bytes.isEmpty() || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("BK task terminal byte length differs");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
            if (in.readInt() != MAGIC || in.readInt() != 2) {
                throw new IllegalArgumentException("BK task terminal magic or version differs");
            }
            var task = KafkaBookKeeperInventoryCodecV2.decodeTask(
                    readBytes(in, KafkaBookKeeperInventoryV2.MAX_TASK_BYTES));
            var scope = M5BookKeeperNativeCreateSpecV2.decode(readBytes(in, M5BookKeeperNativeCreateSpecV2.MAX_BYTES));
            var decision = M5TaskSelectionDecisionV2.decode(readBytes(in, M5TaskSelectionDecisionV2.MAX_BYTES));
            int count = in.readInt();
            if (count != task.parts().size()) {
                throw new IllegalArgumentException("BK terminal physical cut count differs");
            }
            var parts = new ArrayList<DrainedPart>();
            for (int ordinal = 0; ordinal < count; ordinal++) {
                int kind = in.readUnsignedByte();
                if (kind > 2) {
                    throw new IllegalArgumentException("BK terminal physical state differs");
                }
                var id = kind == 0
                        ? Optional.<BookKeeperLedgerIdentity>empty()
                        : Optional.of(new BookKeeperLedgerIdentity(in.readLong()));
                Optional<BookKeeperDeleteTargetV1> seal = Optional.empty();
                if (kind == 2) {
                    var run = task.configuration(ordinal);
                    long lac = in.readLong();
                    long length = in.readLong();
                    int version = in.readInt();
                    long token = in.readLong();
                    seal = Optional.of(new BookKeeperDeleteTargetV1(
                            new RunLedgerHandleV1(
                                    run.providerScopeId(), run.runId(), id.orElseThrow(), run.configurationDigest()),
                            lac,
                            length,
                            run.ensembleSize(),
                            run.writeQuorumSize(),
                            run.ackQuorumSize(),
                            run.digestType(),
                            task.capability().credentialIdentityVersion(),
                            digest(in),
                            version,
                            token,
                            digest(in)));
                }
                parts.add(new DrainedPart(id, seal));
            }
            var terminal = new KafkaBookKeeperTaskTerminalV2(task, scope, decision, parts);
            if (in.read() != -1 || !terminal.encode().equals(bytes)) {
                throw new IllegalArgumentException("BK task terminal is not canonical");
            }
            return terminal;
        } catch (IOException invalid) {
            throw new IllegalArgumentException("BK task terminal is truncated", invalid);
        }
    }

    private static void writeBytes(DataOutputStream out, CanonicalBytes bytes) throws IOException {
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }

    private static CanonicalBytes readBytes(DataInputStream in, int maximum) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > maximum || length > in.available()) {
            throw new IllegalArgumentException("BK task terminal component length exceeds its bound");
        }
        return CanonicalBytes.copyOf(in.readNBytes(length));
    }

    private static Sha256Digest digest(DataInputStream in) throws IOException {
        return Sha256Digest.copyOf(in.readNBytes(32));
    }
}
