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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Part;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.PartKind;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.PartPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Task;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ProviderKind;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

/** Strict KBIV2 immutable task/part wire format. It cannot be interpreted as a selected output or delete intent. */
public final class KafkaBookKeeperInventoryCodecV2 {
    private static final int MAGIC = 0x4b424956;

    private KafkaBookKeeperInventoryCodecV2() {}

    public static CanonicalBytes encodeTask(Task task) {
        Objects.requireNonNull(task, "task");
        return encode(1, output -> {
            writeText(output, "BOOKKEEPER_WAL_ONLY");
            writeBytes(output, task.sourceCut());
            writeDigest(output, task.compactionPlanRootSha256());
            writeDigest(output, task.semanticOutputSha256());
            writeDigest(output, task.semanticValidationRootSha256());
            writeBytes(output, task.namespace().serviceIdentity().bytes());
            writeBytes(output, task.namespace().containerIdentity().bytes());
            writeCapability(output, task.capability());
            output.writeLong(task.attempt());
            output.writeInt(task.parts().size());
            for (PartPlan part : task.parts()) {
                output.writeByte(part.kind() == PartKind.DATA ? 1 : 2);
                output.writeInt(part.entryCount());
                output.writeLong(part.length());
                writeDigest(output, part.entriesRootSha256());
            }
        });
    }

    public static Task decodeTask(CanonicalBytes bytes) {
        return decode(bytes, 1, input -> {
            if (!readText(input).equals("BOOKKEEPER_WAL_ONLY")) {
                throw new IllegalArgumentException("BK inventory profile differs");
            }
            CanonicalBytes cut = readBytes(input, KafkaBookKeeperInventoryV2.MAX_TASK_BYTES / 2);
            Sha256Digest plan = readDigest(input);
            Sha256Digest semantic = readDigest(input);
            Sha256Digest proof = readDigest(input);
            Namespace namespace = new Namespace(
                    ProviderKind.BOOKKEEPER,
                    CanonicalUtf8.fromBytes(readBytes(input, 8192).toByteArray()),
                    CanonicalUtf8.fromBytes(readBytes(input, 8192).toByteArray()));
            BookKeeperCapabilitySnapshotV1 capability = readCapability(input);
            long attempt = input.readLong();
            int count = input.readInt();
            if (count <= 0 || count > KafkaBookKeeperInventoryV2.MAX_PARTS) {
                throw new IllegalArgumentException("BK inventory part count exceeds bound");
            }
            var parts = new ArrayList<PartPlan>(count);
            for (int index = 0; index < count; index++) {
                PartKind kind =
                        switch (input.readUnsignedByte()) {
                            case 1 -> PartKind.DATA;
                            case 2 -> PartKind.INDEX;
                            default -> throw new IllegalArgumentException("unknown BK inventory part kind");
                        };
                parts.add(new PartPlan(kind, input.readInt(), input.readLong(), readDigest(input)));
            }
            Task task = new Task(cut, plan, semantic, proof, namespace, capability, attempt, parts);
            requireCanonical(bytes, encodeTask(task));
            return task;
        });
    }

    public static CanonicalBytes encodePart(Part part) {
        Objects.requireNonNull(part, "part");
        return encode(2, output -> {
            writeDigest(output, part.taskIdSha256());
            output.writeInt(part.ordinal());
            writeBytes(output, PhysicalResourceIdCodecV2.encode(part.resource()));
            writeDigest(output, part.handle().providerScopeId().digest());
            output.write(part.handle().runId().value().bytes().toByteArray());
            writeDigest(output, part.handle().configurationDigest());
        });
    }

    public static Part decodePart(CanonicalBytes bytes) {
        return decode(bytes, 2, input -> {
            Sha256Digest taskId = readDigest(input);
            int ordinal = input.readInt();
            var resource = PhysicalResourceIdCodecV2.decode(readBytes(input, 65536));
            if (!(resource instanceof BookKeeperLedger ledger)) {
                throw new IllegalArgumentException("BK inventory contains a non-BK resource");
            }
            CellProviderScopeId scope = new CellProviderScopeId(readDigest(input));
            byte[] run = new byte[Id128.LENGTH];
            input.readFully(run);
            var handle = new RunLedgerHandleV1(
                    scope,
                    new StorageRunId(Id128.fromBytes(run)),
                    new BookKeeperLedgerIdentity(ledger.ledgerId()),
                    readDigest(input));
            Part part = new Part(taskId, ordinal, ledger, handle);
            requireCanonical(bytes, encodePart(part));
            return part;
        });
    }

    static CanonicalBytes encodeRunIdentity(Sha256Digest taskId, int ordinal) {
        return encode(3, output -> {
            writeDigest(output, taskId);
            output.writeInt(ordinal);
        });
    }

    private static void writeCapability(DataOutputStream output, BookKeeperCapabilitySnapshotV1 value)
            throws IOException {
        writeDigest(output, value.providerScopeId().digest());
        writeText(output, value.clientSourceCommit());
        writeDigest(output, value.clientArtifactSha256());
        writeText(output, value.serverSourceCommit());
        writeDigest(output, value.serverImageManifestSha256());
        writeText(output, value.protocolMode().name());
        output.writeInt(value.clientFrameLimitBytes());
        output.writeInt(value.serverFrameLimitBytes());
        output.writeInt(value.maximumAddPayloadBytes());
        output.writeBoolean(value.explicitEntryIdsSupported());
        output.writeInt(value.ensembleSize());
        output.writeInt(value.writeQuorumSize());
        output.writeInt(value.ackQuorumSize());
        writeText(output, value.digestType().name());
        output.writeBoolean(value.fencingSupported());
        output.writeBoolean(value.recoverySupported());
        output.writeLong(value.timeoutClass().connectMillis());
        output.writeLong(value.timeoutClass().addMillis());
        output.writeLong(value.timeoutClass().readMillis());
        output.writeLong(value.timeoutClass().recoveryMillis());
        writeText(output, value.credentialIdentityVersion());
        writeDigest(output, value.configurationDigest());
    }

    private static BookKeeperCapabilitySnapshotV1 readCapability(DataInputStream input) throws IOException {
        return new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(readDigest(input)),
                readText(input),
                readDigest(input),
                readText(input),
                readDigest(input),
                BookKeeperProtocolModeV1.valueOf(readText(input)),
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readBoolean(),
                input.readInt(),
                input.readInt(),
                input.readInt(),
                BookKeeperDigestTypeV1.valueOf(readText(input)),
                input.readBoolean(),
                input.readBoolean(),
                new BookKeeperTimeoutClassV1(input.readLong(), input.readLong(), input.readLong(), input.readLong()),
                readText(input),
                readDigest(input));
    }

    private static CanonicalBytes encode(int type, Encoder encoder) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(MAGIC);
                output.writeShort(2);
                output.writeByte(type);
                encoder.write(output);
            }
            if (bytes.size() > KafkaBookKeeperInventoryV2.MAX_TASK_BYTES) {
                throw new IllegalArgumentException("BK inventory exceeds encoded bound");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory BK inventory encoding failed", impossible);
        }
    }

    private static <T> T decode(CanonicalBytes bytes, int type, Decoder<T> decoder) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length() > KafkaBookKeeperInventoryV2.MAX_TASK_BYTES) {
            throw new IllegalArgumentException("BK inventory exceeds encoded bound");
        }
        try (var input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (input.readInt() != MAGIC || input.readUnsignedShort() != 2 || input.readUnsignedByte() != type) {
                throw new IllegalArgumentException("BK inventory preamble differs");
            }
            T value = decoder.read(input);
            if (input.available() != 0) {
                throw new IllegalArgumentException("BK inventory has trailing bytes");
            }
            return value;
        } catch (IOException failure) {
            throw new IllegalArgumentException("truncated BK inventory", failure);
        }
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest digest) throws IOException {
        output.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        input.readFully(bytes);
        return Sha256Digest.copyOf(bytes);
    }

    private static void writeBytes(DataOutputStream output, CanonicalBytes bytes) throws IOException {
        output.writeInt(bytes.length());
        output.write(bytes.toByteArray());
    }

    private static CanonicalBytes readBytes(DataInputStream input, int cap) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > cap || length > input.available()) {
            throw new IllegalArgumentException("BK inventory component exceeds bound");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return CanonicalBytes.copyOf(bytes);
    }

    private static void writeText(DataOutputStream output, String value) throws IOException {
        writeBytes(output, CanonicalUtf8.fromString(value).bytes());
    }

    private static String readText(DataInputStream input) throws IOException {
        return CanonicalUtf8.fromBytes(readBytes(input, 128).toByteArray()).value();
    }

    private static void requireCanonical(CanonicalBytes actual, CanonicalBytes expected) {
        if (!actual.equals(expected)) {
            throw new IllegalArgumentException("BK inventory is not canonical");
        }
    }

    private interface Encoder {
        void write(DataOutputStream output) throws IOException;
    }

    private interface Decoder<T> {
        T read(DataInputStream input) throws IOException;
    }
}
