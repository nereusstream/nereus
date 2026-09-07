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

package com.nereusstream.storage.object.gc;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.MemberSnapshot;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.NativeDisposition;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReclamationReason;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReplacementEvidence;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticTransfer;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.UnpublishedEvidence;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.retention.M5RetentionCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BindingTrimFrontierV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Bounded canonical M5ES v2 evidence wire, including exact M4 and M5-C proof bytes. */
public final class DeleteEligibilityCodecV2 {
    private static final int MAGIC = 0x4d354553;
    private static final int VERSION = 2;
    private static final int MAX_FACT_COMPONENT_BYTES = 8192;

    private DeleteEligibilityCodecV2() {}

    public static CanonicalBytes encode(DeleteEligibilitySnapshotV2 snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        snapshot.authorityFacts();
        try {
            BoundedBytes bytes = new BoundedBytes();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(VERSION);
                writeBytes(out, snapshot.resource().canonicalBytes());
                out.writeByte(snapshot.reason().ordinal());
                out.writeLong(snapshot.generation());
                writeFact(out, snapshot.namespaceAdmission());
                writeFact(out, snapshot.completeMemberInventory());
                out.writeInt(snapshot.members().size());
                for (MemberSnapshot member : snapshot.members()) {
                    writeBytes(out, M5RetentionCodecV1.encodeSnapshot(member.floors()));
                    writeBytes(out, M5RetentionCodecV1.encodeReferenceFreeProof(member.physicalReferences()));
                    writeFact(out, member.nativeAuthority());
                    out.writeByte(member.nativeDisposition().ordinal());
                    out.writeBoolean(member.replacement().isPresent());
                    if (member.replacement().isPresent()) {
                        ReplacementEvidence replacement = member.replacement().orElseThrow();
                        writeFact(out, replacement.selectedGeneration());
                        out.writeInt(replacement.replacementResources().size());
                        for (PhysicalResourceIdV2 resource : replacement.replacementResources()) {
                            writeBytes(out, resource.canonicalBytes());
                        }
                        out.writeInt(replacement.transfers().size());
                        for (SemanticTransfer transfer : replacement.transfers()) {
                            out.writeByte(transfer.aspect().ordinal());
                            out.writeByte(transfer.coverage().domain().ordinal());
                            out.writeLong(transfer.coverage().inclusiveStart());
                            out.writeLong(transfer.coverage().exclusiveEnd());
                            writeDigest(out, transfer.priorSemanticRoot());
                            writeDigest(out, transfer.replacementSemanticRoot());
                            writeFact(out, transfer.verifierAuthority());
                        }
                    }
                    out.writeBoolean(member.expiry().isPresent());
                    if (member.expiry().isPresent()) {
                        writeBytes(
                                out,
                                M5RetentionCodecV1.encodeTrimFrontier(
                                        member.expiry().orElseThrow()));
                    }
                    out.writeBoolean(member.unpublished().isPresent());
                    if (member.unpublished().isPresent()) {
                        UnpublishedEvidence evidence = member.unpublished().orElseThrow();
                        writeDigest(out, evidence.taskAttemptId());
                        writeFact(out, evidence.terminalTask());
                        writeFact(out, evidence.fencedTaskOwner());
                        writeFact(out, evidence.closedAdoptionAndResponseLossPaths());
                        out.writeBoolean(evidence.neverAdmittedProtection().isPresent());
                        if (evidence.neverAdmittedProtection().isPresent()) {
                            writeFact(out, evidence.neverAdmittedProtection().orElseThrow());
                        }
                    }
                }
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException error) {
            throw new IllegalStateException("cannot encode deletion eligibility", error);
        }
    }

    public static DeleteEligibilitySnapshotV2 decode(CanonicalBytes bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length() > DeleteEligibilitySnapshotV2.MAX_SNAPSHOT_BYTES) {
            throw new IllegalArgumentException("deletion eligibility exceeds its hard cap");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readInt() != VERSION) {
                throw new IllegalArgumentException("deletion eligibility preamble differs");
            }
            PhysicalResourceIdV2 resource =
                    PhysicalResourceIdCodecV2.decode(readBytes(in, PhysicalResourceIdV2.MAX_ENCODED_BYTES));
            ReclamationReason reason = readEnum(in, ReclamationReason.values());
            long generation = in.readLong();
            AuthorityFactV1 namespace = readFact(in);
            AuthorityFactV1 inventory = readFact(in);
            int count = count(in, DeleteEligibilitySnapshotV2.MAX_MEMBERS);
            List<MemberSnapshot> members = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                var floors = M5RetentionCodecV1.decodeSnapshot(
                        readBytes(in, DeleteEligibilitySnapshotV2.MAX_SNAPSHOT_BYTES));
                var references = M5RetentionCodecV1.decodeReferenceFreeProof(
                        readBytes(in, DeleteEligibilitySnapshotV2.MAX_SNAPSHOT_BYTES));
                AuthorityFactV1 nativeAuthority = readFact(in);
                NativeDisposition disposition = readEnum(in, NativeDisposition.values());
                Optional<ReplacementEvidence> replacement = Optional.empty();
                if (in.readBoolean()) {
                    AuthorityFactV1 selected = readFact(in);
                    int resourceCount = count(in, DeleteEligibilitySnapshotV2.MAX_REPLACEMENTS);
                    List<PhysicalResourceIdV2> resources = new ArrayList<>(resourceCount);
                    for (int j = 0; j < resourceCount; j++) {
                        resources.add(PhysicalResourceIdCodecV2.decode(
                                readBytes(in, PhysicalResourceIdV2.MAX_ENCODED_BYTES)));
                    }
                    int transfersCount = count(in, SemanticAspect.values().length);
                    List<SemanticTransfer> transfers = new ArrayList<>(transfersCount);
                    for (int j = 0; j < transfersCount; j++) {
                        transfers.add(new SemanticTransfer(
                                readEnum(in, SemanticAspect.values()),
                                new ProtocolCoverage(
                                        readEnum(in, PositionDomain.values()), in.readLong(), in.readLong()),
                                readDigest(in),
                                readDigest(in),
                                readFact(in)));
                    }
                    replacement = Optional.of(new ReplacementEvidence(selected, resources, transfers));
                }
                Optional<BindingTrimFrontierV1> expiry = in.readBoolean()
                        ? Optional.of(M5RetentionCodecV1.decodeTrimFrontier(
                                readBytes(in, DeleteEligibilitySnapshotV2.MAX_SNAPSHOT_BYTES)))
                        : Optional.empty();
                Optional<UnpublishedEvidence> unpublished = Optional.empty();
                if (in.readBoolean()) {
                    unpublished = Optional.of(new UnpublishedEvidence(
                            readDigest(in),
                            readFact(in),
                            readFact(in),
                            readFact(in),
                            in.readBoolean() ? Optional.of(readFact(in)) : Optional.empty()));
                }
                members.add(new MemberSnapshot(
                        floors, references, nativeAuthority, disposition, replacement, expiry, unpublished));
            }
            DeleteEligibilitySnapshotV2 snapshot =
                    new DeleteEligibilitySnapshotV2(resource, reason, generation, namespace, inventory, members);
            if (in.available() != 0 || !encode(snapshot).equals(bytes)) {
                throw new IllegalArgumentException("deletion eligibility has trailing or noncanonical bytes");
            }
            return snapshot;
        } catch (IOException error) {
            throw new IllegalArgumentException("truncated deletion eligibility", error);
        }
    }

    private static <T> T readEnum(DataInputStream in, T[] values) throws IOException {
        int code = in.readUnsignedByte();
        if (code >= values.length) {
            throw new IllegalArgumentException("unknown deletion eligibility enum code");
        }
        return values[code];
    }

    private static int count(DataInputStream in, int cap) throws IOException {
        int count = in.readInt();
        if (count <= 0 || count > cap || count > in.available()) {
            throw new IllegalArgumentException("deletion eligibility count exceeds bounds");
        }
        return count;
    }

    private static void writeFact(DataOutputStream out, AuthorityFactV1 fact) throws IOException {
        CanonicalBytes key = CanonicalUtf8.fromString(fact.key()).bytes();
        if (key.length() > MAX_FACT_COMPONENT_BYTES
                || fact.metadataVersion().value().length() > MAX_FACT_COMPONENT_BYTES) {
            throw new IllegalArgumentException("eligibility authority key/version exceeds bounds");
        }
        writeBytes(out, key);
        writeBytes(out, fact.metadataVersion().value());
        writeDigest(out, fact.valueSha256());
    }

    private static AuthorityFactV1 readFact(DataInputStream in) throws IOException {
        return new AuthorityFactV1(
                CanonicalUtf8.fromBytes(readBytes(in, MAX_FACT_COMPONENT_BYTES).toByteArray())
                        .value(),
                new MetadataVersion(readBytes(in, MAX_FACT_COMPONENT_BYTES)),
                readDigest(in));
    }

    private static void writeDigest(DataOutputStream out, Sha256Digest value) throws IOException {
        out.write(value.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream in) throws IOException {
        byte[] value = new byte[Sha256Digest.LENGTH];
        in.readFully(value);
        return Sha256Digest.copyOf(value);
    }

    private static void writeBytes(DataOutputStream out, CanonicalBytes bytes) throws IOException {
        if (bytes.isEmpty() || bytes.length() > DeleteEligibilitySnapshotV2.MAX_SNAPSHOT_BYTES) {
            throw new IllegalArgumentException("deletion eligibility component exceeds bounds");
        }
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }

    private static CanonicalBytes readBytes(DataInputStream in, int cap) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > cap || length > in.available()) {
            throw new IllegalArgumentException("deletion eligibility component exceeds bounds");
        }
        return CanonicalBytes.copyOf(in.readNBytes(length));
    }

    private static final class BoundedBytes extends ByteArrayOutputStream {
        @Override
        public synchronized void write(int value) {
            requireCapacity(1);
            super.write(value);
        }

        @Override
        public synchronized void write(byte[] value, int offset, int length) {
            requireCapacity(length);
            super.write(value, offset, length);
        }

        private void requireCapacity(int additional) {
            if (additional < 0 || additional > DeleteEligibilitySnapshotV2.MAX_SNAPSHOT_BYTES - count) {
                throw new IllegalArgumentException("deletion eligibility exceeds its hard cap");
            }
        }
    }
}
