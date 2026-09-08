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
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.DeleteTerminalOutcomeV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterTicketV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteDoneV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteIntentV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetReadFenceV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Strict canonical M5DA wire codec for one permanent target-scoped delete-authority value. */
public final class M5TargetDeleteAuthorityCodecV1 {
    private static final int MAGIC = 0x4d354441; // M5DA
    private static final int BASE_VERSION = 4;
    private static final int VERSION = 5;
    private static final Sha256Digest PLACEHOLDER = Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]);

    private M5TargetDeleteAuthorityCodecV1() {}

    static TargetDeleteAuthorityV1 finalizeAuthority(TargetDeleteAuthorityV1 draft) {
        TargetDeleteAuthorityV1 placeholder = copyWithCanonicalSha(draft, PLACEHOLDER);
        return copyWithCanonicalSha(draft, Sha256Digest.hash(encodeUnchecked(placeholder)));
    }

    static TargetDeleteAuthorityV1 successor(
            TargetDeleteAuthorityV1 current,
            TargetDeleteAuthorityStateV1 state,
            long closedWriterFenceEpoch,
            Sha256Digest proofSnapshotDigest,
            List<ProofBoundWriterTicketV1> tickets,
            Optional<TargetReadFenceV1> readFence,
            Optional<ExactExternalIdentityV1> externalIdentity,
            Optional<TargetDeleteIntentV1> deleteIntent,
            Optional<TargetDeleteDoneV1> deleteDone) {
        if (current.state() == TargetDeleteAuthorityStateV1.DELETE_DONE_V1) {
            throw new IllegalStateException("DELETE_DONE_V1 is permanent and has no successor");
        }
        CanonicalBytes predecessor = encodeAuthority(current);
        return finalizeAuthority(new TargetDeleteAuthorityV1(
                current.authorityKey(),
                current.target(),
                Math.addExact(current.authorityRevision(), 1),
                Optional.of(Sha256Digest.hash(predecessor)),
                state,
                closedWriterFenceEpoch,
                current.writerEnrollment(),
                proofSnapshotDigest,
                current.eligibilitySnapshot()
                        .filter(snapshot ->
                                tickets.isEmpty() && snapshot.sha256().equals(proofSnapshotDigest)),
                tickets,
                readFence,
                externalIdentity,
                deleteIntent,
                deleteDone,
                current.recoveryVeto(),
                PLACEHOLDER));
    }

    public static CanonicalBytes encodeAuthority(TargetDeleteAuthorityV1 value) {
        TargetDeleteAuthorityV1 placeholder = copyWithCanonicalSha(value, PLACEHOLDER);
        Sha256Digest expected = Sha256Digest.hash(encodeUnchecked(placeholder));
        if (!value.authorityCanonicalSha256().equals(expected)) {
            throw new IllegalArgumentException("target delete authority canonical SHA-256 differs");
        }
        CanonicalBytes encoded = encodeUnchecked(value);
        if (encoded.length() > M5TargetDeleteAuthorityRecordsV1.MAX_AUTHORITY_BYTES) {
            throw new IllegalArgumentException("target delete authority exceeds the metadata value hard cap");
        }
        return encoded;
    }

    public static TargetDeleteAuthorityV1 decodeAuthority(CanonicalBytes encoded) {
        if (encoded.length() > M5TargetDeleteAuthorityRecordsV1.MAX_AUTHORITY_BYTES) {
            throw new IllegalArgumentException("target delete authority exceeds the metadata value hard cap");
        }
        TargetDeleteAuthorityV1 value = decode(encoded, input -> {
            int magic = input.readInt();
            int wireVersion = input.readInt();
            if (magic != MAGIC || (wireVersion != BASE_VERSION && wireVersion != VERSION)) {
                throw new IllegalArgumentException("target delete authority preamble differs");
            }
            String authorityKey = readString(input, ExactMetadataTransactionStoreV1.MAX_KEY_BYTES);
            CanonicalBytes targetBytes =
                    readBytes(input, M5TargetDeleteAuthorityRecordsV1.MAX_TARGET_IDENTITY_BYTES, "target identity");
            PhysicalDeleteTargetV1 target =
                    new PhysicalDeleteTargetV1(PhysicalResourceIdCodecV2.decode(targetBytes), readDigest(input));
            long revision = input.readLong();
            Optional<Sha256Digest> predecessor =
                    input.readBoolean() ? Optional.of(readDigest(input)) : Optional.empty();
            TargetDeleteAuthorityStateV1 state = enumValue(
                    TargetDeleteAuthorityStateV1.values(), input.readUnsignedByte(), "target authority state");
            long fenceEpoch = input.readLong();
            ProofBoundWriterEnrollmentV1 enrollment = readEnrollment(input);
            Sha256Digest proofSnapshotDigest = readDigest(input);
            Optional<DeleteEligibilitySnapshotV2> eligibilitySnapshot = input.readBoolean()
                    ? Optional.of(DeleteEligibilityCodecV2.decode(
                            readBytes(input, DeleteEligibilitySnapshotV2.MAX_SNAPSHOT_BYTES, "eligibility snapshot")))
                    : Optional.empty();
            int ticketCount = boundedCount(
                    input.readInt(), M5TargetDeleteAuthorityRecordsV1.MAX_WRITER_TICKETS, "writer tickets");
            List<ProofBoundWriterTicketV1> tickets = new ArrayList<>(ticketCount);
            for (int index = 0; index < ticketCount; index++) {
                tickets.add(new ProofBoundWriterTicketV1(
                        enumValue(
                                ProofBoundWriterClassV1.values(), input.readUnsignedByte(), "proof-bound writer class"),
                        readDigest(input),
                        readDigest(input),
                        readDigest(input),
                        readDigest(input),
                        input.readLong()));
            }
            Optional<TargetReadFenceV1> readFence = input.readBoolean()
                    ? Optional.of(new TargetReadFenceV1(
                            readDigest(input),
                            input.readLong(),
                            input.readLong(),
                            readDigest(input),
                            readDigest(input),
                            readDigest(input),
                            readObservationContext(input)))
                    : Optional.empty();
            Optional<ExactExternalIdentityV1> externalIdentity = input.readBoolean()
                    ? Optional.of(new ExactExternalIdentityV1(
                            PhysicalResourceIdCodecV2.decode(readBytes(input, 65536, "external physical resource")),
                            readDigest(input),
                            enumValue(
                                    ExternalIdentityObservationV1.values(),
                                    input.readUnsignedByte(),
                                    "external identity observation"),
                            readBytes(
                                    input,
                                    M5TargetDeleteAuthorityRecordsV1.MAX_EXTERNAL_IDENTITY_BYTES,
                                    "external identity"),
                            readDigest(input)))
                    : Optional.empty();
            Optional<TargetDeleteIntentV1> deleteIntent = input.readBoolean()
                    ? Optional.of(new TargetDeleteIntentV1(
                            readDigest(input),
                            input.readLong(),
                            input.readLong(),
                            readDigest(input),
                            input.readBoolean() ? Optional.of(readDigest(input)) : Optional.empty(),
                            readDigest(input),
                            readDigest(input)))
                    : Optional.empty();
            Optional<TargetDeleteDoneV1> deleteDone = input.readBoolean()
                    ? Optional.of(new TargetDeleteDoneV1(
                            readDigest(input),
                            input.readLong(),
                            readDigest(input),
                            input.readLong(),
                            readDigest(input),
                            enumValue(
                                    DeleteTerminalOutcomeV1.values(),
                                    input.readUnsignedByte(),
                                    "delete terminal outcome"),
                            readDigest(input),
                            readDigest(input)))
                    : Optional.empty();
            return new TargetDeleteAuthorityV1(
                    authorityKey,
                    target,
                    revision,
                    predecessor,
                    state,
                    fenceEpoch,
                    enrollment,
                    proofSnapshotDigest,
                    eligibilitySnapshot,
                    tickets,
                    readFence,
                    externalIdentity,
                    deleteIntent,
                    deleteDone,
                    wireVersion == VERSION
                            ? Optional.of(new DeleteRecoveryVetoV2(
                                    enumValue(
                                            DeleteRecoveryVetoV2.Reason.values(),
                                            input.readUnsignedByte(),
                                            "recovery veto"),
                                    input.readLong(),
                                    readDigest(input),
                                    readDigest(input)))
                            : Optional.empty(),
                    readDigest(input));
        });
        if (!encoded.equals(encodeAuthority(value))) {
            throw new IllegalArgumentException("target delete authority is not canonical");
        }
        return value;
    }

    private static TargetDeleteAuthorityV1 copyWithCanonicalSha(
            TargetDeleteAuthorityV1 value, Sha256Digest canonicalSha256) {
        return new TargetDeleteAuthorityV1(
                value.authorityKey(),
                value.target(),
                value.authorityRevision(),
                value.predecessorAuthoritySha256(),
                value.state(),
                value.closedWriterFenceEpoch(),
                value.writerEnrollment(),
                value.proofSnapshotDigest(),
                value.eligibilitySnapshot(),
                value.activeWriterTickets(),
                value.readFence(),
                value.externalIdentity(),
                value.deleteIntent(),
                value.deleteDone(),
                value.recoveryVeto(),
                canonicalSha256);
    }

    private static CanonicalBytes encodeUnchecked(TargetDeleteAuthorityV1 value) {
        return encode(output -> {
            output.writeInt(MAGIC);
            output.writeInt(value.recoveryVeto().isPresent() ? VERSION : BASE_VERSION);
            writeString(output, value.authorityKey());
            writeBytes(output, value.target().resourceId().canonicalBytes());
            writeDigest(output, value.target().targetIdentitySha256());
            output.writeLong(value.authorityRevision());
            output.writeBoolean(value.predecessorAuthoritySha256().isPresent());
            if (value.predecessorAuthoritySha256().isPresent()) {
                writeDigest(output, value.predecessorAuthoritySha256().orElseThrow());
            }
            output.writeByte(value.state().ordinal());
            output.writeLong(value.closedWriterFenceEpoch());
            writeEnrollment(output, value.writerEnrollment());
            writeDigest(output, value.proofSnapshotDigest());
            output.writeBoolean(value.eligibilitySnapshot().isPresent());
            if (value.eligibilitySnapshot().isPresent()) {
                writeBytes(
                        output,
                        DeleteEligibilityCodecV2.encode(
                                value.eligibilitySnapshot().orElseThrow()));
            }
            output.writeInt(value.activeWriterTickets().size());
            for (ProofBoundWriterTicketV1 ticket : value.activeWriterTickets()) {
                output.writeByte(ticket.writerClass().ordinal());
                writeDigest(output, ticket.operationIdSha256());
                writeDigest(output, ticket.capabilitySha256());
                writeDigest(output, ticket.ownerFenceSha256());
                writeDigest(output, ticket.externalFactsRootSha256());
                output.writeLong(ticket.predecessorAuthorityRevision());
            }
            output.writeBoolean(value.readFence().isPresent());
            if (value.readFence().isPresent()) {
                TargetReadFenceV1 fence = value.readFence().orElseThrow();
                writeDigest(output, fence.attemptIdSha256());
                output.writeLong(fence.fencedAuthorityRevision());
                output.writeLong(fence.fenceEpoch());
                writeDigest(output, fence.openAuthorityValueSha256());
                writeDigest(output, fence.proofSnapshotDigest());
                writeDigest(output, fence.eligibilityRootSha256());
                writeObservationContext(output, fence.observationContext());
            }
            output.writeBoolean(value.externalIdentity().isPresent());
            if (value.externalIdentity().isPresent()) {
                ExactExternalIdentityV1 identity = value.externalIdentity().orElseThrow();
                writeBytes(output, identity.resourceId().canonicalBytes());
                writeDigest(output, identity.fencedAuthoritySha256());
                output.writeByte(identity.observation().ordinal());
                writeBytes(output, identity.exactIdentityBytes());
                writeDigest(output, identity.externalIdentitySha256());
            }
            output.writeBoolean(value.deleteIntent().isPresent());
            if (value.deleteIntent().isPresent()) {
                TargetDeleteIntentV1 intent = value.deleteIntent().orElseThrow();
                writeDigest(output, intent.deleteAttemptIdSha256());
                output.writeLong(intent.intentAuthorityRevision());
                output.writeLong(intent.dispatchEpoch());
                writeDigest(output, intent.dispatchOwnerFenceSha256());
                output.writeBoolean(intent.ownerTakeoverProofSha256().isPresent());
                if (intent.ownerTakeoverProofSha256().isPresent()) {
                    writeDigest(output, intent.ownerTakeoverProofSha256().orElseThrow());
                }
                writeDigest(output, intent.capabilityDigestSha256());
                writeDigest(output, intent.dispatchTokenSha256());
            }
            output.writeBoolean(value.deleteDone().isPresent());
            if (value.deleteDone().isPresent()) {
                TargetDeleteDoneV1 done = value.deleteDone().orElseThrow();
                writeDigest(output, done.intentCanonicalSha256());
                output.writeLong(done.intentAuthorityRevision());
                writeDigest(output, done.deleteAttemptIdSha256());
                output.writeLong(done.finalDispatchEpoch());
                writeDigest(output, done.finalDispatchOwnerFenceSha256());
                output.writeByte(done.terminalOutcome().ordinal());
                writeDigest(output, done.absenceInventoryRootSha256());
                writeDigest(output, done.completionProofDigestSha256());
            }
            if (value.recoveryVeto().isPresent()) {
                DeleteRecoveryVetoV2 veto = value.recoveryVeto().orElseThrow();
                output.writeByte(veto.reason().ordinal());
                output.writeLong(veto.rejectedObservationEpoch());
                writeDigest(output, veto.rejectedContextSha256());
                writeDigest(output, veto.rejectedAuthoritySha256());
            }
            writeDigest(output, value.authorityCanonicalSha256());
        });
    }

    static Sha256Digest observationContextSha256(DeleteObservationContextV2 context) {
        return Sha256Digest.hash(encode(output -> writeObservationContext(output, context)));
    }

    private static void writeObservationContext(DataOutputStream out, DeleteObservationContextV2 context)
            throws IOException {
        out.writeLong(context.observationEpoch());
        DeleteEligibilityCodecV2.writeFact(out, context.coordinatorOwner());
        DeleteEligibilityCodecV2.writeFact(out, context.capability());
        out.writeBoolean(context.predecessorOwnerFenced().isPresent());
        if (context.predecessorOwnerFenced().isPresent()) {
            DeleteEligibilityCodecV2.writeFact(
                    out, context.predecessorOwnerFenced().orElseThrow());
        }
    }

    private static DeleteObservationContextV2 readObservationContext(DataInputStream in) throws IOException {
        return new DeleteObservationContextV2(
                in.readLong(),
                DeleteEligibilityCodecV2.readFact(in),
                DeleteEligibilityCodecV2.readFact(in),
                in.readBoolean() ? Optional.of(DeleteEligibilityCodecV2.readFact(in)) : Optional.empty());
    }

    private static void writeEnrollment(DataOutputStream output, ProofBoundWriterEnrollmentV1 value)
            throws IOException {
        output.writeInt(value.writerClasses().size());
        for (ProofBoundWriterClassV1 writerClass : value.writerClasses()) {
            output.writeByte(writerClass.ordinal());
        }
        writeDigest(output, value.capabilitySetRootSha256());
        writeDigest(output, value.implementationRootSha256());
        writeDigest(output, value.policyRootSha256());
    }

    private static ProofBoundWriterEnrollmentV1 readEnrollment(DataInputStream input) throws IOException {
        ProofBoundWriterClassV1[] values = ProofBoundWriterClassV1.values();
        int count = input.readInt();
        if (count != values.length) {
            throw new IllegalArgumentException("writer enrollment count differs from its closed inventory");
        }
        List<ProofBoundWriterClassV1> writerClasses = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            writerClasses.add(enumValue(values, input.readUnsignedByte(), "proof-bound writer class"));
        }
        return new ProofBoundWriterEnrollmentV1(writerClasses, readDigest(input), readDigest(input), readDigest(input));
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        writeBytes(output, CanonicalUtf8.fromString(value).bytes());
    }

    private static String readString(DataInputStream input, int maximum) throws IOException {
        return CanonicalUtf8.fromBytes(readBytes(input, maximum, "UTF-8 value").toByteArray())
                .value();
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest value) throws IOException {
        output.write(value.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] value = input.readNBytes(Sha256Digest.LENGTH);
        if (value.length != Sha256Digest.LENGTH) {
            throw new EOFException("truncated SHA-256 digest");
        }
        return Sha256Digest.copyOf(value);
    }

    private static void writeBytes(DataOutputStream output, CanonicalBytes value) throws IOException {
        output.writeInt(value.length());
        output.write(value.toByteArray());
    }

    private static CanonicalBytes readBytes(DataInputStream input, int maximum, String label) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > maximum) {
            throw new IllegalArgumentException(label + " length exceeds its hard cap");
        }
        byte[] value = input.readNBytes(length);
        if (value.length != length) {
            throw new EOFException("truncated " + label);
        }
        return CanonicalBytes.copyOf(value);
    }

    private static int boundedCount(int count, int maximum, String label) {
        if (count < 0 || count > maximum) {
            throw new IllegalArgumentException(label + " count exceeds its hard cap");
        }
        return count;
    }

    private static <T> T enumValue(T[] values, int ordinal, String label) {
        if (ordinal < 0 || ordinal >= values.length) {
            throw new IllegalArgumentException(label + " code is unknown");
        }
        return values[ordinal];
    }

    private static CanonicalBytes encode(Writer writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writer.write(output);
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException exception) {
            throw new IllegalStateException("failed to encode target delete authority", exception);
        }
    }

    private static <T> T decode(CanonicalBytes encoded, Reader<T> reader) {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            T value = reader.read(input);
            if (input.available() != 0) {
                throw new IllegalArgumentException("target delete authority has trailing bytes");
            }
            return value;
        } catch (EOFException exception) {
            throw new IllegalArgumentException("target delete authority is truncated", exception);
        } catch (IOException exception) {
            throw new IllegalArgumentException("failed to decode target delete authority", exception);
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream output) throws IOException;
    }

    @FunctionalInterface
    private interface Reader<T> {
        T read(DataInputStream input) throws IOException;
    }
}
