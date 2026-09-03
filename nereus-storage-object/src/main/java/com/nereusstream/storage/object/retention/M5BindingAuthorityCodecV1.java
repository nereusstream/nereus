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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BatchAuthoritySlotV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceScanFenceV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceWriterEnrollmentV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BatchMetadataStateV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Strict canonical wire and legacy-selector projection for the single Binding authority cell. */
public final class M5BindingAuthorityCodecV1 {
    private static final int MAGIC = 0x4d355231; // M5R1
    private static final int VERSION = 1;
    private static final Sha256Digest PLACEHOLDER = Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]);

    private M5BindingAuthorityCodecV1() {}

    public static boolean isAuthorityValue(CanonicalBytes value) {
        byte[] bytes = value.toByteArray();
        if (bytes.length < Integer.BYTES) {
            return false;
        }
        int magic =
                ((bytes[0] & 0xff) << 24) | ((bytes[1] & 0xff) << 16) | ((bytes[2] & 0xff) << 8) | (bytes[3] & 0xff);
        return magic == MAGIC;
    }

    public static BindingRetirementAuthorityV1 initial(BindingReadSelector selector) {
        return finalizeAuthority(new BindingRetirementAuthorityV1(
                selector.binding(),
                1,
                Optional.empty(),
                BindingAuthorityStateV1.OPEN_V1,
                selector,
                fullSlots(selector.activeBatches()),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                selector.capability(),
                PLACEHOLDER));
    }

    /** Wrap one exact legacy selector without changing its M4 projection bytes. */
    public static BindingRetirementAuthorityV1 migrateLegacy(CanonicalBytes exactLegacySelector) {
        BindingReadSelector selector = M4ReadControlCodecV1.decodeSelector(exactLegacySelector);
        return finalizeAuthority(new BindingRetirementAuthorityV1(
                selector.binding(),
                1,
                Optional.of(Sha256Digest.hash(exactLegacySelector)),
                BindingAuthorityStateV1.OPEN_V1,
                selector,
                fullSlots(selector.activeBatches()),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                selector.capability(),
                PLACEHOLDER));
    }

    /** Apply an M4 selector successor while preserving every M5-only slot/ticket field. */
    public static BindingRetirementAuthorityV1 selectorSuccessor(
            BindingRetirementAuthorityV1 current, BindingReadSelector successor) {
        if (current.state() != BindingAuthorityStateV1.OPEN_V1) {
            throw new IllegalStateException("REFERENCE_SCAN_FENCED_V1 blocks Binding control mutation");
        }
        if (!current.binding().equals(successor.binding())
                || !current.capability().equals(successor.capability())) {
            throw new IllegalArgumentException("selector successor changes Binding or capability");
        }
        List<SourceRetirementBatch> existingFull = current.batchSlots().stream()
                .filter(slot -> slot.state() == BatchMetadataStateV1.FULL_V1)
                .map(BatchAuthoritySlotV1::fullBatch)
                .toList();
        if (successor.activeBatches().size() < existingFull.size()
                || !successor.activeBatches().subList(0, existingFull.size()).equals(existingFull)) {
            throw new IllegalArgumentException("M4 selector successor removes or reorders a FULL_V1 authority slot");
        }
        List<BatchAuthoritySlotV1> slots = new ArrayList<>(current.batchSlots());
        long ordinal = slots.size();
        for (SourceRetirementBatch batch : successor
                .activeBatches()
                .subList(existingFull.size(), successor.activeBatches().size())) {
            if (slots.stream().anyMatch(slot -> slot.batchIdSha256().equals(batch.batchIdSha256()))) {
                throw new IllegalArgumentException("selector successor reuses a retired or existing BatchId slot");
            }
            slots.add(BatchAuthoritySlotV1.full(++ordinal, batch));
        }
        return successor(
                current,
                BindingAuthorityStateV1.OPEN_V1,
                successor,
                slots,
                Optional.empty(),
                current.referenceMutationTickets());
    }

    /** Apply a successor directly to one legacy predecessor in the migration CAS. */
    public static BindingRetirementAuthorityV1 migrateLegacyWithSuccessor(
            CanonicalBytes exactLegacySelector, BindingReadSelector successor) {
        BindingReadSelector predecessor = M4ReadControlCodecV1.decodeSelector(exactLegacySelector);
        if (!predecessor.binding().equals(successor.binding())
                || !predecessor.capability().equals(successor.capability())
                || successor.activeBatches().size()
                        < predecessor.activeBatches().size()
                || !successor
                        .activeBatches()
                        .subList(0, predecessor.activeBatches().size())
                        .equals(predecessor.activeBatches())) {
            throw new IllegalArgumentException("legacy migration successor changes existing batch authority");
        }
        return finalizeAuthority(new BindingRetirementAuthorityV1(
                successor.binding(),
                1,
                Optional.of(Sha256Digest.hash(exactLegacySelector)),
                BindingAuthorityStateV1.OPEN_V1,
                successor,
                fullSlots(successor.activeBatches()),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                successor.capability(),
                PLACEHOLDER));
    }

    public static BindingRetirementAuthorityV1 successor(
            BindingRetirementAuthorityV1 current,
            BindingAuthorityStateV1 state,
            BindingReadSelector selector,
            List<BatchAuthoritySlotV1> slots,
            Optional<ReferenceScanFenceV1> fence,
            List<ReferenceMutationTicketV1> tickets) {
        return successor(current, state, selector, slots, fence, tickets, current.writerEnrollment());
    }

    public static BindingRetirementAuthorityV1 successor(
            BindingRetirementAuthorityV1 current,
            BindingAuthorityStateV1 state,
            BindingReadSelector selector,
            List<BatchAuthoritySlotV1> slots,
            Optional<ReferenceScanFenceV1> fence,
            List<ReferenceMutationTicketV1> tickets,
            Optional<ReferenceWriterEnrollmentV1> enrollment) {
        CanonicalBytes predecessor = encodeAuthority(current);
        return finalizeAuthority(new BindingRetirementAuthorityV1(
                current.binding(),
                Math.addExact(current.authorityGeneration(), 1),
                Optional.of(Sha256Digest.hash(predecessor)),
                state,
                selector,
                slots,
                fence,
                tickets,
                enrollment,
                current.capability(),
                PLACEHOLDER));
    }

    public static CanonicalBytes encodeAuthority(BindingRetirementAuthorityV1 value) {
        BindingRetirementAuthorityV1 placeholder = copyWithCanonicalSha(value, PLACEHOLDER);
        Sha256Digest expected = Sha256Digest.hash(encodeUnchecked(placeholder));
        if (!value.authorityCanonicalSha256().equals(expected)) {
            throw new IllegalArgumentException("Binding authority canonical SHA-256 differs");
        }
        CanonicalBytes encoded = encodeUnchecked(value);
        if (encoded.length() > M5BindingAuthorityRecordsV1.MAX_AUTHORITY_BYTES) {
            throw new IllegalArgumentException("Binding authority exceeds the metadata value hard cap");
        }
        return encoded;
    }

    public static BindingRetirementAuthorityV1 decodeAuthority(CanonicalBytes encoded) {
        BindingRetirementAuthorityV1 value = decode(encoded, input -> {
            if (input.readInt() != MAGIC || input.readInt() != VERSION) {
                throw new IllegalArgumentException("Binding authority preamble differs");
            }
            long generation = input.readLong();
            Optional<Sha256Digest> predecessor =
                    input.readBoolean() ? Optional.of(readDigest(input)) : Optional.empty();
            CapabilityBinding capability = readCapability(input);
            BindingReadSelector selectorCore = M4ReadControlCodecV1.decodeSelector(readBytes(input));
            int slotCount = input.readInt();
            if (slotCount < 0 || slotCount > M5BindingAuthorityRecordsV1.MAX_BATCH_SLOTS) {
                throw new IllegalArgumentException("Binding authority batch-slot count exceeds its hard cap");
            }
            List<BatchAuthoritySlotV1> slots = new ArrayList<>();
            for (int index = 0; index < slotCount; index++) {
                long ordinal = input.readLong();
                BatchMetadataStateV1 state =
                        enumValue(BatchMetadataStateV1.values(), input.readUnsignedByte(), "batch-slot state");
                Sha256Digest batchId = readDigest(input);
                Sha256Digest fullSha = readDigest(input);
                if (state == BatchMetadataStateV1.FULL_V1) {
                    slots.add(new BatchAuthoritySlotV1(
                            ordinal, state, batchId, fullSha, Optional.of(readBytes(input)), Optional.empty()));
                } else {
                    slots.add(new BatchAuthoritySlotV1(
                            ordinal,
                            state,
                            batchId,
                            fullSha,
                            Optional.empty(),
                            Optional.of(M5RetentionCodecV1.decodeRetiredBatch(readBytes(input)))));
                }
            }
            BindingReadSelector projection = withActiveBatches(
                    selectorCore,
                    slots.stream()
                            .filter(slot -> slot.state() == BatchMetadataStateV1.FULL_V1)
                            .map(BatchAuthoritySlotV1::fullBatch)
                            .toList());
            Optional<ReferenceScanFenceV1> fence = input.readBoolean()
                    ? Optional.of(new ReferenceScanFenceV1(
                            enumValue(
                                    ReferenceTargetKindV1.values(), input.readUnsignedByte(), "scan-fence target kind"),
                            readDigest(input),
                            readDigest(input),
                            readDigest(input)))
                    : Optional.empty();
            int ticketCount = input.readInt();
            if (ticketCount < 0 || ticketCount > M5BindingAuthorityRecordsV1.MAX_REFERENCE_MUTATION_TICKETS) {
                throw new IllegalArgumentException("Binding authority ticket count exceeds its hard cap");
            }
            List<ReferenceMutationTicketV1> tickets = new ArrayList<>();
            for (int index = 0; index < ticketCount; index++) {
                tickets.add(new ReferenceMutationTicketV1(
                        enumValue(ReferenceTargetKindV1.values(), input.readUnsignedByte(), "ticket target kind"),
                        readDigest(input),
                        enumValue(ReferenceKindV1.values(), input.readUnsignedByte(), "ticket reference kind"),
                        readCapability(input),
                        readDigest(input),
                        readDigest(input)));
            }
            Optional<ReferenceWriterEnrollmentV1> enrollment;
            if (input.readBoolean()) {
                CapabilityBinding enrollmentCapability = readCapability(input);
                int floorCount = input.readInt();
                if (floorCount < 0 || floorCount > FloorClassV1.values().length) {
                    throw new IllegalArgumentException("writer-enrollment floor count differs");
                }
                List<FloorClassV1> floors = new ArrayList<>();
                for (int index = 0; index < floorCount; index++) {
                    floors.add(enumValue(FloorClassV1.values(), input.readUnsignedByte(), "enrolled floor class"));
                }
                int referenceCount = input.readInt();
                if (referenceCount < 0 || referenceCount > ReferenceKindV1.values().length) {
                    throw new IllegalArgumentException("writer-enrollment reference count differs");
                }
                List<ReferenceKindV1> references = new ArrayList<>();
                for (int index = 0; index < referenceCount; index++) {
                    references.add(
                            enumValue(ReferenceKindV1.values(), input.readUnsignedByte(), "enrolled reference kind"));
                }
                enrollment = Optional.of(
                        new ReferenceWriterEnrollmentV1(enrollmentCapability, floors, references, readDigest(input)));
            } else {
                enrollment = Optional.empty();
            }
            BindingAuthorityStateV1 authorityState =
                    enumValue(BindingAuthorityStateV1.values(), input.readUnsignedByte(), "Binding authority state");
            Sha256Digest canonicalSha = readDigest(input);
            return new BindingRetirementAuthorityV1(
                    projection.binding(),
                    generation,
                    predecessor,
                    authorityState,
                    projection,
                    slots,
                    fence,
                    tickets,
                    enrollment,
                    capability,
                    canonicalSha);
        });
        if (!Arrays.equals(encoded.toByteArray(), encodeAuthority(value).toByteArray())) {
            throw new IllegalArgumentException("Binding authority value is not canonical");
        }
        return value;
    }

    public static BindingReadSelector projectSelector(CanonicalBytes stored) {
        return isAuthorityValue(stored)
                ? decodeAuthority(stored).selectorProjection()
                : M4ReadControlCodecV1.decodeSelector(stored);
    }

    private static BindingRetirementAuthorityV1 finalizeAuthority(BindingRetirementAuthorityV1 draft) {
        BindingRetirementAuthorityV1 placeholder = copyWithCanonicalSha(draft, PLACEHOLDER);
        return copyWithCanonicalSha(draft, Sha256Digest.hash(encodeUnchecked(placeholder)));
    }

    private static BindingRetirementAuthorityV1 copyWithCanonicalSha(
            BindingRetirementAuthorityV1 value, Sha256Digest canonicalSha) {
        return new BindingRetirementAuthorityV1(
                value.binding(),
                value.authorityGeneration(),
                value.predecessorValueSha256(),
                value.state(),
                value.selectorProjection(),
                value.batchSlots(),
                value.scanFence(),
                value.referenceMutationTickets(),
                value.writerEnrollment(),
                value.capability(),
                canonicalSha);
    }

    private static CanonicalBytes encodeUnchecked(BindingRetirementAuthorityV1 value) {
        return encode(out -> {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeLong(value.authorityGeneration());
            out.writeBoolean(value.predecessorValueSha256().isPresent());
            if (value.predecessorValueSha256().isPresent()) {
                writeDigest(out, value.predecessorValueSha256().orElseThrow());
            }
            writeCapability(out, value.capability());
            writeBytes(
                    out,
                    M4ReadControlCodecV1.encodeSelector(
                            M5BindingAuthorityRecordsV1.withoutActiveBatches(value.selectorProjection())));
            out.writeInt(value.batchSlots().size());
            for (BatchAuthoritySlotV1 slot : value.batchSlots()) {
                out.writeLong(slot.activationOrdinal());
                out.writeByte(slot.state().ordinal());
                writeDigest(out, slot.batchIdSha256());
                writeDigest(out, slot.fullBatchSha256());
                if (slot.state() == BatchMetadataStateV1.FULL_V1) {
                    writeBytes(out, slot.canonicalM4BatchBytes().orElseThrow());
                } else {
                    writeBytes(
                            out,
                            M5RetentionCodecV1.encodeRetiredBatch(
                                    slot.retiredTombstone().orElseThrow()));
                }
            }
            out.writeBoolean(value.scanFence().isPresent());
            if (value.scanFence().isPresent()) {
                ReferenceScanFenceV1 fence = value.scanFence().orElseThrow();
                out.writeByte(fence.targetKind().ordinal());
                writeDigest(out, fence.targetIdentitySha256());
                writeDigest(out, fence.attemptIdSha256());
                writeDigest(out, fence.openAuthorityValueSha256());
            }
            out.writeInt(value.referenceMutationTickets().size());
            for (ReferenceMutationTicketV1 ticket : value.referenceMutationTickets()) {
                out.writeByte(ticket.targetKind().ordinal());
                writeDigest(out, ticket.targetIdentitySha256());
                out.writeByte(ticket.referenceKind().ordinal());
                writeCapability(out, ticket.writerCapability());
                writeDigest(out, ticket.operationIdSha256());
                writeDigest(out, ticket.externalAuthorityRootSha256());
            }
            out.writeBoolean(value.writerEnrollment().isPresent());
            if (value.writerEnrollment().isPresent()) {
                ReferenceWriterEnrollmentV1 enrollment =
                        value.writerEnrollment().orElseThrow();
                writeCapability(out, enrollment.capability());
                out.writeInt(enrollment.floorClasses().size());
                for (FloorClassV1 floor : enrollment.floorClasses()) {
                    out.writeByte(floor.ordinal());
                }
                out.writeInt(enrollment.referenceKinds().size());
                for (ReferenceKindV1 reference : enrollment.referenceKinds()) {
                    out.writeByte(reference.ordinal());
                }
                writeDigest(out, enrollment.implementationRootSha256());
            }
            out.writeByte(value.state().ordinal());
            writeDigest(out, value.authorityCanonicalSha256());
        });
    }

    private static List<BatchAuthoritySlotV1> fullSlots(List<SourceRetirementBatch> batches) {
        List<BatchAuthoritySlotV1> slots = new ArrayList<>();
        long ordinal = 0;
        for (SourceRetirementBatch batch : batches) {
            slots.add(BatchAuthoritySlotV1.full(++ordinal, batch));
        }
        return slots;
    }

    private static BindingReadSelector withActiveBatches(
            BindingReadSelector core, List<SourceRetirementBatch> batches) {
        return new BindingReadSelector(
                core.binding(),
                core.selectedViewSha256(),
                core.ownerEpoch(),
                core.readAdmissionEpoch(),
                core.sourceGeneration(),
                core.mode(),
                core.admissionState(),
                core.fallbackSetSha256(),
                core.capability(),
                core.pendingAnchors(),
                batches);
    }

    private static void writeCapability(DataOutputStream output, CapabilityBinding capability) throws IOException {
        output.writeLong(capability.generation());
        writeDigest(output, capability.evidenceSha256());
    }

    private static CapabilityBinding readCapability(DataInputStream input) throws IOException {
        return new CapabilityBinding(input.readLong(), readDigest(input));
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest digest) throws IOException {
        output.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        return Sha256Digest.copyOf(input.readNBytes(Sha256Digest.LENGTH));
    }

    private static void writeBytes(DataOutputStream output, CanonicalBytes bytes) throws IOException {
        if (bytes.isEmpty() || bytes.length() > M5BindingAuthorityRecordsV1.MAX_AUTHORITY_BYTES) {
            throw new IllegalArgumentException("nested Binding authority bytes exceed their hard cap");
        }
        output.writeInt(bytes.length());
        output.write(bytes.toByteArray());
    }

    private static CanonicalBytes readBytes(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > M5BindingAuthorityRecordsV1.MAX_AUTHORITY_BYTES) {
            throw new IllegalArgumentException("nested Binding authority byte length exceeds its hard cap");
        }
        byte[] value = input.readNBytes(length);
        if (value.length != length) {
            throw new EOFException("truncated nested Binding authority bytes");
        }
        return CanonicalBytes.copyOf(value);
    }

    private static <T> T enumValue(T[] values, int ordinal, String label) {
        if (ordinal < 0 || ordinal >= values.length) {
            throw new IllegalArgumentException(label + " ordinal is unknown");
        }
        return values[ordinal];
    }

    private static CanonicalBytes encode(Writer writer) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(buffer)) {
                writer.write(output);
            }
            return CanonicalBytes.copyOf(buffer.toByteArray());
        } catch (IOException error) {
            throw new IllegalStateException("cannot encode Binding authority", error);
        }
    }

    private static <T> T decode(CanonicalBytes encoded, Reader<T> reader) {
        if (encoded.isEmpty() || encoded.length() > M5BindingAuthorityRecordsV1.MAX_AUTHORITY_BYTES) {
            throw new IllegalArgumentException("Binding authority bytes exceed their hard cap");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            T value = reader.read(input);
            if (input.read() != -1) {
                throw new IllegalArgumentException("Binding authority contains trailing bytes");
            }
            return value;
        } catch (IOException error) {
            throw new IllegalArgumentException("cannot decode Binding authority", error);
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
