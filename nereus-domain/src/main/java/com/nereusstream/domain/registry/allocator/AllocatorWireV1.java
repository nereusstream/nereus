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

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

/** Strict big-endian fixed-width NVAC1/NVAH1/NVAN1 production allocator wire. */
public final class AllocatorWireV1 {
    public static final int CELL_BYTES = 384;
    public static final int HEAD_BYTES = 192;
    public static final int NODE_BYTES = 256;
    private static final byte[] CELL_MAGIC = "NVAC".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HEAD_MAGIC = "NVAH".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] NODE_MAGIC = "NVAN".getBytes(StandardCharsets.US_ASCII);
    private static final int SCHEMA_VERSION = 1;

    private AllocatorWireV1() {}

    public static CanonicalBytes encodeCell(VirtualLedgerCellAllocatorStateV1 value) {
        ByteBuffer output = ByteBuffer.allocate(CELL_BYTES);
        output.put(CELL_MAGIC);
        putU16(output, SCHEMA_VERSION);
        putU16(output, value.mode().code());
        output.putInt(value.allocatorProtocolVersion());
        putDigest(output, value.ledgerIdCompatibilityNamespaceId());
        putDigest(output, value.sliceAssignmentId());
        output.putLong(value.sliceStartInclusive());
        output.putLong(value.sliceEndInclusive());
        output.putLong(value.nextSliceLedgerId());
        output.putLong(value.nextGrantId());
        output.put((byte) (value.reservation().isPresent() ? 1 : 0));
        output.put(new byte[3]);
        if (value.reservation().isPresent()) {
            CellAllocatorReservationV1 reservation = value.reservation().orElseThrow();
            putDigest(output, reservation.managedLedgerIncarnation().value());
            output.putLong(reservation.grantId());
            output.putLong(reservation.rangeStartInclusive());
            output.putLong(reservation.rangeEndExclusive());
            putDigest(output, reservation.requestId());
            putPointer(output, reservation.expectedAllocationState().visibleChainHead());
            output.putLong(reservation.expectedAllocationState().priorGrantId());
            output.putLong(reservation.expectedAllocationState().priorRangeStartInclusive());
            output.putLong(reservation.expectedAllocationState().priorRangeEndExclusive());
            output.putLong(reservation.expectedAllocationState().nextLedgerId());
        } else {
            output.put(new byte[184]);
        }
        output.put(new byte[88]);
        return CanonicalBytes.copyOf(output.array());
    }

    public static VirtualLedgerCellAllocatorStateV1 decodeCell(CanonicalBytes bytes) {
        ByteBuffer input = input(bytes, CELL_BYTES, CELL_MAGIC);
        requireU16(input, SCHEMA_VERSION, "cell schema");
        AllocatorModeV1 mode = AllocatorModeV1.fromCode(readU16(input));
        int protocolVersion = input.getInt();
        Sha256Digest namespace = readDigest(input);
        Sha256Digest assignment = readDigest(input);
        long sliceStart = input.getLong();
        long sliceEnd = input.getLong();
        long nextLedger = input.getLong();
        long nextGrant = input.getLong();
        int present = Byte.toUnsignedInt(input.get());
        requireZero(input, 3);
        Optional<CellAllocatorReservationV1> reservation;
        if (present == 0) {
            requireZero(input, 184);
            reservation = Optional.empty();
        } else if (present == 1) {
            ManagedLedgerIncarnationIdV1 incarnation = new ManagedLedgerIncarnationIdV1(readDigest(input));
            long grant = input.getLong();
            long rangeStart = input.getLong();
            long rangeEnd = input.getLong();
            Sha256Digest request = readDigest(input);
            ChainPointerV1 expectedPointer = readPointer(input);
            AllocatorHeadStateV1 expected = new AllocatorHeadStateV1(
                    expectedPointer, input.getLong(), input.getLong(), input.getLong(), input.getLong());
            reservation = Optional.of(
                    new CellAllocatorReservationV1(incarnation, grant, rangeStart, rangeEnd, request, expected));
        } else {
            throw invalid("cell reservation presence must be zero or one");
        }
        requireZero(input, 88);
        requireEnd(input);
        VirtualLedgerCellAllocatorStateV1 decoded = new VirtualLedgerCellAllocatorStateV1(
                mode, protocolVersion, namespace, assignment, sliceStart, sliceEnd, nextLedger, nextGrant, reservation);
        requireReencode(bytes, encodeCell(decoded));
        return decoded;
    }

    public static CanonicalBytes encodeHead(ManagedLedgerAllocatorHeadV1 value) {
        ByteBuffer output = ByteBuffer.allocate(HEAD_BYTES);
        output.put(HEAD_MAGIC);
        putU16(output, SCHEMA_VERSION);
        putU16(output, 0);
        output.putInt(value.allocatorProtocolVersion());
        putDigest(output, value.managedLedgerIncarnation().value());
        output.putLong(value.ownerEpoch());
        putPointer(output, value.visibleChainHead());
        output.putLong(value.grantId());
        output.putLong(value.rangeStartInclusive());
        output.putLong(value.rangeEndExclusive());
        output.putLong(value.nextLedgerId());
        output.put(new byte[44]);
        return CanonicalBytes.copyOf(output.array());
    }

    public static ManagedLedgerAllocatorHeadV1 decodeHead(CanonicalBytes bytes) {
        ByteBuffer input = input(bytes, HEAD_BYTES, HEAD_MAGIC);
        requireU16(input, SCHEMA_VERSION, "head schema");
        requireU16(input, 0, "head flags");
        ManagedLedgerAllocatorHeadV1 decoded = new ManagedLedgerAllocatorHeadV1(
                input.getInt(),
                new ManagedLedgerIncarnationIdV1(readDigest(input)),
                input.getLong(),
                readPointer(input),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong());
        requireZero(input, 44);
        requireEnd(input);
        requireReencode(bytes, encodeHead(decoded));
        return decoded;
    }

    public static VirtualLedgerCandidateNodeV1 createNode(
            ManagedLedgerIncarnationIdV1 incarnation,
            long ledgerId,
            long grantId,
            long creatorOwnerEpoch,
            ChainPointerV1 expectedPredecessor,
            Sha256Digest ledgerDescriptorDigest) {
        Sha256Digest nodeId = deriveNodeId(incarnation, ledgerId, grantId);
        VirtualLedgerCandidateNodeV1 preimage = new VirtualLedgerCandidateNodeV1(
                VirtualLedgerCellAllocatorStateV1.PROTOCOL_VERSION,
                incarnation,
                ledgerId,
                grantId,
                creatorOwnerEpoch,
                expectedPredecessor,
                ledgerDescriptorDigest,
                nodeId,
                nonZeroPlaceholder());
        Sha256Digest nodeDigest = Sha256Digest.hash(encodeNodeInternal(preimage, true));
        return new VirtualLedgerCandidateNodeV1(
                preimage.allocatorProtocolVersion(),
                incarnation,
                ledgerId,
                grantId,
                creatorOwnerEpoch,
                expectedPredecessor,
                ledgerDescriptorDigest,
                nodeId,
                nodeDigest);
    }

    public static CanonicalBytes encodeNode(VirtualLedgerCandidateNodeV1 value) {
        Sha256Digest expectedId = deriveNodeId(value.managedLedgerIncarnation(), value.ledgerId(), value.grantId());
        if (!expectedId.equals(value.nodeId())) {
            throw invalid("candidate node ID does not match its immutable identity");
        }
        Sha256Digest expectedDigest = Sha256Digest.hash(encodeNodeInternal(value, true));
        if (!expectedDigest.equals(value.nodeDigest())) {
            throw invalid("candidate node digest does not match canonical NVAN1 bytes");
        }
        return encodeNodeInternal(value, false);
    }

    public static VirtualLedgerCandidateNodeV1 decodeNode(CanonicalBytes bytes) {
        ByteBuffer input = input(bytes, NODE_BYTES, NODE_MAGIC);
        requireU16(input, SCHEMA_VERSION, "node schema");
        requireU16(input, 0, "node flags");
        VirtualLedgerCandidateNodeV1 decoded = new VirtualLedgerCandidateNodeV1(
                input.getInt(),
                new ManagedLedgerIncarnationIdV1(readDigest(input)),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                readPointer(input),
                readDigest(input),
                readDigest(input),
                readDigest(input));
        requireZero(input, 28);
        requireEnd(input);
        requireReencode(bytes, encodeNode(decoded));
        return decoded;
    }

    private static CanonicalBytes encodeNodeInternal(VirtualLedgerCandidateNodeV1 value, boolean zeroDigest) {
        ByteBuffer output = ByteBuffer.allocate(NODE_BYTES);
        output.put(NODE_MAGIC);
        putU16(output, SCHEMA_VERSION);
        putU16(output, 0);
        output.putInt(value.allocatorProtocolVersion());
        putDigest(output, value.managedLedgerIncarnation().value());
        output.putLong(value.ledgerId());
        output.putLong(value.grantId());
        output.putLong(value.creatorOwnerEpoch());
        putPointer(output, value.expectedPredecessor());
        putDigest(output, value.ledgerDescriptorDigest());
        putDigest(output, value.nodeId());
        output.put(
                zeroDigest
                        ? new byte[Sha256Digest.LENGTH]
                        : value.nodeDigest().bytes().toByteArray());
        output.put(new byte[28]);
        return CanonicalBytes.copyOf(output.array());
    }

    private static Sha256Digest deriveNodeId(ManagedLedgerIncarnationIdV1 incarnation, long ledgerId, long grantId) {
        ByteBuffer preimage = ByteBuffer.allocate(4 + Sha256Digest.LENGTH + Long.BYTES * 2);
        preimage.put("NVN1".getBytes(StandardCharsets.US_ASCII));
        putDigest(preimage, incarnation.value());
        preimage.putLong(ledgerId).putLong(grantId);
        return Sha256Digest.hash(CanonicalBytes.copyOf(preimage.array()));
    }

    private static Sha256Digest nonZeroPlaceholder() {
        byte[] value = new byte[Sha256Digest.LENGTH];
        value[value.length - 1] = 1;
        return Sha256Digest.copyOf(value);
    }

    private static ByteBuffer input(CanonicalBytes bytes, int exactLength, byte[] magic) {
        if (bytes == null || bytes.length() != exactLength) {
            throw invalid("allocator value has a non-canonical length");
        }
        ByteBuffer input = ByteBuffer.wrap(bytes.toByteArray());
        byte[] actualMagic = new byte[magic.length];
        input.get(actualMagic);
        if (!Arrays.equals(actualMagic, magic)) {
            throw invalid("allocator value magic differs");
        }
        return input;
    }

    private static void putPointer(ByteBuffer output, ChainPointerV1 pointer) {
        putDigest(output, pointer.nodeId());
        putDigest(output, pointer.nodeDigest());
    }

    private static ChainPointerV1 readPointer(ByteBuffer input) {
        return new ChainPointerV1(readDigest(input), readDigest(input));
    }

    private static void putDigest(ByteBuffer output, Sha256Digest digest) {
        output.put(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(ByteBuffer input) {
        byte[] value = new byte[Sha256Digest.LENGTH];
        input.get(value);
        return Sha256Digest.copyOf(value);
    }

    private static void putU16(ByteBuffer output, int value) {
        output.putShort((short) value);
    }

    private static int readU16(ByteBuffer input) {
        return Short.toUnsignedInt(input.getShort());
    }

    private static void requireU16(ByteBuffer input, int expected, String field) {
        if (readU16(input) != expected) {
            throw invalid(field + " differs");
        }
    }

    private static void requireZero(ByteBuffer input, int length) {
        for (int index = 0; index < length; index++) {
            if (input.get() != 0) {
                throw invalid("allocator reserved bytes must be zero");
            }
        }
    }

    private static void requireEnd(ByteBuffer input) {
        if (input.hasRemaining()) {
            throw invalid("allocator decoder left trailing bytes");
        }
    }

    private static void requireReencode(CanonicalBytes supplied, CanonicalBytes canonical) {
        if (!supplied.equals(canonical)) {
            throw invalid("allocator value is not canonical");
        }
    }

    static AllocatorProtocolException invalid(String message) {
        return new AllocatorProtocolException(AllocatorProtocolException.Code.NON_CANONICAL_WIRE, message);
    }
}
