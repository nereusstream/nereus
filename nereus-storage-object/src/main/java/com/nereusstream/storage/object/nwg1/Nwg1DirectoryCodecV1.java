/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;

/** Strict encoder/decoder for the NWD1 authenticated Directory plaintext. */
public final class Nwg1DirectoryCodecV1 {
    private Nwg1DirectoryCodecV1() {}

    public static byte[] encode(Nwg1DirectoryV1 directory) {
        int unitWidth = directory.protocolKind() == 1
                ? Nwg1ConstantsV1.KAFKA_APPEND_UNIT_ROW_BYTES
                : Nwg1ConstantsV1.PULSAR_APPEND_UNIT_ROW_BYTES;
        int ntiBytes = directory.bindings().stream()
                .mapToInt(b -> b.nti1Bytes().length)
                .sum();
        long total = Math.addExact(
                36L,
                Math.addExact(
                        Math.multiplyExact((long) directory.bindings().size(), Nwg1ConstantsV1.BINDING_ROW_BYTES),
                        Math.addExact(
                                Math.multiplyExact(
                                        (long) directory.appendUnits().size(), unitWidth),
                                Math.addExact(
                                        Math.multiplyExact(
                                                (long) directory.frames().size(), Nwg1ConstantsV1.FRAME_ROW_BYTES),
                                        ntiBytes))));
        if (total > Nwg1ConstantsV1.MAX_DIRECTORY_PLAINTEXT_BYTES) {
            throw new IllegalArgumentException("Directory cap");
        }
        ByteBuffer out = ByteBuffer.allocate(Math.toIntExact(total)).order(ByteOrder.BIG_ENDIAN);
        out.put(Nwg1ConstantsV1.DIRECTORY_MAGIC);
        putU16(out, 1);
        putU16(out, Nwg1ConstantsV1.DIRECTORY_PREAMBLE_BYTES);
        putU16(out, directory.protocolKind());
        putU16(out, 0);
        putU32(out, directory.bindings().size());
        putU32(out, directory.appendUnits().size());
        putU32(out, directory.frames().size());
        putU32(out, ntiBytes);
        putU32(out, total);
        int ntiOffset = 0;
        for (Nwg1DirectoryV1.BindingContext binding : directory.bindings()) {
            out.put(binding.bindingId());
            out.put(binding.storageEpochId());
            out.put(binding.ownerFenceCommitment());
            putU32(out, ntiOffset);
            putU32(out, binding.nti1Bytes().length);
            putU16(out, binding.ownerFenceKind());
            putU16(out, binding.ownerFenceVersion());
            putU16(out, binding.positionDomainKind());
            putU16(out, binding.positionDomainVersion());
            putU16(out, binding.framePolicyKind());
            putU16(out, binding.framePolicyVersion());
            ntiOffset = Math.addExact(ntiOffset, binding.nti1Bytes().length);
        }
        for (Nwg1DirectoryV1.AppendUnit unit : directory.appendUnits()) {
            putU32(out, unit.contextOrdinal());
            putU32(out, unit.firstFrameOrdinal());
            putU32(out, unit.frameCount());
            if (unit instanceof Nwg1DirectoryV1.KafkaAppendUnit kafka) {
                putU32(out, kafka.partitionId());
                putU32(out, kafka.kafkaLeaderEpoch());
                out.putInt(0);
                out.putLong(kafka.startOffset());
                out.putLong(kafka.endOffsetExclusive());
            } else if (unit instanceof Nwg1DirectoryV1.PulsarAppendUnit pulsar) {
                out.putInt(0);
                out.putLong(pulsar.virtualLedgerId());
                out.putLong(pulsar.entryId());
            } else {
                throw new IllegalArgumentException("unknown append unit");
            }
            out.put(unit.appendCommitSetId());
            out.put(unit.storageAttemptId());
            out.put(unit.assignedPayloadSha256());
        }
        for (Nwg1DirectoryV1.Frame frame : directory.frames()) {
            putU32(out, frame.appendUnitOrdinal());
            putU32(out, frame.storedBlockBytes());
            out.putLong(frame.storedBodyOffset());
            putU32(out, frame.decodedPayloadBytes());
            putU32(out, frame.payloadCrc32c());
            out.putLong(frame.coverage0());
            out.putLong(frame.coverage1());
            putU16(out, frame.actualCodecKind());
            putU16(out, frame.actualCodecVersion());
            putU16(out, frame.payloadChecksumKind());
            putU16(out, frame.payloadChecksumVersion());
        }
        for (Nwg1DirectoryV1.BindingContext binding : directory.bindings()) {
            out.put(binding.nti1Bytes());
        }
        int crcOffset = out.position();
        out.putInt(0);
        byte[] encoded = out.array();
        ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN).putInt(crcOffset, crc32c(encoded));
        return encoded;
    }

    public static Nwg1DirectoryV1 decode(byte[] encoded, Nwg1HeaderV1 header) {
        try {
            return decodeExact(encoded, header);
        } catch (Nwg1ValidationException e) {
            throw e;
        } catch (BufferUnderflowException e) {
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.TRUNCATED_INPUT,
                    Nwg1ValidationStageV1.DIRECTORY_STRUCTURE,
                    Nwg1IsolationScopeV1.SHARED_OBJECT,
                    "truncated Directory primitive field",
                    e);
        } catch (ArithmeticException e) {
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.ARITHMETIC_OVERFLOW,
                    Nwg1ValidationStageV1.DIRECTORY_STRUCTURE,
                    Nwg1IsolationScopeV1.SHARED_OBJECT,
                    "Directory arithmetic overflow",
                    e);
        }
    }

    private static Nwg1DirectoryV1 decodeExact(byte[] encoded, Nwg1HeaderV1 header) {
        if (encoded == null || encoded.length < 36) {
            fail(Nwg1RejectionV1.TRUNCATED_INPUT, "short Directory");
        }
        if (encoded.length > Nwg1ConstantsV1.MAX_DIRECTORY_PLAINTEXT_BYTES) {
            fail(Nwg1RejectionV1.LIMIT_EXCEEDED, "Directory cap");
        }
        ByteBuffer in = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        byte[] magic = read(in, 4);
        if (!Arrays.equals(magic, Nwg1ConstantsV1.DIRECTORY_MAGIC)) {
            fail(Nwg1RejectionV1.UNSUPPORTED_VERSION, "Directory magic");
        }
        if (u16(in) != 1 || u16(in) != 32) {
            fail(Nwg1RejectionV1.UNSUPPORTED_VERSION, "Directory version");
        }
        int protocol = u16(in);
        if (u16(in) != 0) {
            fail(Nwg1RejectionV1.REQUIRED_ZERO_NONZERO, "directoryFlags");
        }
        int bindingCount = count(in, Nwg1ConstantsV1.MAX_BINDING_CONTEXTS, "bindingCount");
        int unitCount = count(in, Nwg1ConstantsV1.MAX_APPEND_UNITS, "unitCount");
        int frameCount = count(in, Nwg1ConstantsV1.MAX_FRAMES, "frameCount");
        int ntiBytes = countAllowZero(in, Nwg1ConstantsV1.MAX_DIRECTORY_PLAINTEXT_BYTES, "ntiBytes");
        long declaredLength = u32(in);
        if (declaredLength != encoded.length || header.directoryPlaintextLength() != encoded.length) {
            fail(Nwg1RejectionV1.DECLARED_LENGTH_MISMATCH, "Directory length");
        }
        if (protocol != header.protocolKind()
                || bindingCount != header.bindingContextCount()
                || unitCount != header.appendUnitCount()
                || frameCount != header.frameCount()) {
            fail(Nwg1RejectionV1.COUNT_MISMATCH, "Header/Directory counts");
        }
        int unitWidth = protocol == 1 ? 104 : protocol == 2 ? 96 : -1;
        if (unitWidth < 0) {
            fail(Nwg1RejectionV1.UNKNOWN_CODE, "protocolKind");
        }
        long expected = checkedLength(bindingCount, unitCount, frameCount, ntiBytes, unitWidth);
        if (expected != encoded.length) {
            fail(Nwg1RejectionV1.DECLARED_LENGTH_MISMATCH, "section equation");
        }
        record BindingRaw(
                byte[] id,
                byte[] epoch,
                byte[] owner,
                int offset,
                int length,
                int ok,
                int ov,
                int pk,
                int pv,
                int fk,
                int fv) {}
        List<BindingRaw> rawBindings = new ArrayList<>(bindingCount);
        for (int i = 0; i < bindingCount; i++) {
            rawBindings.add(new BindingRaw(
                    read(in, 32),
                    read(in, 32),
                    read(in, 32),
                    Math.toIntExact(u32(in)),
                    Math.toIntExact(u32(in)),
                    u16(in),
                    u16(in),
                    u16(in),
                    u16(in),
                    u16(in),
                    u16(in)));
        }
        List<Nwg1DirectoryV1.AppendUnit> units = new ArrayList<>(unitCount);
        for (int i = 0; i < unitCount; i++) {
            long context = u32(in);
            long first = u32(in);
            long frames = u32(in);
            if (protocol == 1) {
                long partition = u32(in);
                long leader = u32(in);
                if (in.getInt() != 0) {
                    fail(Nwg1RejectionV1.REQUIRED_ZERO_NONZERO, "Kafka reserved");
                }
                long start = in.getLong();
                long end = in.getLong();
                try {
                    units.add(new Nwg1DirectoryV1.KafkaAppendUnit(
                            context,
                            first,
                            frames,
                            partition,
                            leader,
                            start,
                            end,
                            read(in, 16),
                            read(in, 16),
                            read(in, 32)));
                } catch (IllegalArgumentException e) {
                    failSemantic(
                            Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                            Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                            Nwg1IsolationScopeV1.APPEND_UNIT,
                            "invalid Kafka append-unit domain",
                            e);
                }
            } else {
                if (in.getInt() != 0) {
                    fail(Nwg1RejectionV1.REQUIRED_ZERO_NONZERO, "Pulsar reserved");
                }
                long ledger = in.getLong();
                long entry = in.getLong();
                try {
                    units.add(new Nwg1DirectoryV1.PulsarAppendUnit(
                            context, first, frames, ledger, entry, read(in, 16), read(in, 16), read(in, 32)));
                } catch (IllegalArgumentException e) {
                    failSemantic(
                            Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                            Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                            Nwg1IsolationScopeV1.APPEND_UNIT,
                            "invalid Pulsar append-unit domain",
                            e);
                }
            }
        }
        List<Nwg1DirectoryV1.Frame> frames = new ArrayList<>(frameCount);
        for (int i = 0; i < frameCount; i++) {
            try {
                frames.add(new Nwg1DirectoryV1.Frame(
                        u32(in),
                        u32(in),
                        in.getLong(),
                        u32(in),
                        u32(in),
                        in.getLong(),
                        in.getLong(),
                        u16(in),
                        u16(in),
                        u16(in),
                        u16(in)));
            } catch (IllegalArgumentException e) {
                throw new Nwg1ValidationException(
                        Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                        Nwg1ValidationStageV1.DIRECTORY_STRUCTURE,
                        Nwg1IsolationScopeV1.SHARED_OBJECT,
                        "invalid Frame row domain",
                        e);
            }
        }
        byte[] ntiBlob = read(in, ntiBytes);
        int storedCrc = in.getInt();
        if (in.hasRemaining()) {
            fail(Nwg1RejectionV1.TRAILING_BYTES, "Directory trailing bytes");
        }
        if (storedCrc != crc32c(encoded)) {
            failAt(Nwg1RejectionV1.CHECKSUM_MISMATCH, Nwg1ValidationStageV1.DIRECTORY_CRC, "Directory CRC");
        }
        List<Nwg1DirectoryV1.BindingContext> bindings = new ArrayList<>(bindingCount);
        int nextOffset = 0;
        for (BindingRaw raw : rawBindings) {
            if (raw.offset() != nextOffset
                    || raw.length() <= 0
                    || Math.addExact(raw.offset(), raw.length()) > ntiBlob.length) {
                fail(Nwg1RejectionV1.RANGE_GAP, "NTI1 slices");
            }
            byte[] nti = Arrays.copyOfRange(ntiBlob, raw.offset(), raw.offset() + raw.length());
            try {
                bindings.add(new Nwg1DirectoryV1.BindingContext(
                        raw.id(),
                        raw.epoch(),
                        raw.owner(),
                        nti,
                        raw.ok(),
                        raw.ov(),
                        raw.pk(),
                        raw.pv(),
                        raw.fk(),
                        raw.fv()));
            } catch (IllegalArgumentException e) {
                failSemantic(
                        Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                        Nwg1ValidationStageV1.BINDING_SEMANTICS,
                        Nwg1IsolationScopeV1.BINDING,
                        "invalid Binding row domain",
                        e);
            }
            nextOffset += raw.length();
        }
        if (nextOffset != ntiBlob.length) {
            fail(Nwg1RejectionV1.RANGE_GAP, "NTI1 coverage");
        }
        Nwg1DirectoryV1 result;
        try {
            result = new Nwg1DirectoryV1(protocol, bindings, units, frames);
        } catch (IllegalArgumentException e) {
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.REFERENCE_OUT_OF_RANGE,
                    Nwg1ValidationStageV1.DIRECTORY_STRUCTURE,
                    Nwg1IsolationScopeV1.SHARED_OBJECT,
                    "invalid Directory cross-reference",
                    e);
        }
        if (!Arrays.equals(encoded, encode(result))) {
            fail(Nwg1RejectionV1.NON_CANONICAL_ENCODING, "Directory re-encode");
        }
        return result;
    }

    public static int crc32c(byte[] directory) {
        CRC32C crc = new CRC32C();
        crc.update(directory, 0, directory.length - 4);
        crc.update(new byte[4], 0, 4);
        return (int) crc.getValue();
    }

    public static byte[] frameRowBytes(Nwg1DirectoryV1 directory, int ordinal) {
        byte[] encoded = encode(directory);
        int unitWidth = directory.protocolKind() == 1 ? 104 : 96;
        int start =
                32 + directory.bindings().size() * 116 + directory.appendUnits().size() * unitWidth + ordinal * 48;
        return Arrays.copyOfRange(encoded, start, start + 48);
    }

    private static long checkedLength(int bindings, int units, int frames, int nti, int unitWidth) {
        try {
            return Math.addExact(
                    36L,
                    Math.addExact(
                            Math.multiplyExact((long) bindings, 116),
                            Math.addExact(
                                    Math.multiplyExact((long) units, unitWidth),
                                    Math.addExact(Math.multiplyExact((long) frames, 48), nti))));
        } catch (ArithmeticException e) {
            fail(Nwg1RejectionV1.ARITHMETIC_OVERFLOW, "Directory equation");
            return -1;
        }
    }

    private static int count(ByteBuffer in, int max, String field) {
        int value = countAllowZero(in, max, field);
        if (value == 0) {
            fail(Nwg1RejectionV1.COUNT_MISMATCH, field);
        }
        return value;
    }

    private static int countAllowZero(ByteBuffer in, int max, String field) {
        long value = u32(in);
        if (value > max) {
            fail(Nwg1RejectionV1.LIMIT_EXCEEDED, field);
        }
        return (int) value;
    }

    private static byte[] read(ByteBuffer in, int length) {
        if (in.remaining() < length) {
            fail(Nwg1RejectionV1.TRUNCATED_INPUT, "Directory section");
        }
        byte[] result = new byte[length];
        in.get(result);
        return result;
    }

    private static void putU16(ByteBuffer out, int value) {
        out.putShort((short) value);
    }

    private static void putU32(ByteBuffer out, long value) {
        out.putInt((int) value);
    }

    private static int u16(ByteBuffer in) {
        return Short.toUnsignedInt(in.getShort());
    }

    private static long u32(ByteBuffer in) {
        return Integer.toUnsignedLong(in.getInt());
    }

    private static void fail(Nwg1RejectionV1 rejection, String message) {
        failAt(rejection, Nwg1ValidationStageV1.DIRECTORY_STRUCTURE, message);
    }

    private static void failAt(Nwg1RejectionV1 rejection, Nwg1ValidationStageV1 stage, String message) {
        throw new Nwg1ValidationException(rejection, stage, Nwg1IsolationScopeV1.SHARED_OBJECT, message);
    }

    private static void failSemantic(
            Nwg1RejectionV1 rejection,
            Nwg1ValidationStageV1 stage,
            Nwg1IsolationScopeV1 scope,
            String message,
            IllegalArgumentException cause) {
        throw new Nwg1ValidationException(rejection, stage, scope, message, cause);
    }
}
