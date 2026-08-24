/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.control.Nwg1RootAdmissionCaps;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Immutable pre-AEAD plan sealed before lane-sequence allocation. */
public final class GroupEncodingPlanV1 {
    public record PlannedFrame(
            long appendUnitOrdinal,
            byte[] decodedPayload,
            byte[] preAeadBytes,
            long coverage0,
            long coverage1,
            int actualCodecKind,
            int actualCodecVersion) {
        public PlannedFrame {
            decodedPayload =
                    Objects.requireNonNull(decodedPayload, "decodedPayload").clone();
            preAeadBytes = Objects.requireNonNull(preAeadBytes, "preAeadBytes").clone();
            if (decodedPayload.length > Nwg1ConstantsV1.MAX_DECODED_FRAME_BYTES
                    || preAeadBytes.length + 16L > Nwg1ConstantsV1.MAX_STORED_FRAME_BYTES) {
                throw new IllegalArgumentException("frame cap");
            }
            if (actualCodecKind == 0 && actualCodecVersion == 0) {
                if (!Arrays.equals(decodedPayload, preAeadBytes)) {
                    throw new IllegalArgumentException("NONE is not exact");
                }
            } else if (actualCodecKind == 1 && actualCodecVersion == 1) {
                if (decodedPayload.length == 0 || preAeadBytes.length >= decodedPayload.length) {
                    throw new IllegalArgumentException("ZSTD must be positive and smaller");
                }
                if (!Arrays.equals(decodedPayload, Nwg1ZstdV1.decompress(preAeadBytes, decodedPayload.length))) {
                    throw new IllegalArgumentException("ZSTD round trip");
                }
            } else {
                throw new IllegalArgumentException("unknown frame codec");
            }
        }

        @Override
        public byte[] decodedPayload() {
            return decodedPayload.clone();
        }

        @Override
        public byte[] preAeadBytes() {
            return preAeadBytes.clone();
        }
    }

    private final int protocolKind;
    private final long shardId;
    private final long shardRunEpoch;
    private final int laneId;
    private final int packingPolicyVersion;
    private final long resolvedTargetBytes;
    private final long resolvedLingerNanos;
    private final long actualCloseLingerNanos;
    private final int closeReason;
    private final byte[] protocolCellCommitment;
    private final byte[] providerScopeId;
    private final byte[] rootSha256;
    private final byte[] envelopeCommitment;
    private final List<Nwg1DirectoryV1.BindingContext> bindings;
    private final List<Nwg1DirectoryV1.AppendUnit> appendUnits;
    private final List<PlannedFrame> frames;

    @SuppressWarnings("ParameterNumber")
    public GroupEncodingPlanV1(
            int protocolKind,
            long shardId,
            long shardRunEpoch,
            int laneId,
            int packingPolicyVersion,
            long resolvedTargetBytes,
            long resolvedLingerNanos,
            long actualCloseLingerNanos,
            int closeReason,
            byte[] protocolCellCommitment,
            byte[] providerScopeId,
            byte[] rootSha256,
            byte[] envelopeCommitment,
            List<Nwg1DirectoryV1.BindingContext> bindings,
            List<? extends Nwg1DirectoryV1.AppendUnit> appendUnits,
            List<PlannedFrame> frames) {
        this.protocolKind = protocolKind;
        this.shardId = shardId;
        this.shardRunEpoch = shardRunEpoch;
        this.laneId = laneId;
        this.packingPolicyVersion = packingPolicyVersion;
        this.resolvedTargetBytes = resolvedTargetBytes;
        this.resolvedLingerNanos = resolvedLingerNanos;
        this.actualCloseLingerNanos = actualCloseLingerNanos;
        this.closeReason = Nwg1CloseReasonV1.fromCode(closeReason).code();
        this.protocolCellCommitment = exact32(protocolCellCommitment, "protocolCellCommitment");
        this.providerScopeId = exact32(providerScopeId, "providerScopeId");
        this.rootSha256 = exact32(rootSha256, "rootSha256");
        this.envelopeCommitment = exact32(envelopeCommitment, "envelopeCommitment");
        this.bindings = List.copyOf(bindings);
        this.appendUnits = List.copyOf(appendUnits);
        this.frames = List.copyOf(frames);
        if (this.frames.isEmpty()) {
            throw new IllegalArgumentException("empty plan");
        }
        long decoded = 0;
        for (PlannedFrame frame : this.frames) {
            decoded = Math.addExact(decoded, frame.decodedPayload().length);
        }
        if (decoded == 0 && protocolKind != 2) {
            throw new IllegalArgumentException("zero-byte Kafka plan");
        }
        validateUnitDigests();
        requireAdmission(Nwg1RootAdmissionCaps.formatHardCaps());
    }

    private void validateUnitDigests() {
        for (int unitOrdinal = 0; unitOrdinal < appendUnits.size(); unitOrdinal++) {
            Nwg1DirectoryV1.AppendUnit unit = appendUnits.get(unitOrdinal);
            MessageDigest digest = sha256();
            int firstFrame = Math.toIntExact(unit.firstFrameOrdinal());
            int frameCount = Math.toIntExact(unit.frameCount());
            int endFrame = Math.addExact(firstFrame, frameCount);
            if (firstFrame < 0 || endFrame > frames.size()) {
                throw new IllegalArgumentException("append-unit frame range lies outside the plan");
            }
            for (int frameOrdinal = firstFrame; frameOrdinal < endFrame; frameOrdinal++) {
                PlannedFrame frame = frames.get(frameOrdinal);
                if (frame.appendUnitOrdinal() != unitOrdinal) {
                    throw new IllegalArgumentException("frame differs from its dense append-unit range");
                }
                digest.update(frame.decodedPayload());
            }
            if (!MessageDigest.isEqual(unit.assignedPayloadSha256(), digest.digest())) {
                throw new IllegalArgumentException("assigned payload digest mismatch");
            }
        }
    }

    /**
     * Validates every format and Root-narrowed bound and returns the exact pre-sequence body facts. This method
     * allocates no lane sequence and performs no Provider/KMS work.
     */
    public AdmissionFacts requireAdmission(Nwg1RootAdmissionCaps caps) {
        Objects.requireNonNull(caps, "caps");
        int directoryPlaintextBytes = predictedDirectoryLength();
        long directoryPrefixEnd = Math.addExact(
                Nwg1ConstantsV1.HEADER_BYTES,
                Math.addExact((long) directoryPlaintextBytes, Nwg1ConstantsV1.GCM_TAG_BYTES));
        long canonicalBodyBytes = canonicalBodyLength(directoryPrefixEnd);
        long totalDecodedBytes = 0;
        long[] decodedPerAppendUnit = new long[appendUnits.size()];
        for (PlannedFrame frame : frames) {
            long decodedBytes = frame.decodedPayload().length;
            long storedBytes = Math.addExact(frame.preAeadBytes().length, (long) Nwg1ConstantsV1.GCM_TAG_BYTES);
            if (decodedBytes > caps.maxDecodedFrameBytes() || storedBytes > caps.maxStoredFrameBytes()) {
                throw new IllegalArgumentException("NWG1 frame exceeds the exact Root admission caps");
            }
            int appendUnitOrdinal = Math.toIntExact(frame.appendUnitOrdinal());
            decodedPerAppendUnit[appendUnitOrdinal] =
                    Math.addExact(decodedPerAppendUnit[appendUnitOrdinal], decodedBytes);
            totalDecodedBytes = Math.addExact(totalDecodedBytes, decodedBytes);
        }
        for (long decodedBytes : decodedPerAppendUnit) {
            if (decodedBytes > caps.maxDecodedAppendUnitBytes()) {
                throw new IllegalArgumentException("NWG1 append unit exceeds the exact Root admission caps");
            }
        }
        if (directoryPlaintextBytes > caps.maxDirectoryPlaintextBytes()
                || directoryPrefixEnd > caps.maxDirectoryPrefixBytes()
                || bindings.size() > caps.maxBindingContexts()
                || appendUnits.size() > caps.maxAppendUnits()
                || frames.size() > caps.maxFrames()
                || totalDecodedBytes > caps.maxTotalDecodedPayloadBytes()
                || resolvedTargetBytes > caps.maxTotalDecodedPayloadBytes()
                || canonicalBodyBytes > caps.maxCanonicalBodyBytes()) {
            throw new IllegalArgumentException("NWG1 plan exceeds the exact Root admission caps");
        }

        // Reuse the strict Header domain validator before sequence allocation; zero is a valid synthetic sequence.
        new Nwg1HeaderV1(
                protocolKind,
                shardId,
                shardRunEpoch,
                0,
                packingPolicyVersion,
                resolvedTargetBytes,
                resolvedLingerNanos,
                totalDecodedBytes,
                actualCloseLingerNanos,
                directoryPlaintextBytes,
                Math.addExact((long) directoryPlaintextBytes, Nwg1ConstantsV1.GCM_TAG_BYTES),
                bindings.size(),
                appendUnits.size(),
                frames.size(),
                directoryPrefixEnd,
                canonicalBodyBytes,
                laneId,
                closeReason,
                protocolCellCommitment,
                providerScopeId,
                rootSha256,
                envelopeCommitment);
        Nwg1DirectoryV1 directory = buildDirectory(directoryPrefixEnd);
        byte[] encodedDirectory = Nwg1DirectoryCodecV1.encode(directory);
        if (encodedDirectory.length != directoryPlaintextBytes) {
            throw new IllegalStateException("NWG1 planned Directory length changed before admission");
        }
        return new AdmissionFacts(directoryPlaintextBytes, directoryPrefixEnd, canonicalBodyBytes, totalDecodedBytes);
    }

    /** Executes Binding, append-unit, native-payload, and canonical-order semantics before sequence allocation. */
    public void requireSemanticAdmission(Nwg1VerificationContextV1 context) {
        Objects.requireNonNull(context, "context");
        AdmissionFacts facts = requireAdmission(Nwg1RootAdmissionCaps.formatHardCaps());
        Nwg1DirectoryV1 directory = buildDirectory(facts.directoryPrefixEnd());
        java.util.ArrayList<byte[]> decodedFrames = new java.util.ArrayList<>(frames.size());
        for (PlannedFrame frame : frames) {
            decodedFrames.add(frame.decodedPayload());
        }
        Nwg1ObjectReaderV1.validateBindings(directory, context);
        Nwg1ObjectReaderV1.validateAppendUnits(directory, decodedFrames, context);
    }

    /** Exact domain-separated streaming identity of every immutable pre-AEAD plan input. */
    public Sha256Digest canonicalPlanSha256() {
        AdmissionFacts facts = requireAdmission(Nwg1RootAdmissionCaps.formatHardCaps());
        MessageDigest digest = sha256();
        updateBytes(digest, "NEREUS-NWG1-SEALED-PLAN-V1".getBytes(StandardCharsets.US_ASCII));
        updateInt(digest, protocolKind);
        updateLong(digest, shardId);
        updateLong(digest, shardRunEpoch);
        updateInt(digest, laneId);
        updateInt(digest, packingPolicyVersion);
        updateLong(digest, resolvedTargetBytes);
        updateLong(digest, resolvedLingerNanos);
        updateLong(digest, actualCloseLingerNanos);
        updateInt(digest, closeReason);
        updateBytes(digest, protocolCellCommitment);
        updateBytes(digest, providerScopeId);
        updateBytes(digest, rootSha256);
        updateBytes(digest, envelopeCommitment);
        byte[] directory = Nwg1DirectoryCodecV1.encode(buildDirectory(facts.directoryPrefixEnd()));
        updateBytes(digest, directory);
        updateInt(digest, frames.size());
        for (PlannedFrame frame : frames) {
            updateBytes(digest, frame.decodedPayload());
            updateBytes(digest, frame.preAeadBytes());
        }
        return Sha256Digest.copyOf(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 unavailable", failure);
        }
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update(ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(value)
                .array());
    }

    private static void updateLong(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(value)
                .array());
    }

    private static void updateBytes(MessageDigest digest, byte[] value) {
        updateLong(digest, value.length);
        digest.update(value);
    }

    public record AdmissionFacts(
            int directoryPlaintextBytes,
            long directoryPrefixEnd,
            long canonicalBodyBytes,
            long totalDecodedPayloadBytes) {
        public AdmissionFacts {
            if (directoryPlaintextBytes <= 0
                    || directoryPrefixEnd <= 0
                    || canonicalBodyBytes < directoryPrefixEnd
                    || totalDecodedPayloadBytes < 0) {
                throw new IllegalArgumentException("invalid NWG1 admission facts");
            }
        }
    }

    Nwg1DirectoryV1 buildDirectory(long directoryPrefixEnd) {
        long nextOffset = directoryPrefixEnd;
        java.util.ArrayList<Nwg1DirectoryV1.Frame> rows = new java.util.ArrayList<>(frames.size());
        for (PlannedFrame frame : frames) {
            long stored = Math.addExact(frame.preAeadBytes().length, 16L);
            rows.add(new Nwg1DirectoryV1.Frame(
                    frame.appendUnitOrdinal(),
                    stored,
                    nextOffset,
                    frame.decodedPayload().length,
                    crc32c(frame.decodedPayload()),
                    frame.coverage0(),
                    frame.coverage1(),
                    frame.actualCodecKind(),
                    frame.actualCodecVersion(),
                    1,
                    1));
            nextOffset = Math.addExact(nextOffset, stored);
        }
        return new Nwg1DirectoryV1(protocolKind, bindings, appendUnits, rows);
    }

    int predictedDirectoryLength() {
        int unitWidth = protocolKind == 1 ? 104 : 96;
        int nti = bindings.stream()
                .mapToInt(binding -> binding.nti1Bytes().length)
                .sum();
        return Math.toIntExact(
                36L + 116L * bindings.size() + (long) unitWidth * appendUnits.size() + 48L * frames.size() + nti);
    }

    long actualPayloadBytes() {
        return frames.stream().mapToLong(frame -> frame.decodedPayload().length).sum();
    }

    long canonicalBodyLength(long prefixEnd) {
        long result = prefixEnd;
        for (PlannedFrame frame : frames) {
            result = Math.addExact(result, frame.preAeadBytes().length + 16L);
        }
        return result;
    }

    public int protocolKind() {
        return protocolKind;
    }

    public long shardId() {
        return shardId;
    }

    public long shardRunEpoch() {
        return shardRunEpoch;
    }

    public int laneId() {
        return laneId;
    }

    public int packingPolicyVersion() {
        return packingPolicyVersion;
    }

    public long resolvedTargetBytes() {
        return resolvedTargetBytes;
    }

    public long resolvedLingerNanos() {
        return resolvedLingerNanos;
    }

    public long actualCloseLingerNanos() {
        return actualCloseLingerNanos;
    }

    public int closeReason() {
        return closeReason;
    }

    public byte[] protocolCellCommitment() {
        return protocolCellCommitment.clone();
    }

    public byte[] providerScopeId() {
        return providerScopeId.clone();
    }

    public byte[] rootSha256() {
        return rootSha256.clone();
    }

    public byte[] envelopeCommitment() {
        return envelopeCommitment.clone();
    }

    public List<Nwg1DirectoryV1.BindingContext> bindings() {
        return bindings;
    }

    public List<Nwg1DirectoryV1.AppendUnit> appendUnits() {
        return appendUnits;
    }

    public List<PlannedFrame> frames() {
        return frames;
    }

    private static long crc32c(byte[] value) {
        CRC32C crc = new CRC32C();
        crc.update(value, 0, value.length);
        return crc.getValue();
    }

    private static byte[] exact32(byte[] value, String name) {
        if (value == null || value.length != 32) {
            throw new IllegalArgumentException(name + " length");
        }
        return value.clone();
    }
}
