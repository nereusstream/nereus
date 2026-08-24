/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable typed Directory model; row widths are encoded by {@link Nwg1DirectoryCodecV1}. */
public final class Nwg1DirectoryV1 {
    public record BindingContext(
            byte[] bindingId,
            byte[] storageEpochId,
            byte[] ownerFenceCommitment,
            byte[] nti1Bytes,
            int ownerFenceKind,
            int ownerFenceVersion,
            int positionDomainKind,
            int positionDomainVersion,
            int framePolicyKind,
            int framePolicyVersion) {
        public BindingContext {
            bindingId = exact(bindingId, 32, "bindingId");
            storageEpochId = exact(storageEpochId, 32, "storageEpochId");
            ownerFenceCommitment = exact(ownerFenceCommitment, 32, "ownerFenceCommitment");
            nti1Bytes = Objects.requireNonNull(nti1Bytes, "nti1Bytes").clone();
            if (nti1Bytes.length == 0 || nti1Bytes.length > Nwg1ConstantsV1.MAX_NTI1_BYTES) {
                throw new IllegalArgumentException("NTI1 length");
            }
            requireU16(ownerFenceKind, "ownerFenceKind");
            requireU16(ownerFenceVersion, "ownerFenceVersion");
            requireU16(positionDomainKind, "positionDomainKind");
            requireU16(positionDomainVersion, "positionDomainVersion");
            requireU16(framePolicyKind, "framePolicyKind");
            requireU16(framePolicyVersion, "framePolicyVersion");
        }

        @Override
        public byte[] bindingId() {
            return bindingId.clone();
        }

        @Override
        public byte[] storageEpochId() {
            return storageEpochId.clone();
        }

        @Override
        public byte[] ownerFenceCommitment() {
            return ownerFenceCommitment.clone();
        }

        @Override
        public byte[] nti1Bytes() {
            return nti1Bytes.clone();
        }
    }

    public sealed interface AppendUnit permits KafkaAppendUnit, PulsarAppendUnit {
        long contextOrdinal();

        long firstFrameOrdinal();

        long frameCount();

        byte[] appendCommitSetId();

        byte[] storageAttemptId();

        byte[] assignedPayloadSha256();
    }

    public record KafkaAppendUnit(
            long contextOrdinal,
            long firstFrameOrdinal,
            long frameCount,
            long partitionId,
            long kafkaLeaderEpoch,
            long startOffset,
            long endOffsetExclusive,
            byte[] appendCommitSetId,
            byte[] storageAttemptId,
            byte[] assignedPayloadSha256)
            implements AppendUnit {
        public KafkaAppendUnit {
            requireU32(contextOrdinal, "contextOrdinal");
            requireU32(firstFrameOrdinal, "firstFrameOrdinal");
            requireU32(frameCount, "frameCount");
            requireU32(partitionId, "partitionId");
            requireU32(kafkaLeaderEpoch, "kafkaLeaderEpoch");
            if (frameCount == 0 || startOffset < 0 || endOffsetExclusive <= startOffset) {
                throw new IllegalArgumentException("invalid Kafka unit range");
            }
            appendCommitSetId = exact(appendCommitSetId, 16, "appendCommitSetId");
            storageAttemptId = exact(storageAttemptId, 16, "storageAttemptId");
            assignedPayloadSha256 = exact(assignedPayloadSha256, 32, "assignedPayloadSha256");
        }

        @Override
        public byte[] appendCommitSetId() {
            return appendCommitSetId.clone();
        }

        @Override
        public byte[] storageAttemptId() {
            return storageAttemptId.clone();
        }

        @Override
        public byte[] assignedPayloadSha256() {
            return assignedPayloadSha256.clone();
        }
    }

    public record PulsarAppendUnit(
            long contextOrdinal,
            long firstFrameOrdinal,
            long frameCount,
            long virtualLedgerId,
            long entryId,
            byte[] appendCommitSetId,
            byte[] storageAttemptId,
            byte[] assignedPayloadSha256)
            implements AppendUnit {
        public PulsarAppendUnit {
            requireU32(contextOrdinal, "contextOrdinal");
            requireU32(firstFrameOrdinal, "firstFrameOrdinal");
            if (frameCount != 1 || virtualLedgerId <= 0 || entryId < 0) {
                throw new IllegalArgumentException("invalid Pulsar unit");
            }
            appendCommitSetId = exact(appendCommitSetId, 16, "appendCommitSetId");
            storageAttemptId = exact(storageAttemptId, 16, "storageAttemptId");
            assignedPayloadSha256 = exact(assignedPayloadSha256, 32, "assignedPayloadSha256");
        }

        @Override
        public byte[] appendCommitSetId() {
            return appendCommitSetId.clone();
        }

        @Override
        public byte[] storageAttemptId() {
            return storageAttemptId.clone();
        }

        @Override
        public byte[] assignedPayloadSha256() {
            return assignedPayloadSha256.clone();
        }
    }

    public record Frame(
            long appendUnitOrdinal,
            long storedBlockBytes,
            long storedBodyOffset,
            long decodedPayloadBytes,
            long payloadCrc32c,
            long coverage0,
            long coverage1,
            int actualCodecKind,
            int actualCodecVersion,
            int payloadChecksumKind,
            int payloadChecksumVersion) {
        public Frame {
            requireU32(appendUnitOrdinal, "appendUnitOrdinal");
            requireU32(storedBlockBytes, "storedBlockBytes");
            requireU32(decodedPayloadBytes, "decodedPayloadBytes");
            requireU32(payloadCrc32c, "payloadCrc32c");
            if (storedBlockBytes < 16 || storedBodyOffset < 0 || coverage0 < 0 || coverage1 < 0) {
                throw new IllegalArgumentException("invalid Frame row");
            }
            requireU16(actualCodecKind, "actualCodecKind");
            requireU16(actualCodecVersion, "actualCodecVersion");
            requireU16(payloadChecksumKind, "payloadChecksumKind");
            requireU16(payloadChecksumVersion, "payloadChecksumVersion");
        }
    }

    private final int protocolKind;
    private final List<BindingContext> bindings;
    private final List<AppendUnit> appendUnits;
    private final List<Frame> frames;

    public Nwg1DirectoryV1(
            int protocolKind,
            List<BindingContext> bindings,
            List<? extends AppendUnit> appendUnits,
            List<Frame> frames) {
        if (protocolKind != 1 && protocolKind != 2) {
            throw new IllegalArgumentException("protocolKind");
        }
        this.protocolKind = protocolKind;
        this.bindings = List.copyOf(bindings);
        this.appendUnits = List.copyOf(appendUnits);
        this.frames = List.copyOf(frames);
        if (this.bindings.isEmpty() || this.bindings.size() > Nwg1ConstantsV1.MAX_BINDING_CONTEXTS) {
            throw new IllegalArgumentException("binding count");
        }
        if (this.appendUnits.isEmpty() || this.appendUnits.size() > Nwg1ConstantsV1.MAX_APPEND_UNITS) {
            throw new IllegalArgumentException("append-unit count");
        }
        if (this.frames.isEmpty() || this.frames.size() > Nwg1ConstantsV1.MAX_FRAMES) {
            throw new IllegalArgumentException("frame count");
        }
        validateReferences();
    }

    private void validateReferences() {
        long nextFrame = 0;
        for (AppendUnit unit : appendUnits) {
            if (unit.contextOrdinal() >= bindings.size()) {
                throw new IllegalArgumentException("context reference");
            }
            if (unit.firstFrameOrdinal() != nextFrame) {
                throw new IllegalArgumentException("non-dense unit frames");
            }
            nextFrame = Math.addExact(nextFrame, unit.frameCount());
            if ((protocolKind == 1) != (unit instanceof KafkaAppendUnit)) {
                throw new IllegalArgumentException("protocol/unit mismatch");
            }
        }
        if (nextFrame != frames.size()) {
            throw new IllegalArgumentException("frame count mismatch");
        }
        for (int i = 0; i < frames.size(); i++) {
            Frame frame = frames.get(i);
            if (frame.appendUnitOrdinal() >= appendUnits.size()) {
                throw new IllegalArgumentException("unit reference");
            }
        }
    }

    public int protocolKind() {
        return protocolKind;
    }

    public List<BindingContext> bindings() {
        return bindings;
    }

    public List<AppendUnit> appendUnits() {
        return appendUnits;
    }

    public List<Frame> frames() {
        return frames;
    }

    static byte[] exact(byte[] value, int length, String name) {
        Objects.requireNonNull(value, name);
        if (value.length != length) {
            throw new IllegalArgumentException(name + " length");
        }
        return Arrays.copyOf(value, value.length);
    }

    static void requireU16(int value, String name) {
        if (value < 0 || value > 65_535) {
            throw new IllegalArgumentException(name + " outside u16");
        }
    }

    static void requireU32(long value, String name) {
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException(name + " outside u32");
        }
    }
}
