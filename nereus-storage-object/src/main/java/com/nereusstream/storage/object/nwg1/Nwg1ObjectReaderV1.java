/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.protocol.TopicIncarnationIdentity;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Bounded full-body NWG1 reader with typed earliest-stage failures. */
public final class Nwg1ObjectReaderV1 {
    public record DecodedObject(Nwg1HeaderV1 header, Nwg1DirectoryV1 directory, List<byte[]> decodedFrames) {
        public DecodedObject {
            List<byte[]> copies = new ArrayList<>(decodedFrames.size());
            for (byte[] frame : decodedFrames) {
                copies.add(frame.clone());
            }
            decodedFrames = List.copyOf(copies);
        }

        @Override
        public List<byte[]> decodedFrames() {
            List<byte[]> copies = new ArrayList<>(decodedFrames.size());
            for (byte[] frame : decodedFrames) {
                copies.add(frame.clone());
            }
            return List.copyOf(copies);
        }
    }

    public record AuthenticatedPrefix(
            Nwg1HeaderV1 header, Nwg1DirectoryV1 directory, byte[] exactHeader, long expectedBodyLength) {
        public AuthenticatedPrefix {
            exactHeader = Objects.requireNonNull(exactHeader, "exactHeader").clone();
            Objects.requireNonNull(header, "header");
            Objects.requireNonNull(directory, "directory");
            if (exactHeader.length != Nwg1ConstantsV1.HEADER_BYTES
                    || expectedBodyLength != header.canonicalBodyLength()) {
                throw new IllegalArgumentException("authenticated prefix identity mismatch");
            }
        }

        @Override
        public byte[] exactHeader() {
            return exactHeader.clone();
        }
    }

    public record ExactFrameRange(long inclusiveStart, long exclusiveEnd) {
        public ExactFrameRange {
            if (inclusiveStart < 0 || exclusiveEnd <= inclusiveStart) {
                throw new IllegalArgumentException("invalid exact frame range");
            }
        }

        public long length() {
            return Math.subtractExact(exclusiveEnd, inclusiveStart);
        }
    }

    /** Transfers ownership of one exact ciphertext range to the reader; the returned array is always erased. */
    @FunctionalInterface
    public interface ExactFrameSource {
        byte[] read(ExactFrameRange exactRange, long absoluteFrameOrdinal) throws IOException;
    }

    /**
     * Consumes one verified decoded frame synchronously. The read-only payload is borrowed only for the duration of
     * this call and its backing bytes are erased immediately on return or failure.
     */
    @FunctionalInterface
    public interface VerifiedFrameConsumer {
        void accept(VerifiedFrame frame, ByteBuffer readOnlyDecodedPayload) throws IOException;
    }

    public record VerifiedFrame(
            long absoluteFrameOrdinal,
            long appendUnitFrameOrdinal,
            long coverage0,
            long coverage1,
            int decodedPayloadBytes) {
        public VerifiedFrame {
            if (absoluteFrameOrdinal < 0
                    || appendUnitFrameOrdinal < 0
                    || coverage0 < 0
                    || coverage1 < 0
                    || decodedPayloadBytes < 0) {
                throw new IllegalArgumentException("invalid verified frame metadata");
            }
        }
    }

    /** Compact successful fold result; it deliberately contains no decoded frame or ciphertext collection. */
    public record VerifiedAppendUnit(
            int protocolKind,
            long appendUnitOrdinal,
            long contextOrdinal,
            long firstFrameOrdinal,
            long frameCount,
            long decodedPayloadBytes,
            long coverage0,
            long coverage1,
            byte[] appendCommitSetId,
            byte[] storageAttemptId,
            byte[] assignedPayloadSha256) {
        public VerifiedAppendUnit {
            if ((protocolKind != Nwg1ConstantsV1.PROTOCOL_KAFKA && protocolKind != Nwg1ConstantsV1.PROTOCOL_PULSAR)
                    || appendUnitOrdinal < 0
                    || contextOrdinal < 0
                    || firstFrameOrdinal < 0
                    || frameCount <= 0
                    || decodedPayloadBytes < 0
                    || coverage0 < 0
                    || coverage1 < 0) {
                throw new IllegalArgumentException("invalid verified append-unit metadata");
            }
            appendCommitSetId = exactBytes(appendCommitSetId, 16, "appendCommitSetId");
            storageAttemptId = exactBytes(storageAttemptId, 16, "storageAttemptId");
            assignedPayloadSha256 = exactBytes(assignedPayloadSha256, 32, "assignedPayloadSha256");
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

        private static byte[] exactBytes(byte[] value, int expectedLength, String label) {
            Objects.requireNonNull(value, label);
            if (value.length != expectedLength) {
                throw new IllegalArgumentException(label + " length differs");
            }
            return value.clone();
        }
    }

    private Nwg1ObjectReaderV1() {}

    public static DecodedObject read(
            byte[] body, byte[] expectedBodySha, Nwg1VerificationContextV1 context, byte[] walRunKey) {
        return readVerifiedBody(body, expectedBodySha, context, walRunKey, true);
    }

    static DecodedObject readVerifiedBody(
            byte[] body,
            byte[] expectedBodySha,
            Nwg1VerificationContextV1 context,
            byte[] walRunKey,
            boolean verifyBodyDigest) {
        if (body == null || body.length < 256) {
            fail(Nwg1RejectionV1.TRUNCATED_INPUT, Nwg1ValidationStageV1.HEADER_GRAMMAR, "short body");
        }
        if (body.length > Nwg1ConstantsV1.MAX_CANONICAL_BODY_BYTES) {
            fail(Nwg1RejectionV1.LIMIT_EXCEEDED, Nwg1ValidationStageV1.OBJECT_BODY_DIGEST, "body cap");
        }
        if (verifyBodyDigest && !Arrays.equals(expectedBodySha, Nwg1CommitmentsV1.sha256(body))) {
            fail(Nwg1RejectionV1.DIGEST_MISMATCH, Nwg1ValidationStageV1.OBJECT_BODY_DIGEST, "body SHA");
        }
        byte[] exactHeader = Arrays.copyOfRange(body, 0, 256);
        Nwg1HeaderV1 header = Nwg1HeaderCodecV1.decode(exactHeader);
        verifyHeaderAuthority(header, context);
        if (!Arrays.equals(context.walRunRootSha256(), header.walRunRootSha256())) {
            fail(Nwg1RejectionV1.AUTHORITY_MISMATCH, Nwg1ValidationStageV1.HEADER_AUTHORITY, "Root SHA");
        }
        if (header.canonicalBodyLength() != body.length) {
            fail(Nwg1RejectionV1.DECLARED_LENGTH_MISMATCH, Nwg1ValidationStageV1.HEADER_AUTHORITY, "body length");
        }
        byte[] info = Nwg1CryptoV1.objectKeyInfo(
                header.shardId(), header.shardRunEpoch(), header.laneId(), header.laneSequence());
        byte[] key = Nwg1CryptoV1.deriveObjectAeadKey(walRunKey, context.walRunRootSha256(), info);
        try {
            int prefixEnd = Math.toIntExact(header.directoryPrefixEnd());
            if (prefixEnd > body.length) {
                fail(Nwg1RejectionV1.DECLARED_LENGTH_MISMATCH, Nwg1ValidationStageV1.DIRECTORY_STRUCTURE, "prefix end");
            }
            byte[] encryptedDirectory = Arrays.copyOfRange(body, 256, prefixEnd);
            byte[] directoryPlain = Nwg1CryptoV1.decrypt(
                    key,
                    Nwg1CryptoV1.directoryNonce(),
                    Nwg1CryptoV1.directoryAad(exactHeader),
                    encryptedDirectory,
                    Nwg1ValidationStageV1.DIRECTORY_AEAD);
            Nwg1DirectoryV1 directory = Nwg1DirectoryCodecV1.decode(directoryPlain, header);
            validateBindings(directory, context);
            List<byte[]> decodedFrames = new ArrayList<>(directory.frames().size());
            long expectedOffset = prefixEnd;
            long decodedTotal = 0;
            for (int ordinal = 0; ordinal < directory.frames().size(); ordinal++) {
                Nwg1DirectoryV1.Frame row = directory.frames().get(ordinal);
                if (row.storedBlockBytes() > Nwg1ConstantsV1.MAX_STORED_FRAME_BYTES
                        || row.decodedPayloadBytes() > Nwg1ConstantsV1.MAX_DECODED_FRAME_BYTES) {
                    fail(Nwg1RejectionV1.LIMIT_EXCEEDED, Nwg1ValidationStageV1.DIRECTORY_STRUCTURE, "frame cap");
                }
                long end;
                try {
                    end = Math.addExact(row.storedBodyOffset(), row.storedBlockBytes());
                } catch (ArithmeticException e) {
                    throw new Nwg1ValidationException(
                            Nwg1RejectionV1.ARITHMETIC_OVERFLOW,
                            Nwg1ValidationStageV1.DIRECTORY_STRUCTURE,
                            Nwg1IsolationScopeV1.SHARED_OBJECT,
                            "frame end overflow",
                            e);
                }
                if (row.storedBodyOffset() != expectedOffset) {
                    fail(Nwg1RejectionV1.RANGE_GAP, Nwg1ValidationStageV1.DIRECTORY_STRUCTURE, "frame offset");
                }
                if (end > body.length) {
                    fail(
                            Nwg1RejectionV1.DECLARED_LENGTH_MISMATCH,
                            Nwg1ValidationStageV1.DIRECTORY_STRUCTURE,
                            "frame end");
                }
                byte[] ciphertext = Arrays.copyOfRange(body, Math.toIntExact(expectedOffset), Math.toIntExact(end));
                byte[] frameRow = Nwg1DirectoryCodecV1.frameRowBytes(directory, ordinal);
                byte[] preAead = Nwg1CryptoV1.decrypt(
                        key,
                        Nwg1CryptoV1.frameNonce(ordinal),
                        Nwg1CryptoV1.frameAad(exactHeader, ordinal, frameRow),
                        ciphertext,
                        Nwg1ValidationStageV1.FRAME_AEAD);
                byte[] decoded;
                if (row.actualCodecKind() == 0 && row.actualCodecVersion() == 0) {
                    decoded = preAead;
                    if (decoded.length != row.decodedPayloadBytes()) {
                        failAt(
                                Nwg1RejectionV1.CODEC_CONTRACT_VIOLATION,
                                Nwg1ValidationStageV1.FRAME_CODEC,
                                Nwg1IsolationScopeV1.APPEND_UNIT,
                                "NONE length");
                    }
                } else if (row.actualCodecKind() == 1 && row.actualCodecVersion() == 1) {
                    decoded = Nwg1ZstdV1.decompress(preAead, Math.toIntExact(row.decodedPayloadBytes()));
                } else {
                    failAt(
                            Nwg1RejectionV1.UNKNOWN_CODE,
                            Nwg1ValidationStageV1.FRAME_CODEC,
                            Nwg1IsolationScopeV1.APPEND_UNIT,
                            "frame codec");
                    throw new AssertionError();
                }
                CRC32C crc = new CRC32C();
                crc.update(decoded, 0, decoded.length);
                if (crc.getValue() != row.payloadCrc32c()) {
                    failAt(
                            Nwg1RejectionV1.CHECKSUM_MISMATCH,
                            Nwg1ValidationStageV1.FRAME_PAYLOAD_CRC,
                            Nwg1IsolationScopeV1.APPEND_UNIT,
                            "payload CRC");
                }
                decodedFrames.add(decoded);
                decodedTotal = Math.addExact(decodedTotal, decoded.length);
                expectedOffset = end;
            }
            if (expectedOffset != body.length || decodedTotal != header.actualPayloadBytesAtPlanSeal()) {
                fail(Nwg1RejectionV1.COUNT_MISMATCH, Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS, "final totals");
            }
            validateAppendUnits(directory, decodedFrames, context);
            return new DecodedObject(header, directory, decodedFrames);
        } finally {
            Arrays.fill(key, (byte) 0);
            Arrays.fill(info, (byte) 0);
        }
    }

    static DecodedObject readRoutineFrame(
            byte[] body, Nwg1VerificationContextV1 context, byte[] walRunKey, int selectedFrameOrdinal) {
        if (body == null || body.length < Nwg1ConstantsV1.HEADER_BYTES) {
            fail(Nwg1RejectionV1.TRUNCATED_INPUT, Nwg1ValidationStageV1.HEADER_GRAMMAR, "short body");
        }
        byte[] exactHeader = Arrays.copyOf(body, Nwg1ConstantsV1.HEADER_BYTES);
        Nwg1HeaderV1 header = Nwg1HeaderCodecV1.decode(exactHeader);
        verifyHeaderAuthority(header, context);
        if (header.canonicalBodyLength() != body.length) {
            fail(Nwg1RejectionV1.DECLARED_LENGTH_MISMATCH, Nwg1ValidationStageV1.HEADER_AUTHORITY, "body length");
        }
        byte[] info = Nwg1CryptoV1.objectKeyInfo(
                header.shardId(), header.shardRunEpoch(), header.laneId(), header.laneSequence());
        byte[] key = Nwg1CryptoV1.deriveObjectAeadKey(walRunKey, context.walRunRootSha256(), info);
        try {
            int prefixEnd = Math.toIntExact(header.directoryPrefixEnd());
            if (prefixEnd > body.length) {
                fail(Nwg1RejectionV1.DECLARED_LENGTH_MISMATCH, Nwg1ValidationStageV1.DIRECTORY_STRUCTURE, "prefix end");
            }
            byte[] encryptedDirectory = Arrays.copyOfRange(body, Nwg1ConstantsV1.HEADER_BYTES, prefixEnd);
            byte[] directoryPlain = Nwg1CryptoV1.decrypt(
                    key,
                    Nwg1CryptoV1.directoryNonce(),
                    Nwg1CryptoV1.directoryAad(exactHeader),
                    encryptedDirectory,
                    Nwg1ValidationStageV1.DIRECTORY_AEAD);
            Nwg1DirectoryV1 directory = Nwg1DirectoryCodecV1.decode(directoryPlain, header);
            validateBindings(directory, context);
            if (selectedFrameOrdinal < 0
                    || selectedFrameOrdinal >= directory.frames().size()) {
                failAt(
                        Nwg1RejectionV1.REFERENCE_OUT_OF_RANGE,
                        Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                        Nwg1IsolationScopeV1.APPEND_UNIT,
                        "selected frame ordinal");
            }
            Nwg1DirectoryV1.Frame frame = directory.frames().get(selectedFrameOrdinal);
            if (frame.storedBlockBytes() > Nwg1ConstantsV1.MAX_STORED_FRAME_BYTES
                    || frame.decodedPayloadBytes() > Nwg1ConstantsV1.MAX_DECODED_FRAME_BYTES) {
                fail(Nwg1RejectionV1.LIMIT_EXCEEDED, Nwg1ValidationStageV1.DIRECTORY_STRUCTURE, "frame cap");
            }
            long frameEnd;
            try {
                frameEnd = Math.addExact(frame.storedBodyOffset(), frame.storedBlockBytes());
            } catch (ArithmeticException e) {
                throw new Nwg1ValidationException(
                        Nwg1RejectionV1.ARITHMETIC_OVERFLOW,
                        Nwg1ValidationStageV1.DIRECTORY_STRUCTURE,
                        Nwg1IsolationScopeV1.SHARED_OBJECT,
                        "frame end overflow",
                        e);
            }
            if (frame.storedBodyOffset() < prefixEnd || frameEnd > body.length) {
                fail(
                        Nwg1RejectionV1.REFERENCE_OUT_OF_RANGE,
                        Nwg1ValidationStageV1.DIRECTORY_STRUCTURE,
                        "selected frame byte range");
            }
            byte[] ciphertext =
                    Arrays.copyOfRange(body, Math.toIntExact(frame.storedBodyOffset()), Math.toIntExact(frameEnd));
            byte[] frameRow = Nwg1DirectoryCodecV1.frameRowBytes(directory, selectedFrameOrdinal);
            byte[] preAead = Nwg1CryptoV1.decrypt(
                    key,
                    Nwg1CryptoV1.frameNonce(selectedFrameOrdinal),
                    Nwg1CryptoV1.frameAad(exactHeader, selectedFrameOrdinal, frameRow),
                    ciphertext,
                    Nwg1ValidationStageV1.FRAME_AEAD);
            byte[] decoded = decodeFramePayload(frame, preAead);
            verifyFrameCrc(frame, decoded);
            int unitOrdinal = Math.toIntExact(frame.appendUnitOrdinal());
            if (unitOrdinal >= directory.appendUnits().size()) {
                failAt(
                        Nwg1RejectionV1.REFERENCE_OUT_OF_RANGE,
                        Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                        Nwg1IsolationScopeV1.APPEND_UNIT,
                        "selected frame unit");
            }
            Nwg1DirectoryV1.AppendUnit unit = directory.appendUnits().get(unitOrdinal);
            if (selectedFrameOrdinal < unit.firstFrameOrdinal()
                    || selectedFrameOrdinal >= unit.firstFrameOrdinal() + unit.frameCount()) {
                failAt(
                        Nwg1RejectionV1.REFERENCE_OUT_OF_RANGE,
                        Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                        Nwg1IsolationScopeV1.APPEND_UNIT,
                        "selected frame ownership");
            }
            if (unit instanceof Nwg1DirectoryV1.KafkaAppendUnit kafka) {
                context.nativePayloadVerifier()
                        .validateKafka(
                                decoded,
                                kafka.partitionId(),
                                kafka.kafkaLeaderEpoch(),
                                frame.coverage0(),
                                frame.coverage1());
            } else if (unit instanceof Nwg1DirectoryV1.PulsarAppendUnit pulsar) {
                validatePulsarUnit(unitOrdinal, pulsar, directory, context);
            }
            return new DecodedObject(header, directory, List.of(decoded));
        } finally {
            Arrays.fill(key, (byte) 0);
            Arrays.fill(info, (byte) 0);
        }
    }

    /** Authenticates only the exact Header+Directory prefix and validates its complete shared byte-range layout. */
    public static AuthenticatedPrefix readAuthenticatedPrefix(
            byte[] prefix, long expectedBodyLength, Nwg1VerificationContextV1 context, byte[] walRunKey) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(walRunKey, "walRunKey");
        if (prefix.length < Nwg1ConstantsV1.HEADER_BYTES
                || prefix.length > Nwg1ConstantsV1.MAX_DIRECTORY_PREFIX_BYTES) {
            fail(Nwg1RejectionV1.LIMIT_EXCEEDED, Nwg1ValidationStageV1.HEADER_GRAMMAR, "prefix length");
        }
        byte[] exactHeader = Arrays.copyOf(prefix, Nwg1ConstantsV1.HEADER_BYTES);
        Nwg1HeaderV1 header = Nwg1HeaderCodecV1.decode(exactHeader);
        verifyHeaderAuthority(header, context);
        if (header.directoryPrefixEnd() != prefix.length || header.canonicalBodyLength() != expectedBodyLength) {
            fail(
                    Nwg1RejectionV1.DECLARED_LENGTH_MISMATCH,
                    Nwg1ValidationStageV1.HEADER_AUTHORITY,
                    "prefix/Object length");
        }
        byte[] info = Nwg1CryptoV1.objectKeyInfo(
                header.shardId(), header.shardRunEpoch(), header.laneId(), header.laneSequence());
        byte[] key = Nwg1CryptoV1.deriveObjectAeadKey(walRunKey, context.walRunRootSha256(), info);
        try {
            byte[] directoryPlain = Nwg1CryptoV1.decrypt(
                    key,
                    Nwg1CryptoV1.directoryNonce(),
                    Nwg1CryptoV1.directoryAad(exactHeader),
                    Arrays.copyOfRange(prefix, Nwg1ConstantsV1.HEADER_BYTES, prefix.length),
                    Nwg1ValidationStageV1.DIRECTORY_AEAD);
            Nwg1DirectoryV1 directory = Nwg1DirectoryCodecV1.decode(directoryPlain, header);
            long expectedOffset = header.directoryPrefixEnd();
            for (Nwg1DirectoryV1.Frame frame : directory.frames()) {
                long end;
                try {
                    end = Math.addExact(frame.storedBodyOffset(), frame.storedBlockBytes());
                } catch (ArithmeticException failure) {
                    throw new Nwg1ValidationException(
                            Nwg1RejectionV1.ARITHMETIC_OVERFLOW,
                            Nwg1ValidationStageV1.DIRECTORY_STRUCTURE,
                            Nwg1IsolationScopeV1.SHARED_OBJECT,
                            "frame range overflow",
                            failure);
                }
                if (frame.storedBodyOffset() != expectedOffset
                        || frame.storedBlockBytes() > Nwg1ConstantsV1.MAX_STORED_FRAME_BYTES
                        || frame.decodedPayloadBytes() > Nwg1ConstantsV1.MAX_DECODED_FRAME_BYTES
                        || end > expectedBodyLength) {
                    fail(
                            Nwg1RejectionV1.RANGE_GAP,
                            Nwg1ValidationStageV1.DIRECTORY_STRUCTURE,
                            "non-contiguous shared frame layout");
                }
                expectedOffset = end;
            }
            if (expectedOffset != expectedBodyLength) {
                fail(
                        Nwg1RejectionV1.DECLARED_LENGTH_MISMATCH,
                        Nwg1ValidationStageV1.DIRECTORY_STRUCTURE,
                        "final shared frame endpoint");
            }
            return new AuthenticatedPrefix(header, directory, exactHeader, expectedBodyLength);
        } finally {
            Arrays.fill(key, (byte) 0);
            Arrays.fill(info, (byte) 0);
        }
    }

    public static List<ExactFrameRange> selectedAppendUnitRanges(
            AuthenticatedPrefix prefix, long selectedFrameOrdinal) {
        Objects.requireNonNull(prefix, "prefix");
        if (selectedFrameOrdinal < 0
                || selectedFrameOrdinal >= prefix.directory().frames().size()) {
            throw new IllegalArgumentException("selected frame ordinal lies outside the authenticated Directory");
        }
        Nwg1DirectoryV1.Frame selected = prefix.directory().frames().get(Math.toIntExact(selectedFrameOrdinal));
        Nwg1DirectoryV1.AppendUnit unit =
                prefix.directory().appendUnits().get(Math.toIntExact(selected.appendUnitOrdinal()));
        java.util.ArrayList<ExactFrameRange> ranges = new java.util.ArrayList<>(Math.toIntExact(unit.frameCount()));
        int first = Math.toIntExact(unit.firstFrameOrdinal());
        int count = Math.toIntExact(unit.frameCount());
        for (int ordinal = first; ordinal < Math.addExact(first, count); ordinal++) {
            Nwg1DirectoryV1.Frame frame = prefix.directory().frames().get(ordinal);
            ranges.add(new ExactFrameRange(
                    frame.storedBodyOffset(), Math.addExact(frame.storedBodyOffset(), frame.storedBlockBytes())));
        }
        return List.copyOf(ranges);
    }

    /**
     * Verifies and folds only the selected append unit after shared prefix authentication. Each ciphertext and
     * decoded payload is owned, consumed, and erased one frame at a time. Consumers must defer durable publication
     * until this method returns its compact unit result because the assigned-payload digest is final only then.
     */
    public static VerifiedAppendUnit readSelectedAppendUnitStreaming(
            AuthenticatedPrefix prefix,
            ExactFrameSource exactFrameSource,
            long selectedFrameOrdinal,
            Nwg1VerificationContextV1 context,
            byte[] walRunKey,
            VerifiedFrameConsumer consumer)
            throws IOException {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(exactFrameSource, "exactFrameSource");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(walRunKey, "walRunKey");
        Objects.requireNonNull(consumer, "consumer");
        List<ExactFrameRange> ranges = selectedAppendUnitRanges(prefix, selectedFrameOrdinal);
        Nwg1DirectoryV1 directory = prefix.directory();
        Nwg1DirectoryV1.Frame selected = directory.frames().get(Math.toIntExact(selectedFrameOrdinal));
        int unitOrdinal = Math.toIntExact(selected.appendUnitOrdinal());
        Nwg1DirectoryV1.AppendUnit unit = directory.appendUnits().get(unitOrdinal);
        int contextOrdinal = Math.toIntExact(unit.contextOrdinal());
        validateSelectedBinding(directory.bindings().get(contextOrdinal), directory.protocolKind(), context);
        if (unit.frameCount() != ranges.size()) {
            failAt(
                    Nwg1RejectionV1.COUNT_MISMATCH,
                    Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                    Nwg1IsolationScopeV1.APPEND_UNIT,
                    "selected append-unit frame count");
        }
        Nwg1DirectoryV1.KafkaAppendUnit kafka = unit instanceof Nwg1DirectoryV1.KafkaAppendUnit value ? value : null;
        Nwg1DirectoryV1.PulsarAppendUnit pulsar = unit instanceof Nwg1DirectoryV1.PulsarAppendUnit value ? value : null;
        if (kafka != null
                && (kafka.partitionId() > Integer.MAX_VALUE || kafka.kafkaLeaderEpoch() > Integer.MAX_VALUE)) {
            failAt(
                    Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                    Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                    Nwg1IsolationScopeV1.APPEND_UNIT,
                    "selected Kafka Java-int domain");
        }
        if (pulsar != null && (pulsar.frameCount() != 1 || !context.admitsPulsarLedger(pulsar.virtualLedgerId()))) {
            failAt(
                    Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                    Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                    Nwg1IsolationScopeV1.APPEND_UNIT,
                    "selected Pulsar fixed-slice/one-frame contract");
        }

        byte[] info = Nwg1CryptoV1.objectKeyInfo(
                prefix.header().shardId(),
                prefix.header().shardRunEpoch(),
                prefix.header().laneId(),
                prefix.header().laneSequence());
        byte[] key = Nwg1CryptoV1.deriveObjectAeadKey(walRunKey, context.walRunRootSha256(), info);
        try {
            int first = Math.toIntExact(unit.firstFrameOrdinal());
            MessageDigest assigned = sha256();
            long decodedTotal = 0;
            long nextKafkaOffset = kafka == null ? 0 : kafka.startOffset();
            for (int index = 0; index < ranges.size(); index++) {
                int frameOrdinal = Math.addExact(first, index);
                Nwg1DirectoryV1.Frame frame = directory.frames().get(frameOrdinal);
                if (frame.appendUnitOrdinal() != unitOrdinal) {
                    failAt(
                            Nwg1RejectionV1.REFERENCE_OUT_OF_RANGE,
                            Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                            Nwg1IsolationScopeV1.APPEND_UNIT,
                            "selected frame ownership mismatch");
                }
                ExactFrameRange range = ranges.get(index);
                byte[] ciphertext = null;
                byte[] preAead = null;
                byte[] payload = null;
                try {
                    ciphertext = Objects.requireNonNull(
                            exactFrameSource.read(range, frameOrdinal), "exactFrameSource result");
                    if (ciphertext.length != range.length()) {
                        failAt(
                                Nwg1RejectionV1.DECLARED_LENGTH_MISMATCH,
                                Nwg1ValidationStageV1.FRAME_AEAD,
                                Nwg1IsolationScopeV1.APPEND_UNIT,
                                "selected frame range length");
                    }
                    preAead = Nwg1CryptoV1.decrypt(
                            key,
                            Nwg1CryptoV1.frameNonce(frameOrdinal),
                            Nwg1CryptoV1.frameAad(
                                    prefix.exactHeader(),
                                    frameOrdinal,
                                    Nwg1DirectoryCodecV1.frameRowBytes(directory, frameOrdinal)),
                            ciphertext,
                            Nwg1ValidationStageV1.FRAME_AEAD);
                    payload = decodeFramePayload(frame, preAead);
                    verifyFrameCrc(frame, payload);
                    if (kafka != null) {
                        if (frame.coverage0() != nextKafkaOffset || frame.coverage1() <= frame.coverage0()) {
                            failAt(
                                    frame.coverage0() < nextKafkaOffset
                                            ? Nwg1RejectionV1.RANGE_OVERLAP
                                            : Nwg1RejectionV1.RANGE_GAP,
                                    Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                                    Nwg1IsolationScopeV1.APPEND_UNIT,
                                    "selected Kafka frame coverage");
                        }
                        byte[] nativeInput = payload.clone();
                        Nwg1VerificationContextV1.NativeCoverage nativeCoverage;
                        try {
                            nativeCoverage = context.nativePayloadVerifier()
                                    .validateKafka(
                                            nativeInput,
                                            kafka.partitionId(),
                                            kafka.kafkaLeaderEpoch(),
                                            frame.coverage0(),
                                            frame.coverage1());
                        } finally {
                            erase(nativeInput);
                        }
                        if (nativeCoverage == null
                                || nativeCoverage.startInclusive() != frame.coverage0()
                                || nativeCoverage.endExclusive() != frame.coverage1()) {
                            failAt(
                                    Nwg1RejectionV1.COVERAGE_MISMATCH,
                                    Nwg1ValidationStageV1.NATIVE_FRAME,
                                    Nwg1IsolationScopeV1.APPEND_UNIT,
                                    "selected Kafka native coverage result");
                        }
                        nextKafkaOffset = frame.coverage1();
                    } else if (frame.coverage0() != pulsar.virtualLedgerId() || frame.coverage1() != pulsar.entryId()) {
                        failAt(
                                Nwg1RejectionV1.COVERAGE_MISMATCH,
                                Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                                Nwg1IsolationScopeV1.APPEND_UNIT,
                                "selected Pulsar ledger/entry coverage");
                    }
                    assigned.update(payload);
                    try {
                        decodedTotal = Math.addExact(decodedTotal, payload.length);
                    } catch (ArithmeticException failure) {
                        throw new Nwg1ValidationException(
                                Nwg1RejectionV1.ARITHMETIC_OVERFLOW,
                                Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                                Nwg1IsolationScopeV1.APPEND_UNIT,
                                "selected decoded append-unit byte total overflow",
                                failure);
                    }
                    consumer.accept(
                            new VerifiedFrame(
                                    frameOrdinal, index, frame.coverage0(), frame.coverage1(), payload.length),
                            ByteBuffer.wrap(payload).asReadOnlyBuffer());
                } finally {
                    erase(payload);
                    if (preAead != payload) {
                        erase(preAead);
                    }
                    erase(ciphertext);
                }
            }
            byte[] assignedDigest = assigned.digest();
            if (!MessageDigest.isEqual(unit.assignedPayloadSha256(), assignedDigest)) {
                failAt(
                        Nwg1RejectionV1.DIGEST_MISMATCH,
                        Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                        Nwg1IsolationScopeV1.APPEND_UNIT,
                        "selected assigned-payload digest");
            }
            if (kafka != null && nextKafkaOffset != kafka.endOffsetExclusive()) {
                failAt(
                        Nwg1RejectionV1.COVERAGE_MISMATCH,
                        Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                        Nwg1IsolationScopeV1.APPEND_UNIT,
                        "selected Kafka coverage union");
            }
            long coverage0 = kafka == null ? pulsar.virtualLedgerId() : kafka.startOffset();
            long coverage1 = kafka == null ? pulsar.entryId() : kafka.endOffsetExclusive();
            return new VerifiedAppendUnit(
                    directory.protocolKind(),
                    unitOrdinal,
                    unit.contextOrdinal(),
                    unit.firstFrameOrdinal(),
                    unit.frameCount(),
                    decodedTotal,
                    coverage0,
                    coverage1,
                    unit.appendCommitSetId(),
                    unit.storageAttemptId(),
                    assignedDigest);
        } finally {
            Arrays.fill(key, (byte) 0);
            Arrays.fill(info, (byte) 0);
        }
    }

    private static void erase(byte[] value) {
        if (value != null) {
            Arrays.fill(value, (byte) 0);
        }
    }

    private static byte[] decodeFramePayload(Nwg1DirectoryV1.Frame row, byte[] preAead) {
        if (row.actualCodecKind() == 0 && row.actualCodecVersion() == 0) {
            if (preAead.length != row.decodedPayloadBytes()) {
                failAt(
                        Nwg1RejectionV1.CODEC_CONTRACT_VIOLATION,
                        Nwg1ValidationStageV1.FRAME_CODEC,
                        Nwg1IsolationScopeV1.APPEND_UNIT,
                        "NONE length");
            }
            return preAead;
        }
        if (row.actualCodecKind() == 1 && row.actualCodecVersion() == 1) {
            return Nwg1ZstdV1.decompress(preAead, Math.toIntExact(row.decodedPayloadBytes()));
        }
        failAt(
                Nwg1RejectionV1.UNKNOWN_CODE,
                Nwg1ValidationStageV1.FRAME_CODEC,
                Nwg1IsolationScopeV1.APPEND_UNIT,
                "frame codec");
        throw new AssertionError();
    }

    private static void verifyFrameCrc(Nwg1DirectoryV1.Frame row, byte[] decoded) {
        CRC32C crc = new CRC32C();
        crc.update(decoded, 0, decoded.length);
        if (crc.getValue() != row.payloadCrc32c()) {
            failAt(
                    Nwg1RejectionV1.CHECKSUM_MISMATCH,
                    Nwg1ValidationStageV1.FRAME_PAYLOAD_CRC,
                    Nwg1IsolationScopeV1.APPEND_UNIT,
                    "payload CRC");
        }
    }

    private static void verifyHeaderAuthority(Nwg1HeaderV1 header, Nwg1VerificationContextV1 context) {
        if (header.protocolKind() != context.protocolCell().protocolKind().code()
                || !MessageDigest.isEqual(
                        header.protocolCellCommitment(), Nwg1CommitmentsV1.protocolCell(context.exactNpc1()))
                || !MessageDigest.isEqual(header.cellProviderScopeId(), context.cellProviderScopeId())
                || !MessageDigest.isEqual(header.walRunRootSha256(), context.walRunRootSha256())
                || !MessageDigest.isEqual(
                        header.wrappedEnvelopeCommitment(), Nwg1CommitmentsV1.wrappedEnvelope(context.envelope()))) {
            fail(
                    Nwg1RejectionV1.AUTHORITY_MISMATCH,
                    Nwg1ValidationStageV1.HEADER_AUTHORITY,
                    "Header authority commitment mismatch");
        }
    }

    static void validateBindings(Nwg1DirectoryV1 directory, Nwg1VerificationContextV1 context) {
        byte[] previousKey = null;
        for (Nwg1DirectoryV1.BindingContext binding : directory.bindings()) {
            int expectedProtocolCode = directory.protocolKind();
            int expectedOwnerKind = expectedProtocolCode;
            if (binding.ownerFenceKind() != expectedOwnerKind
                    || binding.ownerFenceVersion() != 1
                    || binding.positionDomainKind() != expectedProtocolCode
                    || binding.positionDomainVersion() != 1
                    || binding.framePolicyKind() != 1
                    || binding.framePolicyVersion() != 1) {
                failAt(
                        Nwg1RejectionV1.UNKNOWN_CODE,
                        Nwg1ValidationStageV1.BINDING_SEMANTICS,
                        Nwg1IsolationScopeV1.BINDING,
                        "closed Binding code mismatch");
            }
            TopicIncarnationIdentity incarnation;
            try {
                incarnation = TopicIncarnationIdentityCodecV1.decode(binding.nti1Bytes());
            } catch (IllegalArgumentException e) {
                throw new Nwg1ValidationException(
                        Nwg1RejectionV1.NON_CANONICAL_ENCODING,
                        Nwg1ValidationStageV1.BINDING_SEMANTICS,
                        Nwg1IsolationScopeV1.BINDING,
                        "invalid NTI1",
                        e);
            }
            if (incarnation.protocolKind().code() != expectedProtocolCode) {
                failAt(
                        Nwg1RejectionV1.AUTHORITY_MISMATCH,
                        Nwg1ValidationStageV1.BINDING_SEMANTICS,
                        Nwg1IsolationScopeV1.BINDING,
                        "NTI1 protocol mismatch");
            }
            var derivedBinding = DeterministicTopicIdsV1.deriveBindingId(context.protocolCell(), incarnation);
            byte[] expectedBinding = derivedBinding.digest().bytes().toByteArray();
            byte[] expectedEpoch = DeterministicTopicIdsV1.deriveStorageEpochId(derivedBinding, 0)
                    .digest()
                    .bytes()
                    .toByteArray();
            if (!MessageDigest.isEqual(binding.bindingId(), expectedBinding)
                    || !MessageDigest.isEqual(binding.storageEpochId(), expectedEpoch)) {
                failAt(
                        Nwg1RejectionV1.AUTHORITY_MISMATCH,
                        Nwg1ValidationStageV1.BINDING_SEMANTICS,
                        Nwg1IsolationScopeV1.BINDING,
                        "Binding/NSE1 ordinal-zero derivation mismatch");
            }
            byte[] witness;
            try {
                witness = context.ownerWitnessProvider()
                        .canonicalWitness(binding.bindingId(), binding.ownerFenceKind(), binding.ownerFenceVersion());
            } catch (RuntimeException e) {
                throw new Nwg1ValidationException(
                        Nwg1RejectionV1.AUTHORITY_MISMATCH,
                        Nwg1ValidationStageV1.BINDING_SEMANTICS,
                        Nwg1IsolationScopeV1.BINDING,
                        "owner-fence witness unavailable",
                        e);
            }
            if (witness == null || witness.length == 0) {
                failAt(
                        Nwg1RejectionV1.AUTHORITY_MISMATCH,
                        Nwg1ValidationStageV1.BINDING_SEMANTICS,
                        Nwg1IsolationScopeV1.BINDING,
                        "owner-fence witness absent");
            }
            byte[] expectedOwner =
                    Nwg1CommitmentsV1.ownerFence(binding.ownerFenceKind(), binding.ownerFenceVersion(), witness);
            if (!MessageDigest.isEqual(expectedOwner, binding.ownerFenceCommitment())) {
                failAt(
                        Nwg1RejectionV1.AUTHORITY_MISMATCH,
                        Nwg1ValidationStageV1.BINDING_SEMANTICS,
                        Nwg1IsolationScopeV1.BINDING,
                        "owner-fence commitment mismatch");
            }
            byte[] sortKey = bindingSortKey(binding);
            if (previousKey != null && Arrays.equals(previousKey, sortKey)) {
                failAt(
                        Nwg1RejectionV1.DUPLICATE_IDENTITY,
                        Nwg1ValidationStageV1.BINDING_SEMANTICS,
                        Nwg1IsolationScopeV1.BINDING,
                        "duplicate Binding identity");
            }
            if (previousKey != null && Arrays.compareUnsigned(previousKey, sortKey) > 0) {
                failAt(
                        Nwg1RejectionV1.CANONICAL_ORDER_VIOLATION,
                        Nwg1ValidationStageV1.BINDING_SEMANTICS,
                        Nwg1IsolationScopeV1.BINDING,
                        "Binding canonical order");
            }
            previousKey = sortKey;
        }
    }

    private static void validateSelectedBinding(
            Nwg1DirectoryV1.BindingContext binding, int expectedProtocolCode, Nwg1VerificationContextV1 context) {
        int expectedOwnerKind = expectedProtocolCode;
        if (binding.ownerFenceKind() != expectedOwnerKind
                || binding.ownerFenceVersion() != 1
                || binding.positionDomainKind() != expectedProtocolCode
                || binding.positionDomainVersion() != 1
                || binding.framePolicyKind() != 1
                || binding.framePolicyVersion() != 1) {
            failAt(
                    Nwg1RejectionV1.UNKNOWN_CODE,
                    Nwg1ValidationStageV1.BINDING_SEMANTICS,
                    Nwg1IsolationScopeV1.BINDING,
                    "selected closed Binding code mismatch");
        }
        TopicIncarnationIdentity incarnation;
        try {
            incarnation = TopicIncarnationIdentityCodecV1.decode(binding.nti1Bytes());
        } catch (IllegalArgumentException failure) {
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.NON_CANONICAL_ENCODING,
                    Nwg1ValidationStageV1.BINDING_SEMANTICS,
                    Nwg1IsolationScopeV1.BINDING,
                    "selected invalid NTI1",
                    failure);
        }
        if (incarnation.protocolKind().code() != expectedProtocolCode) {
            failAt(
                    Nwg1RejectionV1.AUTHORITY_MISMATCH,
                    Nwg1ValidationStageV1.BINDING_SEMANTICS,
                    Nwg1IsolationScopeV1.BINDING,
                    "selected NTI1 protocol mismatch");
        }
        var derivedBinding = DeterministicTopicIdsV1.deriveBindingId(context.protocolCell(), incarnation);
        byte[] expectedBinding = derivedBinding.digest().bytes().toByteArray();
        byte[] expectedEpoch = DeterministicTopicIdsV1.deriveStorageEpochId(derivedBinding, 0)
                .digest()
                .bytes()
                .toByteArray();
        if (!MessageDigest.isEqual(binding.bindingId(), expectedBinding)
                || !MessageDigest.isEqual(binding.storageEpochId(), expectedEpoch)) {
            failAt(
                    Nwg1RejectionV1.AUTHORITY_MISMATCH,
                    Nwg1ValidationStageV1.BINDING_SEMANTICS,
                    Nwg1IsolationScopeV1.BINDING,
                    "selected Binding/NSE1 derivation mismatch");
        }
        byte[] witness;
        try {
            witness = context.ownerWitnessProvider()
                    .canonicalWitness(binding.bindingId(), binding.ownerFenceKind(), binding.ownerFenceVersion());
        } catch (RuntimeException failure) {
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.AUTHORITY_MISMATCH,
                    Nwg1ValidationStageV1.BINDING_SEMANTICS,
                    Nwg1IsolationScopeV1.BINDING,
                    "selected owner-fence witness unavailable",
                    failure);
        }
        if (witness == null || witness.length == 0) {
            failAt(
                    Nwg1RejectionV1.AUTHORITY_MISMATCH,
                    Nwg1ValidationStageV1.BINDING_SEMANTICS,
                    Nwg1IsolationScopeV1.BINDING,
                    "selected owner-fence witness absent");
        }
        byte[] expectedOwner =
                Nwg1CommitmentsV1.ownerFence(binding.ownerFenceKind(), binding.ownerFenceVersion(), witness);
        if (!MessageDigest.isEqual(expectedOwner, binding.ownerFenceCommitment())) {
            failAt(
                    Nwg1RejectionV1.AUTHORITY_MISMATCH,
                    Nwg1ValidationStageV1.BINDING_SEMANTICS,
                    Nwg1IsolationScopeV1.BINDING,
                    "selected owner-fence commitment mismatch");
        }
    }

    static void validateAppendUnits(
            Nwg1DirectoryV1 directory, List<byte[]> decodedFrames, Nwg1VerificationContextV1 context) {
        byte[] previousUnitKey = null;
        for (int unitOrdinal = 0; unitOrdinal < directory.appendUnits().size(); unitOrdinal++) {
            Nwg1DirectoryV1.AppendUnit unit = directory.appendUnits().get(unitOrdinal);
            int first = Math.toIntExact(unit.firstFrameOrdinal());
            int count = Math.toIntExact(unit.frameCount());
            if (first < 0 || count <= 0 || first > decodedFrames.size() - count) {
                failAt(
                        Nwg1RejectionV1.REFERENCE_OUT_OF_RANGE,
                        Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                        Nwg1IsolationScopeV1.APPEND_UNIT,
                        "append-unit frame interval");
            }
            MessageDigest assigned = sha256();
            for (int ordinal = first; ordinal < first + count; ordinal++) {
                Nwg1DirectoryV1.Frame frame = directory.frames().get(ordinal);
                if (frame.appendUnitOrdinal() != unitOrdinal) {
                    failAt(
                            Nwg1RejectionV1.REFERENCE_OUT_OF_RANGE,
                            Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                            Nwg1IsolationScopeV1.APPEND_UNIT,
                            "frame ownership mismatch");
                }
                assigned.update(decodedFrames.get(ordinal));
            }
            if (!MessageDigest.isEqual(unit.assignedPayloadSha256(), assigned.digest())) {
                failAt(
                        Nwg1RejectionV1.DIGEST_MISMATCH,
                        Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                        Nwg1IsolationScopeV1.APPEND_UNIT,
                        "assigned payload digest mismatch");
            }
            if (unit instanceof Nwg1DirectoryV1.KafkaAppendUnit kafka) {
                validateKafkaUnit(unitOrdinal, kafka, directory, decodedFrames, context);
            } else if (unit instanceof Nwg1DirectoryV1.PulsarAppendUnit pulsar) {
                validatePulsarUnit(unitOrdinal, pulsar, directory, context);
            }
            byte[] unitKey = unitSortKey(unit);
            if (previousUnitKey != null && Arrays.equals(previousUnitKey, unitKey)) {
                failAt(
                        Nwg1RejectionV1.DUPLICATE_IDENTITY,
                        Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                        Nwg1IsolationScopeV1.APPEND_UNIT,
                        "duplicate append-unit identity");
            }
            if (previousUnitKey != null && Arrays.compareUnsigned(previousUnitKey, unitKey) > 0) {
                failAt(
                        Nwg1RejectionV1.CANONICAL_ORDER_VIOLATION,
                        Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                        Nwg1IsolationScopeV1.APPEND_UNIT,
                        "append-unit canonical order");
            }
            previousUnitKey = unitKey;
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 unavailable", failure);
        }
    }

    private static void validateKafkaUnit(
            int unitOrdinal,
            Nwg1DirectoryV1.KafkaAppendUnit unit,
            Nwg1DirectoryV1 directory,
            List<byte[]> decodedFrames,
            Nwg1VerificationContextV1 context) {
        if (unit.partitionId() > Integer.MAX_VALUE || unit.kafkaLeaderEpoch() > Integer.MAX_VALUE) {
            failAt(
                    Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                    Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                    Nwg1IsolationScopeV1.APPEND_UNIT,
                    "Kafka Java-int domain");
        }
        long next = unit.startOffset();
        int first = Math.toIntExact(unit.firstFrameOrdinal());
        int count = Math.toIntExact(unit.frameCount());
        for (int ordinal = first; ordinal < first + count; ordinal++) {
            Nwg1DirectoryV1.Frame frame = directory.frames().get(ordinal);
            if (frame.coverage0() != next || frame.coverage1() <= frame.coverage0()) {
                failAt(
                        frame.coverage0() < next ? Nwg1RejectionV1.RANGE_OVERLAP : Nwg1RejectionV1.RANGE_GAP,
                        Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                        Nwg1IsolationScopeV1.APPEND_UNIT,
                        "Kafka frame coverage");
            }
            context.nativePayloadVerifier()
                    .validateKafka(
                            decodedFrames.get(ordinal),
                            unit.partitionId(),
                            unit.kafkaLeaderEpoch(),
                            frame.coverage0(),
                            frame.coverage1());
            next = frame.coverage1();
        }
        if (next != unit.endOffsetExclusive()) {
            failAt(
                    Nwg1RejectionV1.COVERAGE_MISMATCH,
                    Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                    Nwg1IsolationScopeV1.APPEND_UNIT,
                    "Kafka unit coverage union");
        }
    }

    private static void validatePulsarUnit(
            int unitOrdinal,
            Nwg1DirectoryV1.PulsarAppendUnit unit,
            Nwg1DirectoryV1 directory,
            Nwg1VerificationContextV1 context) {
        if (unit.frameCount() != 1 || !context.admitsPulsarLedger(unit.virtualLedgerId())) {
            failAt(
                    Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                    Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                    Nwg1IsolationScopeV1.APPEND_UNIT,
                    "Pulsar fixed-slice/one-frame contract");
        }
        Nwg1DirectoryV1.Frame frame = directory.frames().get(Math.toIntExact(unit.firstFrameOrdinal()));
        if (frame.coverage0() != unit.virtualLedgerId() || frame.coverage1() != unit.entryId()) {
            failAt(
                    Nwg1RejectionV1.COVERAGE_MISMATCH,
                    Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS,
                    Nwg1IsolationScopeV1.APPEND_UNIT,
                    "Pulsar ledger/entry coverage");
        }
    }

    private static byte[] bindingSortKey(Nwg1DirectoryV1.BindingContext binding) {
        byte[] nti = binding.nti1Bytes();
        ByteBuffer out = ByteBuffer.allocate(32 + 32 + 2 + 2 + 32 + 2 + 2 + 2 + 2 + nti.length)
                .order(ByteOrder.BIG_ENDIAN);
        out.put(binding.bindingId())
                .put(binding.storageEpochId())
                .putShort((short) binding.ownerFenceKind())
                .putShort((short) binding.ownerFenceVersion())
                .put(binding.ownerFenceCommitment())
                .putShort((short) binding.positionDomainKind())
                .putShort((short) binding.positionDomainVersion())
                .putShort((short) binding.framePolicyKind())
                .putShort((short) binding.framePolicyVersion())
                .put(nti);
        return out.array();
    }

    private static byte[] unitSortKey(Nwg1DirectoryV1.AppendUnit unit) {
        if (unit instanceof Nwg1DirectoryV1.KafkaAppendUnit kafka) {
            return ByteBuffer.allocate(4 + 8 + 8 + 8 + 16)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putInt((int) unit.contextOrdinal())
                    .putLong(kafka.partitionId())
                    .putLong(kafka.startOffset())
                    .putLong(kafka.endOffsetExclusive())
                    .put(unit.appendCommitSetId())
                    .array();
        } else if (unit instanceof Nwg1DirectoryV1.PulsarAppendUnit pulsar) {
            return ByteBuffer.allocate(4 + 8 + 8 + 16)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putInt((int) unit.contextOrdinal())
                    .putLong(pulsar.virtualLedgerId())
                    .putLong(pulsar.entryId())
                    .put(unit.appendCommitSetId())
                    .array();
        }
        throw new IllegalArgumentException("unknown append unit");
    }

    private static void fail(Nwg1RejectionV1 rejection, Nwg1ValidationStageV1 stage, String message) {
        failAt(rejection, stage, Nwg1IsolationScopeV1.SHARED_OBJECT, message);
    }

    private static void failAt(
            Nwg1RejectionV1 rejection, Nwg1ValidationStageV1 stage, Nwg1IsolationScopeV1 scope, String message) {
        throw new Nwg1ValidationException(rejection, stage, scope, message);
    }
}
