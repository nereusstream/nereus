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

package com.nereusstream.kafka.bookkeeper.nbke2;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32C;

/** Canonical, strict-EOF NBKE2 major-version-one codec. */
public final class Nbke2CodecV1 {
    private static final int RUN_BINDING_FIXED_BYTES = 146;

    private Nbke2CodecV1() {}

    public static byte[] encode(long ledgerId, long entryId, Nbke2FrameV1 frame) {
        if (ledgerId < 0 || entryId < 0) {
            throw new IllegalArgumentException("ledger and entry IDs must be non-negative");
        }
        validatePhysicalEntryBinding(entryId, frame);
        int flags =
                frame instanceof Nbke2DataV1 data && data.terminalDescriptor().isPresent()
                        ? Nbke2ConstantsV1.DATA_TERMINAL_DESCRIPTOR_FLAG
                        : 0;
        int totalLength = encodedLength(frame);
        ByteBuffer target = ByteBuffer.allocate(totalLength).order(ByteOrder.BIG_ENDIAN);
        writeHeader(target, frame.frameType(), flags, totalLength, ledgerId, entryId);
        writeRunBinding(target, frame.runBinding());
        if (frame instanceof Nbke2RunHeaderV1 header) {
            writeRunHeader(target, header);
        } else if (frame instanceof Nbke2DataV1 data) {
            writeData(target, data);
        } else if (frame instanceof Nbke2RangeIndexBlockV1 block) {
            writeRangeIndexBlock(target, block);
        } else if (frame instanceof Nbke2ProtocolCheckpointV1 checkpoint) {
            writeProtocolCheckpoint(target, checkpoint);
        } else if (frame instanceof Nbke2RunFooterV1 footer) {
            writeRunFooter(target, footer);
        } else {
            throw new IllegalArgumentException("unsupported NBKE2 v1 frame implementation");
        }
        if (target.position() != totalLength - Nbke2ConstantsV1.CRC32C_BYTES) {
            throw new IllegalStateException("NBKE2 encoder length accounting mismatch");
        }
        target.putInt(crc32c(target.array(), 0, target.position()));
        return target.array();
    }

    public static Nbke2FrameV1 decode(byte[] bytes, long expectedLedgerId, long expectedEntryId) {
        if (expectedLedgerId < 0 || expectedEntryId < 0) {
            throw new IllegalArgumentException("expected ledger and entry IDs must be non-negative");
        }
        if (bytes == null || bytes.length < Nbke2ConstantsV1.FIXED_HEADER_BYTES + Nbke2ConstantsV1.CRC32C_BYTES) {
            throw reject(Nbke2RejectionV1.TRUNCATED, "frame is shorter than the fixed header and CRC");
        }
        if (bytes.length > Nbke2ConstantsV1.FORMAT_MAX_FRAME_BYTES) {
            throw reject(Nbke2RejectionV1.LENGTH_LIMIT_EXCEEDED, "frame exceeds the v1 format cap");
        }
        try {
            Reader reader = new Reader(bytes);
            for (byte expected : Nbke2ConstantsV1.MAGIC) {
                if (reader.i8() != expected) {
                    throw reject(Nbke2RejectionV1.BAD_MAGIC, "NBKE2 magic mismatch");
                }
            }
            int major = reader.u8();
            int minor = reader.u8();
            if (major != Nbke2ConstantsV1.MAJOR_VERSION) {
                throw reject(Nbke2RejectionV1.UNKNOWN_MAJOR, "unsupported NBKE2 major version: " + major);
            }
            if (minor != Nbke2ConstantsV1.MINOR_VERSION) {
                throw reject(Nbke2RejectionV1.UNKNOWN_MINOR, "unsupported NBKE2 minor version: " + minor);
            }
            Nbke2FrameTypeV1 type = Nbke2FrameTypeV1.fromCode(reader.u8());
            int flags = reader.u8();
            int knownFlags = type == Nbke2FrameTypeV1.DATA ? Nbke2ConstantsV1.KNOWN_DATA_FLAGS : 0;
            if ((flags & ~knownFlags) != 0 || type != Nbke2FrameTypeV1.DATA && flags != 0) {
                throw reject(Nbke2RejectionV1.UNKNOWN_FLAGS, "unknown or illegal flags for " + type);
            }
            if (reader.u8() != 0) {
                throw reject(Nbke2RejectionV1.RESERVED_NON_ZERO, "reserved header byte is non-zero");
            }
            if (reader.u16() != Nbke2ConstantsV1.FIXED_HEADER_BYTES) {
                throw reject(Nbke2RejectionV1.HEADER_LENGTH_MISMATCH, "fixed header length mismatch");
            }
            long totalLength = reader.u32();
            if (totalLength != bytes.length
                    || totalLength > Nbke2ConstantsV1.FORMAT_MAX_FRAME_BYTES
                    || totalLength < Nbke2ConstantsV1.FIXED_HEADER_BYTES + Nbke2ConstantsV1.CRC32C_BYTES) {
                throw reject(Nbke2RejectionV1.TOTAL_LENGTH_INVALID, "declared total length differs from input");
            }
            long ledgerId = reader.i64();
            long entryId = reader.i64();
            if (ledgerId < 0 || ledgerId != expectedLedgerId) {
                throw reject(Nbke2RejectionV1.LEDGER_ID_MISMATCH, "ledger ID mismatch");
            }
            if (entryId < 0 || entryId != expectedEntryId) {
                throw reject(Nbke2RejectionV1.ENTRY_ID_MISMATCH, "entry ID mismatch");
            }

            int storedCrc = ByteBuffer.wrap(bytes, bytes.length - Nbke2ConstantsV1.CRC32C_BYTES, 4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
            int computedCrc = crc32c(bytes, 0, bytes.length - Nbke2ConstantsV1.CRC32C_BYTES);
            if (storedCrc != computedCrc) {
                throw reject(Nbke2RejectionV1.CRC32C_MISMATCH, "entry-local CRC32C mismatch");
            }
            reader.limit(bytes.length - Nbke2ConstantsV1.CRC32C_BYTES);
            Nbke2RunBindingV1 binding = reader.runBinding();
            Nbke2FrameV1 frame =
                    switch (type) {
                        case RUN_HEADER -> reader.runHeader(binding);
                        case DATA -> reader.data(binding, flags);
                        case RANGE_INDEX_BLOCK -> reader.rangeIndexBlock(binding);
                        case PROTOCOL_CHECKPOINT -> reader.protocolCheckpoint(binding);
                        case RUN_FOOTER -> reader.runFooter(binding);
                    };
            if (reader.remaining() != 0) {
                throw reject(Nbke2RejectionV1.TRAILING_BYTES, "unconsumed bytes before the CRC");
            }
            try {
                validatePhysicalEntryBinding(entryId, frame);
            } catch (IllegalArgumentException failure) {
                throw new Nbke2DecodingException(
                        Nbke2RejectionV1.FIELD_OUT_OF_DOMAIN,
                        "frame physical bounds differ from the BookKeeper entry ID",
                        failure);
            }
            return frame;
        } catch (Nbke2DecodingException rejection) {
            throw rejection;
        } catch (BufferUnderflowException failure) {
            throw new Nbke2DecodingException(Nbke2RejectionV1.TRUNCATED, "truncated NBKE2 field", failure);
        } catch (ArithmeticException failure) {
            throw new Nbke2DecodingException(
                    Nbke2RejectionV1.ARITHMETIC_OVERFLOW, "checked NBKE2 arithmetic overflow", failure);
        } catch (IllegalArgumentException failure) {
            throw new Nbke2DecodingException(
                    Nbke2RejectionV1.FIELD_OUT_OF_DOMAIN, "invalid NBKE2 semantic field", failure);
        }
    }

    public static int encodedLength(Nbke2FrameV1 frame) {
        try {
            int length = Math.addExact(Nbke2ConstantsV1.FIXED_HEADER_BYTES, runBindingLength(frame.runBinding()));
            length = Math.addExact(length, semanticPayloadLength(frame));
            length = Math.addExact(length, Nbke2ConstantsV1.CRC32C_BYTES);
            if (length > Nbke2ConstantsV1.FORMAT_MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("encoded frame exceeds the NBKE2 v1 format cap");
            }
            return length;
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("encoded frame length overflows", failure);
        }
    }

    /** Exact encoded DATA frame bytes other than the raw assigned RecordBatch. */
    public static int dataFrameOverheadBytes(Nbke2RunBindingV1 binding, boolean terminalDescriptorPresent) {
        try {
            int length = Math.addExact(Nbke2ConstantsV1.FIXED_HEADER_BYTES, runBindingLength(binding));
            length = Math.addExact(length, 56);
            if (terminalDescriptorPresent) {
                length = Math.addExact(length, Nbke2ConstantsV1.APPEND_GROUP_DESCRIPTOR_BYTES);
            }
            return Math.addExact(length, Nbke2ConstantsV1.CRC32C_BYTES);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("NBKE2 DATA overhead overflows", failure);
        }
    }

    private static int semanticPayloadLength(Nbke2FrameV1 frame) {
        if (frame instanceof Nbke2RunHeaderV1) {
            return 48;
        }
        if (frame instanceof Nbke2DataV1 data) {
            return Math.addExact(
                    56 + (data.terminalDescriptor().isPresent() ? Nbke2ConstantsV1.APPEND_GROUP_DESCRIPTOR_BYTES : 0),
                    data.rawAssignedRecordBatch().length());
        }
        if (frame instanceof Nbke2RangeIndexBlockV1 block) {
            return Math.addExact(
                    56 + 4 + Nbke2ConstantsV1.SHA256_BYTES,
                    Math.multiplyExact(block.locators().size(), Nbke2ConstantsV1.LOCATOR_BYTES));
        }
        if (frame instanceof Nbke2ProtocolCheckpointV1 checkpoint) {
            return Math.addExact(
                    32 + 12 + Nbke2ConstantsV1.SHA256_BYTES,
                    Math.addExact(
                            checkpoint.producerState().length(),
                            Math.addExact(
                                    checkpoint.transactionIndex().length(),
                                    checkpoint.leaderEpochIndex().length())));
        }
        if (frame instanceof Nbke2RunFooterV1 footer) {
            return Math.addExact(
                    40 + 4 + Nbke2ConstantsV1.SHA256_BYTES,
                    Math.multiplyExact(footer.indexDirectory().size(), Nbke2ConstantsV1.INDEX_DIRECTORY_ENTRY_BYTES));
        }
        throw new IllegalArgumentException("unsupported NBKE2 v1 frame implementation");
    }

    private static int runBindingLength(Nbke2RunBindingV1 binding) {
        return Math.addExact(
                RUN_BINDING_FIXED_BYTES,
                binding.topicIncarnation().topicName().bytes().length());
    }

    private static void validatePhysicalEntryBinding(long entryId, Nbke2FrameV1 frame) {
        if (frame instanceof Nbke2RunHeaderV1 header) {
            if (entryId != 0 || header.firstDataEntryId() <= entryId) {
                throw new IllegalArgumentException("RUN_HEADER must be entry zero before its first DATA entry");
            }
        } else if (frame instanceof Nbke2DataV1 data) {
            data.terminalDescriptor().ifPresent(descriptor -> {
                if (descriptor.lastDataEntryId() != entryId) {
                    throw new IllegalArgumentException("terminal DATA descriptor does not bind its entry ID");
                }
            });
        } else if (frame instanceof Nbke2RangeIndexBlockV1 block) {
            if (entryId <= block.lastDataEntryId() || block.successorDataEntryId() <= entryId) {
                throw new IllegalArgumentException("range-index control entry is not between DATA spans");
            }
        } else if (frame instanceof Nbke2RunFooterV1 footer) {
            if (footer.lastPhysicalEntryIdExclusive() != Math.incrementExact(entryId)) {
                throw new IllegalArgumentException("RUN_FOOTER does not terminate the physical entry range");
            }
        }
    }

    private static void writeHeader(
            ByteBuffer target, Nbke2FrameTypeV1 type, int flags, int totalLength, long ledgerId, long entryId) {
        target.put(Nbke2ConstantsV1.MAGIC);
        target.put((byte) Nbke2ConstantsV1.MAJOR_VERSION);
        target.put((byte) Nbke2ConstantsV1.MINOR_VERSION);
        target.put((byte) type.code());
        target.put((byte) flags);
        target.put((byte) 0);
        target.putShort((short) Nbke2ConstantsV1.FIXED_HEADER_BYTES);
        target.putInt(totalLength);
        target.putLong(ledgerId);
        target.putLong(entryId);
    }

    private static void writeRunBinding(ByteBuffer target, Nbke2RunBindingV1 binding) {
        target.put(binding.bindingId().digest().bytes().toByteArray());
        target.put(binding.topicIncarnation().topicId().value().bytes().toByteArray());
        byte[] topicName = binding.topicIncarnation().topicName().bytes().toByteArray();
        target.putShort((short) topicName.length);
        target.put(topicName);
        target.putInt(binding.partitionId());
        target.put(binding.storageEpochId().digest().bytes().toByteArray());
        target.putLong(binding.creatorOwnerEpoch());
        target.putInt(binding.kafkaLeaderEpoch());
        target.put(binding.providerScopeId().digest().bytes().toByteArray());
        target.put(binding.runId().value().bytes().toByteArray());
    }

    private static void writeRunHeader(ByteBuffer target, Nbke2RunHeaderV1 header) {
        target.putLong(header.kafkaStartOffset());
        target.putLong(header.firstDataEntryId());
        target.put(header.ledgerConfigurationDigest().bytes().toByteArray());
    }

    private static void writeData(ByteBuffer target, Nbke2DataV1 data) {
        target.putLong(data.baseOffset());
        target.putInt(data.lastOffsetDelta());
        putU32(target, data.rawAssignedRecordBatch().length());
        putU32(target, data.memberOrdinal());
        putU32(target, data.memberCount());
        target.put(data.appendGroupId().bytes().toByteArray());
        target.put(data.storageAttemptId().bytes().toByteArray());
        data.terminalDescriptor().ifPresent(descriptor -> {
            target.putLong(descriptor.groupStartOffset());
            target.putLong(descriptor.groupEndOffsetExclusive());
            target.putLong(descriptor.firstDataEntryId());
            target.putLong(descriptor.lastDataEntryId());
            target.put(descriptor.aggregateAssignedPayloadSha256().bytes().toByteArray());
        });
        target.put(data.rawAssignedRecordBatch().toByteArray());
    }

    private static void writeRangeIndexBlock(ByteBuffer target, Nbke2RangeIndexBlockV1 block) {
        target.putLong(block.anchorOffset());
        target.putLong(block.anchorEntryId());
        target.putLong(block.coveredThroughOffset());
        target.putLong(block.firstDataEntryId());
        target.putLong(block.lastDataEntryId());
        target.putLong(block.predecessorBlockEntryId());
        target.putLong(block.successorDataEntryId());
        putU32(target, block.locators().size());
        for (Nbke2BatchLocatorV1 locator : block.locators()) {
            target.putLong(locator.baseOffsetDelta());
            putU32(target, locator.logicalOffsetCount());
            putU32(target, locator.entryIdDelta());
            putU32(target, locator.appendGroupDelta());
            putU32(target, locator.payloadOffset());
            putU32(target, locator.payloadLength());
            putU32(target, locator.physicalChecksumGeneration());
        }
        target.put(sha256(target.array(), 0, target.position()).bytes().toByteArray());
    }

    private static void writeProtocolCheckpoint(ByteBuffer target, Nbke2ProtocolCheckpointV1 checkpoint) {
        target.putLong(checkpoint.rangeIndexCoveredThrough());
        target.putLong(checkpoint.producerStateCoveredThrough());
        target.putLong(checkpoint.transactionIndexCoveredThrough());
        target.putLong(checkpoint.leaderEpochCoveredThrough());
        putU32(target, checkpoint.producerState().length());
        putU32(target, checkpoint.transactionIndex().length());
        putU32(target, checkpoint.leaderEpochIndex().length());
        target.put(checkpoint.producerState().toByteArray());
        target.put(checkpoint.transactionIndex().toByteArray());
        target.put(checkpoint.leaderEpochIndex().toByteArray());
        target.put(sha256(target.array(), 0, target.position()).bytes().toByteArray());
    }

    private static void writeRunFooter(ByteBuffer target, Nbke2RunFooterV1 footer) {
        target.putLong(footer.kafkaEndOffsetExclusive());
        target.putLong(footer.lastPhysicalEntryIdExclusive());
        target.putLong(footer.latestIndexBlockEntryId());
        target.putLong(footer.protocolCheckpointEntryId());
        target.putLong(footer.sealOwnerEpoch());
        putU32(target, footer.indexDirectory().size());
        for (Nbke2IndexDirectoryEntryV1 entry : footer.indexDirectory()) {
            target.putLong(entry.indexBlockEntryId());
            target.putLong(entry.blockStartOffset());
            target.putLong(entry.blockCoveredThroughOffset());
        }
        target.put(sha256(target.array(), 0, target.position()).bytes().toByteArray());
    }

    private static void putU32(ByteBuffer target, long value) {
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException("unsigned 32-bit value is out of range: " + value);
        }
        target.putInt((int) value);
    }

    private static Sha256Digest sha256(byte[] bytes, int offset, int length) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOfRange(bytes, offset, offset + length)));
    }

    private static int crc32c(byte[] bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    private static Nbke2DecodingException reject(Nbke2RejectionV1 rejection, String message) {
        return new Nbke2DecodingException(rejection, message);
    }

    private static final class Reader {
        private final byte[] bytes;
        private final ByteBuffer source;

        private Reader(byte[] bytes) {
            this.bytes = bytes;
            this.source = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        }

        private void limit(int limit) {
            source.limit(limit);
        }

        private int remaining() {
            return source.remaining();
        }

        private byte i8() {
            return source.get();
        }

        private int u8() {
            return Byte.toUnsignedInt(source.get());
        }

        private int u16() {
            return Short.toUnsignedInt(source.getShort());
        }

        private long u32() {
            return Integer.toUnsignedLong(source.getInt());
        }

        private long i64() {
            return source.getLong();
        }

        private int checkedInt(long value, long maximum, Nbke2RejectionV1 rejection, String name) {
            if (value < 0 || value > maximum || value > Integer.MAX_VALUE) {
                throw reject(rejection, name + " exceeds its persisted/allocation cap");
            }
            return (int) value;
        }

        private byte[] fixedBytes(int length) {
            if (length < 0 || length > source.remaining()) {
                throw reject(Nbke2RejectionV1.TRUNCATED, "fixed field is truncated");
            }
            byte[] value = new byte[length];
            source.get(value);
            return value;
        }

        private Nbke2RunBindingV1 runBinding() {
            TopicBindingId bindingId = new TopicBindingId(Sha256Digest.copyOf(fixedBytes(Sha256Digest.LENGTH)));
            KafkaTopicId topicId = new KafkaTopicId(Id128.fromBytes(fixedBytes(Id128.LENGTH)));
            int topicNameLength = u16();
            if (topicNameLength <= 0 || topicNameLength > Nbke2ConstantsV1.FORMAT_MAX_TOPIC_NAME_BYTES) {
                throw reject(Nbke2RejectionV1.LENGTH_LIMIT_EXCEEDED, "topic name length is outside its v1 cap");
            }
            KafkaTopicName topicName = KafkaTopicName.fromBytes(fixedBytes(topicNameLength));
            int partitionId = source.getInt();
            StorageEpochId storageEpochId = new StorageEpochId(Sha256Digest.copyOf(fixedBytes(Sha256Digest.LENGTH)));
            long creatorOwnerEpoch = i64();
            int kafkaLeaderEpoch = source.getInt();
            CellProviderScopeId providerScopeId =
                    new CellProviderScopeId(Sha256Digest.copyOf(fixedBytes(Sha256Digest.LENGTH)));
            StorageRunId runId = new StorageRunId(Id128.fromBytes(fixedBytes(Id128.LENGTH)));
            return new Nbke2RunBindingV1(
                    bindingId,
                    new KafkaTopicIncarnationIdentity(topicId, topicName),
                    partitionId,
                    storageEpochId,
                    creatorOwnerEpoch,
                    kafkaLeaderEpoch,
                    providerScopeId,
                    runId);
        }

        private Nbke2RunHeaderV1 runHeader(Nbke2RunBindingV1 binding) {
            return new Nbke2RunHeaderV1(binding, i64(), i64(), Sha256Digest.copyOf(fixedBytes(Sha256Digest.LENGTH)));
        }

        private Nbke2DataV1 data(Nbke2RunBindingV1 binding, int flags) {
            long baseOffset = i64();
            int lastOffsetDelta = source.getInt();
            int rawLength = checkedInt(
                    u32(),
                    Nbke2ConstantsV1.FORMAT_MAX_DATA_PAYLOAD_BYTES,
                    Nbke2RejectionV1.LENGTH_LIMIT_EXCEEDED,
                    "DATA raw length");
            int memberOrdinal =
                    checkedInt(u32(), Integer.MAX_VALUE, Nbke2RejectionV1.COUNT_LIMIT_EXCEEDED, "member ordinal");
            int memberCount =
                    checkedInt(u32(), Integer.MAX_VALUE, Nbke2RejectionV1.COUNT_LIMIT_EXCEEDED, "member count");
            if (baseOffset < 0 || lastOffsetDelta < 0 || memberCount <= 0 || memberOrdinal >= memberCount) {
                throw reject(Nbke2RejectionV1.FIELD_OUT_OF_DOMAIN, "DATA coverage/member fields are invalid");
            }
            try {
                Math.addExact(baseOffset, Math.addExact((long) lastOffsetDelta, 1L));
            } catch (ArithmeticException failure) {
                throw new Nbke2DecodingException(
                        Nbke2RejectionV1.ARITHMETIC_OVERFLOW, "DATA offset end overflows", failure);
            }
            Id128 appendGroupId = Id128.fromBytes(fixedBytes(Id128.LENGTH));
            Id128 storageAttemptId = Id128.fromBytes(fixedBytes(Id128.LENGTH));
            Optional<Nbke2AppendGroupDescriptorV1> descriptor = Optional.empty();
            if ((flags & Nbke2ConstantsV1.DATA_TERMINAL_DESCRIPTOR_FLAG) != 0) {
                descriptor = Optional.of(new Nbke2AppendGroupDescriptorV1(
                        i64(), i64(), i64(), i64(), Sha256Digest.copyOf(fixedBytes(Sha256Digest.LENGTH))));
            }
            if (source.remaining() != rawLength) {
                throw reject(Nbke2RejectionV1.TOTAL_LENGTH_INVALID, "DATA raw length does not reach strict EOF");
            }
            return new Nbke2DataV1(
                    binding,
                    baseOffset,
                    lastOffsetDelta,
                    memberOrdinal,
                    memberCount,
                    appendGroupId,
                    storageAttemptId,
                    descriptor,
                    CanonicalBytes.copyOf(fixedBytes(rawLength)));
        }

        private Nbke2RangeIndexBlockV1 rangeIndexBlock(Nbke2RunBindingV1 binding) {
            long anchorOffset = i64();
            long anchorEntryId = i64();
            long coveredThrough = i64();
            long firstDataEntryId = i64();
            long lastDataEntryId = i64();
            long predecessorBlockEntryId = i64();
            long successorDataEntryId = i64();
            int count = checkedInt(
                    u32(),
                    Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT,
                    Nbke2RejectionV1.COUNT_LIMIT_EXCEEDED,
                    "locator count");
            int expectedRemaining = Math.addExact(
                    Math.multiplyExact(count, Nbke2ConstantsV1.LOCATOR_BYTES), Nbke2ConstantsV1.SHA256_BYTES);
            if (source.remaining() != expectedRemaining) {
                throw reject(Nbke2RejectionV1.TOTAL_LENGTH_INVALID, "locator count does not reach strict EOF");
            }
            List<Nbke2BatchLocatorV1> locators = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                locators.add(new Nbke2BatchLocatorV1(i64(), u32(), u32(), u32(), u32(), u32(), u32()));
            }
            verifySha256();
            try {
                return new Nbke2RangeIndexBlockV1(
                        binding,
                        anchorOffset,
                        anchorEntryId,
                        coveredThrough,
                        firstDataEntryId,
                        lastDataEntryId,
                        predecessorBlockEntryId,
                        successorDataEntryId,
                        locators);
            } catch (IllegalArgumentException failure) {
                throw new Nbke2DecodingException(
                        Nbke2RejectionV1.ORDERING_VIOLATION,
                        "range-index bounds or locator ordering is invalid",
                        failure);
            }
        }

        private Nbke2ProtocolCheckpointV1 protocolCheckpoint(Nbke2RunBindingV1 binding) {
            long range = i64();
            long producer = i64();
            long transaction = i64();
            long leader = i64();
            int producerLength = checkpointLength("producer state");
            int transactionLength = checkpointLength("transaction index");
            int leaderLength = checkpointLength("leader-epoch index");
            int sectionBytes = Math.addExact(producerLength, Math.addExact(transactionLength, leaderLength));
            if (source.remaining() != Math.addExact(sectionBytes, Nbke2ConstantsV1.SHA256_BYTES)) {
                throw reject(Nbke2RejectionV1.TOTAL_LENGTH_INVALID, "checkpoint lengths do not reach strict EOF");
            }
            CanonicalBytes producerState = CanonicalBytes.copyOf(fixedBytes(producerLength));
            CanonicalBytes transactionIndex = CanonicalBytes.copyOf(fixedBytes(transactionLength));
            CanonicalBytes leaderEpochIndex = CanonicalBytes.copyOf(fixedBytes(leaderLength));
            verifySha256();
            return new Nbke2ProtocolCheckpointV1(
                    binding, range, producer, transaction, leader, producerState, transactionIndex, leaderEpochIndex);
        }

        private int checkpointLength(String name) {
            return checkedInt(
                    u32(),
                    Nbke2ConstantsV1.FORMAT_MAX_CHECKPOINT_SECTION_BYTES,
                    Nbke2RejectionV1.LENGTH_LIMIT_EXCEEDED,
                    name + " length");
        }

        private Nbke2RunFooterV1 runFooter(Nbke2RunBindingV1 binding) {
            long kafkaEnd = i64();
            long lastPhysical = i64();
            long latestIndex = i64();
            long checkpoint = i64();
            long sealOwnerEpoch = i64();
            int count = checkedInt(
                    u32(),
                    Nbke2ConstantsV1.FORMAT_MAX_INDEX_DIRECTORY_COUNT,
                    Nbke2RejectionV1.COUNT_LIMIT_EXCEEDED,
                    "index-directory count");
            int expectedRemaining = Math.addExact(
                    Math.multiplyExact(count, Nbke2ConstantsV1.INDEX_DIRECTORY_ENTRY_BYTES),
                    Nbke2ConstantsV1.SHA256_BYTES);
            if (source.remaining() != expectedRemaining) {
                throw reject(Nbke2RejectionV1.TOTAL_LENGTH_INVALID, "footer count does not reach strict EOF");
            }
            List<Nbke2IndexDirectoryEntryV1> directory = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                directory.add(new Nbke2IndexDirectoryEntryV1(i64(), i64(), i64()));
            }
            verifySha256();
            try {
                return new Nbke2RunFooterV1(
                        binding, kafkaEnd, lastPhysical, latestIndex, checkpoint, sealOwnerEpoch, directory);
            } catch (IllegalArgumentException failure) {
                throw new Nbke2DecodingException(
                        Nbke2RejectionV1.ORDERING_VIOLATION,
                        "footer bounds or index-directory ordering is invalid",
                        failure);
            }
        }

        private void verifySha256() {
            int digestPosition = source.position();
            Sha256Digest stored = Sha256Digest.copyOf(fixedBytes(Sha256Digest.LENGTH));
            Sha256Digest computed = sha256(bytes, 0, digestPosition);
            if (!stored.equals(computed)) {
                throw reject(Nbke2RejectionV1.SHA256_MISMATCH, "authenticated control-frame SHA-256 mismatch");
            }
        }
    }
}
