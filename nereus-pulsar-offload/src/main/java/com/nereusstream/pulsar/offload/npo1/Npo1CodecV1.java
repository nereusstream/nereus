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

package com.nereusstream.pulsar.offload.npo1;

import com.nereusstream.pulsar.offload.PulsarOffloadKeysV1;
import com.nereusstream.pulsar.offload.PulsarOffloadLimitCandidateV1;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.SparseBlock;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Closed canonical NPO1 root codec for one immutable sealed-ledger attempt. */
public final class Npo1CodecV1 {
    public static final int HEADER_BYTES = 32;
    public static final int SECTION_HEADER_BYTES = 16;
    public static final int SELF_DIGEST_BYTES = 32;
    public static final int SPARSE_ROW_BYTES = 80;
    public static final int MAX_ROOT_BYTES = 8 * 1_024 * 1_024;
    public static final int MAX_LEDGER_METADATA_BYTES = 1 * 1_024 * 1_024;
    public static final int MAX_DATA_DESCRIPTOR_BYTES = 256 * 1_024;
    public static final int MAX_DATA_KEY_BYTES = 1_024;
    public static final int MAX_WRAPPING_KEY_ID_BYTES = 4 * 1_024;
    public static final int MAX_WRAPPING_KEY_VERSION_BYTES = 1_024;
    public static final int MAX_WRAPPED_KEY_BYTES = 16 * 1_024;
    public static final int MAX_SPARSE_ROWS = 65_536;
    public static final int MAX_CUSTOM_METADATA_ENTRIES = 1_024;
    public static final int MAX_CUSTOM_METADATA_BYTES = 1 * 1_024 * 1_024;
    public static final int MAX_ENSEMBLE_SEGMENTS = 65_536;
    public static final int MAX_MEMBERS_PER_ENSEMBLE = 1_024;
    public static final int MAX_BOOKIE_ID_BYTES = 4 * 1_024;
    public static final int MAX_STRING_BYTES = 64 * 1_024;
    public static final long MAX_ENTRY_COUNT = Integer.MAX_VALUE;

    private static final int MAGIC = 0x4e504f31;
    private static final int VERSION = 1;
    private static final int SHA256_FAMILY = 1;
    private static final Comparator<String> UNSIGNED_UTF8 = (left, right) ->
            Arrays.compareUnsigned(left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));

    private Npo1CodecV1() {}

    public enum DigestType {
        CRC32C(1),
        MAC(2),
        CRC32(3),
        DUMMY(4);

        private final int id;

        DigestType(int id) {
            this.id = id;
        }

        int id() {
            return id;
        }

        static DigestType fromId(int id) {
            return switch (id) {
                case 1 -> CRC32C;
                case 2 -> MAC;
                case 3 -> CRC32;
                case 4 -> DUMMY;
                default -> throw rejected("unknown sealed-ledger digest type");
            };
        }
    }

    /** Immutable binary BookKeeper custom-metadata value; a password is never represented here. */
    public record CustomMetadataValue(byte[] bytes) {
        public CustomMetadataValue {
            Objects.requireNonNull(bytes, "bytes");
            bytes = bytes.clone();
            if (bytes.length > MAX_CUSTOM_METADATA_BYTES) {
                throw rejected("custom metadata value exceeds its total byte cap");
            }
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof CustomMetadataValue value && Arrays.equals(bytes, value.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    /** Opaque KMS/provider envelope for the random AES-256 attempt key. */
    public record AttemptKeyEnvelope(
            int formatVersion,
            String providerId,
            String wrappingKeyId,
            String wrappingKeyVersion,
            String wrappingAlgorithm,
            byte[] wrappedKey) {
        public AttemptKeyEnvelope {
            requireIdentifier(providerId, "key-envelope provider");
            requireString(wrappingKeyId, MAX_WRAPPING_KEY_ID_BYTES, "wrapping key ID");
            requireString(wrappingKeyVersion, MAX_WRAPPING_KEY_VERSION_BYTES, "wrapping key version");
            requireIdentifier(wrappingAlgorithm, "key-wrap algorithm");
            Objects.requireNonNull(wrappedKey, "wrappedKey");
            wrappedKey = wrappedKey.clone();
            if (formatVersion != VERSION || wrappedKey.length == 0 || wrappedKey.length > MAX_WRAPPED_KEY_BYTES) {
                throw rejected("attempt key envelope version or wrapped bytes are outside hard bounds");
            }
        }

        @Override
        public byte[] wrappedKey() {
            return wrappedKey.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof AttemptKeyEnvelope envelope
                    && formatVersion == envelope.formatVersion
                    && providerId.equals(envelope.providerId)
                    && wrappingKeyId.equals(envelope.wrappingKeyId)
                    && wrappingKeyVersion.equals(envelope.wrappingKeyVersion)
                    && wrappingAlgorithm.equals(envelope.wrappingAlgorithm)
                    && Arrays.equals(wrappedKey, envelope.wrappedKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    formatVersion,
                    providerId,
                    wrappingKeyId,
                    wrappingKeyVersion,
                    wrappingAlgorithm,
                    Arrays.hashCode(wrappedKey));
        }
    }

    public record AttemptSection(
            long ledgerId,
            UUID attemptUuid,
            String providerScopePrefix,
            int keyDerivationVersion,
            RetentionClass retentionClass,
            int blockTargetBytes,
            AttemptKeyEnvelope keyEnvelope) {
        public AttemptSection {
            Objects.requireNonNull(attemptUuid, "attemptUuid");
            Objects.requireNonNull(retentionClass, "retentionClass");
            Objects.requireNonNull(keyEnvelope, "keyEnvelope");
            PulsarOffloadKeysV1.derive(providerScopePrefix, ledgerId, attemptUuid);
            if (keyDerivationVersion != PulsarOffloadKeysV1.KEY_DERIVATION_VERSION || blockTargetBytes <= 0) {
                throw rejected("Object-key derivation version or block target is invalid");
            }
        }

        public PulsarOffloadKeysV1 keys() {
            return PulsarOffloadKeysV1.derive(providerScopePrefix, ledgerId, attemptUuid);
        }
    }

    public record EnsembleSegment(long firstEntryId, List<String> bookieIds) {
        public EnsembleSegment {
            bookieIds = List.copyOf(bookieIds);
            if (firstEntryId < 0 || bookieIds.isEmpty() || bookieIds.size() > MAX_MEMBERS_PER_ENSEMBLE) {
                throw rejected("ensemble segment is outside count/range bounds");
            }
            for (String bookieId : bookieIds) {
                requireString(bookieId, MAX_BOOKIE_ID_BYTES, "bookie ID");
            }
        }
    }

    public record SealedLedgerSection(
            long lastAddConfirmed,
            long entryCount,
            long logicalLength,
            long creationTimestampMillis,
            long fencedOwnerEpoch,
            int ensembleSize,
            int writeQuorum,
            int ackQuorum,
            DigestType digestType,
            Map<String, CustomMetadataValue> customMetadata,
            List<EnsembleSegment> ensembles) {
        public SealedLedgerSection {
            Objects.requireNonNull(digestType, "digestType");
            customMetadata = canonicalMap(customMetadata);
            ensembles = List.copyOf(ensembles);
            if (lastAddConfirmed < 0
                    || entryCount != checkedAdd(lastAddConfirmed, 1)
                    || entryCount > MAX_ENTRY_COUNT
                    || logicalLength < 0
                    || creationTimestampMillis < 0
                    || fencedOwnerEpoch < 0
                    || ensembleSize <= 0
                    || writeQuorum <= 0
                    || writeQuorum > ensembleSize
                    || ackQuorum <= 0
                    || ackQuorum > writeQuorum) {
                throw rejected("sealed-ledger scalar fact is invalid");
            }
            validateEnsembles(ensembles, ensembleSize, lastAddConfirmed);
        }
    }

    public record DataExtentSection(
            int dataFormatVersion, String dataKey, long dataBytes, String dataSha256, String immutableVersion) {
        public DataExtentSection {
            requireString(dataKey, MAX_DATA_KEY_BYTES, "data key");
            requireString(immutableVersion, MAX_STRING_BYTES, "immutable version");
            if (dataFormatVersion != Npd1CodecV1.VERSION
                    || dataBytes <= Npd1CodecV1.DATA_HEADER_BYTES
                    || !isSha(dataSha256)) {
                throw rejected("data extent fact is invalid");
            }
        }
    }

    public record Root(
            AttemptSection attempt,
            SealedLedgerSection sealedLedger,
            DataExtentSection dataExtent,
            List<SparseBlock> sparseIndex) {
        public Root {
            Objects.requireNonNull(attempt, "attempt");
            Objects.requireNonNull(sealedLedger, "sealedLedger");
            Objects.requireNonNull(dataExtent, "dataExtent");
            sparseIndex = List.copyOf(sparseIndex);
        }
    }

    public static byte[] canonicalBytes(Root root, PulsarOffloadLimitCandidateV1 limits) {
        validate(root, limits);
        byte[] attempt = attemptBody(root.attempt());
        byte[] ledger = sealedLedgerBody(root.sealedLedger());
        byte[] data = dataExtentBody(root.dataExtent());
        byte[] sparse = sparseIndexBody(root.sparseIndex());
        if (ledger.length > MAX_LEDGER_METADATA_BYTES || data.length > MAX_DATA_DESCRIPTOR_BYTES) {
            throw rejected("NPO1 section crosses its hard byte cap");
        }
        long total = HEADER_BYTES + SELF_DIGEST_BYTES;
        for (byte[] section : List.of(attempt, ledger, data, sparse)) {
            total = checkedAdd(total, checkedAdd(SECTION_HEADER_BYTES, section.length));
        }
        if (total > MAX_ROOT_BYTES) {
            throw rejected("NPO1 root crosses the 8 MiB hard cap");
        }
        ByteBuffer output = ByteBuffer.allocate(checkedInt(total)).order(ByteOrder.BIG_ENDIAN);
        output.putInt(MAGIC)
                .putShort((short) VERSION)
                .putShort((short) VERSION)
                .putInt(0)
                .putInt(HEADER_BYTES)
                .putInt(4)
                .putLong(total)
                .putInt(SHA256_FAMILY);
        section(output, 1, attempt);
        section(output, 2, ledger);
        section(output, 3, data);
        section(output, 4, sparse);
        int digestEnd = output.position();
        output.put(digest(Arrays.copyOf(output.array(), digestEnd)));
        return output.array();
    }

    public static Root parseCanonical(byte[] bytes, PulsarOffloadLimitCandidateV1 limits) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(limits, "limits");
        if (bytes.length < HEADER_BYTES + 4 * SECTION_HEADER_BYTES + SELF_DIGEST_BYTES
                || bytes.length > MAX_ROOT_BYTES) {
            throw rejected("NPO1 root bytes are outside hard bounds");
        }
        ByteBuffer input = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (input.getInt() != MAGIC
                || Short.toUnsignedInt(input.getShort()) != VERSION
                || Short.toUnsignedInt(input.getShort()) != VERSION
                || input.getInt() != 0
                || input.getInt() != HEADER_BYTES
                || input.getInt() != 4
                || input.getLong() != bytes.length
                || input.getInt() != SHA256_FAMILY) {
            throw rejected("NPO1 fixed header differs");
        }
        int digestOffset = bytes.length - SELF_DIGEST_BYTES;
        if (!MessageDigest.isEqual(
                digest(Arrays.copyOf(bytes, digestOffset)), Arrays.copyOfRange(bytes, digestOffset, bytes.length))) {
            throw rejected("NPO1 root self-digest differs");
        }
        byte[] attemptBytes = section(input, 1, digestOffset, MAX_ROOT_BYTES);
        byte[] ledgerBytes = section(input, 2, digestOffset, MAX_LEDGER_METADATA_BYTES);
        byte[] dataBytes = section(input, 3, digestOffset, MAX_DATA_DESCRIPTOR_BYTES);
        byte[] sparseBytes = section(input, 4, digestOffset, MAX_ROOT_BYTES);
        if (input.position() != digestOffset) {
            throw rejected("NPO1 has trailing or missing section bytes");
        }
        Root root = new Root(
                parseAttempt(attemptBytes),
                parseSealedLedger(ledgerBytes),
                parseDataExtent(dataBytes),
                parseSparseIndex(sparseBytes));
        validate(root, limits);
        if (!Arrays.equals(bytes, canonicalBytes(root, limits))) {
            throw rejected("NPO1 bytes are not the canonical re-encoding");
        }
        return root;
    }

    public static String rootSha256(byte[] canonicalRoot) {
        return HexFormat.of().formatHex(digest(canonicalRoot));
    }

    private static void validate(Root root, PulsarOffloadLimitCandidateV1 limits) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(limits, "limits");
        if (!limits.blockTargetBytes().contains(root.attempt().blockTargetBytes())) {
            throw rejected("attempt block target is not one of the evidence candidates");
        }
        if (!root.dataExtent().dataKey().equals(root.attempt().keys().dataKey())
                || root.dataExtent().dataBytes() > limits.maxDataObjectBytes()) {
            throw rejected("data extent key or candidate maximum differs");
        }
        List<SparseBlock> rows = root.sparseIndex();
        if (rows.isEmpty() || rows.size() > MAX_SPARSE_ROWS) {
            throw rejected("sparse row count is outside hard bounds");
        }
        long nextEntry = 0;
        long nextOffset = Npd1CodecV1.DATA_HEADER_BYTES;
        for (int ordinal = 0; ordinal < rows.size(); ordinal++) {
            SparseBlock row = rows.get(ordinal);
            if (row.blockOrdinal() != ordinal
                    || row.firstEntryId() != nextEntry
                    || row.blockOffset() != nextOffset
                    || row.entryCount() > limits.maxEntriesPerBlock()
                    || row.decodedBlockBytes() > limits.maxDecodedBlockBytes()) {
                throw rejected("sparse row entry/byte coverage or candidate cap differs");
            }
            nextEntry = checkedAdd(nextEntry, row.entryCount());
            nextOffset = checkedAdd(nextOffset, row.encodedBlockBytes());
        }
        if (nextEntry != root.sealedLedger().entryCount()
                || checkedAdd(nextEntry, -1) != root.sealedLedger().lastAddConfirmed()
                || nextOffset != root.dataExtent().dataBytes()) {
            throw rejected("sparse rows do not cover exact ledger entries and data bytes");
        }
    }

    private static byte[] attemptBody(AttemptSection section) {
        return body(output -> {
            output.writeLong(section.ledgerId());
            output.writeLong(section.attemptUuid().getMostSignificantBits());
            output.writeLong(section.attemptUuid().getLeastSignificantBits());
            output.writeInt(section.keyDerivationVersion());
            output.writeInt(retentionId(section.retentionClass()));
            output.writeInt(section.blockTargetBytes());
            string(output, section.providerScopePrefix());
            output.writeInt(section.keyEnvelope().formatVersion());
            string(output, section.keyEnvelope().providerId());
            string(output, section.keyEnvelope().wrappingKeyId());
            string(output, section.keyEnvelope().wrappingKeyVersion());
            string(output, section.keyEnvelope().wrappingAlgorithm());
            blob(output, section.keyEnvelope().wrappedKey());
        });
    }

    private static AttemptSection parseAttempt(byte[] bytes) {
        Cursor input = new Cursor(bytes);
        long ledgerId = input.longValue();
        UUID uuid = new UUID(input.longValue(), input.longValue());
        int keyVersion = input.intValue();
        int retention = input.intValue();
        int blockTarget = input.intValue();
        String scope = input.string(MAX_STRING_BYTES);
        int envelopeVersion = input.intValue();
        String envelopeProvider = input.string(64);
        String wrappingKeyId = input.string(MAX_WRAPPING_KEY_ID_BYTES);
        String wrappingKeyVersion = input.string(MAX_WRAPPING_KEY_VERSION_BYTES);
        String wrappingAlgorithm = input.string(64);
        byte[] wrappedKey = input.blob(MAX_WRAPPED_KEY_BYTES);
        input.end();
        return new AttemptSection(
                ledgerId,
                uuid,
                scope,
                keyVersion,
                retentionClass(retention),
                blockTarget,
                new AttemptKeyEnvelope(
                        envelopeVersion,
                        envelopeProvider,
                        wrappingKeyId,
                        wrappingKeyVersion,
                        wrappingAlgorithm,
                        wrappedKey));
    }

    private static byte[] sealedLedgerBody(SealedLedgerSection section) {
        return body(output -> {
            output.writeLong(section.lastAddConfirmed());
            output.writeLong(section.entryCount());
            output.writeLong(section.logicalLength());
            output.writeLong(section.creationTimestampMillis());
            output.writeLong(section.fencedOwnerEpoch());
            output.writeInt(section.ensembleSize());
            output.writeInt(section.writeQuorum());
            output.writeInt(section.ackQuorum());
            output.writeInt(section.digestType().id());
            output.writeInt(section.customMetadata().size());
            for (Map.Entry<String, CustomMetadataValue> row :
                    section.customMetadata().entrySet()) {
                string(output, row.getKey());
                blob(output, row.getValue().bytes());
            }
            output.writeInt(section.ensembles().size());
            for (EnsembleSegment segment : section.ensembles()) {
                output.writeLong(segment.firstEntryId());
                output.writeInt(segment.bookieIds().size());
                for (String bookieId : segment.bookieIds()) {
                    string(output, bookieId);
                }
            }
        });
    }

    private static SealedLedgerSection parseSealedLedger(byte[] bytes) {
        Cursor input = new Cursor(bytes);
        long lac = input.longValue();
        long entryCount = input.longValue();
        long logicalLength = input.longValue();
        long creation = input.longValue();
        long ownerEpoch = input.longValue();
        int ensembleSize = input.intValue();
        int writeQuorum = input.intValue();
        int ackQuorum = input.intValue();
        DigestType digestType = DigestType.fromId(input.intValue());
        int metadataCount = input.intValue();
        if (metadataCount < 0 || metadataCount > MAX_CUSTOM_METADATA_ENTRIES) {
            throw rejected("custom metadata count is outside hard bounds");
        }
        if (metadataCount > Math.max(0, input.remaining() - Integer.BYTES) / 9) {
            throw rejected("custom metadata count exceeds remaining canonical bytes");
        }
        Map<String, CustomMetadataValue> metadata = new LinkedHashMap<>();
        int metadataBytes = 0;
        String previous = null;
        for (int index = 0; index < metadataCount; index++) {
            String key = input.string(MAX_STRING_BYTES);
            byte[] value = input.blob(MAX_CUSTOM_METADATA_BYTES);
            metadataBytes = Math.addExact(
                    metadataBytes, Math.addExact(key.getBytes(StandardCharsets.UTF_8).length, value.length));
            if (metadataBytes > MAX_CUSTOM_METADATA_BYTES
                    || (previous != null && UNSIGNED_UTF8.compare(previous, key) >= 0)
                    || metadata.put(key, new CustomMetadataValue(value)) != null) {
                throw rejected("custom metadata is over cap, duplicated, or non-canonical");
            }
            previous = key;
        }
        int ensembleCount = input.count(MAX_ENSEMBLE_SEGMENTS, "ensemble segment");
        if (ensembleCount > input.remaining() / 17) {
            throw rejected("ensemble count exceeds remaining canonical bytes");
        }
        List<EnsembleSegment> ensembles = new ArrayList<>(ensembleCount);
        for (int index = 0; index < ensembleCount; index++) {
            long firstEntry = input.longValue();
            int memberCount = input.count(MAX_MEMBERS_PER_ENSEMBLE, "ensemble member");
            if (memberCount > input.remaining() / 5) {
                throw rejected("ensemble member count exceeds remaining canonical bytes");
            }
            List<String> members = new ArrayList<>(memberCount);
            for (int member = 0; member < memberCount; member++) {
                members.add(input.string(MAX_BOOKIE_ID_BYTES));
            }
            ensembles.add(new EnsembleSegment(firstEntry, members));
        }
        input.end();
        return new SealedLedgerSection(
                lac,
                entryCount,
                logicalLength,
                creation,
                ownerEpoch,
                ensembleSize,
                writeQuorum,
                ackQuorum,
                digestType,
                metadata,
                ensembles);
    }

    private static byte[] dataExtentBody(DataExtentSection section) {
        return body(output -> {
            output.writeInt(section.dataFormatVersion());
            string(output, section.dataKey());
            output.writeLong(section.dataBytes());
            output.write(HexFormat.of().parseHex(section.dataSha256()));
            string(output, section.immutableVersion());
        });
    }

    private static DataExtentSection parseDataExtent(byte[] bytes) {
        Cursor input = new Cursor(bytes);
        int version = input.intValue();
        String key = input.string(MAX_DATA_KEY_BYTES);
        long length = input.longValue();
        String sha = HexFormat.of().formatHex(input.bytes(32));
        String immutableVersion = input.string(MAX_STRING_BYTES);
        input.end();
        return new DataExtentSection(version, key, length, sha, immutableVersion);
    }

    private static byte[] sparseIndexBody(List<SparseBlock> rows) {
        return body(output -> {
            output.writeInt(rows.size());
            for (SparseBlock row : rows) {
                output.writeInt(row.blockOrdinal());
                output.writeInt(0);
                output.writeLong(row.firstEntryId());
                output.writeInt(row.entryCount());
                output.writeShort(compressionId(row.compressionFamily()));
                output.writeShort(row.encryptionFamily() == Npd1CodecV1.EncryptionFamily.AES_GCM_256 ? 1 : -1);
                output.writeLong(row.blockOffset());
                output.writeLong(row.encodedBlockBytes());
                output.writeLong(row.decodedBlockBytes());
                output.write(HexFormat.of().parseHex(row.encodedBlockSha256()));
            }
        });
    }

    private static List<SparseBlock> parseSparseIndex(byte[] bytes) {
        Cursor input = new Cursor(bytes);
        int count = input.count(MAX_SPARSE_ROWS, "sparse row");
        if (input.remaining() != Math.multiplyExact(count, SPARSE_ROW_BYTES)) {
            throw rejected("sparse row count does not match exact body bytes");
        }
        List<SparseBlock> rows = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            int ordinal = input.intValue();
            if (input.intValue() != 0) {
                throw rejected("sparse row reserved bits are non-zero");
            }
            long firstEntryId = input.longValue();
            int entryCount = input.intValue();
            int compression = Short.toUnsignedInt(input.shortValue());
            int encryption = Short.toUnsignedInt(input.shortValue());
            long offset = input.longValue();
            long encodedLength = input.longValue();
            long decodedLength = input.longValue();
            String sha = HexFormat.of().formatHex(input.bytes(32));
            if (compression > 1 || encryption != 1) {
                throw rejected("sparse row codec or encryption family differs");
            }
            rows.add(new SparseBlock(
                    ordinal,
                    firstEntryId,
                    entryCount,
                    offset,
                    checkedInt(encodedLength),
                    decodedLength,
                    compressionFamily(compression),
                    Npd1CodecV1.EncryptionFamily.AES_GCM_256,
                    sha));
        }
        input.end();
        return rows;
    }

    private static void section(ByteBuffer output, int kind, byte[] body) {
        output.putShort((short) kind)
                .putShort((short) VERSION)
                .putInt(0)
                .putLong(body.length)
                .put(body);
    }

    private static byte[] section(ByteBuffer input, int expectedKind, int digestOffset, int maximumBodyBytes) {
        if (input.remaining() < SECTION_HEADER_BYTES + SELF_DIGEST_BYTES) {
            throw rejected("NPO1 section header is truncated");
        }
        int kind = Short.toUnsignedInt(input.getShort());
        int version = Short.toUnsignedInt(input.getShort());
        int flags = input.getInt();
        long length = input.getLong();
        if (kind != expectedKind
                || version != VERSION
                || flags != 0
                || length < 0
                || length > maximumBodyBytes
                || length > digestOffset - input.position()) {
            throw rejected("NPO1 section kind/version/flags/length differs");
        }
        byte[] result = new byte[checkedInt(length)];
        input.get(result);
        return result;
    }

    private static byte[] body(BodyWriter writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writer.write(output);
            }
            return bytes.toByteArray();
        } catch (IOException failure) {
            throw new Npo1RejectedException("cannot construct canonical NPO1 body", failure);
        }
    }

    private static void string(DataOutputStream output, String value) throws IOException {
        byte[] bytes = requireString(value, MAX_STRING_BYTES, "string");
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static void blob(DataOutputStream output, byte[] value) throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    private static Map<String, CustomMetadataValue> canonicalMap(Map<String, CustomMetadataValue> input) {
        Objects.requireNonNull(input, "customMetadata");
        if (input.size() > MAX_CUSTOM_METADATA_ENTRIES) {
            throw rejected("custom metadata count exceeds hard cap");
        }
        List<Map.Entry<String, CustomMetadataValue>> rows = new ArrayList<>(input.entrySet());
        rows.sort(Map.Entry.comparingByKey(UNSIGNED_UTF8));
        Map<String, CustomMetadataValue> result = new LinkedHashMap<>();
        int totalBytes = 0;
        for (Map.Entry<String, CustomMetadataValue> row : rows) {
            byte[] key = requireString(row.getKey(), MAX_STRING_BYTES, "custom metadata key");
            CustomMetadataValue value = Objects.requireNonNull(row.getValue(), "custom metadata value");
            byte[] valueBytes = value.bytes();
            totalBytes = Math.addExact(totalBytes, Math.addExact(key.length, valueBytes.length));
            if (totalBytes > MAX_CUSTOM_METADATA_BYTES
                    || result.put(row.getKey(), new CustomMetadataValue(valueBytes)) != null) {
                throw rejected("custom metadata bytes or uniqueness differ");
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private static void validateEnsembles(List<EnsembleSegment> ensembles, int ensembleSize, long lac) {
        if (ensembles.isEmpty()
                || ensembles.size() > MAX_ENSEMBLE_SEGMENTS
                || ensembles.get(0).firstEntryId() != 0) {
            throw rejected("ensemble coverage must begin at zero within count cap");
        }
        long previous = -1;
        for (EnsembleSegment segment : ensembles) {
            Set<String> uniqueMembers = new HashSet<>(segment.bookieIds());
            if (segment.firstEntryId() <= previous
                    || segment.firstEntryId() > lac
                    || segment.bookieIds().size() != ensembleSize
                    || uniqueMembers.size() != ensembleSize) {
                throw rejected("ensemble segments are not strictly ordered within ledger coverage");
            }
            previous = segment.firstEntryId();
        }
    }

    private static int retentionId(RetentionClass retentionClass) {
        return switch (retentionClass) {
            case RETAIN_BK -> 0;
            case DELETE_AFTER_VERIFIED -> 1;
        };
    }

    private static RetentionClass retentionClass(int id) {
        return switch (id) {
            case 0 -> RetentionClass.RETAIN_BK;
            case 1 -> RetentionClass.DELETE_AFTER_VERIFIED;
            default -> throw rejected("unknown retention class");
        };
    }

    private static int compressionId(Npd1CodecV1.CompressionFamily compression) {
        return switch (compression) {
            case NONE -> 0;
            case ZSTD -> 1;
        };
    }

    private static Npd1CodecV1.CompressionFamily compressionFamily(int id) {
        return switch (id) {
            case 0 -> Npd1CodecV1.CompressionFamily.NONE;
            case 1 -> Npd1CodecV1.CompressionFamily.ZSTD;
            default -> throw rejected("unknown compression family");
        };
    }

    private static byte[] requireString(String value, int maximumBytes, String field) {
        Objects.requireNonNull(value, field);
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > maximumBytes) {
            throw rejected(field + " bytes are outside hard bounds");
        }
        return bytes;
    }

    private static void requireIdentifier(String value, String field) {
        requireString(value, 64, field);
        if (!value.matches("[a-z0-9][a-z0-9._-]{0,63}")) {
            throw rejected(field + " is not a canonical identifier");
        }
    }

    private static String decode(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException failure) {
            throw new Npo1RejectedException("NPO1 string is not strict UTF-8", failure);
        }
    }

    private static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JDK lacks SHA-256", failure);
        }
    }

    private static boolean isSha(String value) {
        return value != null && value.matches("[0-9a-f]{64}");
    }

    private static long checkedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException failure) {
            throw new Npo1RejectedException("NPO1 checked arithmetic overflowed", failure);
        }
    }

    private static int checkedInt(long value) {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw rejected("NPO1 actual allocation exceeds Java array domain");
        }
        return (int) value;
    }

    private static Npo1RejectedException rejected(String message) {
        return new Npo1RejectedException(message);
    }

    @FunctionalInterface
    private interface BodyWriter {
        void write(DataOutputStream output) throws IOException;
    }

    private static final class Cursor {
        private final ByteBuffer input;

        private Cursor(byte[] bytes) {
            input = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        }

        private int intValue() {
            require(4);
            return input.getInt();
        }

        private short shortValue() {
            require(2);
            return input.getShort();
        }

        private long longValue() {
            require(8);
            return input.getLong();
        }

        private byte[] bytes(int length) {
            require(length);
            byte[] result = new byte[length];
            input.get(result);
            return result;
        }

        private String string(int maximumBytes) {
            int length = intValue();
            if (length <= 0 || length > maximumBytes) {
                throw rejected("NPO1 string length is outside hard bounds");
            }
            return decode(bytes(length));
        }

        private byte[] blob(int maximumBytes) {
            int length = intValue();
            if (length < 0 || length > maximumBytes) {
                throw rejected("NPO1 binary length is outside hard bounds");
            }
            return bytes(length);
        }

        private int count(int maximum, String field) {
            int count = intValue();
            if (count <= 0 || count > maximum) {
                throw rejected(field + " count is outside hard bounds");
            }
            return count;
        }

        private void end() {
            if (input.hasRemaining()) {
                throw rejected("NPO1 section has trailing bytes");
            }
        }

        private int remaining() {
            return input.remaining();
        }

        private void require(int bytes) {
            if (bytes < 0 || input.remaining() < bytes) {
                throw rejected("NPO1 section is truncated");
            }
        }
    }

    public static final class Npo1RejectedException extends IllegalArgumentException {
        public Npo1RejectedException(String message) {
            super(message);
        }

        public Npo1RejectedException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
