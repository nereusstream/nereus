/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ApiLimits;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Strict canonical binary codec for a durable exact source-set snapshot. */
public final class ExactSourceSetCodecV1 {
    public static final int MAX_ENCODED_BYTES = 64 << 10;

    private static final int MAGIC = 0x45585331; // EXS1
    private static final int VERSION = 1;
    private static final int MAX_STRING_BYTES = 65_535;
    private static final int MAX_SCHEMA_REFS = 4_096;

    private final ReadTargetCodecRegistry targetCodecs;

    public ExactSourceSetCodecV1() {
        this(ReadTargetCodecRegistry.phase15());
    }

    ExactSourceSetCodecV1(ReadTargetCodecRegistry targetCodecs) {
        this.targetCodecs = Objects.requireNonNull(targetCodecs, "targetCodecs");
    }

    public byte[] encode(ExactSourceSet sourceSet) {
        ExactSourceSet exact = Objects.requireNonNull(sourceSet, "sourceSet");
        try {
            Writer writer = new Writer();
            writer.intValue(MAGIC);
            writer.shortValue(VERSION);
            writer.shortValue(exact.view().wireId());
            writer.longValue(exact.coverage().startOffset());
            writer.longValue(exact.coverage().endOffset());
            writer.checksum(exact.sourceSetSha256());
            writer.intValue(exact.sources().size());
            for (SourceGeneration source : exact.sources()) {
                writeSource(writer, source);
            }
            byte[] encoded = writer.bytes();
            if (encoded.length > MAX_ENCODED_BYTES) {
                throw new IllegalArgumentException("exact source-set encoding exceeds its byte limit");
            }
            return encoded;
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory exact source-set encoding failed", failure);
        }
    }

    public ExactSourceSet decode(byte[] bytes) {
        byte[] exact = Objects.requireNonNull(bytes, "bytes").clone();
        if (exact.length == 0 || exact.length > MAX_ENCODED_BYTES) {
            throw malformed("exact source-set payload has an invalid length", null);
        }
        try {
            Reader reader = new Reader(exact);
            if (reader.intValue("magic") != MAGIC || reader.unsignedShort("version") != VERSION) {
                throw malformed("unsupported exact source-set header", null);
            }
            ReadView view = ReadView.fromWireId(reader.unsignedShort("view"));
            OffsetRange coverage =
                    new OffsetRange(reader.longValue("coverageStart"), reader.longValue("coverageEnd"));
            Checksum expectedDigest = reader.checksum("sourceSetSha256");
            int sourceCount =
                    reader.count(
                            "sourceCount",
                            MaterializationPolicy.MAX_SOURCE_RANGES,
                            Long.BYTES * 12);
            if (sourceCount == 0) {
                throw malformed("exact source-set source count cannot be zero", null);
            }
            ArrayList<SourceGeneration> sources = new ArrayList<>(sourceCount);
            for (int index = 0; index < sourceCount; index++) {
                sources.add(readSource(reader));
            }
            reader.requireConsumed();
            return new ExactSourceSet(view, coverage, sources, expectedDigest);
        } catch (IllegalArgumentException failure) {
            if (failure.getMessage() != null
                    && failure.getMessage().startsWith("malformed exact source set:")) {
                throw failure;
            }
            throw malformed("invalid exact source-set fields", failure);
        }
    }

    private void writeSource(Writer writer, SourceGeneration source) throws IOException {
        writer.shortValue(source.view().wireId());
        writer.longValue(source.range().startOffset());
        writer.longValue(source.range().endOffset());
        writer.longValue(source.generation());
        writer.longValue(source.commitVersion());
        writer.text(source.indexKey());
        writer.longValue(source.indexMetadataVersion());
        writer.checksum(source.indexRecordSha256());
        writeTarget(writer, targetCodecs.encode(source.readTarget()));
        writer.checksum(source.targetIdentitySha256());
        writer.optional(source.materializationPolicySha256().isPresent());
        if (source.materializationPolicySha256().isPresent()) {
            writer.checksum(source.materializationPolicySha256().orElseThrow());
        }
        writer.text(source.payloadFormat().name());
        writer.optional(source.projectionRef().isPresent());
        if (source.projectionRef().isPresent()) {
            ProjectionRef projection = source.projectionRef().orElseThrow();
            writer.text(projection.type().name());
            writer.text(projection.value());
        }
        writer.intValue(source.recordCount());
        writer.intValue(source.entryCount());
        writer.longValue(source.logicalBytes());
        writer.intValue(source.schemaRefs().size());
        for (SchemaRef schema : source.schemaRefs()) {
            writer.text(schema.namespace());
            writer.text(schema.id());
            writer.longValue(schema.version());
        }
        writer.longValue(source.cumulativeSizeAtStart());
        writer.longValue(source.cumulativeSizeAtEnd());
    }

    private SourceGeneration readSource(Reader reader) {
        ReadView view = ReadView.fromWireId(reader.unsignedShort("sourceView"));
        OffsetRange range =
                new OffsetRange(reader.longValue("sourceStart"), reader.longValue("sourceEnd"));
        long generation = reader.longValue("generation");
        long commitVersion = reader.longValue("commitVersion");
        String indexKey = reader.text("indexKey");
        long indexMetadataVersion = reader.longValue("indexMetadataVersion");
        Checksum indexSha = reader.checksum("indexRecordSha256");
        ReadTargetRecord targetRecord = readTarget(reader);
        Checksum targetIdentity = reader.checksum("targetIdentitySha256");
        Optional<Checksum> policy =
                reader.optional("materializationPolicyPresent")
                        ? Optional.of(reader.checksum("materializationPolicySha256"))
                        : Optional.empty();
        PayloadFormat payloadFormat =
                enumValue(PayloadFormat.class, reader.text("payloadFormat"), "payloadFormat");
        Optional<ProjectionRef> projection =
                reader.optional("projectionPresent")
                        ? Optional.of(
                                new ProjectionRef(
                                        enumValue(
                                                ProjectionType.class,
                                                reader.text("projectionType"),
                                                "projectionType"),
                                        reader.text("projectionValue")))
                        : Optional.empty();
        int recordCount = reader.intValue("recordCount");
        int entryCount = reader.intValue("entryCount");
        long logicalBytes = reader.longValue("logicalBytes");
        int schemaCount = reader.count("schemaCount", MAX_SCHEMA_REFS, Long.BYTES);
        ArrayList<SchemaRef> schemas = new ArrayList<>(schemaCount);
        for (int index = 0; index < schemaCount; index++) {
            schemas.add(
                    new SchemaRef(
                            reader.text("schemaNamespace"),
                            reader.text("schemaId"),
                            reader.longValue("schemaVersion")));
        }
        return new SourceGeneration(
                view,
                range,
                generation,
                commitVersion,
                indexKey,
                indexMetadataVersion,
                indexSha,
                targetCodecs.decode(targetRecord),
                targetIdentity,
                policy,
                payloadFormat,
                projection,
                recordCount,
                entryCount,
                logicalBytes,
                schemas,
                reader.longValue("cumulativeSizeAtStart"),
                reader.longValue("cumulativeSizeAtEnd"));
    }

    private static void writeTarget(Writer writer, ReadTargetRecord target) throws IOException {
        writer.text(target.targetType());
        writer.intValue(target.targetVersion());
        writer.text(target.payloadEncoding());
        writer.byteArray(target.payload(), ApiLimits.MAX_READ_TARGET_ENCODED_BYTES);
        writer.text(target.identityChecksumType());
        writer.text(target.identityChecksumValue());
    }

    private static ReadTargetRecord readTarget(Reader reader) {
        return new ReadTargetRecord(
                reader.text("targetType"),
                reader.intValue("targetVersion"),
                reader.text("targetPayloadEncoding"),
                reader.byteArray(
                        "targetPayload", ApiLimits.MAX_READ_TARGET_ENCODED_BYTES),
                reader.text("targetIdentityChecksumType"),
                reader.text("targetIdentityChecksumValue"));
    }

    private static <E extends Enum<E>> E enumValue(
            Class<E> type, String value, String field) {
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException failure) {
            throw malformed("unknown " + field, failure);
        }
    }

    private static IllegalArgumentException malformed(String message, Throwable cause) {
        return new IllegalArgumentException(
                "malformed exact source set: " + message, cause);
    }

    private static final class Writer {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final DataOutputStream output = new DataOutputStream(bytes);

        void shortValue(int value) throws IOException {
            if (value < 0 || value > 0xffff) {
                throw new IllegalArgumentException("unsigned short value is out of range");
            }
            output.writeShort(value);
        }

        void intValue(int value) throws IOException {
            output.writeInt(value);
        }

        void longValue(long value) throws IOException {
            output.writeLong(value);
        }

        void optional(boolean present) throws IOException {
            output.writeByte(present ? 1 : 0);
        }

        void checksum(Checksum checksum) throws IOException {
            Objects.requireNonNull(checksum, "checksum");
            if (checksum.type() != ChecksumType.SHA256) {
                throw new IllegalArgumentException("exact source-set checksum must use SHA256");
            }
            text(checksum.value());
        }

        void text(String value) throws IOException {
            byte[] encoded = Objects.requireNonNull(value, "value")
                    .getBytes(StandardCharsets.UTF_8);
            if (encoded.length > MAX_STRING_BYTES) {
                throw new IllegalArgumentException("exact source-set string exceeds its byte limit");
            }
            output.writeInt(encoded.length);
            output.write(encoded);
        }

        void byteArray(byte[] value, int maximum) throws IOException {
            byte[] exact = Objects.requireNonNull(value, "value");
            if (exact.length > maximum) {
                throw new IllegalArgumentException("exact source-set byte array exceeds its limit");
            }
            output.writeInt(exact.length);
            output.write(exact);
        }

        byte[] bytes() throws IOException {
            output.flush();
            return bytes.toByteArray();
        }
    }

    private static final class Reader {
        private final ByteBuffer input;

        Reader(byte[] bytes) {
            input = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        }

        int unsignedShort(String field) {
            require(Short.BYTES, field);
            return Short.toUnsignedInt(input.getShort());
        }

        int intValue(String field) {
            require(Integer.BYTES, field);
            return input.getInt();
        }

        long longValue(String field) {
            require(Long.BYTES, field);
            return input.getLong();
        }

        boolean optional(String field) {
            require(1, field);
            byte value = input.get();
            if (value != 0 && value != 1) {
                throw malformed(field + " is not a canonical boolean", null);
            }
            return value == 1;
        }

        Checksum checksum(String field) {
            return new Checksum(ChecksumType.SHA256, text(field));
        }

        String text(String field) {
            byte[] bytes = byteArray(field, MAX_STRING_BYTES);
            try {
                return StandardCharsets.UTF_8
                        .newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(bytes))
                        .toString();
            } catch (CharacterCodingException failure) {
                throw malformed(field + " is not strict UTF-8", failure);
            }
        }

        byte[] byteArray(String field, int maximum) {
            int length = intValue(field + "Length");
            if (length < 0 || length > maximum) {
                throw malformed(field + " length exceeds its bound", null);
            }
            require(length, field);
            byte[] value = new byte[length];
            input.get(value);
            return value;
        }

        int count(String field, int maximum, int minimumBytesPerItem) {
            int count = intValue(field);
            if (count < 0
                    || count > maximum
                    || (minimumBytesPerItem > 0
                            && count > input.remaining() / minimumBytesPerItem)) {
                throw malformed(field + " is outside its bound", null);
            }
            return count;
        }

        void requireConsumed() {
            if (input.hasRemaining()) {
                throw malformed("payload contains trailing bytes", null);
            }
        }

        private void require(int bytes, String field) {
            if (bytes < 0 || input.remaining() < bytes) {
                throw malformed("truncated " + field, null);
            }
        }
    }
}
