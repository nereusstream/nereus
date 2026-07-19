/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.api.SchemaRef;
import com.nereusstream.metadata.oxia.records.MaterializationOutputRecord;
import com.nereusstream.metadata.oxia.records.MaterializationPolicyRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import com.nereusstream.metadata.oxia.records.SourceGenerationRecord;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.metadata.oxia.records.WorkerClaimRecord;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Dual-reader task codec. V1 remains byte-for-byte stable; V2 dictionaries
 * repeated source facts and target descriptors so 128 exact sources fit the
 * frozen 64 KiB metadata payload bound.
 */
public final class MaterializationTaskRecordCodecV2
        implements MetadataRecordCodec<MaterializationTaskRecord> {
    static final int VERSION = MaterializationTaskRecord.CURRENT_SCHEMA_VERSION;
    private static final int LEGACY_VERSION = 1;
    private static final int SHA256_BYTES = 32;
    private static final int MAX_SOURCES = 128;

    private final MaterializationTaskRecordCodecV1 legacy =
            new MaterializationTaskRecordCodecV1();

    @Override
    public String recordType() {
        return MaterializationTaskRecord.class.getSimpleName();
    }

    @Override
    public int schemaVersion() {
        return VERSION;
    }

    @Override
    public int minReaderSchemaVersion() {
        return VERSION;
    }

    @Override
    public int schemaVersion(MaterializationTaskRecord value) {
        return Objects.requireNonNull(value, "value").schemaVersion();
    }

    @Override
    public int minReaderSchemaVersion(MaterializationTaskRecord value) {
        return schemaVersion(value);
    }

    @Override
    public boolean supportsEnvelopeSchema(
            int writerSchemaVersion, int minimumReaderSchemaVersion) {
        return (writerSchemaVersion == LEGACY_VERSION
                        && minimumReaderSchemaVersion == LEGACY_VERSION)
                || (writerSchemaVersion == VERSION
                        && minimumReaderSchemaVersion == VERSION);
    }

    @Override
    public byte[] encode(MaterializationTaskRecord value) {
        Objects.requireNonNull(value, "value");
        return switch (value.schemaVersion()) {
            case LEGACY_VERSION -> legacy.encode(value);
            case VERSION -> encodeV2(value);
            default -> throw new MetadataCodecException(
                    "unsupported MaterializationTaskRecord schema version: "
                            + value.schemaVersion());
        };
    }

    @Override
    public MaterializationTaskRecord decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length < Short.BYTES) {
            throw new MetadataCodecException(
                    "truncated MaterializationTaskRecord payload version");
        }
        int version = Short.toUnsignedInt(ByteBuffer.wrap(bytes).getShort());
        return switch (version) {
            case LEGACY_VERSION -> legacy.decode(bytes);
            case VERSION -> decodeV2(bytes);
            default -> throw new MetadataCodecException(
                    "unsupported MaterializationTaskRecord payload version: "
                            + version);
        };
    }

    private static byte[] encodeV2(MaterializationTaskRecord value) {
        try {
            F4Binary.Writer writer = new F4Binary.Writer();
            writer.writeUnsignedShort(VERSION);
            writer.writeString(value.taskId());
            writer.writeLong(value.taskSequence());
            writer.writeString(value.streamId());
            writer.writeUnsignedShort(value.readViewId());
            writer.writeUnsignedShort(value.taskKindId());
            writer.writeLong(value.offsetStart());
            writer.writeLong(value.offsetEnd());

            SourceTables tables = SourceTables.from(value.sources());
            writeSourceFacts(writer, tables.facts());
            writeTargetDescriptors(writer, tables.targets());
            writer.writeInt(value.sources().size());
            for (SourceGenerationRecord source : value.sources()) {
                writeSource(
                        writer,
                        source,
                        tables.factIndexes().get(SourceFacts.from(source)),
                        tables.targetIndexes().get(
                                TargetDescriptor.from(source.readTarget())));
            }

            writer.writeString(value.sourceSetSha256());
            writer.writeString(value.policyId());
            writer.writeLong(value.policyVersion());
            writer.writeString(value.policySha256());
            MaterializationTaskRecordCodecV1.writePolicy(writer, value.policy());
            writer.writeUnsignedShort(value.lifecycle().wireId());
            writer.writeLong(value.attempt());
            writer.writeOptional(value.workerClaim().isPresent());
            value.workerClaim().ifPresent(
                    claim -> MaterializationTaskRecordCodecV1.writeClaim(
                            writer, claim));
            writer.writeOptional(value.output().isPresent());
            value.output().ifPresent(
                    output -> MaterializationTaskRecordCodecV1.writeOutput(
                            writer, output));
            writer.writeOptional(value.allocatedGeneration().isPresent());
            value.allocatedGeneration().ifPresent(writer::writeLong);
            writer.writeString(value.publicationId());
            writer.writeUnsignedShort(value.failureClassId());
            writer.writeString(value.failureMessage());
            writer.writeLong(value.retryNotBeforeMillis());
            writer.writeLong(value.createdAtMillis());
            writer.writeLong(value.updatedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw F4Binary.malformed(
                    MaterializationTaskRecord.class.getSimpleName(), failure);
        }
    }

    private static MaterializationTaskRecord decodeV2(byte[] bytes) {
        try {
            F4Binary.Reader reader = new F4Binary.Reader(bytes);
            int version = reader.readUnsignedShort("schemaVersion");
            if (version != VERSION) {
                throw new MetadataCodecException(
                        "unsupported MaterializationTaskRecord payload version: "
                                + version);
            }
            String taskId = reader.readString("taskId");
            long taskSequence = reader.readLong("taskSequence");
            String streamId = reader.readString("streamId");
            int readViewId = reader.readUnsignedShort("readViewId");
            int taskKindId = reader.readUnsignedShort("taskKindId");
            long offsetStart = reader.readLong("offsetStart");
            long offsetEnd = reader.readLong("offsetEnd");

            List<SourceFacts> facts = readSourceFacts(reader);
            List<TargetDescriptor> targets = readTargetDescriptors(reader);
            int sourceCount = reader.readCount(
                    "sourceCount", Long.BYTES * 8, MAX_SOURCES);
            List<SourceGenerationRecord> sources =
                    new ArrayList<>(sourceCount);
            for (int index = 0; index < sourceCount; index++) {
                sources.add(readSource(reader, facts, targets));
            }

            String sourceSetSha256 = reader.readString("sourceSetSha256");
            String policyId = reader.readString("policyId");
            long policyVersion = reader.readLong("policyVersion");
            String policySha256 = reader.readString("policySha256");
            MaterializationPolicyRecord policy =
                    MaterializationTaskRecordCodecV1.readPolicy(reader);
            TaskLifecycle lifecycle = TaskLifecycle.fromWireId(
                    reader.readUnsignedShort("lifecycle"));
            long attempt = reader.readLong("attempt");
            Optional<WorkerClaimRecord> claim = reader.readOptional(
                            "workerClaimPresent")
                    ? Optional.of(MaterializationTaskRecordCodecV1.readClaim(
                            reader))
                    : Optional.empty();
            Optional<MaterializationOutputRecord> output = reader.readOptional(
                            "outputPresent")
                    ? Optional.of(MaterializationTaskRecordCodecV1.readOutput(
                            reader))
                    : Optional.empty();
            OptionalLong generation = reader.readOptional(
                            "allocatedGenerationPresent")
                    ? OptionalLong.of(reader.readLong("allocatedGeneration"))
                    : OptionalLong.empty();
            MaterializationTaskRecord value = new MaterializationTaskRecord(
                    VERSION,
                    taskId,
                    taskSequence,
                    streamId,
                    readViewId,
                    taskKindId,
                    offsetStart,
                    offsetEnd,
                    sources,
                    sourceSetSha256,
                    policyId,
                    policyVersion,
                    policySha256,
                    policy,
                    lifecycle,
                    attempt,
                    claim,
                    output,
                    generation,
                    reader.readString("publicationId"),
                    reader.readUnsignedShort("failureClassId"),
                    reader.readString("failureMessage"),
                    reader.readLong("retryNotBeforeMillis"),
                    reader.readLong("createdAtMillis"),
                    reader.readLong("updatedAtMillis"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw F4Binary.malformed(
                    MaterializationTaskRecord.class.getSimpleName(), failure);
        }
    }

    private static void writeSourceFacts(
            F4Binary.Writer writer, List<SourceFacts> values) {
        writer.writeInt(values.size());
        for (SourceFacts value : values) {
            writer.writeUnsignedShort(value.readViewId());
            writer.writeString(value.payloadFormat());
            writer.writeString(value.projectionRef());
            F4Binary.writeSchemaRefs(writer, value.schemaRefs());
        }
    }

    private static List<SourceFacts> readSourceFacts(F4Binary.Reader reader) {
        int count = reader.readCount("sourceFactCount", 14, MAX_SOURCES);
        List<SourceFacts> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            values.add(new SourceFacts(
                    reader.readUnsignedShort("sourceFactReadViewId"),
                    reader.readString("sourceFactPayloadFormat"),
                    reader.readString("sourceFactProjectionRef"),
                    F4Binary.readSchemaRefs(reader, "sourceFactSchemaRefs")));
        }
        return List.copyOf(values);
    }

    private static void writeTargetDescriptors(
            F4Binary.Writer writer, List<TargetDescriptor> values) {
        writer.writeInt(values.size());
        for (TargetDescriptor value : values) {
            writer.writeString(value.targetType());
            writer.writeInt(value.targetVersion());
            writer.writeString(value.payloadEncoding());
            writer.writeString(value.identityChecksumType());
        }
    }

    private static List<TargetDescriptor> readTargetDescriptors(
            F4Binary.Reader reader) {
        int count = reader.readCount("targetDescriptorCount", 16, MAX_SOURCES);
        List<TargetDescriptor> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            values.add(new TargetDescriptor(
                    reader.readString("targetDescriptorType"),
                    reader.readInt("targetDescriptorVersion"),
                    reader.readString("targetDescriptorPayloadEncoding"),
                    reader.readString("targetDescriptorIdentityChecksumType")));
        }
        return List.copyOf(values);
    }

    private static void writeSource(
            F4Binary.Writer writer,
            SourceGenerationRecord value,
            Integer factIndex,
            Integer targetIndex) {
        if (factIndex == null || targetIndex == null) {
            throw new MetadataCodecException(
                    "materialization source dictionary identity is absent");
        }
        if (!value.targetIdentitySha256().equals(
                value.readTarget().identityChecksumValue())) {
            throw new MetadataCodecException(
                    "materialization source target digest does not match its read target");
        }
        writer.writeUnsignedShort(factIndex);
        writer.writeLong(value.offsetStart());
        writer.writeLong(value.offsetEnd());
        writer.writeLong(value.generation());
        writer.writeLong(value.commitVersion());
        writer.writeString(value.indexKey());
        writer.writeLong(value.indexMetadataVersion());
        writeSha256(writer, value.indexRecordSha256(), "indexRecordSha256");
        writer.writeUnsignedShort(targetIndex);
        writer.writeBytes(value.readTarget().payload());
        writeSha256(writer, value.targetIdentitySha256(), "targetIdentitySha256");
        writer.writeOptional(!value.materializationPolicySha256().isEmpty());
        if (!value.materializationPolicySha256().isEmpty()) {
            writeSha256(
                    writer,
                    value.materializationPolicySha256(),
                    "materializationPolicySha256");
        }
        writer.writeInt(value.recordCount());
        writer.writeInt(value.entryCount());
        writer.writeLong(value.logicalBytes());
        writer.writeLong(value.cumulativeSizeAtStart());
        writer.writeLong(value.cumulativeSizeAtEnd());
    }

    private static SourceGenerationRecord readSource(
            F4Binary.Reader reader,
            List<SourceFacts> facts,
            List<TargetDescriptor> targets) {
        SourceFacts fact = tableValue(
                facts,
                reader.readUnsignedShort("sourceFactIndex"),
                "source fact");
        long offsetStart = reader.readLong("sourceOffsetStart");
        long offsetEnd = reader.readLong("sourceOffsetEnd");
        long generation = reader.readLong("sourceGeneration");
        long commitVersion = reader.readLong("sourceCommitVersion");
        String indexKey = reader.readString("sourceIndexKey");
        long indexMetadataVersion = reader.readLong(
                "sourceIndexMetadataVersion");
        String indexRecordSha256 = readSha256(
                reader, "sourceIndexRecordSha256");
        TargetDescriptor target = tableValue(
                targets,
                reader.readUnsignedShort("sourceTargetDescriptorIndex"),
                "target descriptor");
        byte[] targetPayload = reader.readBytes("sourceTargetPayload");
        String targetIdentitySha256 = readSha256(
                reader, "sourceTargetIdentitySha256");
        ReadTargetRecord readTarget = new ReadTargetRecord(
                target.targetType(),
                target.targetVersion(),
                target.payloadEncoding(),
                targetPayload,
                target.identityChecksumType(),
                targetIdentitySha256);
        ReadTargetCodecRegistry.phase15().decode(readTarget);
        String materializationPolicySha256 = reader.readOptional(
                        "sourceMaterializationPolicyPresent")
                ? readSha256(reader, "sourceMaterializationPolicySha256")
                : "";
        return new SourceGenerationRecord(
                fact.readViewId(),
                offsetStart,
                offsetEnd,
                generation,
                commitVersion,
                indexKey,
                indexMetadataVersion,
                indexRecordSha256,
                readTarget,
                targetIdentitySha256,
                materializationPolicySha256,
                fact.payloadFormat(),
                fact.projectionRef(),
                reader.readInt("sourceRecordCount"),
                reader.readInt("sourceEntryCount"),
                reader.readLong("sourceLogicalBytes"),
                fact.schemaRefs(),
                reader.readLong("sourceCumulativeSizeAtStart"),
                reader.readLong("sourceCumulativeSizeAtEnd"));
    }

    private static void writeSha256(
            F4Binary.Writer writer, String value, String name) {
        byte[] bytes;
        try {
            bytes = HexFormat.of().parseHex(value);
        } catch (IllegalArgumentException failure) {
            throw new MetadataCodecException(name + " is not lowercase SHA-256", failure);
        }
        if (bytes.length != SHA256_BYTES) {
            throw new MetadataCodecException(name + " must contain 32 bytes");
        }
        writer.writeBytes(bytes);
    }

    private static String readSha256(F4Binary.Reader reader, String name) {
        return HexFormat.of().formatHex(
                reader.readFixedBytes(name, SHA256_BYTES));
    }

    private static <T> T tableValue(
            List<T> values, int index, String name) {
        if (index < 0 || index >= values.size()) {
            throw new MetadataCodecException(name + " index is outside its table");
        }
        return values.get(index);
    }

    private record SourceFacts(
            int readViewId,
            String payloadFormat,
            String projectionRef,
            List<SchemaRef> schemaRefs) {
        private SourceFacts {
            schemaRefs = List.copyOf(schemaRefs);
        }

        private static SourceFacts from(SourceGenerationRecord source) {
            return new SourceFacts(
                    source.readViewId(),
                    source.payloadFormat(),
                    source.projectionRef(),
                    source.schemaRefs());
        }
    }

    private record TargetDescriptor(
            String targetType,
            int targetVersion,
            String payloadEncoding,
            String identityChecksumType) {
        private static TargetDescriptor from(ReadTargetRecord target) {
            return new TargetDescriptor(
                    target.targetType(),
                    target.targetVersion(),
                    target.payloadEncoding(),
                    target.identityChecksumType());
        }
    }

    private record SourceTables(
            List<SourceFacts> facts,
            Map<SourceFacts, Integer> factIndexes,
            List<TargetDescriptor> targets,
            Map<TargetDescriptor, Integer> targetIndexes) {
        private static SourceTables from(
                List<SourceGenerationRecord> sources) {
            LinkedHashMap<SourceFacts, Integer> facts = new LinkedHashMap<>();
            LinkedHashMap<TargetDescriptor, Integer> targets =
                    new LinkedHashMap<>();
            for (SourceGenerationRecord source : sources) {
                facts.computeIfAbsent(
                        SourceFacts.from(source), ignored -> facts.size());
                targets.computeIfAbsent(
                        TargetDescriptor.from(source.readTarget()),
                        ignored -> targets.size());
            }
            return new SourceTables(
                    List.copyOf(facts.keySet()),
                    Map.copyOf(facts),
                    List.copyOf(targets.keySet()),
                    Map.copyOf(targets));
        }
    }
}
