/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

/** Canonical length-delimited binary identities for policy, sources, and deterministic task ids. */
final class MaterializationCanonical {
    private static final Comparator<SourceGeneration> SOURCE_ORDER = Comparator
            .comparingLong((SourceGeneration source) -> source.range().startOffset())
            .thenComparingLong(source -> source.range().endOffset())
            .thenComparingLong(SourceGeneration::generation)
            .thenComparing(SourceGeneration::indexKey, MaterializationCanonical::compareUtf8);

    private MaterializationCanonical() {
    }

    static List<SourceGeneration> canonicalSources(List<SourceGeneration> sources) {
        List<SourceGeneration> copy = new ArrayList<>(sources);
        copy.sort(SOURCE_ORDER);
        return List.copyOf(copy);
    }

    static Checksum policyDigest(MaterializationPolicy policy) {
        CanonicalWriter writer = new CanonicalWriter();
        writer.text("nereus-materialization-policy-v1");
        writer.text(policy.policyId());
        writer.longValue(policy.policyVersion());
        writer.intValue(policy.view().wireId());
        writer.intValue(policy.taskKind().wireId());
        writer.text(policy.targetPhysicalFormat());
        writer.intValue(policy.minMergeSourceRanges());
        writer.intValue(policy.maxSourceRanges());
        writer.longValue(policy.maxRangeRecords());
        writer.longValue(policy.targetObjectBytes());
        writer.intValue(policy.targetRowGroupRecords());
        writer.text(policy.compression());
        writer.booleanValue(policy.topicCompaction().isPresent());
        policy.topicCompaction().ifPresent(spec -> {
            writer.text(spec.strategyId());
            writer.longValue(spec.strategyVersion());
            writer.text(spec.keyCodecId());
        });
        return sha256(writer.bytes());
    }

    static long operatorPolicyVersion(
            int minMergeSourceRanges,
            int maxSourceRanges,
            long maxRangeRecords,
            long targetObjectBytes,
            int targetRowGroupRecords,
            String compression) {
        CanonicalWriter writer = new CanonicalWriter();
        writer.text("nereus-lossless-committed-operator-policy-v1");
        writer.intValue(minMergeSourceRanges);
        writer.intValue(maxSourceRanges);
        writer.longValue(maxRangeRecords);
        writer.longValue(targetObjectBytes);
        writer.intValue(targetRowGroupRecords);
        writer.text(compression);
        byte[] digest = sha256Bytes(writer.bytes());
        long version = ByteBuffer.wrap(digest, 0, Long.BYTES).getLong() & Long.MAX_VALUE;
        return version == 0 ? 1 : version;
    }

    static long topicOperatorPolicyVersion(
            TopicCompactionSpec topicCompaction,
            int minMergeSourceRanges,
            int maxSourceRanges,
            long maxRangeRecords,
            long targetObjectBytes,
            int targetRowGroupRecords,
            String compression) {
        CanonicalWriter writer = new CanonicalWriter();
        writer.text("nereus-topic-compacted-operator-policy-v1");
        writer.text(topicCompaction.strategyId());
        writer.longValue(topicCompaction.strategyVersion());
        writer.text(topicCompaction.keyCodecId());
        writer.intValue(minMergeSourceRanges);
        writer.intValue(maxSourceRanges);
        writer.longValue(maxRangeRecords);
        writer.longValue(targetObjectBytes);
        writer.intValue(targetRowGroupRecords);
        writer.text(compression);
        byte[] digest = sha256Bytes(writer.bytes());
        long version = ByteBuffer.wrap(digest, 0, Long.BYTES).getLong() & Long.MAX_VALUE;
        return version == 0 ? 1 : version;
    }

    static Checksum sourceSetDigest(List<SourceGeneration> sources) {
        List<SourceGeneration> canonical = canonicalSources(sources);
        CanonicalWriter writer = new CanonicalWriter();
        writer.text("nereus-materialization-source-set-v1");
        writer.intValue(canonical.size());
        canonical.forEach(source -> writeSource(writer, source));
        return sha256(writer.bytes());
    }

    static String taskId(
            StreamId streamId,
            com.nereusstream.api.ReadView view,
            TaskKind taskKind,
            OffsetRange coverage,
            Checksum sourceSetSha256,
            Checksum policyDigestSha256) {
        CanonicalWriter writer = new CanonicalWriter();
        writer.text("nereus-materialization-task-v1");
        writer.text(streamId.value());
        writer.intValue(view.wireId());
        writer.intValue(taskKind.wireId());
        writer.longValue(coverage.startOffset());
        writer.longValue(coverage.endOffset());
        writer.checksum(sourceSetSha256);
        writer.checksum(policyDigestSha256);
        return "mat1-" + DeterministicIds.stableHashBytes(writer.bytes());
    }

    private static void writeSource(CanonicalWriter writer, SourceGeneration source) {
        writer.intValue(source.view().wireId());
        writer.longValue(source.range().startOffset());
        writer.longValue(source.range().endOffset());
        writer.longValue(source.generation());
        writer.longValue(source.commitVersion());
        writer.text(source.indexKey());
        writer.longValue(source.indexMetadataVersion());
        writer.checksum(source.indexRecordSha256());
        ReadTargetRecord target = ReadTargetCodecRegistry.phase15().encode(source.readTarget());
        writer.text(target.targetType());
        writer.intValue(target.targetVersion());
        writer.text(target.payloadEncoding());
        writer.byteArray(target.payload());
        writer.text(target.identityChecksumType());
        writer.text(target.identityChecksumValue());
        writer.checksum(source.targetIdentitySha256());
        writer.booleanValue(source.materializationPolicySha256().isPresent());
        source.materializationPolicySha256().ifPresent(writer::checksum);
        writer.text(source.payloadFormat().name());
        writeProjection(writer, source.projectionRef());
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

    private static void writeProjection(
            CanonicalWriter writer,
            java.util.Optional<ProjectionRef> projection) {
        writer.booleanValue(projection.isPresent());
        projection.ifPresent(value -> {
            writer.text(value.type().name());
            writer.text(value.value());
        });
    }

    private static Checksum sha256(byte[] bytes) {
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(sha256Bytes(bytes)));
    }

    private static byte[] sha256Bytes(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(bytes);
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static int compareUtf8(String left, String right) {
        byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
        byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
        int limit = Math.min(leftBytes.length, rightBytes.length);
        for (int index = 0; index < limit; index++) {
            int comparison = Integer.compare(leftBytes[index] & 0xff, rightBytes[index] & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(leftBytes.length, rightBytes.length);
    }

    private static final class CanonicalWriter {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final DataOutputStream output = new DataOutputStream(bytes);

        void intValue(int value) {
            write(() -> output.writeInt(value));
        }

        void longValue(long value) {
            write(() -> output.writeLong(value));
        }

        void booleanValue(boolean value) {
            write(() -> output.writeBoolean(value));
        }

        void text(String value) {
            byteArray(value.getBytes(StandardCharsets.UTF_8));
        }

        void checksum(Checksum checksum) {
            text(checksum.type().name());
            text(checksum.value());
        }

        void byteArray(byte[] value) {
            intValue(value.length);
            write(() -> output.write(value));
        }

        byte[] bytes() {
            write(output::flush);
            return bytes.toByteArray();
        }

        private void write(IoAction action) {
            try {
                action.run();
            } catch (IOException failure) {
                throw new IllegalStateException("in-memory canonical encoding failed", failure);
            }
        }
    }

    @FunctionalInterface
    private interface IoAction {
        void run() throws IOException;
    }
}
