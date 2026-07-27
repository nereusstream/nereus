/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ExactSourceSetCodecV1Test {
    private final ExactSourceSetCodecV1 codec = new ExactSourceSetCodecV1();

    @Test
    void roundTripsCanonicalSourcesAndFreezesEveryPhysicalTargetFact() {
        ExactSourceSet sourceSet = sourceSet();

        byte[] first = codec.encode(sourceSet);
        ExactSourceSet decoded = codec.decode(first);

        assertThat(decoded).isEqualTo(sourceSet);
        assertThat(codec.encode(decoded)).isEqualTo(first);
        assertThat(decoded.sources().get(1).projectionRef())
                .contains(new ProjectionRef(ProjectionType.PROTOCOL_HINT, "kafka-v2"));
        assertThat(decoded.sources().get(1).schemaRefs())
                .containsExactly(new SchemaRef("kafka", "record-batch", 2));
    }

    @Test
    void rejectsDigestCorruptionTruncationTrailingBytesAndUnknownEnums() {
        byte[] encoded = codec.encode(sourceSet());
        byte[] digestCorruption = encoded.clone();
        digestCorruption[32] ^= 1;
        byte[] truncated = java.util.Arrays.copyOf(encoded, encoded.length - 1);
        byte[] trailing = java.util.Arrays.copyOf(encoded, encoded.length + 1);

        assertThatThrownBy(() -> codec.decode(digestCorruption))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("malformed exact source set");
        assertThatThrownBy(() -> codec.decode(truncated))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("malformed exact source set");
        assertThatThrownBy(() -> codec.decode(trailing))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing bytes");
    }

    private static ExactSourceSet sourceSet() {
        SourceGeneration first = source(0, 2, 0, 1, Optional.empty());
        SourceGeneration second =
                source(
                        2,
                        5,
                        1,
                        2,
                        Optional.of(new Checksum(ChecksumType.SHA256, "c".repeat(64))));
        return ExactSourceSet.create(
                ReadView.COMMITTED, new OffsetRange(0, 5), List.of(first, second));
    }

    private static SourceGeneration source(
            long start,
            long end,
            long generation,
            long commitVersion,
            Optional<Checksum> materializationPolicy) {
        ObjectSliceReadTarget target =
                new ObjectSliceReadTarget(
                        1,
                        new ObjectId("o-" + start),
                        new ObjectKey("compacted/source-" + start),
                        ObjectType.STREAM_COMPACTED_OBJECT,
                        "NEREUS_COMPACTED_PARQUET_V2",
                        PayloadFormat.KAFKA_RECORD_BATCH.name(),
                        start + "-" + end,
                        0,
                        1_024,
                        new Checksum(ChecksumType.CRC32C, "a".repeat(8)),
                        new EntryIndexRef(
                                EntryIndexLocation.OBJECT_FOOTER,
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                992,
                                32,
                                new Checksum(ChecksumType.CRC32C, "d".repeat(8))));
        long logicalBytes = (end - start) * 100;
        return new SourceGeneration(
                ReadView.COMMITTED,
                new OffsetRange(start, end),
                generation,
                commitVersion,
                "index/" + start,
                commitVersion,
                new Checksum(ChecksumType.SHA256, "b".repeat(64)),
                target,
                ReadTargetIdentities.sha256(target),
                materializationPolicy,
                PayloadFormat.KAFKA_RECORD_BATCH,
                start == 0
                        ? Optional.empty()
                        : Optional.of(
                                new ProjectionRef(ProjectionType.PROTOCOL_HINT, "kafka-v2")),
                Math.toIntExact(end - start),
                1,
                logicalBytes,
                start == 0
                        ? List.of()
                        : List.of(new SchemaRef("kafka", "record-batch", 2)),
                start * 100,
                end * 100);
    }
}
