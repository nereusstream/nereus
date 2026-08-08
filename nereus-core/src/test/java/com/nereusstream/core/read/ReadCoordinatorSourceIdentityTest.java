/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV1;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ReadCoordinatorSourceIdentityTest {
    private static final ProjectionRef GENERATION_PROJECTION =
            new ProjectionRef(ProjectionType.VIRTUAL_LEDGER, "nereus-ml-v1.source-identity");

    @Test
    void admitsEmptyNcp1BatchProjectionAgainstGenerationAdmissionIdentity() {
        ResolvedRange range = range(
                target(CompactedObjectFormatV1.COMMITTED_PHYSICAL_FORMAT, PayloadFormat.PULSAR_ENTRY_BATCH.name()),
                PayloadFormat.PULSAR_ENTRY_BATCH,
                GENERATION_PROJECTION);

        assertThatCode(() -> ReadCoordinator.requireExactLogicalSources(
                        List.of(range), List.of(batch(range, Optional.empty()))))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsEmptyBatchProjectionForNcp2() {
        ResolvedRange range = range(
                target(CompactedObjectFormatV2.COMMITTED_PHYSICAL_FORMAT, CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT),
                PayloadFormat.KAFKA_RECORD_BATCH,
                GENERATION_PROJECTION);

        assertInvariantViolation(range, Optional.empty());
    }

    @Test
    void rejectsDifferentNonEmptyBatchProjectionForNcp1() {
        ResolvedRange range = range(
                target(CompactedObjectFormatV1.COMMITTED_PHYSICAL_FORMAT, PayloadFormat.PULSAR_ENTRY_BATCH.name()),
                PayloadFormat.PULSAR_ENTRY_BATCH,
                GENERATION_PROJECTION);
        ProjectionRef different = new ProjectionRef(ProjectionType.VIRTUAL_LEDGER, "nereus-ml-v1.different-source");

        assertInvariantViolation(range, Optional.of(different));
    }

    private static void assertInvariantViolation(ResolvedRange range, Optional<ProjectionRef> batchProjection) {
        assertThatThrownBy(() -> ReadCoordinator.requireExactLogicalSources(
                        List.of(range), List.of(batch(range, batchProjection))))
                .isInstanceOfSatisfying(NereusException.class, error -> assertThat(error.code())
                        .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
    }

    private static ResolvedRange range(
            ObjectSliceReadTarget target, PayloadFormat payloadFormat, ProjectionRef projectionRef) {
        return new ResolvedRange(
                new OffsetRange(0, 1), 2, target, payloadFormat, 1, 1, 1, List.of(), Optional.of(projectionRef), 9);
    }

    private static ReadBatch batch(ResolvedRange range, Optional<ProjectionRef> projectionRef) {
        return new ReadBatch(
                range.offsetRange(),
                range.payloadFormat(),
                new byte[] {1},
                range.schemaRefs(),
                projectionRef,
                new ReadSourceRef(
                        range.offsetRange(),
                        range.generation(),
                        range.commitVersion(),
                        range.readTarget(),
                        ReadTargetIdentities.sha256(range.readTarget())));
    }

    private static ObjectSliceReadTarget target(String physicalFormat, String logicalFormat) {
        ObjectSliceReadTarget base =
                ReadTargetReaderRegistryTest.target(ObjectType.STREAM_COMPACTED_OBJECT, physicalFormat);
        return new ObjectSliceReadTarget(
                base.version(),
                base.objectId(),
                base.objectKey(),
                base.objectType(),
                base.physicalFormat(),
                logicalFormat,
                base.sliceId(),
                base.objectOffset(),
                base.objectLength(),
                base.sliceChecksum(),
                base.entryIndexRef());
    }
}
