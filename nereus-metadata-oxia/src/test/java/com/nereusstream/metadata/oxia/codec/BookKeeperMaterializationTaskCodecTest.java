/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.SourceGenerationRecord;
import java.util.List;
import org.junit.jupiter.api.Test;

class BookKeeperMaterializationTaskCodecTest {
    @Test
    void roundTripsBookKeeperSource() {
        BookKeeperEntryRangeReadTarget target = new BookKeeperEntryRangeReadTarget(
                1,
                "primary",
                9_001,
                17,
                2,
                BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                new Checksum(ChecksumType.SHA256, "a".repeat(64)));
        var encodedTarget = ReadTargetCodecRegistry.phase15().encode(target);
        MaterializationTaskRecord template = F4MetadataTestValues.taskV2();
        SourceGenerationRecord sourceTemplate = template.sources().get(0);
        SourceGenerationRecord source = new SourceGenerationRecord(
                sourceTemplate.readViewId(),
                sourceTemplate.offsetStart(),
                sourceTemplate.offsetEnd(),
                sourceTemplate.generation(),
                sourceTemplate.commitVersion(),
                sourceTemplate.indexKey(),
                sourceTemplate.indexMetadataVersion(),
                sourceTemplate.indexRecordSha256(),
                encodedTarget,
                encodedTarget.identityChecksumValue(),
                sourceTemplate.materializationPolicySha256(),
                sourceTemplate.payloadFormat(),
                sourceTemplate.projectionRef(),
                sourceTemplate.recordCount(),
                sourceTemplate.entryCount(),
                sourceTemplate.logicalBytes(),
                sourceTemplate.schemaRefs(),
                sourceTemplate.cumulativeSizeAtStart(),
                sourceTemplate.cumulativeSizeAtEnd());
        MaterializationTaskRecord task = new MaterializationTaskRecord(
                template.schemaVersion(),
                template.taskId(),
                template.taskSequence(),
                template.streamId(),
                template.readViewId(),
                template.taskKindId(),
                template.offsetStart(),
                template.offsetEnd(),
                List.of(source),
                template.sourceSetSha256(),
                template.policyId(),
                template.policyVersion(),
                template.policySha256(),
                template.policy(),
                template.lifecycle(),
                template.attempt(),
                template.workerClaim(),
                template.output(),
                template.allocatedGeneration(),
                template.publicationId(),
                template.failureClassId(),
                template.failureMessage(),
                template.retryNotBeforeMillis(),
                template.createdAtMillis(),
                template.updatedAtMillis(),
                template.metadataVersion());

        byte[] envelope = F4MetadataCodecs.encodeEnvelope(task, MaterializationTaskRecord.class);
        MaterializationTaskRecord recovered = F4MetadataCodecs.decodeEnvelope(
                envelope, MaterializationTaskRecord.class);

        assertThat(recovered).isEqualTo(task);
        assertThat(recovered.sources()).singleElement().satisfies(value -> {
            assertThat(value.readTarget().targetType()).isEqualTo("BOOKKEEPER_ENTRY_RANGE");
            assertThat(ReadTargetCodecRegistry.phase15().decode(value.readTarget()))
                    .isEqualTo(target);
            assertThat(value.targetIdentitySha256())
                    .isEqualTo(encodedTarget.identityChecksumValue());
        });
    }
}
