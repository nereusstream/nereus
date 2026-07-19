/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.GenerationSequenceRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementProtectionRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class F4MetadataCodecGoldenTest {
    @Test
    void everyLifecycleAndOptionalBranchMatchesFrozenEnvelopeHex() throws IOException {
        Map<String, String> actual = canonicalEnvelopeHex();
        Properties expected = new Properties();
        try (InputStream input = F4MetadataCodecGoldenTest.class.getResourceAsStream(
                "f4-metadata-codec-golden.properties")) {
            assertThat(input).isNotNull();
            expected.load(new java.io.InputStreamReader(input, StandardCharsets.UTF_8));
        }

        assertThat(expected.stringPropertyNames()).containsExactlyInAnyOrderElementsOf(actual.keySet());
        actual.forEach((key, value) -> assertThat(value).as(key).isEqualTo(expected.getProperty(key)));
    }

    @Test
    void everyGoldenVectorRoundTripsThroughTheExplicitRegistryAndFactory() {
        vectors().forEach(F4MetadataCodecGoldenTest::assertRoundTrip);
    }

    @Test
    void taskEnvelopeAndPayloadSchemaVersionsCannotAlias() {
        byte[] v1 = F4MetadataCodecs.encodeEnvelope(
                F4MetadataTestValues.task(TaskLifecycle.PLANNED),
                MaterializationTaskRecord.class);
        byte[] v2 = F4MetadataCodecs.encodeEnvelope(
                F4MetadataTestValues.taskV2(),
                MaterializationTaskRecord.class);
        MetadataRecordEnvelope.DecodedEnvelope v1Envelope =
                MetadataRecordEnvelope.decode(v1);
        MetadataRecordEnvelope.DecodedEnvelope v2Envelope =
                MetadataRecordEnvelope.decode(v2);

        assertThat(v1Envelope.schemaVersion()).isEqualTo(1);
        assertThat(v1Envelope.minReaderSchemaVersion()).isEqualTo(1);
        assertThat(v2Envelope.schemaVersion())
                .isEqualTo(MaterializationTaskRecord.CURRENT_SCHEMA_VERSION);
        assertThat(v2Envelope.minReaderSchemaVersion())
                .isEqualTo(MaterializationTaskRecord.CURRENT_SCHEMA_VERSION);

        byte[] v1PayloadInV2Envelope = MetadataRecordEnvelope.encode(
                v1Envelope.recordType(),
                MaterializationTaskRecord.CURRENT_SCHEMA_VERSION,
                MaterializationTaskRecord.CURRENT_SCHEMA_VERSION,
                v1Envelope.payloadEncoding(),
                v1Envelope.payload());
        byte[] v2PayloadInV1Envelope = MetadataRecordEnvelope.encode(
                v2Envelope.recordType(),
                1,
                1,
                v2Envelope.payloadEncoding(),
                v2Envelope.payload());

        assertThatThrownBy(() -> F4MetadataCodecs.decodeEnvelope(
                        v1PayloadInV2Envelope, MaterializationTaskRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("payload schema");
        assertThatThrownBy(() -> MetadataRecordCodecFactory.decodeEnvelope(
                        v2PayloadInV1Envelope, MaterializationTaskRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("payload schema");
    }

    private static Map<String, String> canonicalEnvelopeHex() {
        Map<String, String> result = new LinkedHashMap<>();
        vectors().forEach(sample -> result.put(
                sample.key(), F4MetadataCodecs.envelopeHex(sample.value(), sample.type())));
        return result;
    }

    private static List<Sample> vectors() {
        List<Sample> values = new ArrayList<>();
        values.add(sample("sequence.empty", F4MetadataTestValues.emptySequence(), GenerationSequenceRecord.class));
        values.add(sample(
                "sequence.allocated", F4MetadataTestValues.allocatedSequence(), GenerationSequenceRecord.class));
        for (GenerationLifecycle lifecycle : GenerationLifecycle.values()) {
            values.add(sample(
                    "generation." + key(lifecycle),
                    F4MetadataTestValues.generation(lifecycle),
                    GenerationIndexRecord.class));
        }
        values.add(sample(
                "generation.topic-compacted",
                F4MetadataTestValues.topicCompactedGeneration(),
                GenerationIndexRecord.class));
        values.add(sample(
                "activation.prepared",
                F4MetadataTestValues.preparedActivation(),
                GenerationProtocolActivationRecord.class));
        values.add(sample(
                "activation.publication",
                F4MetadataTestValues.publicationActivation(),
                GenerationProtocolActivationRecord.class));
        values.add(sample(
                "activation.deletion",
                F4MetadataTestValues.deletionActivation(),
                GenerationProtocolActivationRecord.class));
        values.add(sample(
                "registration.full",
                F4MetadataTestValues.registration(F4MetadataTestValues.STREAM, 7),
                MaterializationStreamRegistrationRecord.class));
        for (TaskLifecycle lifecycle : TaskLifecycle.values()) {
            values.add(sample(
                    "task." + key(lifecycle),
                    F4MetadataTestValues.task(lifecycle),
                    MaterializationTaskRecord.class));
        }
        values.add(sample(
                "task.publishing-unallocated",
                F4MetadataTestValues.publishingTaskWithoutGeneration(),
                MaterializationTaskRecord.class));
        values.add(sample(
                "task-v2.planned",
                F4MetadataTestValues.taskV2(),
                MaterializationTaskRecord.class));
        values.add(sample(
                "checkpoint.empty",
                F4MetadataTestValues.emptyMaterializationCheckpoint(),
                MaterializationCheckpointRecord.class));
        values.add(sample(
                "checkpoint.advanced",
                F4MetadataTestValues.advancedMaterializationCheckpoint(),
                MaterializationCheckpointRecord.class));
        values.add(sample(
                "retention-stats.full",
                F4MetadataTestValues.retentionStats(0, 2, 1),
                RangeRetentionStatsRecord.class));
        values.add(sample(
                "recovery.empty",
                F4MetadataTestValues.emptyRecoveryRoot(),
                RecoveryCheckpointRootRecord.class));
        values.add(sample(
                "recovery.full",
                F4MetadataTestValues.fullRecoveryRoot(),
                RecoveryCheckpointRootRecord.class));
        for (PhysicalObjectLifecycle lifecycle : PhysicalObjectLifecycle.values()) {
            values.add(sample(
                    "physical-root." + key(lifecycle),
                    F4MetadataTestValues.physicalRoot(lifecycle),
                    PhysicalObjectRootRecord.class));
        }
        values.add(sample(
                "physical-root.deleted-tombstone",
                F4MetadataTestValues.deletedRootWithTombstone(),
                PhysicalObjectRootRecord.class));
        values.add(sample(
                "reader-lease.full",
                F4MetadataTestValues.readerLease(),
                ObjectReaderLeaseRecord.class));
        for (ObjectProtectionType type : ObjectProtectionType.values()) {
            values.add(sample(
                    "protection." + key(type),
                    F4MetadataTestValues.protection(type),
                    ObjectProtectionRecord.class));
        }
        values.add(sample(
                "gc-retirement.manifest",
                F4MetadataTestValues.gcRetirementManifest(),
                GcRetirementManifestRecord.class));
        values.add(sample(
                "gc-retirement.protection",
                F4MetadataTestValues.gcRetirementProtection(),
                GcRetirementProtectionRecord.class));
        values.add(sample(
                "gc-retirement.removal",
                F4MetadataTestValues.gcRetirementRemoval(),
                GcRetirementRemovalRecord.class));
        return List.copyOf(values);
    }

    private static Sample sample(String key, Object value, Class<?> type) {
        return new Sample(key, value, type);
    }

    private static String key(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT).replace('_', '-');
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void assertRoundTrip(Sample sample) {
        byte[] encoded = F4MetadataCodecs.encodeEnvelope(sample.value(), (Class) sample.type());
        Object direct = F4MetadataCodecs.decodeEnvelope(encoded, (Class) sample.type());
        Object factory = MetadataRecordCodecFactory.decodeEnvelope(encoded, (Class) sample.type());
        assertThat(direct).as(sample.key()).isEqualTo(sample.value());
        assertThat(factory).as(sample.key()).isEqualTo(sample.value());
        assertThat(MetadataRecordCodecFactory.recordType(encoded)).isEqualTo(sample.type().getSimpleName());
    }

    private record Sample(String key, Object value, Class<?> type) {
    }
}
