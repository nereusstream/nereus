/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static com.nereusstream.metadata.oxia.F4MetadataTestValues.CLUSTER;
import static com.nereusstream.metadata.oxia.F4MetadataTestValues.STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class GenerationIndexCompatibilityTest {
    @Test
    void committedIndexHydratesThroughTheGenericReadTargetCodec() {
        GenerationMetadataStore store = new OxiaJavaGenerationMetadataStore(
                new PartitionedOxiaClient(new InMemoryPartitionedOxiaBackend()),
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
        GenerationIndexRecord preparedValue = withCanonicalProjection(
                F4MetadataTestValues.generation(GenerationLifecycle.PREPARED));
        VersionedGenerationIndex prepared = store.createPrepared(CLUSTER, preparedValue).join();
        VersionedGenerationIndex committed = store.compareAndSetIndex(
                CLUSTER,
                withCanonicalProjection(F4MetadataTestValues.generation(GenerationLifecycle.COMMITTED)),
                prepared.metadataVersion()).join();

        OffsetIndexEntry entry = GenerationIndexValidator.phase15Targets().requireCommitted(
                committed, new StreamId(STREAM), ReadView.COMMITTED, 2, 1);

        assertThat(entry.generation()).isEqualTo(3);
        assertThat(entry.range().startOffset()).isZero();
        assertThat(entry.range().endOffset()).isEqualTo(2);
        assertThat(entry.readTarget()).isEqualTo(
                com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry.phase15()
                        .decode(committed.value().readTarget()));
    }

    @Test
    void preparedAndWrongViewIndexesCannotEnterCommittedResolverAdapter() {
        GenerationMetadataStore store = new OxiaJavaGenerationMetadataStore(
                new PartitionedOxiaClient(new InMemoryPartitionedOxiaBackend()),
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
        VersionedGenerationIndex prepared = store.createPrepared(
                CLUSTER, F4MetadataTestValues.generation(GenerationLifecycle.PREPARED)).join();

        assertThatThrownBy(() -> GenerationIndexValidator.phase15Targets().requireCommitted(
                        prepared, new StreamId(STREAM), ReadView.COMMITTED, 2, 1))
                .isInstanceOf(com.nereusstream.api.NereusException.class);
        assertThatThrownBy(() -> GenerationIndexValidator.phase15Targets().requireCommitted(
                        new VersionedGenerationIndex(
                                prepared.key(),
                                F4MetadataTestValues.topicCompactedGeneration().withMetadataVersion(
                                        prepared.metadataVersion()),
                                prepared.metadataVersion(),
                                prepared.durableValueSha256()),
                        new StreamId(STREAM),
                        ReadView.COMMITTED,
                        2,
                        1))
                .isInstanceOf(com.nereusstream.api.NereusException.class);
    }

    private static GenerationIndexRecord withCanonicalProjection(GenerationIndexRecord value) {
        return new GenerationIndexRecord(
                value.schemaVersion(), value.streamId(), value.readViewId(), value.offsetStart(), value.offsetEnd(),
                value.generation(), value.publicationId(), value.taskId(), value.lifecycle(), value.sourceSetSha256(),
                value.policySha256(), value.readTarget(), value.targetIdentitySha256(),
                value.materializationPolicySha256(), value.payloadFormat(), value.sourceRecordCount(),
                value.outputRecordCount(), value.entryCount(), value.logicalBytes(), value.cumulativeSizeAtStart(),
                value.cumulativeSizeAtEnd(), value.firstCommitVersion(), value.lastCommitVersion(),
                value.schemaRefs(), CommitSliceRequest.emptyProjectionIdentity(), value.createdAtMillis(),
                value.committedAtMillis(), value.stateReason(), value.stateChangedAtMillis(), 0);
    }
}
