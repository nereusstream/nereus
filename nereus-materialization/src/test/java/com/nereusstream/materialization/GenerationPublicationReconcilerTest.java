/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.GenerationPublicationTestSupport.CLUSTER;
import static com.nereusstream.materialization.GenerationPublicationTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import org.junit.jupiter.api.Test;

class GenerationPublicationReconcilerTest {
    @Test
    void convergesAnAlreadyCommittedAndPublishedTaskWithoutAllocatingAgain() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            DefaultGenerationCommitter committer = context.committer(
                    context.generations(), GenerationPublicationTestSupport.successfulGuard());
            GenerationCommitResult first = committer.publish(
                    context.task(), context.output()).join();

            GenerationCommitResult recovered = new GenerationPublicationReconciler(committer)
                    .reconcile(context.task(), context.output()).join();

            assertThat(recovered.committedByThisCall()).isFalse();
            assertThat(recovered.streamId()).isEqualTo(first.streamId());
            assertThat(recovered.view()).isEqualTo(first.view());
            assertThat(recovered.coverage()).isEqualTo(first.coverage());
            assertThat(recovered.generation()).isEqualTo(first.generation());
            assertThat(recovered.publicationId()).isEqualTo(first.publicationId());
            assertThat(recovered.indexKey()).isEqualTo(first.indexKey());
            assertThat(recovered.indexMetadataVersion()).isEqualTo(first.indexMetadataVersion());
            assertThat(recovered.indexRecordSha256()).isEqualTo(first.indexRecordSha256());
        }
    }

    @Test
    void provesAbortedThenClearsTheOldAllocationBeforeStartingANewPublication() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            assertThatThrownBy(() -> context.committer(
                            context.generations(),
                            GenerationPublicationTestSupport.failFirstRevalidation())
                    .publish(context.task(), context.output()).join())
                    .hasRootCauseInstanceOf(
                            com.nereusstream.metadata.oxia.F4MetadataConditionFailedException.class);
            var publishing = context.generations().getTask(
                    CLUSTER, STREAM, context.task().taskId()).join().orElseThrow();
            long abortedGeneration = publishing.value().allocatedGeneration().orElseThrow();
            String abortedPublication = publishing.value().publicationId();
            GenerationIndexIdentity abortedIdentity = new GenerationIndexIdentity(
                    STREAM,
                    context.task().view(),
                    context.task().coverage().endOffset(),
                    abortedGeneration);
            var prepared = context.generations().getIndex(
                    CLUSTER, abortedIdentity).join().orElseThrow();
            var aborted = context.generations().compareAndSetIndex(
                    CLUSTER,
                    MaterializationRecordMapper.abortedIndex(
                            prepared.value(), "source-changed", 1_000),
                    prepared.metadataVersion()).join();

            GenerationCommitResult recovered = new GenerationPublicationReconciler(context.committer(
                            context.generations(), GenerationPublicationTestSupport.successfulGuard()))
                    .reconcile(context.task(), context.output()).join();

            assertThat(aborted.value().lifecycle()).isEqualTo(GenerationLifecycle.ABORTED);
            assertThat(recovered.generation().value()).isGreaterThan(abortedGeneration);
            assertThat(recovered.publicationId().value()).isNotEqualTo(abortedPublication);
            assertThat(context.generations().getIndex(
                            CLUSTER, abortedIdentity).join().orElseThrow().value().lifecycle())
                    .isEqualTo(GenerationLifecycle.ABORTED);
            assertThat(context.generations().getIndex(
                            CLUSTER,
                            new GenerationIndexIdentity(
                                    STREAM,
                                    context.task().view(),
                                    context.task().coverage().endOffset(),
                                    recovered.generation().value())).join().orElseThrow().value().lifecycle())
                    .isEqualTo(GenerationLifecycle.COMMITTED);
        }
    }
}
