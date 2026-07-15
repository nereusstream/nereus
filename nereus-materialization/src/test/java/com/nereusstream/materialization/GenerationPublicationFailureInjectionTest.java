/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.GenerationPublicationTestSupport.CLUSTER;
import static com.nereusstream.materialization.GenerationPublicationTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import org.junit.jupiter.api.Test;

class GenerationPublicationFailureInjectionTest {
    @Test
    void resolvesLostCommittedCasResponseFromTheExactIndex() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            GenerationMetadataStore lossy =
                    GenerationPublicationTestSupport.loseFirstCommittedIndexResponse(
                            context.generations());
            GenerationCommitResult result = context.committer(
                            lossy, GenerationPublicationTestSupport.successfulGuard())
                    .publish(context.task(), context.output()).join();

            assertThat(result.committedByThisCall()).isFalse();
            assertThat(context.generations().getIndex(
                            CLUSTER,
                            new GenerationIndexIdentity(
                                    STREAM,
                                    result.view(),
                                    result.coverage().endOffset(),
                                    result.generation().value())).join().orElseThrow().value().lifecycle())
                    .isEqualTo(GenerationLifecycle.COMMITTED);
        }
    }

    @Test
    void resolvesLostTaskGenerationAttachmentWithoutAllocatingAReplacement() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            GenerationMetadataStore lossy =
                    GenerationPublicationTestSupport.loseFirstGenerationAttachmentResponse(
                            context.generations());
            GenerationCommitResult result = context.committer(
                            lossy, GenerationPublicationTestSupport.successfulGuard())
                    .publish(context.task(), context.output()).join();

            assertThat(result.generation().value()).isEqualTo(2);
            assertThat(context.generations().getSequence(
                            CLUSTER, STREAM, result.view()).join().orElseThrow().value().lastAllocatedGeneration())
                    .isEqualTo(2);
        }
    }

    @Test
    void leavesPreparedInvisibleWhenActivationChangesThenRestartCompletesSamePublication() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            DefaultGenerationCommitter failing = context.committer(
                    context.generations(),
                    GenerationPublicationTestSupport.failFirstRevalidation());
            assertThatThrownBy(() -> failing.publish(
                            context.task(), context.output()).join())
                    .hasRootCauseInstanceOf(
                            com.nereusstream.metadata.oxia.F4MetadataConditionFailedException.class);

            var publishingTask = context.generations().getTask(
                    CLUSTER, STREAM, context.task().taskId()).join().orElseThrow();
            assertThat(publishingTask.value().lifecycle()).isEqualTo(TaskLifecycle.PUBLISHING);
            long generation = publishingTask.value().allocatedGeneration().orElseThrow();
            var prepared = context.generations().getIndex(
                    CLUSTER,
                    new GenerationIndexIdentity(
                            STREAM, context.task().view(), context.task().coverage().endOffset(), generation))
                    .join().orElseThrow();
            assertThat(prepared.value().lifecycle()).isEqualTo(GenerationLifecycle.PREPARED);

            GenerationCommitResult recovered = new GenerationPublicationReconciler(context.committer(
                            context.generations(), GenerationPublicationTestSupport.successfulGuard()))
                    .reconcile(context.task(), context.output()).join();
            assertThat(recovered.generation().value()).isEqualTo(generation);
            assertThat(recovered.publicationId().value())
                    .isEqualTo(publishingTask.value().publicationId());
            assertThat(context.generations().getIndex(
                            CLUSTER,
                            new GenerationIndexIdentity(
                                    STREAM,
                                    context.task().view(),
                                    context.task().coverage().endOffset(),
                                    generation)).join().orElseThrow().value().lifecycle())
                    .isEqualTo(GenerationLifecycle.COMMITTED);
        }
    }

    @Test
    void sourceIdentityLossCannotCrossThePublicationCas() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            GenerationMetadataStore hiddenSources =
                    GenerationPublicationTestSupport.hideSources(context.generations());
            assertThatThrownBy(() -> context.committer(
                            hiddenSources, GenerationPublicationTestSupport.successfulGuard())
                    .publish(context.task(), context.output()).join())
                    .hasRootCauseInstanceOf(
                            com.nereusstream.metadata.oxia.F4MetadataConditionFailedException.class);

            var publishingTask = context.generations().getTask(
                    CLUSTER, STREAM, context.task().taskId()).join().orElseThrow();
            var index = context.generations().getIndex(
                    CLUSTER,
                    new GenerationIndexIdentity(
                            STREAM,
                            context.task().view(),
                            context.task().coverage().endOffset(),
                            publishingTask.value().allocatedGeneration().orElseThrow()))
                    .join().orElseThrow();
            assertThat(index.value().lifecycle()).isEqualTo(GenerationLifecycle.PREPARED);
        }
    }
}
