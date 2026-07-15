/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.GenerationPublicationTestSupport.CLUSTER;
import static com.nereusstream.materialization.GenerationPublicationTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.api.ReadView;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class GenerationIndexPublicationTest {
    @Test
    void publishesAtOneIndexCasAndReturnsOnlyAfterIndexOwnedProtection() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.context()) {
            DefaultGenerationCommitter committer = context.committer(
                    context.generations(), GenerationPublicationTestSupport.successfulGuard());

            GenerationCommitResult result = committer.publish(
                    context.task(), context.output()).join();

            assertThat(result.committedByThisCall()).isTrue();
            var index = context.generations().getIndex(
                    CLUSTER,
                    new GenerationIndexIdentity(
                            STREAM,
                            result.view(),
                            result.coverage().endOffset(),
                            result.generation().value())).join().orElseThrow();
            assertThat(index.value().lifecycle()).isEqualTo(GenerationLifecycle.COMMITTED);
            assertThat(index.metadataVersion()).isEqualTo(result.indexMetadataVersion());
            assertThat(index.durableValueSha256()).isEqualTo(result.indexRecordSha256());

            var task = context.generations().getTask(
                    CLUSTER, STREAM, context.task().taskId()).join().orElseThrow();
            assertThat(task.value().lifecycle()).isEqualTo(TaskLifecycle.PUBLISHED);
            assertThat(task.value().allocatedGeneration()).hasValue(result.generation().value());
            assertThat(task.value().publicationId()).isEqualTo(result.publicationId().value());

            var protections = context.physical().scanProtections(
                    CLUSTER,
                    context.output().objectKeyHash(),
                    Optional.empty(),
                    1_000).join().values();
            assertThat(protections).hasSize(2);
            assertThat(protections).anySatisfy(protection -> {
                assertThat(protection.value().protectionTypeId())
                        .isEqualTo(ObjectProtectionType.VISIBLE_GENERATION.wireId());
                assertThat(protection.value().ownerKey()).isEqualTo(index.key());
                assertThat(protection.value().ownerMetadataVersion())
                        .isEqualTo(index.metadataVersion());
                assertThat(protection.value().ownerIdentitySha256())
                        .isEqualTo(index.durableValueSha256().value());
            });
            assertThat(protections).anySatisfy(protection -> {
                assertThat(protection.value().protectionTypeId())
                        .isEqualTo(ObjectProtectionType.MATERIALIZATION_OUTPUT.wireId());
                assertThat(protection.value().ownerKey()).isEqualTo(task.key());
                assertThat(protection.value().ownerMetadataVersion())
                        .as("terminal publication must not recreate temporary protections at PUBLISHED")
                        .isLessThan(task.metadataVersion());
            });
        }
    }

    @Test
    void publishesTopicCompactionOnlyIntoTheIsolatedTargetView() {
        try (GenerationPublicationTestSupport.Context context =
                GenerationPublicationTestSupport.topicContext()) {
            DefaultGenerationCommitter committer = context.committer(
                    context.generations(), GenerationPublicationTestSupport.successfulGuard());

            GenerationCommitResult result = committer.publish(
                    context.task(), context.output()).join();

            assertThat(result.view()).isEqualTo(ReadView.TOPIC_COMPACTED);
            assertThat(context.task().sourceView()).isEqualTo(ReadView.COMMITTED);
            assertThat(context.generations().getIndex(
                            CLUSTER,
                            new GenerationIndexIdentity(
                                    STREAM,
                                    ReadView.TOPIC_COMPACTED,
                                    result.coverage().endOffset(),
                                    result.generation().value()))
                    .join())
                    .isPresent();
            var sameNumericCommittedGeneration = context.generations().getIndex(
                            CLUSTER,
                            new GenerationIndexIdentity(
                                    STREAM,
                                    ReadView.COMMITTED,
                                    result.coverage().endOffset(),
                                    result.generation().value()))
                    .join();
            assertThat(sameNumericCommittedGeneration).isPresent();
            assertThat(sameNumericCommittedGeneration.orElseThrow().value().taskId())
                    .isNotEqualTo(context.task().taskId());
        }
    }
}
