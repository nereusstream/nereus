/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class GenerationPublicationPropertyTest {
    @Test
    void concurrentPublishersConvergeToOneTaskAttachedGenerationAndOnePublicationPoint() {
        for (int publisherCount : List.of(2, 3, 5, 8, 13)) {
            try (GenerationPublicationTestSupport.Context context =
                    GenerationPublicationTestSupport.context()) {
                DefaultGenerationCommitter committer = context.committer(
                        context.generations(), GenerationPublicationTestSupport.successfulGuard());
                List<CompletableFuture<GenerationCommitResult>> futures = new ArrayList<>();
                for (int index = 0; index < publisherCount; index++) {
                    futures.add(committer.publish(context.task(), context.output()));
                }
                CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
                List<GenerationCommitResult> results = futures.stream()
                        .map(CompletableFuture::join)
                        .toList();

                assertThat(results).extracting(GenerationCommitResult::generation)
                        .containsOnly(results.get(0).generation());
                assertThat(results).extracting(GenerationCommitResult::publicationId)
                        .containsOnly(results.get(0).publicationId());
                assertThat(results).extracting(GenerationCommitResult::indexRecordSha256)
                        .containsOnly(results.get(0).indexRecordSha256());
                assertThat(results.stream().filter(GenerationCommitResult::committedByThisCall))
                        .hasSize(1);
            }
        }
    }
}
