/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class GenerationAllocatorConcurrencyTest {
    @Test
    void concurrentPublicationsReceiveUniqueViewScopedGenerations() throws Exception {
        GenerationMetadataStore store = new OxiaJavaGenerationMetadataStore(
                new PartitionedOxiaClient(new InMemoryPartitionedOxiaBackend()),
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
        GenerationAllocator allocator = new GenerationAllocator("cluster", store);
        StreamId stream = new StreamId("stream");
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<CompletableFuture<AllocatedGeneration>> futures = IntStream.range(0, 24)
                    .mapToObj(index -> CompletableFuture.supplyAsync(
                            () -> allocator.allocate(
                                            stream,
                                            ReadView.COMMITTED,
                                            new PublicationId(publication(index)))
                                    .join(),
                            executor))
                    .toList();
            List<AllocatedGeneration> allocations = futures.stream()
                    .map(CompletableFuture::join)
                    .toList();

            assertThat(allocations)
                    .extracting(value -> value.generation().value())
                    .doesNotHaveDuplicates()
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.rangeClosed(1, 24).mapToObj(value -> (long) value).toList());
            AllocatedGeneration latest = allocations.stream()
                    .max(Comparator.comparingLong(value -> value.generation().value()))
                    .orElseThrow();
            AllocatedGeneration replay = allocator.allocate(
                    stream, ReadView.COMMITTED, latest.publicationId()).join();
            assertThat(replay).isEqualTo(latest);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static String publication(int index) {
        char suffix = (char) ('a' + index);
        return "a".repeat(25) + suffix;
    }
}
