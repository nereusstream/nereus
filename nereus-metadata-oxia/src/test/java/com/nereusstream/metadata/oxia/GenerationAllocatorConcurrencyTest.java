/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

    @Test
    void lostAllocationResponseWithInterleavingBurnsOneGapAndAttachesAUniqueRetry()
            throws Exception {
        GenerationMetadataStore durable = new OxiaJavaGenerationMetadataStore(
                new PartitionedOxiaClient(new InMemoryPartitionedOxiaBackend()),
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
        AtomicBoolean loseFirstResponse = new AtomicBoolean(true);
        AtomicReference<AllocatedGeneration> burned = new AtomicReference<>();
        GenerationMetadataStore lossy = loseFirstAllocationResponse(
                durable, loseFirstResponse, burned);
        GenerationAllocator allocator = new GenerationAllocator("cluster", lossy);
        GenerationAllocator independent = new GenerationAllocator("cluster", durable);
        StreamId stream = new StreamId("lost-response-stream");
        PublicationId firstPublication = new PublicationId(publication(0));
        PublicationId interleavingPublication = new PublicationId(publication(1));
        try {
            assertThatThrownBy(() -> allocator.allocate(
                                    stream,
                                    ReadView.COMMITTED,
                                    firstPublication)
                            .join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseMessage("injected lost generation allocation response");

            AllocatedGeneration interleaving = independent.allocate(
                            stream,
                            ReadView.COMMITTED,
                            interleavingPublication)
                    .join();
            AllocatedGeneration attached = allocator.allocate(
                            stream,
                            ReadView.COMMITTED,
                            firstPublication)
                    .join();
            AllocatedGeneration replay = allocator.allocate(
                            stream,
                            ReadView.COMMITTED,
                            firstPublication)
                    .join();

            assertThat(burned.get().generation().value()).isEqualTo(1);
            assertThat(interleaving.generation().value()).isEqualTo(2);
            assertThat(attached.generation().value()).isEqualTo(3);
            assertThat(replay).isEqualTo(attached);
            assertThat(List.of(
                            burned.get().generation().value(),
                            interleaving.generation().value(),
                            attached.generation().value()))
                    .doesNotHaveDuplicates();
            assertThat(durable.getSequence(
                                    "cluster", stream, ReadView.COMMITTED)
                            .join()
                            .orElseThrow()
                            .value()
                            .lastPublicationId())
                    .isEqualTo(firstPublication.value());
        } finally {
            durable.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static GenerationMetadataStore loseFirstAllocationResponse(
            GenerationMetadataStore delegate,
            AtomicBoolean loseFirstResponse,
            AtomicReference<AllocatedGeneration> burned) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "lossy-generation-allocation-store";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    }
                    try {
                        Object result = method.invoke(delegate, args);
                        if (method.getName().equals("allocateGeneration")
                                && loseFirstResponse.compareAndSet(true, false)) {
                            return ((CompletableFuture<AllocatedGeneration>) result)
                                    .thenCompose(allocation -> {
                                        burned.set(allocation);
                                        return CompletableFuture.failedFuture(
                                                new IllegalStateException(
                                                        "injected lost generation allocation response"));
                                    });
                        }
                        return result;
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static String publication(int index) {
        char suffix = (char) ('a' + index);
        return "a".repeat(25) + suffix;
    }
}
