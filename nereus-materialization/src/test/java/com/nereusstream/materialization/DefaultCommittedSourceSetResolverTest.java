/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DefaultCommittedSourceSetResolverTest {
    @Test
    void resolvesAndRevalidatesTheDeterministicExactCommittedPath() {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero(
                        "/index/source-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero(
                        "/index/source-4", 2, 4, 100, 100, 4),
                MaterializationPlannerTestSupport.higher(
                        "/index/merged-4",
                        0,
                        4,
                        3,
                        0,
                        200,
                        4,
                        MaterializationPlannerTestSupport.sha('a'),
                        "NCP2"));
        GenerationMetadataStore generations =
                MaterializationPlannerTestSupport.generationStore(
                        candidates, List.of(), null);
        DefaultCommittedSourceSetResolver resolver =
                new DefaultCommittedSourceSetResolver(
                        CLUSTER,
                        MaterializationPlannerTestSupport.l0Store(
                                MaterializationPlannerTestSupport.snapshot(0, 4)),
                        generations,
                        2);

        CommittedSourceSetResolution resolution =
                resolver.resolve(STREAM, new OffsetRange(0, 4)).join();

        assertThat(resolution.sourceSet().coverage())
                .isEqualTo(new OffsetRange(0, 4));
        assertThat(resolution.sourceSet().sources())
                .singleElement()
                .satisfies(source -> {
                    assertThat(source.generation()).isEqualTo(3);
                    assertThat(source.indexKey()).isEqualTo("/index/merged-4");
                });
        resolver.revalidate(resolution).join();
    }

    @Test
    void refusesToClipAStraddlingSourceOrSkipAGap() {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero(
                        "/index/source-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero(
                        "/index/source-4", 2, 4, 100, 100, 4));
        DefaultCommittedSourceSetResolver resolver =
                new DefaultCommittedSourceSetResolver(
                        CLUSTER,
                        MaterializationPlannerTestSupport.l0Store(
                                MaterializationPlannerTestSupport.snapshot(1, 4)),
                        MaterializationPlannerTestSupport.generationStore(
                                candidates, List.of(), null),
                        2);

        assertThatThrownBy(
                        () -> resolver.resolve(
                                        STREAM, new OffsetRange(1, 4))
                                .join())
                .hasRootCauseMessage(
                        "no exact gap-free authoritative COMMITTED source path covers the requested range");
    }

    @Test
    void failsClosedWhenAnIndexDisappearsAfterTheScan() {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero(
                        "/index/source-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero(
                        "/index/source-4", 2, 4, 100, 100, 4));
        GenerationMetadataStore visible =
                MaterializationPlannerTestSupport.generationStore(
                        candidates, List.of(), null);
        GenerationMetadataStore disappearing = hideExactCandidates(visible);
        DefaultCommittedSourceSetResolver resolver =
                new DefaultCommittedSourceSetResolver(
                        CLUSTER,
                        MaterializationPlannerTestSupport.l0Store(
                                MaterializationPlannerTestSupport.snapshot(0, 4)),
                        disappearing,
                        2);

        assertThatThrownBy(
                        () -> resolver.resolve(
                                        STREAM, new OffsetRange(0, 4))
                                .join())
                .hasRootCauseMessage(
                        "authoritative COMMITTED source changed during resolution");
    }

    @Test
    void rejectsDuplicatePositiveGenerationIdentities() {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.higher(
                        "/index/duplicate-a",
                        0,
                        2,
                        3,
                        0,
                        100,
                        2,
                        MaterializationPlannerTestSupport.sha('a'),
                        "NCP2"),
                MaterializationPlannerTestSupport.higher(
                        "/index/duplicate-b",
                        2,
                        4,
                        3,
                        100,
                        100,
                        4,
                        MaterializationPlannerTestSupport.sha('a'),
                        "NCP2"));
        DefaultCommittedSourceSetResolver resolver =
                new DefaultCommittedSourceSetResolver(
                        CLUSTER,
                        MaterializationPlannerTestSupport.l0Store(
                                MaterializationPlannerTestSupport.snapshot(0, 4)),
                        MaterializationPlannerTestSupport.generationStore(
                                candidates, List.of(), null),
                        2);

        assertThatThrownBy(
                        () -> resolver.resolve(
                                        STREAM, new OffsetRange(0, 4))
                                .join())
                .hasRootCauseMessage(
                        "one COMMITTED source view contains duplicate positive generations");
    }

    @Test
    void permitsAppendOnlyHeadAdvanceButRejectsTrimIntoCoverage() {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero(
                        "/index/source-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero(
                        "/index/source-4", 2, 4, 100, 100, 4));
        AtomicReference<StreamMetadataSnapshot> head =
                new AtomicReference<>(
                        MaterializationPlannerTestSupport.snapshot(0, 4));
        DefaultCommittedSourceSetResolver resolver =
                new DefaultCommittedSourceSetResolver(
                        CLUSTER,
                        mutableL0Store(head),
                        MaterializationPlannerTestSupport.generationStore(
                                candidates, List.of(), null),
                        2);
        CommittedSourceSetResolution resolution =
                resolver.resolve(STREAM, new OffsetRange(0, 4)).join();

        head.set(MaterializationPlannerTestSupport.snapshot(0, 6));
        resolver.revalidate(resolution).join();

        head.set(MaterializationPlannerTestSupport.snapshot(2, 6));
        assertThatThrownBy(() -> resolver.revalidate(resolution).join())
                .hasRootCauseMessage(
                        "requested COMMITTED source range is outside authoritative retained bounds");
    }

    private static GenerationMetadataStore hideExactCandidates(
            GenerationMetadataStore delegate) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getCandidate")) {
                        return CompletableFuture.completedFuture(java.util.Optional.empty());
                    }
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "disappearing-generation-store";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> throw new UnsupportedOperationException(
                                    method.getName());
                        };
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static OxiaMetadataStore mutableL0Store(
            AtomicReference<StreamMetadataSnapshot> head) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getStreamSnapshot" ->
                            CompletableFuture.completedFuture(head.get());
                    case "close" -> null;
                    case "toString" -> "mutable-l0-store";
                    default -> throw new UnsupportedOperationException(
                            method.getName());
                });
    }
}
