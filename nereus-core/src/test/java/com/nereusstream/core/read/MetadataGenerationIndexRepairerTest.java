/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.DerivedIndexRepairCursor;
import com.nereusstream.metadata.oxia.DerivedIndexRepairResult;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MetadataGenerationIndexRepairerTest {
    private static final String CLUSTER = "cluster";
    private static final StreamId STREAM = new StreamId("stream");

    @Test
    void accumulatesBoundedLiveRepairPagesUntilTargetIsCovered() {
        AtomicInteger calls = new AtomicInteger();
        MetadataGenerationIndexRepairer repairer = new MetadataGenerationIndexRepairer(
                CLUSTER, store(calls), 2);

        GenerationIndexRepairResult result = repairer.repair(
                STREAM, 0, Duration.ofSeconds(1)).join();

        assertThat(result.source()).isEqualTo(GenerationIndexRepairSource.LIVE_COMMIT);
        assertThat(result.scannedRecords()).isEqualTo(2);
        assertThat(calls).hasValue(2);
    }

    @Test
    void failsClosedWhenContinuationWouldExceedRepairBudget() {
        AtomicInteger calls = new AtomicInteger();
        MetadataGenerationIndexRepairer repairer = new MetadataGenerationIndexRepairer(
                CLUSTER, store(calls), 1);

        assertThatThrownBy(() -> repairer.repair(
                        STREAM, 0, Duration.ofSeconds(1)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code())
                                        .isEqualTo(ErrorCode.READ_RESOLUTION_FAILED)));
        assertThat(calls).hasValue(1);
    }

    private static OxiaMetadataStore store(AtomicInteger calls) {
        DerivedIndexRepairCursor cursor = new DerivedIndexRepairCursor(
                STREAM,
                0,
                "head",
                2,
                "commit-1",
                1,
                7,
                1);
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "repairDerivedStreamIndexes" -> {
                        int call = calls.getAndIncrement();
                        yield CompletableFuture.completedFuture(call == 0
                                ? new DerivedIndexRepairResult(
                                        STREAM,
                                        0,
                                        0,
                                        1,
                                        0,
                                        false,
                                        true,
                                        Optional.of(cursor),
                                        2)
                                : new DerivedIndexRepairResult(
                                        STREAM,
                                        0,
                                        1,
                                        1,
                                        1,
                                        true,
                                        false,
                                        Optional.empty(),
                                        2));
                    }
                    case "close" -> null;
                    case "toString" -> "metadata-generation-index-repairer-test";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
