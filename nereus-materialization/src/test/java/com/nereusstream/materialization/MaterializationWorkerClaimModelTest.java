/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.objectstore.compacted.CompactedObjectWriteResult;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MaterializationWorkerClaimModelTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void lostClaimCasResponseReusesOneClaimIdAndOneAttempt() {
        AtomicBoolean loseClaimResponse = new AtomicBoolean(true);
        var scenario = MaterializationWorkerTestHarness.scenario(delegate ->
                loseFirstClaimResponse(delegate, loseClaimResponse));
        var protections = new MaterializationWorkerTestHarness.TrackingProtections();
        var exactReader = new MaterializationWorkerTestHarness.TrackingExactReader();
        AtomicInteger claimIds = new AtomicInteger();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try (LocalFileObjectStore objects = new LocalFileObjectStore(
                temporaryDirectory.resolve("lost-claim-objects"))) {
            DefaultMaterializationWorker worker = MaterializationWorkerTestHarness.worker(
                    "r".repeat(26),
                    scenario,
                    protections,
                    exactReader,
                    (request, rows) -> CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.OBJECT_UPLOAD_FAILED,
                            true,
                            "stop after recovered claim")),
                    objects,
                    (task, output, timeout) -> CompletableFuture.completedFuture(null),
                    () -> {
                        claimIds.incrementAndGet();
                        return "c".repeat(26);
                    },
                    scheduler);

            assertThatThrownBy(() -> worker.execute(scenario.task()).join())
                    .hasRootCauseMessage("stop after recovered claim");

            MaterializationTaskRecord durable = task(scenario);
            assertThat(loseClaimResponse).isFalse();
            assertThat(claimIds).hasValue(1);
            assertThat(durable.attempt()).isEqualTo(1);
            assertThat(durable.lifecycle()).isEqualTo(TaskLifecycle.RETRY_WAIT);
            assertThat(durable.failureClassId())
                    .isEqualTo(TaskFailureClass.RETRYABLE_OBJECT_STORE.wireId());
            assertThat(protections.acquired()).isEqualTo(2);
            assertThat(protections.released()).isEqualTo(2);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void twoIndependentWorkersCannotExecuteTheSameDurableClaimConcurrently() throws Exception {
        var scenario = MaterializationWorkerTestHarness.scenario(delegate -> delegate);
        var firstProtections = new MaterializationWorkerTestHarness.TrackingProtections();
        var secondProtections = new MaterializationWorkerTestHarness.TrackingProtections();
        var exactReader = new MaterializationWorkerTestHarness.TrackingExactReader();
        CompletableFuture<CompactedObjectWriteResult> blockedWrite = new CompletableFuture<>();
        CountDownLatch firstWriterEntered = new CountDownLatch(1);
        AtomicInteger firstWriterCalls = new AtomicInteger();
        AtomicInteger secondWriterCalls = new AtomicInteger();
        ScheduledExecutorService firstScheduler = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService secondScheduler = Executors.newSingleThreadScheduledExecutor();
        try (LocalFileObjectStore objects = new LocalFileObjectStore(
                temporaryDirectory.resolve("contended-claim-objects"))) {
            DefaultMaterializationWorker first = MaterializationWorkerTestHarness.worker(
                    "r".repeat(26),
                    scenario,
                    firstProtections,
                    exactReader,
                    (request, rows) -> {
                        firstWriterCalls.incrementAndGet();
                        firstWriterEntered.countDown();
                        return blockedWrite;
                    },
                    objects,
                    (task, output, timeout) -> CompletableFuture.completedFuture(null),
                    () -> "c".repeat(26),
                    firstScheduler);
            DefaultMaterializationWorker second = MaterializationWorkerTestHarness.worker(
                    "s".repeat(26),
                    scenario,
                    secondProtections,
                    exactReader,
                    (request, rows) -> {
                        secondWriterCalls.incrementAndGet();
                        return CompletableFuture.failedFuture(new AssertionError(
                                "losing worker must not write"));
                    },
                    objects,
                    (task, output, timeout) -> CompletableFuture.completedFuture(null),
                    () -> "d".repeat(26),
                    secondScheduler);

            CompletableFuture<MaterializationOutput> firstResult = first.execute(scenario.task());
            assertThat(firstWriterEntered.await(5, TimeUnit.SECONDS)).isTrue();
            MaterializationTaskRecord claimed = task(scenario);
            assertThat(claimed.lifecycle()).isEqualTo(TaskLifecycle.CLAIMED);
            assertThat(claimed.workerClaim()).isPresent().get().satisfies(claim -> {
                assertThat(claim.claimId()).isEqualTo("c".repeat(26));
                assertThat(claim.processRunId()).isEqualTo("r".repeat(26));
            });

            assertThatThrownBy(() -> second.execute(scenario.task()).join())
                    .hasRootCauseMessage("materialization task is already claimed by another worker");
            assertThat(firstWriterCalls).hasValue(1);
            assertThat(secondWriterCalls).hasValue(0);
            assertThat(secondProtections.acquired()).isZero();

            blockedWrite.completeExceptionally(new NereusException(
                    ErrorCode.OBJECT_UPLOAD_FAILED,
                    true,
                    "release contended worker"));
            assertThatThrownBy(firstResult::join)
                    .hasRootCauseMessage("release contended worker");
            MaterializationTaskRecord retry = task(scenario);
            assertThat(retry.lifecycle()).isEqualTo(TaskLifecycle.RETRY_WAIT);
            assertThat(retry.attempt()).isEqualTo(1);
            assertThat(firstProtections.acquired()).isEqualTo(2);
            assertThat(firstProtections.released()).isEqualTo(2);
        } finally {
            firstScheduler.shutdownNow();
            secondScheduler.shutdownNow();
        }
    }

    private static GenerationMetadataStore loseFirstClaimResponse(
            GenerationMetadataStore delegate,
            AtomicBoolean loseClaimResponse) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return method.invoke(delegate, args);
                    }
                    Object result;
                    try {
                        result = method.invoke(delegate, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    if (!method.getName().equals("compareAndSetTask")
                            || !(args[1] instanceof MaterializationTaskRecord replacement)
                            || replacement.lifecycle() != TaskLifecycle.CLAIMED
                            || !loseClaimResponse.compareAndSet(true, false)) {
                        return result;
                    }
                    @SuppressWarnings("unchecked")
                    CompletableFuture<VersionedMaterializationTask> written =
                            (CompletableFuture<VersionedMaterializationTask>) result;
                    return written.thenCompose(ignored -> CompletableFuture.failedFuture(
                            new F4MetadataConditionFailedException(
                                    "injected lost CLAIMED response")));
                });
    }

    private static MaterializationTaskRecord task(
            MaterializationWorkerTestHarness.Scenario scenario) {
        return scenario.tasks()
                .get(scenario.task().streamId(), scenario.task().taskId())
                .join()
                .orElseThrow()
                .value();
    }
}
