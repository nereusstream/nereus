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
import com.nereusstream.objectstore.compacted.CompactedObjectVerifier;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectReader;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectWriter;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MaterializationWorkerFailureInjectionTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void sourceChangeAfterProtectionCancelsAndReleasesTheExactClaim() {
        AtomicBoolean sourcesChanged = new AtomicBoolean();
        var scenario = MaterializationWorkerTestHarness.scenario(delegate -> proxy(
                delegate,
                (method, args, result) -> method.getName().equals("getCandidate")
                                && sourcesChanged.get()
                        ? CompletableFuture.completedFuture(Optional.empty())
                        : result));
        sourcesChanged.set(true);
        var protections = new MaterializationWorkerTestHarness.TrackingProtections();
        var exactReader = new MaterializationWorkerTestHarness.TrackingExactReader();
        AtomicInteger writerCalls = new AtomicInteger();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try (LocalFileObjectStore objects = new LocalFileObjectStore(
                temporaryDirectory.resolve("source-change-objects"))) {
            DefaultMaterializationWorker worker = MaterializationWorkerTestHarness.worker(
                    "r".repeat(26),
                    scenario,
                    protections,
                    exactReader,
                    (request, rows) -> {
                        writerCalls.incrementAndGet();
                        return CompletableFuture.failedFuture(new AssertionError("writer must not run"));
                    },
                    objects,
                    (task, output, timeout) -> CompletableFuture.completedFuture(null),
                    () -> "c".repeat(26),
                    scheduler);

            assertThatThrownBy(() -> worker.execute(scenario.task()).join())
                    .hasRootCauseMessage("materialization source changed after protection");

            MaterializationTaskRecord durable = task(scenario);
            assertThat(durable.lifecycle()).isEqualTo(TaskLifecycle.CANCELLED);
            assertThat(durable.failureClassId()).isEqualTo(TaskFailureClass.SOURCE_CHANGED.wireId());
            assertThat(durable.workerClaim()).isEmpty();
            assertThat(durable.output()).isEmpty();
            assertThat(protections.acquired()).isOne();
            assertThat(protections.released()).isOne();
            assertThat(writerCalls).hasValue(0);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void checksumFailureBecomesTerminalAndCannotFreezeAnOutput() {
        var scenario = MaterializationWorkerTestHarness.scenario(delegate -> delegate);
        var protections = new MaterializationWorkerTestHarness.TrackingProtections();
        var exactReader = new MaterializationWorkerTestHarness.TrackingExactReader();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try (LocalFileObjectStore objects = new LocalFileObjectStore(
                temporaryDirectory.resolve("checksum-objects"))) {
            DefaultMaterializationWorker worker = MaterializationWorkerTestHarness.worker(
                    "r".repeat(26),
                    scenario,
                    protections,
                    exactReader,
                    (request, rows) -> CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.OBJECT_CHECKSUM_MISMATCH,
                            false,
                            "injected corrupt staged output")),
                    objects,
                    (task, output, timeout) -> CompletableFuture.completedFuture(null),
                    () -> "c".repeat(26),
                    scheduler);

            assertThatThrownBy(() -> worker.execute(scenario.task()).join())
                    .hasRootCauseMessage("injected corrupt staged output");

            MaterializationTaskRecord durable = task(scenario);
            assertThat(durable.lifecycle()).isEqualTo(TaskLifecycle.TERMINAL_FAILED);
            assertThat(durable.failureClassId()).isEqualTo(TaskFailureClass.CORRUPT_SOURCE.wireId());
            assertThat(durable.output()).isEmpty();
            assertThat(protections.acquired()).isEqualTo(2);
            assertThat(protections.released()).isEqualTo(2);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void lostOutputReadyCasResponseReloadsTheExactFrozenOutput() throws Exception {
        AtomicBoolean loseOutputReady = new AtomicBoolean(true);
        var scenario = MaterializationWorkerTestHarness.scenario(delegate -> proxy(
                delegate,
                (method, args, result) -> {
                    if (!method.getName().equals("compareAndSetTask")
                            || !(args[1] instanceof MaterializationTaskRecord replacement)
                            || replacement.lifecycle() != TaskLifecycle.OUTPUT_READY
                            || !loseOutputReady.compareAndSet(true, false)) {
                        return result;
                    }
                    @SuppressWarnings("unchecked")
                    CompletableFuture<VersionedMaterializationTask> written =
                            (CompletableFuture<VersionedMaterializationTask>) result;
                    return written.thenCompose(ignored -> CompletableFuture.failedFuture(
                            new F4MetadataConditionFailedException(
                                    "injected lost OUTPUT_READY response")));
                }));
        var protections = new MaterializationWorkerTestHarness.TrackingProtections();
        var exactReader = new MaterializationWorkerTestHarness.TrackingExactReader();
        Path stagingPath = Files.createDirectory(temporaryDirectory.resolve("lost-ready-staging"));
        Files.setPosixFilePermissions(stagingPath, PosixFilePermissions.fromString("rwx------"));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try (StagingFileManager staging = new StagingFileManager(
                        stagingPath,
                        32L << 20,
                        StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                        Duration.ofHours(1),
                        Runnable::run);
                LocalFileObjectStore objects = new LocalFileObjectStore(
                        temporaryDirectory.resolve("lost-ready-objects"))) {
            ParquetCompactedObjectReader reader =
                    new ParquetCompactedObjectReader(objects, Runnable::run);
            DefaultMaterializationOutputVerifier verifier = new DefaultMaterializationOutputVerifier(
                    objects,
                    new CompactedMaterializationFormatVerifier(
                            new CompactedObjectVerifier(objects, reader)));
            DefaultMaterializationWorker worker = MaterializationWorkerTestHarness.worker(
                    "r".repeat(26),
                    scenario,
                    protections,
                    exactReader,
                    new ParquetCompactedObjectWriter(staging, Runnable::run),
                    objects,
                    verifier,
                    () -> "c".repeat(26),
                    scheduler);

            MaterializationOutput output = worker.execute(scenario.task()).join();

            assertThat(loseOutputReady).isFalse();
            assertThat(task(scenario).lifecycle()).isEqualTo(TaskLifecycle.OUTPUT_READY);
            assertThat(task(scenario).output())
                    .contains(MaterializationRecordMapper.outputRecord(output));
            assertThat(protections.acquired()).isEqualTo(3);
            assertThat(protections.transferred()).isEqualTo(5);
            assertThat(protections.released()).isZero();
            assertThat(exactReader.maximumActive()).isOne();
            assertThat(exactReader.completedSources()).isEqualTo(2);
            assertThat(staging.reservedBytes()).isZero();
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static MaterializationTaskRecord task(
            MaterializationWorkerTestHarness.Scenario scenario) {
        return scenario.tasks()
                .get(scenario.task().streamId(), scenario.task().taskId())
                .join()
                .orElseThrow()
                .value();
    }

    private static GenerationMetadataStore proxy(
            GenerationMetadataStore delegate,
            ResultInterceptor interceptor) {
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
                    return interceptor.intercept(method, args, result);
                });
    }

    @FunctionalInterface
    private interface ResultInterceptor {
        Object intercept(java.lang.reflect.Method method, Object[] args, Object result);
    }
}
