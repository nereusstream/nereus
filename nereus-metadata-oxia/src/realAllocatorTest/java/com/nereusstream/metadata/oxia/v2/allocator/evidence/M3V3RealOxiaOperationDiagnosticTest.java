/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealOxiaActors.InstrumentedClient;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealOxiaActors.InstrumentedClient.OperationDiagnosticSnapshot;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealOxiaActors.InstrumentedClient.OperationSample;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Diagnostic-only real-Oxia primitive, conflict-reread, scheduler, callback, and outstanding measurement. */
class M3V3RealOxiaOperationDiagnosticTest {
    @Test
    void realOxiaOperationsRemainNonzeroAcrossEveryFrozenLatency() throws Exception {
        String serviceAddress = M3V3DiagnosticOutput.requiredProperty("nereus.m3.allocator.v3.oxiaServiceAddress");
        String runId = M3V3DiagnosticOutput.requiredProperty("nereus.m3.allocator.v3.diagnosticRunId");
        List<Row> rows = new ArrayList<>();
        try (M3RealOxiaActors actors = new M3RealOxiaActors(serviceAddress)) {
            for (int latency : List.of(1, 5, 10, 25)) {
                actors.setControlledLatencyMillis(latency);
                InstrumentedClient first = actors.actor(0).client();
                InstrumentedClient second = actors.actor(1).client();
                first.beginDiagnosticCapture();
                second.beginDiagnosticCapture();
                String key = "/nereus/m3/allocator-v3-diagnostic/" + runId + "/operations/" + latency;
                CanonicalBytes initial = bytes("initial-" + latency);
                CanonicalBytes updated = bytes("updated-" + latency);

                assertThat(first.read(key).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEmpty();
                first.createIfAbsent(key, initial).toCompletableFuture().get(5, TimeUnit.SECONDS);
                AuthorityRecord exact = first.read(key).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
                first.compareAndSet(key, updated, exact.versionId())
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS);
                AuthorityRecord predecessor = first.read(key)
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS)
                        .orElseThrow();

                CompletableFuture<Void> firstCas = first.compareAndSet(
                                key, bytes("first-" + latency), predecessor.versionId())
                        .toCompletableFuture();
                CompletableFuture<Void> secondCas = second.compareAndSet(
                                key, bytes("second-" + latency), predecessor.versionId())
                        .toCompletableFuture();
                CompletableFuture.allOf(settle(firstCas), settle(secondCas)).get(5, TimeUnit.SECONDS);
                assertThat(List.of(firstCas, secondCas).stream().filter(future -> !future.isCompletedExceptionally()))
                        .hasSize(1);

                InstrumentedClient loser = firstCas.isCompletedExceptionally() ? first : second;
                AuthorityRecord reread = loser.read(key).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
                loser.compareAndSet(key, bytes("retry-" + latency), reread.versionId())
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS);

                OperationDiagnosticSnapshot firstSnapshot = first.endDiagnosticCapture();
                OperationDiagnosticSnapshot secondSnapshot = second.endDiagnosticCapture();
                List<OperationSample> samples = new ArrayList<>(firstSnapshot.samples());
                samples.addAll(secondSnapshot.samples());
                Row row = Row.from(latency, samples, Math.max(
                        firstSnapshot.outstandingMaximum(), secondSnapshot.outstandingMaximum()));
                assertThat(row.operationCount()).isGreaterThanOrEqualTo(9);
                assertThat(row.readCount()).isGreaterThanOrEqualTo(4);
                assertThat(row.createCount()).isEqualTo(1);
                assertThat(row.casCount()).isGreaterThanOrEqualTo(4);
                assertThat(row.realRttP99Micros()).isPositive();
                assertThat(row.injectedLatencyMillis()).isEqualTo(latency);
                rows.add(row);
            }
        }
        M3V3DiagnosticOutput.writeNew("real-oxia-operation-diagnostic.json", json(rows));
    }

    private static CompletableFuture<Void> settle(CompletableFuture<Void> source) {
        return source.handle((ignored, failure) -> null);
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String json(List<Row> rows) {
        StringBuilder json = new StringBuilder(2048);
        json.append("{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_OPERATION_DIAGNOSTIC_V3\"")
                .append(",\"diagnosticOnly\":true,\"authority\":false,\"selectionEligible\":false")
                .append(",\"delaySchedulerThreadsPerActor\":")
                .append(M3RealOxiaActors.CONTROLLED_DELAY_SCHEDULER_THREADS_PER_ACTOR)
                .append(",\"rows\":[");
        for (int index = 0; index < rows.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            Row row = rows.get(index);
            json.append("{\"latencyMillis\":").append(row.injectedLatencyMillis())
                    .append(",\"operationCount\":").append(row.operationCount())
                    .append(",\"readCount\":").append(row.readCount())
                    .append(",\"createCount\":").append(row.createCount())
                    .append(",\"casCount\":").append(row.casCount())
                    .append(",\"realRttP99Micros\":").append(row.realRttP99Micros())
                    .append(",\"schedulerLagP99Micros\":").append(row.schedulerLagP99Micros())
                    .append(",\"callbackLagP99Micros\":").append(row.callbackLagP99Micros())
                    .append(",\"operationOutstandingMax\":").append(row.operationOutstandingMax())
                    .append('}');
        }
        return json.append("]}\n").toString();
    }

    private record Row(
            int injectedLatencyMillis,
            int operationCount,
            long readCount,
            long createCount,
            long casCount,
            long realRttP99Micros,
            long schedulerLagP99Micros,
            long callbackLagP99Micros,
            int operationOutstandingMax) {
        private static Row from(int latency, List<OperationSample> samples, int outstandingMaximum) {
            return new Row(
                    latency,
                    samples.size(),
                    samples.stream().filter(sample -> sample.kind().equals("READ")).count(),
                    samples.stream().filter(sample -> sample.kind().equals("CREATE_IF_ABSENT")).count(),
                    samples.stream().filter(sample -> sample.kind().equals("COMPARE_AND_SET")).count(),
                    percentile99(samples.stream().map(OperationSample::realRttMicros).toList()),
                    percentile99(samples.stream().map(OperationSample::schedulerLagMicros).toList()),
                    percentile99(samples.stream().map(OperationSample::callbackLagMicros).toList()),
                    outstandingMaximum);
        }

        private static long percentile99(List<Long> values) {
            if (values.isEmpty()) {
                return 0;
            }
            List<Long> sorted = values.stream().sorted().toList();
            return sorted.get(Math.max(0, (int) Math.ceil(sorted.size() * 0.99) - 1));
        }
    }
}
