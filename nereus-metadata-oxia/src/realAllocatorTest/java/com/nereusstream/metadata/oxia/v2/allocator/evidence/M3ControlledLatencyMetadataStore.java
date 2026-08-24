/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;

/** Delays completion of actual Pulsar metadata futures; it never substitutes a sleep for the metadata operation. */
final class M3ControlledLatencyMetadataStore extends FaultInjectionMetadataStore {
    private final AtomicInteger latencyMillis = new AtomicInteger();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4, runnable -> {
        Thread thread = new Thread(runnable, "m3-native-metadata-latency");
        thread.setDaemon(true);
        return thread;
    });

    M3ControlledLatencyMetadataStore(MetadataStoreExtended delegate) {
        super(delegate);
    }

    void setLatencyMillis(int value) {
        if (value != 0 && !M3AllocatorWorkloadPlan.METADATA_LATENCY_P99_MILLIS.contains(value)) {
            throw new IllegalArgumentException("native metadata latency differs from ADR 0094");
        }
        latencyMillis.set(value);
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(String path, Set<Option> options) {
        return delayed(super.get(path, options));
    }

    @Override
    public CompletableFuture<List<String>> getChildren(String path, Set<Option> options) {
        return delayed(super.getChildren(path, options));
    }

    @Override
    public CompletableFuture<List<String>> getChildrenFromStore(String path, Set<Option> options) {
        return delayed(super.getChildrenFromStore(path, options));
    }

    @Override
    public CompletableFuture<Boolean> exists(String path, Set<Option> options) {
        return delayed(super.exists(path, options));
    }

    @Override
    public CompletableFuture<Stat> put(
            String path, byte[] value, Optional<Long> expectedVersion, Set<Option> options) {
        return delayed(super.put(path, value, expectedVersion, options));
    }

    @Override
    public CompletableFuture<Void> delete(String path, Optional<Long> expectedVersion, Set<Option> options) {
        return delayed(super.delete(path, expectedVersion, options));
    }

    @Override
    public CompletableFuture<Void> deleteRecursive(String path, Set<Option> options) {
        return delayed(super.deleteRecursive(path, options));
    }

    @Override
    public CompletableFuture<Void> scanChildren(String parentPath, ScanConsumer consumer, Set<Option> options) {
        return delayed(super.scanChildren(parentPath, consumer, options));
    }

    private <T> CompletableFuture<T> delayed(CompletableFuture<T> source) {
        CompletableFuture<T> output = new CompletableFuture<>();
        source.whenComplete((value, failure) -> scheduler.schedule(
                () -> {
                    if (failure == null) {
                        output.complete(value);
                    } else {
                        output.completeExceptionally(failure);
                    }
                },
                latencyMillis.get(),
                TimeUnit.MILLISECONDS));
        return output;
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        scheduler.shutdownNow();
        if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
            failure = new IllegalStateException("native metadata latency scheduler did not stop");
        }
        try {
            super.close();
        } catch (Exception closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
