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

package com.nereusstream.metadata.oxia;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

final class OxiaJavaClientBackend implements PartitionedOxiaClient.Backend {
    private final SyncOxiaClient client;
    private final Executor requestExecutor;
    private final Executor notificationExecutor;
    private final java.util.concurrent.CopyOnWriteArrayList<PrefixWatch> watches =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    OxiaJavaClientBackend(
            SyncOxiaClient client,
            Executor requestExecutor,
            Executor notificationExecutor) {
        this.client = java.util.Objects.requireNonNull(client, "client");
        this.requestExecutor = java.util.Objects.requireNonNull(requestExecutor, "requestExecutor");
        this.notificationExecutor = java.util.Objects.requireNonNull(notificationExecutor, "notificationExecutor");
        client.notifications(notification -> watches.forEach(watch -> {
            if (watch.active().get() && notification.key().startsWith(watch.prefix())) {
                try {
                    notificationExecutor.execute(watch.callback());
                } catch (java.util.concurrent.RejectedExecutionException ignored) {
                    // Adapter shutdown races may drop invalidation hints.
                }
            }
        }));
    }

    @Override
    public CompletableFuture<Optional<PartitionedOxiaClient.VersionedValue>> get(
            String key,
            PartitionKey partitionKey) {
        return supply(() -> Optional.ofNullable(client.get(
                        key, Set.of(GetOption.PartitionKey(partitionKey.value()))))
                .map(OxiaJavaClientBackend::versioned));
    }

    @Override
    public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfAbsent(
            String key,
            byte[] value,
            PartitionKey partitionKey) {
        return supply(() -> new PartitionedOxiaClient.WriteResult(client.put(
                        key,
                        value,
                        Set.of(PutOption.PartitionKey(partitionKey.value()), PutOption.IfRecordDoesNotExist))
                .version().versionId()));
    }

    @Override
    public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfVersion(
            String key,
            byte[] value,
            long expectedVersion,
            PartitionKey partitionKey) {
        return supply(() -> new PartitionedOxiaClient.WriteResult(client.put(
                        key,
                        value,
                        Set.of(
                                PutOption.PartitionKey(partitionKey.value()),
                                PutOption.IfVersionIdEquals(expectedVersion)))
                .version().versionId()));
    }

    @Override
    public CompletableFuture<Void> deleteIfVersion(
            String key,
            long expectedVersion,
            PartitionKey partitionKey) {
        return supply(() -> {
            boolean deleted;
            try {
                deleted = client.delete(
                        key,
                        Set.of(
                                DeleteOption.PartitionKey(partitionKey.value()),
                                DeleteOption.IfVersionIdEquals(expectedVersion)));
            } catch (io.oxia.client.api.exceptions.UnexpectedVersionIdException failure) {
                throw new F4MetadataConditionFailedException("unexpected version during conditional delete", failure);
            }
            if (!deleted) {
                throw new F4MetadataConditionFailedException("key is absent during conditional delete");
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<List<String>> list(
            String fromInclusive,
            String toExclusive,
            PartitionKey partitionKey) {
        return supply(() -> client.list(
                fromInclusive,
                toExclusive,
                Set.of(ListOption.PartitionKey(partitionKey.value()))));
    }

    @Override
    public CompletableFuture<List<PartitionedOxiaClient.VersionedValue>> rangeScan(
            String fromInclusive,
            String toExclusive,
            int limit,
            PartitionKey partitionKey) {
        return supply(() -> {
            List<PartitionedOxiaClient.VersionedValue> values = new ArrayList<>();
            try (var scan = client.rangeScan(
                    fromInclusive,
                    toExclusive,
                    Set.of(RangeScanOption.PartitionKey(partitionKey.value())))) {
                for (GetResult value : scan) {
                    values.add(versioned(value));
                    if (values.size() == limit) {
                        break;
                    }
                }
            }
            return List.copyOf(values);
        });
    }

    @Override
    public WatchRegistration watchPrefix(
            String prefix,
            PartitionKey partitionKey,
            Runnable invalidationCallback) {
        var active = new java.util.concurrent.atomic.AtomicBoolean(true);
        PrefixWatch watch = new PrefixWatch(prefix, active, invalidationCallback);
        watches.add(watch);
        return () -> {
            if (active.compareAndSet(true, false)) {
                watches.remove(watch);
            }
        };
    }

    private <T> CompletableFuture<T> supply(java.util.concurrent.Callable<T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return operation.call();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new java.util.concurrent.CompletionException(e);
            }
        }, requestExecutor);
    }

    private static PartitionedOxiaClient.VersionedValue versioned(GetResult result) {
        return new PartitionedOxiaClient.VersionedValue(
                result.key(), result.value(), result.version().versionId());
    }

    private record PrefixWatch(
            String prefix,
            java.util.concurrent.atomic.AtomicBoolean active,
            Runnable callback) {
    }
}
