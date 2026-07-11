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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class PartitionedOxiaClientTest {
    private final RecordingBackend backend = new RecordingBackend();
    private final PartitionedOxiaClient client = new PartitionedOxiaClient(backend);
    private final PartitionKey partitionKey = new PartitionKey("stream-1");

    @Test
    void rejectsMissingPartitionKeyBeforeCallingBackend() {
        assertThatThrownBy(() -> client.get("/key", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("partitionKey");

        assertThat(backend.operations).isEmpty();
    }

    @Test
    void passesPartitionKeyToEveryOperation() {
        client.get("/get", partitionKey).join();
        client.putIfAbsent("/put-if-absent", new byte[] {1}, partitionKey).join();
        client.putIfVersion("/put-if-version", new byte[] {2}, 3, partitionKey).join();
        client.list("/list/a", "/list/z", partitionKey).join();
        client.rangeScan("/range/a", "/range/z", 7, partitionKey).join();
        client.watchPrefix("/watch", partitionKey, () -> { }).close();

        assertThat(backend.operations)
                .extracting(Operation::partitionKey)
                .containsOnly(partitionKey.value());
        assertThat(backend.operations)
                .extracting(Operation::name)
                .containsExactly(
                        "get",
                        "putIfAbsent",
                        "putIfVersion",
                        "list",
                        "rangeScan",
                        "watchPrefix");
    }

    @Test
    void copiesWrittenValuesBeforeCallingBackend() {
        byte[] value = new byte[] {1, 2, 3};

        client.putIfAbsent("/key", value, partitionKey).join();
        value[0] = 9;

        assertThat(backend.operations.get(0).value()).containsExactly(1, 2, 3);
    }

    private static final class RecordingBackend implements PartitionedOxiaClient.Backend {
        private final List<Operation> operations = new ArrayList<>();

        @Override
        public CompletableFuture<Optional<PartitionedOxiaClient.VersionedValue>> get(
                String key,
                PartitionKey partitionKey) {
            operations.add(new Operation("get", key, "", partitionKey.value(), new byte[0]));
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfAbsent(
                String key,
                byte[] value,
                PartitionKey partitionKey) {
            operations.add(new Operation("putIfAbsent", key, "", partitionKey.value(), value));
            return CompletableFuture.completedFuture(new PartitionedOxiaClient.WriteResult(1));
        }

        @Override
        public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfVersion(
                String key,
                byte[] value,
                long expectedVersion,
                PartitionKey partitionKey) {
            operations.add(new Operation("putIfVersion", key, Long.toString(expectedVersion), partitionKey.value(), value));
            return CompletableFuture.completedFuture(new PartitionedOxiaClient.WriteResult(expectedVersion + 1));
        }

        @Override
        public CompletableFuture<List<String>> list(
                String fromInclusive,
                String toExclusive,
                PartitionKey partitionKey) {
            operations.add(new Operation("list", fromInclusive, toExclusive, partitionKey.value(), new byte[0]));
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        public CompletableFuture<List<PartitionedOxiaClient.VersionedValue>> rangeScan(
                String fromInclusive,
                String toExclusive,
                int limit,
                PartitionKey partitionKey) {
            operations.add(new Operation(
                    "rangeScan", fromInclusive, toExclusive + ":" + limit, partitionKey.value(), new byte[0]));
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        public WatchRegistration watchPrefix(
                String prefix,
                PartitionKey partitionKey,
                Runnable invalidationCallback) {
            operations.add(new Operation("watchPrefix", prefix, "", partitionKey.value(), new byte[0]));
            return () -> { };
        }
    }

    private record Operation(
            String name,
            String key,
            String rangeEnd,
            String partitionKey,
            byte[] value) {
    }
}
