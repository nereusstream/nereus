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

package io.nereus.metadata.oxia;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

final class PartitionedOxiaClient {
    private final Backend backend;

    PartitionedOxiaClient(Backend backend) {
        this.backend = Objects.requireNonNull(backend, "backend");
    }

    CompletableFuture<Optional<VersionedValue>> get(String key, PartitionKey partitionKey) {
        return backend.get(requireKey(key, "key"), requirePartitionKey(partitionKey));
    }

    CompletableFuture<WriteResult> putIfAbsent(String key, byte[] value, PartitionKey partitionKey) {
        return backend.putIfAbsent(
                requireKey(key, "key"),
                copyValue(value),
                requirePartitionKey(partitionKey));
    }

    CompletableFuture<WriteResult> putIfVersion(
            String key,
            byte[] value,
            long expectedVersion,
            PartitionKey partitionKey) {
        if (expectedVersion < 0) {
            throw new IllegalArgumentException("expectedVersion must be non-negative");
        }
        return backend.putIfVersion(
                requireKey(key, "key"),
                copyValue(value),
                expectedVersion,
                requirePartitionKey(partitionKey));
    }

    CompletableFuture<List<String>> list(
            String fromInclusive,
            String toExclusive,
            PartitionKey partitionKey) {
        return backend.list(
                requireKey(fromInclusive, "fromInclusive"),
                requireKey(toExclusive, "toExclusive"),
                requirePartitionKey(partitionKey));
    }

    CompletableFuture<List<VersionedValue>> rangeScan(
            String fromInclusive,
            String toExclusive,
            PartitionKey partitionKey) {
        return backend.rangeScan(
                requireKey(fromInclusive, "fromInclusive"),
                requireKey(toExclusive, "toExclusive"),
                requirePartitionKey(partitionKey));
    }

    WatchRegistration watchPrefix(
            String prefix,
            PartitionKey partitionKey,
            Runnable invalidationCallback) {
        return backend.watchPrefix(
                requireKey(prefix, "prefix"),
                requirePartitionKey(partitionKey),
                Objects.requireNonNull(invalidationCallback, "invalidationCallback"));
    }

    private static PartitionKey requirePartitionKey(PartitionKey partitionKey) {
        return Objects.requireNonNull(partitionKey, "partitionKey");
    }

    private static String requireKey(String key, String fieldName) {
        Objects.requireNonNull(key, fieldName);
        if (key.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return key;
    }

    private static byte[] copyValue(byte[] value) {
        return Objects.requireNonNull(value, "value").clone();
    }

    interface Backend {
        CompletableFuture<Optional<VersionedValue>> get(String key, PartitionKey partitionKey);

        CompletableFuture<WriteResult> putIfAbsent(String key, byte[] value, PartitionKey partitionKey);

        CompletableFuture<WriteResult> putIfVersion(
                String key,
                byte[] value,
                long expectedVersion,
                PartitionKey partitionKey);

        CompletableFuture<List<String>> list(
                String fromInclusive,
                String toExclusive,
                PartitionKey partitionKey);

        CompletableFuture<List<VersionedValue>> rangeScan(
                String fromInclusive,
                String toExclusive,
                PartitionKey partitionKey);

        WatchRegistration watchPrefix(
                String prefix,
                PartitionKey partitionKey,
                Runnable invalidationCallback);
    }

    record VersionedValue(String key, byte[] value, long version) {
        VersionedValue {
            key = requireKey(key, "key");
            value = copyValue(value);
            if (version < 0) {
                throw new IllegalArgumentException("version must be non-negative");
            }
        }

        @Override
        public byte[] value() {
            return value.clone();
        }
    }

    record WriteResult(long version) {
        WriteResult {
            if (version < 0) {
                throw new IllegalArgumentException("version must be non-negative");
            }
        }
    }
}
