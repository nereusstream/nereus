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

package com.nereusstream.metadata.oxia.v2.mutation;

import com.nereusstream.domain.bytes.CanonicalBytes;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.options.PutOption;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/** Production narrow port backed by one shared O1 {@link AsyncOxiaClient}. */
public final class AsyncOxiaConditionalClient implements OxiaConditionalClient {
    private final AsyncOxiaClient client;

    public AsyncOxiaConditionalClient(AsyncOxiaClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override
    public CompletionStage<Optional<AuthorityRecord>> read(String key) {
        requireKey(key);
        return client.get(key).thenApply(result -> toAuthorityRecord(key, result));
    }

    @Override
    public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes storedBytes) {
        requireKey(key);
        Objects.requireNonNull(storedBytes, "storedBytes");
        return client.put(key, storedBytes.toByteArray(), Set.of(PutOption.IfRecordDoesNotExist))
                .thenApply(ignored -> null);
    }

    @Override
    public CompletionStage<Void> compareAndSet(String key, CanonicalBytes storedBytes, long expectedVersionId) {
        requireKey(key);
        Objects.requireNonNull(storedBytes, "storedBytes");
        if (expectedVersionId < 0) {
            throw new IllegalArgumentException("expectedVersionId must not be negative");
        }
        return client.put(key, storedBytes.toByteArray(), Set.of(PutOption.IfVersionIdEquals(expectedVersionId)))
                .thenApply(ignored -> null);
    }

    private static Optional<AuthorityRecord> toAuthorityRecord(String expectedKey, GetResult result) {
        if (result == null) {
            return Optional.empty();
        }
        if (!expectedKey.equals(result.key())) {
            throw new IllegalStateException("Oxia exact get returned a different authority key");
        }
        Objects.requireNonNull(result.value(), "Oxia authority value");
        Objects.requireNonNull(result.version(), "Oxia authority version");
        return Optional.of(new AuthorityRecord(
                result.key(),
                CanonicalBytes.copyOf(result.value()),
                result.version().versionId()));
    }

    private static void requireKey(String key) {
        Objects.requireNonNull(key, "key");
        if (key.isBlank()) {
            throw new IllegalArgumentException("authority key must not be blank");
        }
    }
}
