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

package com.nereusstream.storage.object.gc;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Bounded positive cache of immutable compact terminals. Missing and active states are never cached. */
public final class M5PermanentDoneCacheV2 implements ExactMetadataTransactionStoreV1 {
    public record Snapshot(int entries, long encodedBytes, long hits, long authoritativeReads, long evictions) {}

    private final ExactMetadataTransactionStoreV1 delegate;
    private final int maximumEntries;
    private final long maximumEncodedBytes;
    private final LinkedHashMap<String, VersionedValue> entries = new LinkedHashMap<>(16, 0.75f, true);
    private long encodedBytes;
    private long hits;
    private long authoritativeReads;
    private long evictions;

    public M5PermanentDoneCacheV2(
            ExactMetadataTransactionStoreV1 delegate, int maximumEntries, long maximumEncodedBytes) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (maximumEntries <= 0 || maximumEncodedBytes <= 0) {
            throw new IllegalArgumentException("done cache requires positive resident count and encoded byte bounds");
        }
        this.maximumEntries = maximumEntries;
        this.maximumEncodedBytes = maximumEncodedBytes;
    }

    @Override
    public CompletionStage<Optional<VersionedValue>> read(String key) {
        Objects.requireNonNull(key, "key");
        synchronized (this) {
            var cached = entries.get(key);
            if (cached != null) {
                hits++;
                return CompletableFuture.completedFuture(Optional.of(cached));
            }
            authoritativeReads++;
        }
        return delegate.read(key).thenApply(observed -> {
            observed.ifPresent(value -> {
                if (!key.equals(value.key())) {
                    throw new IllegalArgumentException("done cache authoritative read returned another key");
                }
                if (M5TargetDeleteDoneV2.isCompactDone(value.canonicalStoredBytes())) {
                    var done = M5TargetDeleteStoredValueV2.decode(value);
                    if (done.compactDone().isPresent()) {
                        remember(value);
                    }
                }
            });
            return observed;
        });
    }

    @Override
    public CompletionStage<MutationOutcome> compareAndSet(
            Optional<VersionedValue> predecessor, String key, CanonicalBytes candidate) {
        // Always retain the native conditional check, including a create after resident eviction.
        return delegate.compareAndSet(predecessor, key, candidate);
    }

    @Override
    public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
        Objects.requireNonNull(transaction, "transaction");
        return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
    }

    @Override
    public boolean supportsAtomicMultiKeyTransactions() {
        return false;
    }

    public synchronized Snapshot snapshot() {
        return new Snapshot(entries.size(), encodedBytes, hits, authoritativeReads, evictions);
    }

    private synchronized void remember(VersionedValue value) {
        long weight = weight(value);
        if (weight > maximumEncodedBytes) {
            return;
        }
        var prior = entries.get(value.key());
        if (prior != null) {
            if (!prior.equals(value)) {
                throw new IllegalStateException("permanent done changed its value or native revision");
            }
            return;
        }
        while (entries.size() >= maximumEntries || encodedBytes > maximumEncodedBytes - weight) {
            var first = entries.entrySet().iterator();
            var removed = first.next().getValue();
            first.remove();
            encodedBytes -= weight(removed);
            evictions++;
        }
        entries.put(value.key(), value);
        encodedBytes += weight;
    }

    private static long weight(VersionedValue value) {
        return (long) CanonicalUtf8.fromString(value.key()).bytes().length()
                + value.canonicalStoredBytes().length()
                + value.metadataVersion().value().length();
    }
}
