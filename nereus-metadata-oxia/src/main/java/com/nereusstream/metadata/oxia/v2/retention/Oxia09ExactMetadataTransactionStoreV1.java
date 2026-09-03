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

package com.nereusstream.metadata.oxia.v2.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Honest M5 metadata adapter for the source-locked Oxia Java 0.9 client.
 *
 * <p>Single-key exact CAS is available. Atomic multi-key transactions are not part of that client
 * or server contract, so the transaction method performs no calls and returns {@code UNSUPPORTED}.
 * This prevents a sequential-CAS approximation from creating a selector/batch split state.
 */
public final class Oxia09ExactMetadataTransactionStoreV1 implements ExactMetadataTransactionStoreV1 {
    private final OxiaConditionalClient client;
    private final MutationFailureClassifier failureClassifier = new MutationFailureClassifier();

    public Oxia09ExactMetadataTransactionStoreV1(AsyncOxiaClient client) {
        this(new AsyncOxiaConditionalClient(client));
    }

    public Oxia09ExactMetadataTransactionStoreV1(OxiaConditionalClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override
    public CompletionStage<Optional<VersionedValue>> read(String key) {
        return client.read(requireKey(key)).thenApply(value -> value.map(Oxia09ExactMetadataTransactionStoreV1::map));
    }

    @Override
    public CompletionStage<MutationOutcome> compareAndSet(
            Optional<VersionedValue> exactPredecessor, String key, CanonicalBytes exactCandidate) {
        String requiredKey = requireKey(key);
        Optional<VersionedValue> predecessor = Objects.requireNonNull(exactPredecessor, "exactPredecessor");
        CanonicalBytes candidate = requireCandidate(exactCandidate);
        predecessor.ifPresent(value -> {
            if (!requiredKey.equals(value.key())) {
                throw new IllegalArgumentException("CAS predecessor key differs from the mutation key");
            }
        });
        return read(requiredKey).thenCompose(current -> {
            if (!current.equals(predecessor)) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            CompletionStage<Void> mutation = predecessor.isEmpty()
                    ? client.createIfAbsent(requiredKey, candidate)
                    : client.compareAndSet(
                            requiredKey,
                            candidate,
                            MetadataVersionMapper.toOxia(
                                    predecessor.orElseThrow().metadataVersion()));
            return mutation.handle((ignored, failure) ->
                            new Attempt(failure == null ? null : failureClassifier.classify(failure)))
                    .thenCompose(attempt -> reconcile(requiredKey, predecessor, candidate, attempt));
        });
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

    private CompletionStage<MutationOutcome> reconcile(
            String key, Optional<VersionedValue> predecessor, CanonicalBytes candidate, Attempt attempt) {
        return read(key).handle((observed, readFailure) -> {
            if (readFailure != null) {
                return MutationOutcome.RESPONSE_UNKNOWN;
            }
            Optional<VersionedValue> exactObserved = Objects.requireNonNull(observed, "observed");
            if (exactObserved.isPresent()
                    && exactObserved.orElseThrow().canonicalStoredBytes().equals(candidate)) {
                return MutationOutcome.APPLIED_EXACT;
            }
            if (exactObserved.equals(predecessor)) {
                return attempt.failureKind() == MutationFailureClassifier.Kind.CONDITION_FAILED
                        ? MutationOutcome.PREDECESSOR_UNCHANGED
                        : MutationOutcome.RESPONSE_UNKNOWN;
            }
            return attempt.failureKind() == MutationFailureClassifier.Kind.CONDITION_FAILED
                    ? MutationOutcome.DEFINITIVE_CONFLICT
                    : MutationOutcome.RESPONSE_UNKNOWN;
        });
    }

    private static VersionedValue map(AuthorityRecord value) {
        return VersionedValue.of(value.key(), value.storedBytes(), MetadataVersionMapper.fromOxia(value.versionId()));
    }

    private static String requireKey(String key) {
        Objects.requireNonNull(key, "key");
        if (key.isBlank()) {
            throw new IllegalArgumentException("metadata key must not be blank");
        }
        return key;
    }

    private static CanonicalBytes requireCandidate(CanonicalBytes candidate) {
        Objects.requireNonNull(candidate, "exactCandidate");
        if (candidate.isEmpty()) {
            throw new IllegalArgumentException("metadata candidate must not be empty");
        }
        return candidate;
    }

    private record Attempt(MutationFailureClassifier.Kind failureKind) {}
}
