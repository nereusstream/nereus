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

package com.nereusstream.metadata.spi.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Exact-value metadata port for M5 retention and retirement.
 *
 * <p>The transaction method is deliberately distinct from one-key CAS. An implementation must not
 * emulate it with sequential mutations: {@link TransactionOutcome#UNSUPPORTED} is the only valid
 * result when the backend cannot atomically validate every condition and apply every put.
 */
public interface ExactMetadataTransactionStoreV1 {
    int MAX_TRANSACTION_KEYS = 2_048;
    int MAX_KEY_BYTES = 1_024;
    int MAX_VALUE_BYTES = 1_048_576;

    enum MutationOutcome {
        APPLIED_EXACT,
        PREDECESSOR_UNCHANGED,
        DEFINITIVE_CONFLICT,
        RESPONSE_UNKNOWN
    }

    enum TransactionOutcome {
        APPLIED_EXACT,
        CONDITIONS_UNCHANGED,
        DEFINITIVE_CONFLICT,
        RESPONSE_UNKNOWN,
        UNSUPPORTED
    }

    /** One exact authoritative value; absence is represented outside this record. */
    record VersionedValue(
            String key,
            CanonicalBytes canonicalStoredBytes,
            Sha256Digest canonicalStoredSha256,
            MetadataVersion metadataVersion) {
        public VersionedValue {
            key = requireKey(key);
            Objects.requireNonNull(canonicalStoredBytes, "canonicalStoredBytes");
            Objects.requireNonNull(canonicalStoredSha256, "canonicalStoredSha256");
            Objects.requireNonNull(metadataVersion, "metadataVersion");
            if (canonicalStoredBytes.isEmpty() || canonicalStoredBytes.length() > MAX_VALUE_BYTES) {
                throw new IllegalArgumentException("metadata value length is outside the M5 hard cap");
            }
            if (!Sha256Digest.hash(canonicalStoredBytes).equals(canonicalStoredSha256)) {
                throw new IllegalArgumentException("metadata value SHA-256 differs from its exact bytes");
            }
            if (metadataVersion.value().isEmpty()) {
                throw new IllegalArgumentException("metadata version is empty");
            }
        }

        public static VersionedValue of(String key, CanonicalBytes bytes, MetadataVersion version) {
            return new VersionedValue(key, bytes, Sha256Digest.hash(bytes), version);
        }
    }

    /** Exact present or absent transaction predicate. */
    record ExactCondition(String key, Optional<VersionedValue> expected) {
        public ExactCondition {
            key = requireKey(key);
            expected = Objects.requireNonNull(expected, "expected");
            if (expected.isPresent() && !key.equals(expected.orElseThrow().key())) {
                throw new IllegalArgumentException("condition key differs from its expected value key");
            }
        }

        public static ExactCondition absent(String key) {
            return new ExactCondition(key, Optional.empty());
        }

        public static ExactCondition present(VersionedValue expected) {
            Objects.requireNonNull(expected, "expected");
            return new ExactCondition(expected.key(), Optional.of(expected));
        }
    }

    /** Candidate for a key that also appears exactly once in the condition set. */
    record ExactPut(String key, CanonicalBytes canonicalCandidate, Sha256Digest candidateSha256) {
        public ExactPut {
            key = requireKey(key);
            Objects.requireNonNull(canonicalCandidate, "canonicalCandidate");
            Objects.requireNonNull(candidateSha256, "candidateSha256");
            if (canonicalCandidate.isEmpty() || canonicalCandidate.length() > MAX_VALUE_BYTES) {
                throw new IllegalArgumentException("transaction candidate length is outside the M5 hard cap");
            }
            if (!Sha256Digest.hash(canonicalCandidate).equals(candidateSha256)) {
                throw new IllegalArgumentException("transaction candidate SHA-256 differs from its exact bytes");
            }
        }

        public static ExactPut of(String key, CanonicalBytes candidate) {
            return new ExactPut(key, candidate, Sha256Digest.hash(candidate));
        }
    }

    /** One all-or-nothing conditional mutation, routed by an immutable backend partition key. */
    record ExactTransaction(String partitionKey, List<ExactCondition> conditions, List<ExactPut> puts) {
        public ExactTransaction {
            partitionKey = requireKey(partitionKey);
            conditions = List.copyOf(Objects.requireNonNull(conditions, "conditions"));
            puts = List.copyOf(Objects.requireNonNull(puts, "puts"));
            if (conditions.isEmpty()
                    || puts.isEmpty()
                    || conditions.size() > MAX_TRANSACTION_KEYS
                    || puts.size() > MAX_TRANSACTION_KEYS) {
                throw new IllegalArgumentException("metadata transaction count is outside the M5 hard cap");
            }
            requireSortedUniqueConditions(conditions);
            requireSortedUniquePuts(puts);
            Set<String> conditionKeys = new HashSet<>();
            conditions.forEach(condition -> conditionKeys.add(condition.key()));
            if (puts.stream().anyMatch(put -> !conditionKeys.contains(put.key()))) {
                throw new IllegalArgumentException("every transaction put must have one exact condition");
            }
        }
    }

    CompletionStage<Optional<VersionedValue>> read(String key);

    CompletionStage<MutationOutcome> compareAndSet(
            Optional<VersionedValue> exactPredecessor, String key, CanonicalBytes exactCandidate);

    CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction);

    /** True only when {@link #conditionalTransaction(ExactTransaction)} is an actual atomic backend primitive. */
    boolean supportsAtomicMultiKeyTransactions();

    private static String requireKey(String key) {
        Objects.requireNonNull(key, "key");
        if (key.isBlank() || key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("metadata key is blank or exceeds the M5 hard cap");
        }
        return key;
    }

    private static void requireSortedUniqueConditions(List<ExactCondition> conditions) {
        List<String> keys = conditions.stream().map(ExactCondition::key).toList();
        if (!keys.equals(keys.stream().sorted().toList())
                || keys.stream().distinct().count() != keys.size()) {
            throw new IllegalArgumentException("transaction conditions are not sorted unique");
        }
    }

    private static void requireSortedUniquePuts(List<ExactPut> puts) {
        List<String> keys = puts.stream().map(ExactPut::key).toList();
        if (!keys.equals(keys.stream().sorted(Comparator.naturalOrder()).toList())
                || keys.stream().distinct().count() != keys.size()) {
            throw new IllegalArgumentException("transaction puts are not sorted unique");
        }
    }
}
