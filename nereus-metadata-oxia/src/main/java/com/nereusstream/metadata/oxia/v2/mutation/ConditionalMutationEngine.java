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
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Resolves every dispatched conditional mutation through one exact same-key reread. */
public final class ConditionalMutationEngine {
    private final OxiaConditionalClient client;
    private final MutationFailureClassifier failureClassifier;

    public ConditionalMutationEngine(OxiaConditionalClient client, MutationFailureClassifier failureClassifier) {
        this.client = Objects.requireNonNull(client, "client");
        this.failureClassifier = Objects.requireNonNull(failureClassifier, "failureClassifier");
    }

    public <T> CompletionStage<CreateMutationResult<T>> create(
            String key, CanonicalBytes candidateBytes, ExactRecordResolver<T> resolver) {
        Objects.requireNonNull(resolver, "resolver");
        return mutationAttempt(() -> client.createIfAbsent(key, candidateBytes))
                .thenCompose(attempt -> reread(key).thenApply(read -> resolveCreate(attempt, read, resolver)));
    }

    public <T> CompletionStage<CreateMutationResult<T>> createUsingAcknowledgedSuccess(
            String key, CanonicalBytes candidateBytes, ExactRecordResolver<T> resolver) {
        Objects.requireNonNull(resolver, "resolver");
        return acknowledgedMutationAttempt(() -> client.createIfAbsentAcknowledged(key, candidateBytes))
                .thenCompose(attempt -> {
                    Optional<MutationAcknowledgement> acknowledgement = exactAcknowledgement(key, attempt);
                    if (acknowledgement.isPresent()) {
                        return CompletableFuture.completedFuture(resolveAcknowledgedCreate(
                                key, candidateBytes, acknowledgement.orElseThrow(), resolver));
                    }
                    return reread(key).thenApply(read -> resolveCreate(attempt, read, resolver));
                });
    }

    public <T> CompletionStage<ConditionalCasResult<T>> compareAndSet(
            String key, CanonicalBytes candidateBytes, long expectedVersionId, ExactRecordResolver<T> resolver) {
        Objects.requireNonNull(resolver, "resolver");
        return mutationAttempt(() -> client.compareAndSet(key, candidateBytes, expectedVersionId))
                .thenCompose(attempt -> reread(key).thenApply(read -> resolveCas(attempt, read, resolver)));
    }

    public <T> CompletionStage<ConditionalCasResult<T>> compareAndSetUsingAcknowledgedSuccess(
            String key, CanonicalBytes candidateBytes, long expectedVersionId, ExactRecordResolver<T> resolver) {
        Objects.requireNonNull(resolver, "resolver");
        return acknowledgedMutationAttempt(
                        () -> client.compareAndSetAcknowledged(key, candidateBytes, expectedVersionId))
                .thenCompose(attempt -> {
                    Optional<MutationAcknowledgement> acknowledgement = exactAcknowledgement(key, attempt);
                    if (acknowledgement.isPresent()) {
                        return CompletableFuture.completedFuture(
                                resolveAcknowledgedCas(key, candidateBytes, acknowledgement.orElseThrow(), resolver));
                    }
                    return reread(key).thenApply(read -> resolveCas(attempt, read, resolver));
                });
    }

    private CompletionStage<MutationAttempt> mutationAttempt(MutationDispatch dispatch) {
        CompletionStage<Void> dispatched;
        try {
            dispatched = Objects.requireNonNull(dispatch.run(), "conditional mutation stage");
        } catch (RuntimeException failure) {
            dispatched = CompletableFuture.failedFuture(failure);
        }
        return dispatched.handle((ignored, failure) -> {
            if (failure == null) {
                return MutationAttempt.success();
            }
            return MutationAttempt.failed(failureClassifier.classify(failure));
        });
    }

    private CompletionStage<MutationAttempt> acknowledgedMutationAttempt(AcknowledgedMutationDispatch dispatch) {
        CompletionStage<Optional<MutationAcknowledgement>> dispatched;
        try {
            dispatched = Objects.requireNonNull(dispatch.run(), "acknowledged conditional mutation stage");
        } catch (RuntimeException failure) {
            dispatched = CompletableFuture.failedFuture(failure);
        }
        return dispatched.handle((acknowledgement, failure) -> {
            if (failure == null) {
                return MutationAttempt.success(
                        Objects.requireNonNull(acknowledgement, "conditional mutation acknowledgement"));
            }
            return MutationAttempt.failed(failureClassifier.classify(failure));
        });
    }

    private static Optional<MutationAcknowledgement> exactAcknowledgement(String key, MutationAttempt attempt) {
        if (!attempt.succeeded()) {
            return Optional.empty();
        }
        return attempt.acknowledgement().filter(value -> value.key().equals(key));
    }

    private static <T> CreateMutationResult<T> resolveAcknowledgedCreate(
            String key,
            CanonicalBytes candidateBytes,
            MutationAcknowledgement acknowledgement,
            ExactRecordResolver<T> resolver) {
        Decoded<T> decoded = decode(
                ReadAttempt.completed(
                        Optional.of(new AuthorityRecord(key, candidateBytes, acknowledgement.versionId()))),
                resolver);
        return decoded.valid() && resolver.isCandidateExact(decoded.snapshot())
                ? CreateMutationResult.created(decoded.snapshot())
                : CreateMutationResult.indeterminate();
    }

    private static <T> ConditionalCasResult<T> resolveAcknowledgedCas(
            String key,
            CanonicalBytes candidateBytes,
            MutationAcknowledgement acknowledgement,
            ExactRecordResolver<T> resolver) {
        Decoded<T> decoded = decode(
                ReadAttempt.completed(
                        Optional.of(new AuthorityRecord(key, candidateBytes, acknowledgement.versionId()))),
                resolver);
        return decoded.valid() && resolver.isCandidateExact(decoded.snapshot())
                ? ConditionalCasResult.appliedExact(decoded.snapshot())
                : ConditionalCasResult.indeterminate();
    }

    private CompletionStage<ReadAttempt> reread(String key) {
        CompletionStage<Optional<AuthorityRecord>> read;
        try {
            read = Objects.requireNonNull(client.read(key), "authority reread stage");
        } catch (RuntimeException failure) {
            return CompletableFuture.completedFuture(ReadAttempt.failed());
        }
        return read.handle((record, failure) -> failure == null ? ReadAttempt.completed(record) : ReadAttempt.failed());
    }

    private static <T> CreateMutationResult<T> resolveCreate(
            MutationAttempt mutation, ReadAttempt read, ExactRecordResolver<T> resolver) {
        Decoded<T> decoded = decode(read, resolver);
        if (!decoded.valid()) {
            return CreateMutationResult.indeterminate();
        }
        T snapshot = decoded.snapshot();
        if (resolver.isCandidateExact(snapshot)) {
            return mutation.succeeded()
                    ? CreateMutationResult.created(snapshot)
                    : CreateMutationResult.existingExact(snapshot);
        }
        if (mutation.succeeded()) {
            return CreateMutationResult.indeterminate();
        }
        if (mutation.failureKind() == MutationFailureClassifier.Kind.RESPONSE_UNKNOWN) {
            return CreateMutationResult.indeterminate();
        }
        return CreateMutationResult.definitiveConflict();
    }

    private static <T> ConditionalCasResult<T> resolveCas(
            MutationAttempt mutation, ReadAttempt read, ExactRecordResolver<T> resolver) {
        Decoded<T> decoded = decode(read, resolver);
        if (!decoded.valid()) {
            return ConditionalCasResult.indeterminate();
        }
        T snapshot = decoded.snapshot();
        if (resolver.isCandidateExact(snapshot)) {
            return ConditionalCasResult.appliedExact(snapshot);
        }
        if (mutation.succeeded()) {
            return ConditionalCasResult.indeterminate();
        }
        if (resolver.isPredecessorExact(snapshot)) {
            return ConditionalCasResult.predecessorUnchanged(snapshot);
        }
        if (mutation.failureKind() == MutationFailureClassifier.Kind.RESPONSE_UNKNOWN) {
            return ConditionalCasResult.indeterminate();
        }
        return ConditionalCasResult.definitiveConflict();
    }

    private static <T> Decoded<T> decode(ReadAttempt read, ExactRecordResolver<T> resolver) {
        if (!read.completed() || read.record().isEmpty()) {
            return Decoded.invalid();
        }
        try {
            return Decoded.valid(
                    Objects.requireNonNull(resolver.decode(read.record().get()), "decoded snapshot"));
        } catch (RuntimeException failure) {
            return Decoded.invalid();
        }
    }

    @FunctionalInterface
    private interface MutationDispatch {
        CompletionStage<Void> run();
    }

    @FunctionalInterface
    private interface AcknowledgedMutationDispatch {
        CompletionStage<Optional<MutationAcknowledgement>> run();
    }

    private record MutationAttempt(
            boolean succeeded,
            MutationFailureClassifier.Kind failureKind,
            Optional<MutationAcknowledgement> acknowledgement) {
        private MutationAttempt {
            Objects.requireNonNull(acknowledgement, "acknowledgement");
        }

        private static MutationAttempt success() {
            return success(Optional.empty());
        }

        private static MutationAttempt success(Optional<MutationAcknowledgement> acknowledgement) {
            return new MutationAttempt(true, null, acknowledgement);
        }

        private static MutationAttempt failed(MutationFailureClassifier.Kind kind) {
            return new MutationAttempt(false, Objects.requireNonNull(kind, "kind"), Optional.empty());
        }
    }

    private record ReadAttempt(boolean completed, Optional<AuthorityRecord> record) {
        private ReadAttempt {
            Objects.requireNonNull(record, "record");
        }

        private static ReadAttempt completed(Optional<AuthorityRecord> record) {
            return new ReadAttempt(true, Objects.requireNonNull(record, "record"));
        }

        private static ReadAttempt failed() {
            return new ReadAttempt(false, Optional.empty());
        }
    }

    private record Decoded<T>(boolean valid, T snapshot) {
        private static <T> Decoded<T> valid(T snapshot) {
            return new Decoded<>(true, snapshot);
        }

        private static <T> Decoded<T> invalid() {
            return new Decoded<>(false, null);
        }
    }
}
