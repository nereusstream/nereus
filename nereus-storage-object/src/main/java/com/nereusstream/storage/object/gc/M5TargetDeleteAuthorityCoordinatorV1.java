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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.DeleteTerminalOutcomeV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterTicketV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Persists every M5-D authority transition with exact same-key CAS and authoritative reread.
 *
 * <p>This coordinator never calls {@code conditionalTransaction}; unsupported multi-key transactions remain
 * unsupported and are never emulated with sequential CAS.
 */
public final class M5TargetDeleteAuthorityCoordinatorV1 {
    public enum Outcome {
        APPLIED_EXACT,
        EXISTING_EXACT,
        EXISTING_TERMINAL,
        PREDECESSOR_UNCHANGED,
        DEFINITIVE_CONFLICT,
        RESPONSE_UNKNOWN,
        QUARANTINED
    }

    public record VersionedAuthorityV1(VersionedValue exactStoredValue, TargetDeleteAuthorityV1 authority) {
        public VersionedAuthorityV1 {
            Objects.requireNonNull(exactStoredValue, "exactStoredValue");
            Objects.requireNonNull(authority, "authority");
            if (!exactStoredValue.key().equals(authority.authorityKey())
                    || !exactStoredValue
                            .canonicalStoredBytes()
                            .equals(M5TargetDeleteAuthorityCodecV1.encodeAuthority(authority))) {
                throw new IllegalArgumentException("versioned authority differs from its exact stored value");
            }
        }
    }

    public record MutationResultV1(
            Outcome outcome, String authorityKey, CanonicalBytes exactCandidate, Optional<VersionedValue> observed) {
        public MutationResultV1 {
            Objects.requireNonNull(outcome, "outcome");
            M5TargetDeleteAuthorityRecordsV1.requireKey(authorityKey);
            Objects.requireNonNull(exactCandidate, "exactCandidate");
            if (exactCandidate.isEmpty()) {
                throw new IllegalArgumentException("exact candidate is empty");
            }
            observed = Objects.requireNonNull(observed, "observed");
            observed.ifPresent(value -> {
                if (!authorityKey.equals(value.key())) {
                    throw new IllegalArgumentException("observed authority key differs");
                }
            });
        }

        /** Permanent completion can survive compaction even though the full candidate is no longer resident. */
        public boolean exactTerminalIsAuthoritative() {
            if (outcome != Outcome.APPLIED_EXACT
                    && outcome != Outcome.EXISTING_EXACT
                    && outcome != Outcome.EXISTING_TERMINAL) {
                return false;
            }
            if (observed.isEmpty()) {
                return false;
            }
            if (isExactCompactedSuccessor(exactCandidate, observed.orElseThrow())) {
                return true;
            }
            if (!exactCandidateIsAuthoritative()) {
                return false;
            }
            return M5TargetDeleteStoredValueV2.decode(observed.orElseThrow()).state()
                    == TargetDeleteAuthorityStateV1.DELETE_DONE_V1;
        }

        public boolean exactCandidateIsAuthoritative() {
            return (outcome == Outcome.APPLIED_EXACT || outcome == Outcome.EXISTING_EXACT)
                    && observed.map(value -> value.canonicalStoredBytes().equals(exactCandidate))
                            .orElse(false);
        }
    }

    /** A rejected recovery remains failed even if its conservative veto could not be durably reconciled. */
    public static final class RecoveryRejectedException extends IllegalStateException {
        private final MutationResultV1 vetoResult;

        private RecoveryRejectedException(Throwable rejection, MutationResultV1 vetoResult) {
            super("READ_FENCED recovery rejected; veto persistence outcome: " + vetoResult.outcome(), rejection);
            this.vetoResult = vetoResult;
        }

        public MutationResultV1 vetoResult() {
            return vetoResult;
        }
    }

    private static final class AuthorityRejection extends IllegalStateException {
        private final DeleteRecoveryVetoV2.Reason reason;

        private AuthorityRejection(DeleteRecoveryVetoV2.Reason reason, Throwable failure) {
            super(reason.name(), failure);
            this.reason = reason;
        }
    }

    private record Attempt(MutationOutcome outcome, Throwable failure) {}

    private final ExactMetadataTransactionStoreV1 metadata;
    private final DeleteObservationAuthorityVerifierV2 observationAuthority;

    public M5TargetDeleteAuthorityCoordinatorV1(ExactMetadataTransactionStoreV1 metadata) {
        this(metadata, DeleteObservationAuthorityVerifierV2.unsupported());
    }

    public M5TargetDeleteAuthorityCoordinatorV1(
            ExactMetadataTransactionStoreV1 metadata, DeleteObservationAuthorityVerifierV2 observationAuthority) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.observationAuthority = Objects.requireNonNull(observationAuthority, "observationAuthority");
    }

    /** Active/full compatibility view. Use inspect to include permanent compact terminals. */
    public CompletionStage<Optional<VersionedAuthorityV1>> read(String authorityKey) {
        String key = M5TargetDeleteAuthorityRecordsV1.requireKey(authorityKey);
        return metadata.read(key).thenApply(observed -> observed.map(value -> exactAuthority(key, value)));
    }

    public CompletionStage<Optional<M5TargetDeleteStoredValueV2>> inspect(String authorityKey) {
        String key = M5TargetDeleteAuthorityRecordsV1.requireKey(authorityKey);
        return metadata.read(key)
                .thenApply(observed -> observed.map(value -> {
                    if (!key.equals(value.key())) {
                        throw new IllegalArgumentException("observed delete authority key differs");
                    }
                    return M5TargetDeleteStoredValueV2.decode(value);
                }));
    }

    /** One exact same-key CAS compacts an already permanent full done; no proof is inferred from a cache. */
    public CompletionStage<MutationResultV1> compactDone(VersionedValue exactFullDone) {
        var exact = exactAuthority(exactFullDone.key(), exactFullDone);
        var compact = M5TargetDeleteDoneV2.from(exact.authority());
        return mutate(compact.authorityKey(), Optional.of(exactFullDone), compact.encode(), false);
    }

    public CompletionStage<MutationResultV1> create(TargetDeleteAuthorityV1 initial) {
        Objects.requireNonNull(initial, "initial");
        if (initial.state() != TargetDeleteAuthorityStateV1.OPEN_V1
                || initial.authorityRevision() != 1
                || initial.predecessorAuthoritySha256().isPresent()) {
            throw new IllegalArgumentException("initial target authority must be revision-one OPEN_V1");
        }
        CanonicalBytes candidate = M5TargetDeleteAuthorityCodecV1.encodeAuthority(initial);
        CompletionStage<Void> admission = initial.eligibilitySnapshot()
                .map(this::requireFreshEligibility)
                .orElseGet(() -> CompletableFuture.completedFuture(null));
        return admission.thenCompose(ignored -> mutate(initial.authorityKey(), Optional.empty(), candidate, true));
    }

    public CompletionStage<MutationResultV1> acquireWriterTicket(
            VersionedValue exactOpenAuthority, ProofBoundWriterTicketV1 ticket) {
        VersionedAuthorityV1 exact = exactAuthority(exactOpenAuthority.key(), exactOpenAuthority);
        TargetDeleteAuthorityV1 candidate =
                M5TargetDeleteAuthorityStateMachineV1.acquireWriterTicket(exact.authority(), ticket);
        return mutate(
                exact.authority().authorityKey(),
                Optional.of(exact.exactStoredValue()),
                M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                false);
    }

    public CompletionStage<MutationResultV1> completeWriterTicket(
            VersionedValue exactOpenAuthority,
            Sha256Digest operationIdSha256,
            Sha256Digest reconciledProofSnapshotDigest) {
        VersionedAuthorityV1 exact = exactAuthority(exactOpenAuthority.key(), exactOpenAuthority);
        TargetDeleteAuthorityV1 candidate = M5TargetDeleteAuthorityStateMachineV1.completeWriterTicket(
                exact.authority(), operationIdSha256, reconciledProofSnapshotDigest);
        return mutate(
                exact.authority().authorityKey(),
                Optional.of(exact.exactStoredValue()),
                M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                false);
    }

    public CompletionStage<MutationResultV1> qualifyEligibility(
            VersionedValue exactOpenAuthority, DeleteEligibilitySnapshotV2 snapshot) {
        VersionedAuthorityV1 exact = exactAuthority(exactOpenAuthority.key(), exactOpenAuthority);
        TargetDeleteAuthorityV1 candidate =
                M5TargetDeleteAuthorityStateMachineV1.qualifyEligibility(exact.authority(), snapshot);
        return requireFreshEligibility(snapshot)
                .thenCompose(ignored -> mutate(
                        exact.authority().authorityKey(),
                        Optional.of(exact.exactStoredValue()),
                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                        false));
    }

    public CompletionStage<MutationResultV1> prepareIdentityRead(
            VersionedValue exactOpenAuthority, Sha256Digest attemptIdSha256, DeleteObservationContextV2 context) {
        VersionedAuthorityV1 exact = exactAuthority(exactOpenAuthority.key(), exactOpenAuthority);
        TargetDeleteAuthorityV1 candidate =
                M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(exact.authority(), attemptIdSha256, context);
        return requireObservationAuthority(candidate)
                .thenCompose(ignored -> mutate(
                        exact.authority().authorityKey(),
                        Optional.of(exact.exactStoredValue()),
                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                        false));
    }

    public CompletionStage<MutationResultV1> refreshIdentityRead(
            VersionedValue exactFencedAuthority,
            DeleteObservationContextV2 successor,
            DeleteEligibilitySnapshotV2 snapshot) {
        VersionedAuthorityV1 exact = exactAuthority(exactFencedAuthority.key(), exactFencedAuthority);
        TargetDeleteAuthorityV1 candidate =
                M5TargetDeleteAuthorityStateMachineV1.refreshIdentityRead(exact.authority(), successor, snapshot);
        DeleteObservationContextV2 previous =
                exact.authority().readFence().orElseThrow().observationContext();
        CompletionStage<Void> fenced = previous.coordinatorOwner().equals(successor.coordinatorOwner())
                ? CompletableFuture.completedFuture(null)
                : checkedAuthority(
                        () -> observationAuthority.requirePredecessorFenced(
                                exact.authority().target().resourceId(), previous, successor),
                        DeleteRecoveryVetoV2.Reason.PREDECESSOR_OWNER_AUTHORITY_REJECTED);
        return validateOrRecordRecoveryVeto(
                exact, candidate, successor, fenced.thenCompose(ignored -> requireObservationAuthority(candidate)));
    }

    public CompletionStage<MutationResultV1> bindDeleteIntent(
            VersionedValue exactFencedAuthority,
            ExactExternalIdentityV1 externalIdentity,
            Sha256Digest deleteAttemptIdSha256) {
        VersionedAuthorityV1 exact = exactAuthority(exactFencedAuthority.key(), exactFencedAuthority);
        TargetDeleteAuthorityV1 candidate = M5TargetDeleteAuthorityStateMachineV1.bindDeleteIntent(
                exact.authority(), externalIdentity, deleteAttemptIdSha256);
        return validateOrRecordRecoveryVeto(
                exact,
                candidate,
                exact.authority().readFence().orElseThrow().observationContext(),
                requireObservationAuthority(exact.authority()));
    }

    private CompletionStage<MutationResultV1> validateOrRecordRecoveryVeto(
            VersionedAuthorityV1 exact,
            TargetDeleteAuthorityV1 candidate,
            DeleteObservationContextV2 rejectedContext,
            CompletionStage<Void> validation) {
        // The exposed observer does not own either validation or the durable veto/CAS lifetime.
        return validation
                .<CompletionStage<MutationResultV1>>handle((ignored, failure) -> {
                    if (failure == null) {
                        return mutate(
                                candidate.authorityKey(),
                                Optional.of(exact.exactStoredValue()),
                                M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                                false);
                    }
                    Throwable rejection = failure;
                    while (!(rejection instanceof AuthorityRejection) && rejection.getCause() != null) {
                        rejection = rejection.getCause();
                    }
                    DeleteRecoveryVetoV2.Reason reason = rejection instanceof AuthorityRejection authorityRejection
                            ? authorityRejection.reason
                            : DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED;
                    return recordRecoveryVeto(exact, reason, rejectedContext)
                            .thenCompose(vetoResult ->
                                    CompletableFuture.failedFuture(new RecoveryRejectedException(failure, vetoResult)));
                })
                .thenCompose(stage -> stage);
    }

    /**
     * Records conservative proof-collection failure even when no complete eligibility snapshot can be built.
     * This diagnostic grants no owner/eligibility authority and can only retain a closed READ_FENCED resource.
     */
    public CompletionStage<MutationResultV1> recordRecoveryVeto(
            VersionedValue exactFencedAuthority,
            DeleteRecoveryVetoV2.Reason reason,
            DeleteObservationContextV2 rejectedContext) {
        return recordRecoveryVeto(
                exactAuthority(exactFencedAuthority.key(), exactFencedAuthority), reason, rejectedContext);
    }

    private CompletionStage<MutationResultV1> recordRecoveryVeto(
            VersionedAuthorityV1 exact,
            DeleteRecoveryVetoV2.Reason reason,
            DeleteObservationContextV2 rejectedContext) {
        var current = exact.authority();
        var contextSha = M5TargetDeleteAuthorityCodecV1.observationContextSha256(rejectedContext);
        if (current.recoveryVeto()
                .filter(veto -> veto.reason() == reason
                        && veto.rejectedObservationEpoch() == rejectedContext.observationEpoch()
                        && veto.rejectedContextSha256().equals(contextSha))
                .isPresent()) {
            // Repeated rejection never grows a history or consumes a new authority revision.
            return reconcile(
                    current.authorityKey(),
                    Optional.of(exact.exactStoredValue()),
                    exact.exactStoredValue().canonicalStoredBytes(),
                    false,
                    new Attempt(MutationOutcome.RESPONSE_UNKNOWN, null));
        }
        var vetoed = M5TargetDeleteAuthorityStateMachineV1.recordRecoveryVeto(current, reason, rejectedContext);
        return mutate(
                current.authorityKey(),
                Optional.of(exact.exactStoredValue()),
                M5TargetDeleteAuthorityCodecV1.encodeAuthority(vetoed),
                false);
    }

    public CompletionStage<MutationResultV1> takeOverDispatch(
            VersionedValue exactIntentAuthority,
            long nextDispatchEpoch,
            Sha256Digest nextDispatchOwnerFenceSha256,
            Sha256Digest oldOwnerFencedProofSha256) {
        VersionedAuthorityV1 exact = exactAuthority(exactIntentAuthority.key(), exactIntentAuthority);
        TargetDeleteAuthorityV1 candidate = M5TargetDeleteAuthorityStateMachineV1.takeOverDispatch(
                exact.authority(), nextDispatchEpoch, nextDispatchOwnerFenceSha256, oldOwnerFencedProofSha256);
        return mutate(
                exact.authority().authorityKey(),
                Optional.of(exact.exactStoredValue()),
                M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                false);
    }

    public CompletionStage<MutationResultV1> completeDelete(
            VersionedValue exactIntentAuthority,
            DeleteTerminalOutcomeV1 terminalOutcome,
            Sha256Digest absenceInventoryRootSha256,
            Sha256Digest completionProofDigestSha256) {
        VersionedAuthorityV1 exact = exactAuthority(exactIntentAuthority.key(), exactIntentAuthority);
        TargetDeleteAuthorityV1 candidate = M5TargetDeleteAuthorityStateMachineV1.completeDelete(
                exact.authority(), terminalOutcome, absenceInventoryRootSha256, completionProofDigestSha256);
        return mutate(
                exact.authority().authorityKey(),
                Optional.of(exact.exactStoredValue()),
                M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                false);
    }

    private VersionedAuthorityV1 exactAuthority(String authorityKey, VersionedValue exactStoredValue) {
        Objects.requireNonNull(exactStoredValue, "exactStoredValue");
        if (!authorityKey.equals(exactStoredValue.key())) {
            throw new IllegalArgumentException("exact authority key differs");
        }
        return new VersionedAuthorityV1(
                exactStoredValue,
                M5TargetDeleteAuthorityCodecV1.decodeAuthority(exactStoredValue.canonicalStoredBytes()));
    }

    private CompletionStage<Void> requireFreshEligibility(DeleteEligibilitySnapshotV2 snapshot) {
        return requireFreshFacts(snapshot.authorityFacts());
    }

    private CompletionStage<Void> requireObservationAuthority(TargetDeleteAuthorityV1 authority) {
        DeleteObservationContextV2 context = authority.readFence().orElseThrow().observationContext();
        java.util.Map<String, AuthorityFactV1> facts = new java.util.TreeMap<>();
        for (AuthorityFactV1 fact :
                authority.eligibilitySnapshot().orElseThrow().authorityFacts()) {
            facts.put(fact.key(), fact);
        }
        for (AuthorityFactV1 fact : context.authorityFacts()) {
            AuthorityFactV1 previous = facts.putIfAbsent(fact.key(), fact);
            if (previous != null && !previous.equals(fact)) {
                return CompletableFuture.failedFuture(new AuthorityRejection(
                        DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED,
                        new IllegalArgumentException("observation and eligibility authority conflict")));
            }
        }
        return checkedAuthority(
                        () -> observationAuthority.requireCurrent(
                                authority.target().resourceId(), context),
                        DeleteRecoveryVetoV2.Reason.CURRENT_OBSERVATION_AUTHORITY_REJECTED)
                .thenCompose(ignored -> checkedAuthority(
                        () -> requireFreshFacts(List.copyOf(facts.values())),
                        DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED));
    }

    private static CompletionStage<Void> checkedAuthority(
            Supplier<CompletionStage<Void>> operation, DeleteRecoveryVetoV2.Reason reason) {
        try {
            return Objects.requireNonNull(operation.get(), "authority validation stage")
                    .handle((ignored, failure) -> {
                        if (failure != null) {
                            throw new AuthorityRejection(reason, failure);
                        }
                        return null;
                    });
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(new AuthorityRejection(reason, failure));
        }
    }

    private CompletionStage<Void> requireFreshFacts(List<AuthorityFactV1> facts) {
        CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
        for (var fact : facts) {
            stage = stage.thenCompose(ignored -> metadata.read(fact.key()).thenAccept(observed -> {
                VersionedValue value = observed.orElseThrow(
                        () -> new IllegalStateException("eligibility authority is absent: " + fact.key()));
                if (!value.metadataVersion().equals(fact.metadataVersion())
                        || !value.canonicalStoredSha256().equals(fact.valueSha256())) {
                    throw new IllegalStateException("eligibility authority changed: " + fact.key());
                }
            }));
        }
        return stage;
    }

    private static boolean isExactCompactedSuccessor(CanonicalBytes fullCandidate, VersionedValue observed) {
        if (!M5TargetDeleteDoneV2.isCompactDone(observed.canonicalStoredBytes())
                || M5TargetDeleteDoneV2.isCompactDone(fullCandidate)) {
            return false;
        }
        try {
            var full = M5TargetDeleteAuthorityCodecV1.decodeAuthority(fullCandidate);
            if (full.state() != TargetDeleteAuthorityStateV1.DELETE_DONE_V1) {
                return false;
            }
            var compact = M5TargetDeleteDoneV2.from(full);
            return compact.authorityKey().equals(observed.key())
                    && compact.encode().equals(observed.canonicalStoredBytes());
        } catch (IllegalArgumentException | ArithmeticException invalid) {
            return false;
        }
    }

    private CompletionStage<MutationResultV1> mutate(
            String authorityKey, Optional<VersionedValue> predecessor, CanonicalBytes candidate, boolean create) {
        CompletionStage<MutationOutcome> mutation;
        try {
            mutation = metadata.compareAndSet(predecessor, authorityKey, candidate);
        } catch (Throwable failure) {
            return reconcile(authorityKey, predecessor, candidate, create, new Attempt(null, failure));
        }
        return mutation.handle((outcome, failure) -> new Attempt(outcome, failure))
                .thenCompose(attempt -> reconcile(authorityKey, predecessor, candidate, create, attempt));
    }

    private CompletionStage<MutationResultV1> reconcile(
            String authorityKey,
            Optional<VersionedValue> predecessor,
            CanonicalBytes candidate,
            boolean create,
            Attempt attempt) {
        CompletionStage<Optional<VersionedValue>> read;
        try {
            read = metadata.read(authorityKey);
        } catch (Throwable failure) {
            return CompletableFuture.completedFuture(
                    new MutationResultV1(Outcome.RESPONSE_UNKNOWN, authorityKey, candidate, Optional.empty()));
        }
        return read.handle((observed, readFailure) -> {
            if (readFailure != null || observed == null) {
                return new MutationResultV1(Outcome.RESPONSE_UNKNOWN, authorityKey, candidate, Optional.empty());
            }
            if (observed.map(value -> value.canonicalStoredBytes().equals(candidate))
                    .orElse(false)) {
                Outcome exactOutcome = attempt.failure() == null && attempt.outcome() == MutationOutcome.APPLIED_EXACT
                        ? Outcome.APPLIED_EXACT
                        : Outcome.EXISTING_EXACT;
                return new MutationResultV1(exactOutcome, authorityKey, candidate, observed);
            }
            if (observed.isPresent() && isExactCompactedSuccessor(candidate, observed.orElseThrow())) {
                return new MutationResultV1(Outcome.EXISTING_TERMINAL, authorityKey, candidate, observed);
            }
            if (observed.equals(predecessor)) {
                Outcome predecessorOutcome =
                        attempt.failure() == null && attempt.outcome() == MutationOutcome.APPLIED_EXACT
                                ? Outcome.RESPONSE_UNKNOWN
                                : Outcome.PREDECESSOR_UNCHANGED;
                return new MutationResultV1(predecessorOutcome, authorityKey, candidate, observed);
            }
            if (create && observed.isPresent()) {
                try {
                    var existing = M5TargetDeleteStoredValueV2.decode(observed.orElseThrow());
                    TargetDeleteAuthorityV1 requested = M5TargetDeleteAuthorityCodecV1.decodeAuthority(candidate);
                    // Rediscovery never reopens a compact done after cache eviction or coordinator restart.
                    Outcome outcome =
                            existing.resource().equals(requested.target().resourceId())
                                    ? Outcome.DEFINITIVE_CONFLICT
                                    : Outcome.QUARANTINED;
                    return new MutationResultV1(outcome, authorityKey, candidate, observed);
                } catch (IllegalArgumentException failure) {
                    return new MutationResultV1(Outcome.QUARANTINED, authorityKey, candidate, observed);
                }
            }
            if (!create && observed.isEmpty()) {
                return new MutationResultV1(Outcome.QUARANTINED, authorityKey, candidate, observed);
            }
            return new MutationResultV1(Outcome.DEFINITIVE_CONFLICT, authorityKey, candidate, observed);
        });
    }
}
