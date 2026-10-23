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
    private final DeleteExternalIdentityReaderV2 externalReader;

    public M5TargetDeleteAuthorityCoordinatorV1(ExactMetadataTransactionStoreV1 metadata) {
        this(metadata, DeleteObservationAuthorityVerifierV2.unsupported());
    }

    public M5TargetDeleteAuthorityCoordinatorV1(
            ExactMetadataTransactionStoreV1 metadata, DeleteObservationAuthorityVerifierV2 observationAuthority) {
        this(metadata, observationAuthority, DeleteExternalIdentityReaderV2.unsupported());
    }

    public M5TargetDeleteAuthorityCoordinatorV1(
            ExactMetadataTransactionStoreV1 metadata,
            DeleteObservationAuthorityVerifierV2 observationAuthority,
            DeleteExternalIdentityReaderV2 externalReader) {
        this.externalReader = Objects.requireNonNull(externalReader, "externalReader");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.observationAuthority = Objects.requireNonNull(observationAuthority, "observationAuthority");
    }

    /**
     * Owns one exact mark candidate and its in-process monotonic clock. The caller retains this original session
     * across response uncertainty; reconstructing a session selects another domain and must wait from the start.
     * This component proves elapsed grace only, never completeness of the caller's initial observation or rescan.
     */
    public final class EpochGraceSession {
        private final TargetDeleteAuthorityV1 candidate;
        private final VersionedValue predecessor;
        private final DeleteEpochGraceMarkV1 mark;
        private boolean started;
        private boolean invalidated;
        private long confirmedTick;
        private VersionedValue confirmedMark;

        private EpochGraceSession(VersionedValue predecessor, DeleteEpochGraceMarkV1 mark) {
            this.predecessor = predecessor;
            this.mark = mark;
            var current = exactAuthority(predecessor.key(), predecessor).authority();
            candidate = switch (current.state()) {
                case READ_FENCED_V1 -> M5TargetDeleteAuthorityStateMachineV1.renewReadEpochGrace(current, mark);
                case DELETE_INTENT_V1 -> M5TargetDeleteAuthorityStateMachineV1.renewIntentEpochGrace(current, mark);
                default -> M5TargetDeleteAuthorityStateMachineV1.markEpochGrace(current, mark);
            };
        }

        public DeleteEpochGraceMarkV1 mark() {
            return mark;
        }

        /** A cancelled observer cannot cancel the original CAS/reconciliation or start the clock early. */
        public CompletionStage<Void> confirmMark() {
            CompletionStage<Void> original = observationAuthority
                    .requireCurrent(candidate.target().resourceId(), mark.authority())
                    .thenCompose(ignored -> requireRenewalPredecessor())
                    .thenCompose(ignored -> mutate(
                            candidate.authorityKey(),
                            Optional.of(predecessor),
                            M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                            false))
                    .thenCompose(result -> {
                        if (!result.exactCandidateIsAuthoritative()) {
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("epoch grace mark is not exactly confirmed"));
                        }
                        return requireMarkCurrent().thenAccept(actual -> {
                            if (!result.observed().equals(Optional.of(actual))) {
                                throw new IllegalStateException("epoch grace mark changed before clock confirmation");
                            }
                            startConfirmedClock(actual);
                        });
                    });
            return original.thenApply(ignored -> null);
        }

        private CompletionStage<Void> requireRenewalPredecessor() {
            if (candidate.state() == TargetDeleteAuthorityStateV1.OPEN_V1) {
                return CompletableFuture.completedFuture(null);
            }
            var previous = candidate.state() == TargetDeleteAuthorityStateV1.DELETE_INTENT_V1
                    ? M5TargetDeleteAuthorityStateMachineV1.dispatchContext(candidate)
                    : candidate.readFence().orElseThrow().observationContext();
            return previous.coordinatorOwner().equals(mark.authority().coordinatorOwner())
                    ? CompletableFuture.completedFuture(null)
                    : observationAuthority.requirePredecessorFenced(
                            candidate.target().resourceId(), previous, mark.authority());
        }

        private synchronized void startConfirmedClock(VersionedValue actual) {
            if (invalidated) {
                throw new IllegalStateException("epoch grace clock is invalidated");
            }
            if (started && !actual.equals(confirmedMark)) {
                throw new IllegalStateException("epoch grace confirmed authority changed");
            }
            if (!started) {
                confirmedMark = actual;
                confirmedTick = System.nanoTime();
                started = true;
            }
        }

        private synchronized void requireConfirmedMark(VersionedValue actual) {
            if (started && !actual.equals(confirmedMark)) {
                throw new IllegalStateException("epoch grace confirmed authority changed");
            }
            if (!started
                    && !actual.canonicalStoredBytes()
                            .equals(M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate))) {
                throw new IllegalStateException("epoch grace original candidate changed");
            }
        }

        private synchronized void requireElapsedClock() {
            if (!started || invalidated) {
                throw new IllegalStateException("epoch grace has no confirmed local clock");
            }
            long elapsed = System.nanoTime() - confirmedTick;
            if (elapsed < 0) {
                invalidated = true;
                throw new IllegalStateException("epoch grace monotonic clock is uncertain");
            }
            if (elapsed < Math.multiplyExact(mark.durationMillis(), 1_000_000)) {
                throw new IllegalStateException("epoch grace duration has not elapsed");
            }
        }

        private CompletionStage<VersionedValue> requireMarkCurrent() {
            return observationAuthority
                    .requireCurrent(candidate.target().resourceId(), mark.authority())
                    .thenCompose(ignored -> metadata.read(candidate.authorityKey()))
                    .thenApply(observed -> {
                        var stored = observed.orElseThrow(
                                () -> new IllegalStateException("epoch grace authority is absent"));
                        var current = exactAuthority(stored.key(), stored).authority();
                        if (!current.target().equals(candidate.target())
                                || !current.epochGrace().equals(Optional.of(mark))) {
                            throw new IllegalStateException("epoch grace mark was changed or invalidated by a writer");
                        }
                        requireConfirmedMark(stored);
                        return stored;
                    })
                    .thenCompose(stored -> observationAuthority
                            .requireCurrent(candidate.target().resourceId(), mark.authority())
                            .thenApply(ignored -> stored));
        }

        private CompletionStage<Void> requireNativeClockCurrent() {
            return observationAuthority
                    .requireCurrent(candidate.target().resourceId(), mark.authority())
                    .thenRun(this::requireElapsedClock);
        }

        /** Readiness is issued by this original producer after actual elapsed time and current durable fencing. */
        public CompletionStage<EpochGraceReady> requireElapsed() {
            CompletionStage<EpochGraceReady> original = requireMarkCurrent().thenApply(ignored -> {
                requireElapsedClock();
                return new EpochGraceReady(this);
            });
            return original.thenApply(value -> value);
        }
    }

    /** Opaque local qualification; a numeric deadline or a reconstructed persisted mark cannot instantiate it. */
    public static final class EpochGraceReady {
        private final EpochGraceSession session;

        private EpochGraceReady(EpochGraceSession session) {
            this.session = session;
        }

        public DeleteEpochGraceMarkV1 mark() {
            return session.mark();
        }

        public CompletionStage<Void> requireCurrent() {
            return session.requireElapsed().thenApply(ignored -> null);
        }

        /** After qualification the target has a new version; its exact mark and native clock must still match. */
        public CompletionStage<Void> requireAdmittedCurrent(
                com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2 resource, DeleteEpochGraceMarkV1 expected) {
            if (!session.candidate.target().resourceId().equals(resource) || !mark().equals(expected)) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException("elapsed readiness belongs to another resource or mark"));
            }
            return session.requireNativeClockCurrent();
        }
    }

    public EpochGraceSession prepareEpochGrace(
            VersionedValue exactOpenAuthority,
            com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceCapture initial,
            long admittedDurationMillis) {
        return prepareEpochGrace(
                exactOpenAuthority, initial.epochAuthority(), admittedDurationMillis, initial.rootSha256());
    }

    public EpochGraceSession prepareEpochGrace(
            VersionedValue exactOpenAuthority,
            DeleteObservationContextV2 authority,
            long admittedDurationMillis,
            Sha256Digest completeInitialObservationRoot) {
        var mark = new DeleteEpochGraceMarkV1(
                java.util.UUID.randomUUID(), authority, admittedDurationMillis, completeInitialObservationRoot);
        return new EpochGraceSession(exactOpenAuthority, mark);
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
        requireLegacyGrace(initial, initial.eligibilitySnapshot());
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
        requireLegacyGrace(exact.authority(), Optional.of(snapshot));
        TargetDeleteAuthorityV1 candidate =
                M5TargetDeleteAuthorityStateMachineV1.qualifyEligibility(exact.authority(), snapshot);
        return requireFreshEligibility(snapshot)
                .thenCompose(ignored -> mutate(
                        exact.authority().authorityKey(),
                        Optional.of(exact.exactStoredValue()),
                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                        false));
    }

    private static void requireLegacyGrace(
            TargetDeleteAuthorityV1 authority, Optional<DeleteEligibilitySnapshotV2> snapshot) {
        if (authority.epochGrace().isPresent()
                || snapshot.stream()
                        .flatMap(value -> value.members().stream())
                        .anyMatch(member ->
                                member.physicalReferences().epochGrace().isPresent())) {
            throw new IllegalArgumentException("epoch grace requires original producer readiness");
        }
    }

    private static void requireEpochProof(
            TargetDeleteAuthorityV1 authority, DeleteEligibilitySnapshotV2 snapshot, EpochGraceReady ready) {
        var mark = authority
                .epochGrace()
                .orElseThrow(() -> new IllegalArgumentException("target lacks the original epoch grace mark"));
        if (!authority
                        .target()
                        .resourceId()
                        .equals(ready.session.candidate.target().resourceId())
                || !mark.equals(ready.mark())
                || snapshot.members().stream()
                        .anyMatch(member ->
                                !member.physicalReferences().epochGrace().equals(Optional.of(mark)))) {
            throw new IllegalArgumentException("eligibility has another epoch grace qualification");
        }
    }

    public CompletionStage<MutationResultV1> qualifyEligibility(
            VersionedValue exactOpenAuthority, DeleteEligibilitySnapshotV2 snapshot, EpochGraceReady ready) {
        var exact = exactAuthority(exactOpenAuthority.key(), exactOpenAuthority);
        requireEpochProof(exact.authority(), snapshot, ready);
        var candidate = M5TargetDeleteAuthorityStateMachineV1.qualifyEligibility(exact.authority(), snapshot);
        var encoded = M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate);
        return metadata.read(exactOpenAuthority.key()).thenCompose(current -> {
            boolean alreadyApplied = current.map(
                            value -> value.canonicalStoredBytes().equals(encoded))
                    .orElse(false);
            var elapsed = alreadyApplied
                    ? ready.requireAdmittedCurrent(candidate.target().resourceId(), ready.mark())
                    : ready.requireCurrent();
            return elapsed.thenCompose(ignored -> requireFreshEligibility(snapshot))
                    .thenCompose(ignored ->
                            mutate(candidate.authorityKey(), Optional.of(exactOpenAuthority), encoded, false));
        });
    }

    public CompletionStage<MutationResultV1> prepareIdentityRead(
            VersionedValue exactOpenAuthority, Sha256Digest attemptIdSha256, DeleteObservationContextV2 context) {
        VersionedAuthorityV1 exact = exactAuthority(exactOpenAuthority.key(), exactOpenAuthority);
        requireLegacyGrace(exact.authority(), exact.authority().eligibilitySnapshot());
        TargetDeleteAuthorityV1 candidate =
                M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(exact.authority(), attemptIdSha256, context);
        return requireObservationAuthority(candidate)
                .thenCompose(ignored -> mutate(
                        exact.authority().authorityKey(),
                        Optional.of(exact.exactStoredValue()),
                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                        false));
    }

    public CompletionStage<MutationResultV1> prepareIdentityRead(
            VersionedValue exactOpenAuthority,
            Sha256Digest attemptIdSha256,
            DeleteObservationContextV2 context,
            EpochGraceReady ready) {
        var exact = exactAuthority(exactOpenAuthority.key(), exactOpenAuthority);
        var snapshot = exact.authority().eligibilitySnapshot().orElseThrow();
        requireEpochProof(exact.authority(), snapshot, ready);
        if (!context.equals(ready.mark().authority())) {
            throw new IllegalArgumentException("read fence has another epoch grace owner");
        }
        var candidate =
                M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(exact.authority(), attemptIdSha256, context);
        return ready.requireAdmittedCurrent(candidate.target().resourceId(), ready.mark())
                .thenCompose(ignored -> requireFreshEligibility(snapshot))
                .thenCompose(ignored -> requireObservationAuthority(candidate))
                .thenCompose(ignored -> mutate(
                        candidate.authorityKey(),
                        Optional.of(exactOpenAuthority),
                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                        false));
    }

    public CompletionStage<MutationResultV1> refreshIdentityRead(
            VersionedValue exactFencedAuthority,
            DeleteObservationContextV2 successor,
            DeleteEligibilitySnapshotV2 snapshot) {
        VersionedAuthorityV1 exact = exactAuthority(exactFencedAuthority.key(), exactFencedAuthority);
        requireLegacyGrace(exact.authority(), Optional.of(snapshot));
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

    /** Completes a renewed closed read only with the original timer and a fully rescanned matching proof. */
    public CompletionStage<MutationResultV1> refreshIdentityRead(
            VersionedValue exactFencedAuthority,
            DeleteObservationContextV2 successor,
            DeleteEligibilitySnapshotV2 snapshot,
            EpochGraceReady ready) {
        var exact = exactAuthority(exactFencedAuthority.key(), exactFencedAuthority);
        requireEpochProof(exact.authority(), snapshot, ready);
        if (!successor.equals(ready.mark().authority())
                || !exact.authority()
                        .recoveryVeto()
                        .filter(veto -> veto.reason() == DeleteRecoveryVetoV2.Reason.EPOCH_GRACE_RENEWAL_PENDING)
                        .isPresent()) {
            throw new IllegalArgumentException("read refresh requires its exact pending epoch renewal");
        }
        var candidate =
                M5TargetDeleteAuthorityStateMachineV1.refreshIdentityRead(exact.authority(), successor, snapshot);
        var previous = exact.authority().readFence().orElseThrow().observationContext();
        // Native readiness and predecessor checks may share the same original SDK admission. Start the
        // predecessor check only after readiness completes, retaining both checks and their exact lifetimes.
        Supplier<CompletionStage<Void>> fenced =
                () -> previous.coordinatorOwner().equals(successor.coordinatorOwner())
                        ? CompletableFuture.completedFuture(null)
                        : checkedAuthority(
                                () -> observationAuthority.requirePredecessorFenced(
                                        exact.authority().target().resourceId(), previous, successor),
                                DeleteRecoveryVetoV2.Reason.PREDECESSOR_OWNER_AUTHORITY_REJECTED);
        return ready.requireCurrent()
                .thenCompose(ignored -> validateOrRecordRecoveryVeto(
                        exact,
                        candidate,
                        successor,
                        fenced.get().thenCompose(unused -> requireObservationAuthority(candidate))));
    }

    public CompletionStage<MutationResultV1> bindDeleteIntent(
            VersionedValue exactFencedAuthority,
            ExactExternalIdentityV1 externalIdentity,
            Sha256Digest deleteAttemptIdSha256) {
        VersionedAuthorityV1 exact = exactAuthority(exactFencedAuthority.key(), exactFencedAuthority);
        requireLegacyGrace(exact.authority(), exact.authority().eligibilitySnapshot());
        TargetDeleteAuthorityV1 candidate = M5TargetDeleteAuthorityStateMachineV1.bindDeleteIntent(
                exact.authority(), externalIdentity, deleteAttemptIdSha256);
        return validateOrRecordRecoveryVeto(
                exact,
                candidate,
                exact.authority().readFence().orElseThrow().observationContext(),
                requireObservationAuthority(exact.authority()));
    }

    public CompletionStage<MutationResultV1> bindDeleteIntent(
            VersionedValue exactFencedAuthority,
            ExactExternalIdentityV1 externalIdentity,
            Sha256Digest deleteAttemptIdSha256,
            EpochGraceReady ready) {
        var exact = exactAuthority(exactFencedAuthority.key(), exactFencedAuthority);
        requireEpochProof(
                exact.authority(), exact.authority().eligibilitySnapshot().orElseThrow(), ready);
        var candidate = M5TargetDeleteAuthorityStateMachineV1.bindDeleteIntent(
                exact.authority(), externalIdentity, deleteAttemptIdSha256);
        return ready.requireAdmittedCurrent(candidate.target().resourceId(), ready.mark())
                .thenCompose(ignored -> validateOrRecordRecoveryVeto(
                        exact,
                        candidate,
                        exact.authority().readFence().orElseThrow().observationContext(),
                        requireObservationAuthority(exact.authority())));
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
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "hash-only dispatch takeover requires typed context, current capability and native identity reread"));
    }

    /** Refreshes an INTENT at the same permanent key after fencing/revalidating all native inputs. */
    public CompletionStage<MutationResultV1> refreshDispatch(
            VersionedValue exactIntentAuthority,
            DeleteObservationContextV2 successor,
            DeleteEligibilitySnapshotV2 snapshot) {
        VersionedAuthorityV1 exact = exactAuthority(exactIntentAuthority.key(), exactIntentAuthority);
        TargetDeleteAuthorityV1 current = exact.authority();
        requireLegacyGrace(current, Optional.of(snapshot));
        return refreshDispatchChecked(exact, successor, snapshot);
    }

    public CompletionStage<MutationResultV1> refreshDispatch(
            VersionedValue exactIntentAuthority,
            DeleteObservationContextV2 successor,
            DeleteEligibilitySnapshotV2 snapshot,
            EpochGraceReady ready) {
        var exact = exactAuthority(exactIntentAuthority.key(), exactIntentAuthority);
        requireEpochProof(exact.authority(), snapshot, ready);
        if (!successor.equals(ready.mark().authority())
                || exact.authority()
                        .recoveryVeto()
                        .filter(veto -> veto.reason() == DeleteRecoveryVetoV2.Reason.EPOCH_GRACE_RENEWAL_PENDING)
                        .isEmpty()) {
            throw new IllegalArgumentException("intent refresh requires its exact pending epoch renewal");
        }
        return ready.requireCurrent().thenCompose(ignored -> refreshDispatchChecked(exact, successor, snapshot));
    }

    private CompletionStage<MutationResultV1> refreshDispatchChecked(
            VersionedAuthorityV1 exact, DeleteObservationContextV2 successor, DeleteEligibilitySnapshotV2 snapshot) {
        var current = exact.authority();
        DeleteObservationContextV2 previous = M5TargetDeleteAuthorityStateMachineV1.dispatchContext(current);
        // Validate the proposed shape before starting native work; no observation is persisted from this candidate.
        M5TargetDeleteAuthorityStateMachineV1.refreshDispatch(
                current,
                successor,
                snapshot,
                current.externalIdentity().orElseThrow().observation());
        CompletionStage<Void> fenced = previous.coordinatorOwner().equals(successor.coordinatorOwner())
                ? CompletableFuture.completedFuture(null)
                : checkedAuthority(
                        () -> observationAuthority.requirePredecessorFenced(
                                current.target().resourceId(), previous, successor),
                        DeleteRecoveryVetoV2.Reason.PREDECESSOR_OWNER_AUTHORITY_REJECTED);
        CompletionStage<MutationResultV1> operation = fenced.thenCompose(
                        ignored -> requireObservationAuthority(current, successor, snapshot))
                .thenCompose(ignored -> requireExactCurrent(exact))
                .thenCompose(ignored ->
                        externalReader.rereadExact(current.externalIdentity().orElseThrow()))
                .thenCompose(observation -> {
                    TargetDeleteAuthorityV1 candidate = M5TargetDeleteAuthorityStateMachineV1.refreshDispatch(
                            current, successor, snapshot, Objects.requireNonNull(observation, "native observation"));
                    return requireObservationAuthority(candidate, successor, snapshot)
                            .thenCompose(ignored -> mutate(
                                    current.authorityKey(),
                                    Optional.of(exact.exactStoredValue()),
                                    M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                                    false));
                });
        // Cancelling the exposed observer must not abort an accepted native read or its exact CAS reconciliation.
        return operation.thenApply(result -> result);
    }

    private CompletionStage<Void> requireExactCurrent(VersionedAuthorityV1 exact) {
        return metadata.read(exact.authority().authorityKey()).thenAccept(observed -> {
            if (!observed.equals(Optional.of(exact.exactStoredValue()))) {
                throw new IllegalStateException("dispatch authority changed before native identity read");
            }
        });
    }

    /** Revalidates the exact persisted INTENT, native owner, facts and physical identity for dispatch. */
    public CompletionStage<M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1> requireCurrentDispatch(
            VersionedValue exactIntentAuthority) {
        VersionedAuthorityV1 exact = exactAuthority(exactIntentAuthority.key(), exactIntentAuthority);
        TargetDeleteAuthorityV1 current = exact.authority();
        if (current.recoveryVeto().isPresent()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("epoch grace renewal requires fresh eligibility before dispatch"));
        }
        DeleteObservationContextV2 context = M5TargetDeleteAuthorityStateMachineV1.dispatchContext(current);
        DeleteEligibilitySnapshotV2 snapshot = current.eligibilitySnapshot().orElseThrow();
        CompletionStage<M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1> operation =
                requireObservationAuthority(current, context, snapshot)
                        .thenCompose(ignored -> requireExactCurrent(exact))
                        .thenCompose(ignored -> externalReader.rereadExact(
                                current.externalIdentity().orElseThrow()))
                        .thenCompose(observation -> requireObservationAuthority(current, context, snapshot)
                                .thenCompose(ignored -> requireExactCurrent(exact))
                                .thenApply(ignored -> Objects.requireNonNull(observation, "native observation")));
        return operation.thenApply(observation -> observation);
    }

    /** Read-only reconciliation of historical terminal candidates; supplied hashes can never create a terminal. */
    public CompletionStage<MutationResultV1> completeDelete(
            VersionedValue exactIntentAuthority,
            DeleteTerminalOutcomeV1 terminalOutcome,
            Sha256Digest absenceInventoryRootSha256,
            Sha256Digest completionProofDigestSha256) {
        VersionedAuthorityV1 exact = exactAuthority(exactIntentAuthority.key(), exactIntentAuthority);
        TargetDeleteAuthorityV1 candidate = M5TargetDeleteAuthorityStateMachineV1.completeDelete(
                exact.authority(), terminalOutcome, absenceInventoryRootSha256, completionProofDigestSha256);
        return reconcile(
                candidate.authorityKey(),
                Optional.of(exactIntentAuthority),
                M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                false,
                new Attempt(MutationOutcome.RESPONSE_UNKNOWN, null));
    }

    /** Reconciles native absence without claiming that this coordinator dispatched the physical deletion. */
    public CompletionStage<MutationResultV1> completeAbsent(VersionedValue exactIntentAuthority) {
        VersionedAuthorityV1 exact = exactAuthority(exactIntentAuthority.key(), exactIntentAuthority);
        TargetDeleteAuthorityV1 current = exact.authority();
        if (current.recoveryVeto().isPresent()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("epoch grace renewal requires fresh eligibility before dispatch"));
        }
        DeleteObservationContextV2 context = M5TargetDeleteAuthorityStateMachineV1.dispatchContext(current);
        DeleteEligibilitySnapshotV2 snapshot = current.eligibilitySnapshot().orElseThrow();
        Sha256Digest absence = Sha256Digest.hash(
                com.nereusstream.domain.bytes.CanonicalUtf8.fromString("NEREUS_V2_M5_NATIVE_ABSENCE_V2:"
                                + exactIntentAuthority.canonicalStoredSha256().toHex())
                        .bytes());
        Sha256Digest completion = M5TargetDeleteAuthorityKeysV1.refreshedDispatchTokenSha256(
                current.deleteIntent().orElseThrow().dispatchTokenSha256(),
                context.capability().valueSha256(),
                snapshot.sha256(),
                absence);
        TargetDeleteAuthorityV1 candidate = M5TargetDeleteAuthorityStateMachineV1.completeDelete(
                current, DeleteTerminalOutcomeV1.ALREADY_ABSENT_EXACT_V1, absence, completion);
        CanonicalBytes candidateBytes = M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate);
        CompletionStage<MutationResultV1> operation = metadata.read(current.authorityKey())
                .thenCompose(observed -> {
                    if (observed.isPresent()) {
                        VersionedValue value = observed.orElseThrow();
                        if (isExactCompactedSuccessor(candidateBytes, value)) {
                            return CompletableFuture.completedFuture(new MutationResultV1(
                                    Outcome.EXISTING_TERMINAL, current.authorityKey(), candidateBytes, observed));
                        }
                        if (value.canonicalStoredBytes().equals(candidateBytes)) {
                            return CompletableFuture.completedFuture(new MutationResultV1(
                                    Outcome.EXISTING_EXACT, current.authorityKey(), candidateBytes, observed));
                        }
                    }
                    return requireObservationAuthority(current, context, snapshot)
                            .thenCompose(ignored -> requireExactCurrent(exact))
                            .thenCompose(ignored -> externalReader.rereadExact(
                                    current.externalIdentity().orElseThrow()))
                            .thenCompose(observation -> {
                                if (observation
                                        != M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1
                                                .ABSENT_EXACT_V1) {
                                    return CompletableFuture.failedFuture(
                                            new IllegalStateException("native target absence was not established"));
                                }
                                return requireObservationAuthority(current, context, snapshot)
                                        .thenCompose(ignored -> mutate(
                                                current.authorityKey(),
                                                Optional.of(exactIntentAuthority),
                                                candidateBytes,
                                                false));
                            });
                });
        return operation.thenApply(result -> result);
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
        snapshot.requireCanonicalM4ReleaseKeys();
        return requireFreshFacts(snapshot.authorityFacts());
    }

    private CompletionStage<Void> requireObservationAuthority(TargetDeleteAuthorityV1 authority) {
        DeleteObservationContextV2 context = authority.readFence().orElseThrow().observationContext();
        return requireObservationAuthority(
                authority, context, authority.eligibilitySnapshot().orElseThrow());
    }

    private CompletionStage<Void> requireObservationAuthority(
            TargetDeleteAuthorityV1 authority,
            DeleteObservationContextV2 context,
            DeleteEligibilitySnapshotV2 snapshot) {
        snapshot.requireCanonicalM4ReleaseKeys();
        java.util.Map<String, AuthorityFactV1> facts = new java.util.TreeMap<>();
        for (AuthorityFactV1 fact : snapshot.authorityFacts()) {
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
