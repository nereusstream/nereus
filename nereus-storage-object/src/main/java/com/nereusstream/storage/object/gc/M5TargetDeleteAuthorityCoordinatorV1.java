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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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

        public boolean exactCandidateIsAuthoritative() {
            return (outcome == Outcome.APPLIED_EXACT || outcome == Outcome.EXISTING_EXACT)
                    && observed.map(value -> value.canonicalStoredBytes().equals(exactCandidate))
                            .orElse(false);
        }
    }

    private record Attempt(MutationOutcome outcome, Throwable failure) {}

    private final ExactMetadataTransactionStoreV1 metadata;

    public M5TargetDeleteAuthorityCoordinatorV1(ExactMetadataTransactionStoreV1 metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    public CompletionStage<Optional<VersionedAuthorityV1>> read(String authorityKey) {
        String key = M5TargetDeleteAuthorityRecordsV1.requireKey(authorityKey);
        return metadata.read(key).thenApply(observed -> observed.map(value -> exactAuthority(key, value)));
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
            VersionedValue exactOpenAuthority, Sha256Digest attemptIdSha256) {
        VersionedAuthorityV1 exact = exactAuthority(exactOpenAuthority.key(), exactOpenAuthority);
        TargetDeleteAuthorityV1 candidate =
                M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(exact.authority(), attemptIdSha256);
        return requireFreshEligibility(exact.authority().eligibilitySnapshot().orElseThrow())
                .thenCompose(ignored -> mutate(
                        exact.authority().authorityKey(),
                        Optional.of(exact.exactStoredValue()),
                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                        false));
    }

    public CompletionStage<MutationResultV1> bindDeleteIntent(
            VersionedValue exactFencedAuthority,
            ExactExternalIdentityV1 externalIdentity,
            Sha256Digest deleteAttemptIdSha256,
            Sha256Digest dispatchOwnerFenceSha256,
            Sha256Digest capabilityDigestSha256) {
        VersionedAuthorityV1 exact = exactAuthority(exactFencedAuthority.key(), exactFencedAuthority);
        TargetDeleteAuthorityV1 candidate = M5TargetDeleteAuthorityStateMachineV1.bindDeleteIntent(
                exact.authority(),
                externalIdentity,
                deleteAttemptIdSha256,
                dispatchOwnerFenceSha256,
                capabilityDigestSha256);
        return requireFreshEligibility(exact.authority().eligibilitySnapshot().orElseThrow())
                .thenCompose(ignored -> mutate(
                        exact.authority().authorityKey(),
                        Optional.of(exact.exactStoredValue()),
                        M5TargetDeleteAuthorityCodecV1.encodeAuthority(candidate),
                        false));
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
        CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
        for (var fact : snapshot.authorityFacts()) {
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
            if (observed.equals(predecessor)) {
                Outcome predecessorOutcome =
                        attempt.failure() == null && attempt.outcome() == MutationOutcome.APPLIED_EXACT
                                ? Outcome.RESPONSE_UNKNOWN
                                : Outcome.PREDECESSOR_UNCHANGED;
                return new MutationResultV1(predecessorOutcome, authorityKey, candidate, observed);
            }
            if (create && observed.isPresent()) {
                try {
                    TargetDeleteAuthorityV1 existing =
                            exactAuthority(authorityKey, observed.orElseThrow()).authority();
                    TargetDeleteAuthorityV1 requested = M5TargetDeleteAuthorityCodecV1.decodeAuthority(candidate);
                    // Rediscovery can refresh eligibility but must converge on the existing physical resource.
                    Outcome outcome = existing.target().equals(requested.target())
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
