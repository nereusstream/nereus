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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1.MutationResultV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1.Outcome;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1.VersionedAuthorityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterTicketV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Ensures a proof-changing writer has a visible durable target ticket before any external dispatch. */
public final class M5TargetDeleteWriterGuardV1 {
    public enum ExternalMutationOutcomeV1 {
        APPLIED_RECONCILED_V1,
        NOT_APPLIED_RECONCILED_V1,
        RESPONSE_UNKNOWN_V1
    }

    public enum GuardOutcomeV1 {
        COMPLETED_RECONCILED_V1,
        TICKET_RETAINED_RESPONSE_UNKNOWN_V1,
        TICKET_RETAINED_CONCURRENT_AUTHORITY_CHANGE_V1,
        NOT_DISPATCHED_V1,
        QUARANTINED_V1
    }

    public record ExternalMutationResultV1(
            ExternalMutationOutcomeV1 outcome, Optional<Sha256Digest> reconciledProofSnapshotDigest) {
        public ExternalMutationResultV1 {
            Objects.requireNonNull(outcome, "outcome");
            reconciledProofSnapshotDigest =
                    Objects.requireNonNull(reconciledProofSnapshotDigest, "reconciledProofSnapshotDigest");
            if ((outcome == ExternalMutationOutcomeV1.RESPONSE_UNKNOWN_V1) != reconciledProofSnapshotDigest.isEmpty()) {
                throw new IllegalArgumentException("external mutation outcome and reconciliation proof disagree");
            }
            reconciledProofSnapshotDigest.ifPresent(
                    value -> M5TargetDeleteAuthorityRecordsV1.requireDigest(value, "reconciledProofSnapshotDigest"));
        }

        public static ExternalMutationResultV1 applied(Sha256Digest proofSnapshotDigest) {
            return new ExternalMutationResultV1(
                    ExternalMutationOutcomeV1.APPLIED_RECONCILED_V1, Optional.of(proofSnapshotDigest));
        }

        public static ExternalMutationResultV1 notApplied(Sha256Digest proofSnapshotDigest) {
            return new ExternalMutationResultV1(
                    ExternalMutationOutcomeV1.NOT_APPLIED_RECONCILED_V1, Optional.of(proofSnapshotDigest));
        }

        public static ExternalMutationResultV1 responseUnknown() {
            return new ExternalMutationResultV1(ExternalMutationOutcomeV1.RESPONSE_UNKNOWN_V1, Optional.empty());
        }
    }

    public record DurableWriterDispatchV1(
            String authorityKey, VersionedValue exactTicketedAuthority, ProofBoundWriterTicketV1 ticket) {
        public DurableWriterDispatchV1 {
            M5TargetDeleteAuthorityRecordsV1.requireKey(authorityKey);
            Objects.requireNonNull(exactTicketedAuthority, "exactTicketedAuthority");
            Objects.requireNonNull(ticket, "ticket");
            if (!authorityKey.equals(exactTicketedAuthority.key())) {
                throw new IllegalArgumentException("durable writer dispatch key differs");
            }
        }
    }

    public record GuardResultV1(
            GuardOutcomeV1 outcome, boolean externalMutationInvoked, Optional<VersionedValue> lastObservedAuthority) {
        public GuardResultV1 {
            Objects.requireNonNull(outcome, "outcome");
            lastObservedAuthority = Objects.requireNonNull(lastObservedAuthority, "lastObservedAuthority");
            if (!externalMutationInvoked && outcome == GuardOutcomeV1.COMPLETED_RECONCILED_V1) {
                throw new IllegalArgumentException("completed guarded mutation was not invoked");
            }
        }
    }

    @FunctionalInterface
    public interface ProofBoundMutationV1 {
        CompletionStage<ExternalMutationResultV1> reconcileOrDispatch(DurableWriterDispatchV1 dispatch);
    }

    private record ExternalAttempt(ExternalMutationResultV1 result, Throwable failure) {}

    private final M5TargetDeleteAuthorityCoordinatorV1 coordinator;

    public M5TargetDeleteWriterGuardV1(M5TargetDeleteAuthorityCoordinatorV1 coordinator) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    }

    public CompletionStage<GuardResultV1> execute(
            VersionedValue exactOpenAuthority, ProofBoundWriterTicketV1 ticket, ProofBoundMutationV1 mutation) {
        Objects.requireNonNull(exactOpenAuthority, "exactOpenAuthority");
        Objects.requireNonNull(ticket, "ticket");
        Objects.requireNonNull(mutation, "mutation");
        return coordinator.acquireWriterTicket(exactOpenAuthority, ticket).thenCompose(acquired -> {
            if (!acquired.exactCandidateIsAuthoritative()) {
                return CompletableFuture.completedFuture(new GuardResultV1(
                        acquired.outcome() == Outcome.QUARANTINED
                                ? GuardOutcomeV1.QUARANTINED_V1
                                : GuardOutcomeV1.NOT_DISPATCHED_V1,
                        false,
                        acquired.observed()));
            }
            VersionedValue exactTicketed = acquired.observed().orElseThrow();
            VersionedAuthorityV1 decoded = new VersionedAuthorityV1(
                    exactTicketed,
                    M5TargetDeleteAuthorityCodecV1.decodeAuthority(exactTicketed.canonicalStoredBytes()));
            if (decoded.authority().state() != TargetDeleteAuthorityStateV1.OPEN_V1
                    || decoded.authority().activeWriterTickets().stream().noneMatch(ticket::equals)) {
                return CompletableFuture.completedFuture(
                        new GuardResultV1(GuardOutcomeV1.QUARANTINED_V1, false, Optional.of(exactTicketed)));
            }
            CompletionStage<ExternalMutationResultV1> external;
            try {
                external = mutation.reconcileOrDispatch(
                        new DurableWriterDispatchV1(decoded.authority().authorityKey(), exactTicketed, ticket));
            } catch (Throwable failure) {
                return CompletableFuture.completedFuture(new GuardResultV1(
                        GuardOutcomeV1.TICKET_RETAINED_RESPONSE_UNKNOWN_V1, true, Optional.of(exactTicketed)));
            }
            if (external == null) {
                return CompletableFuture.completedFuture(new GuardResultV1(
                        GuardOutcomeV1.TICKET_RETAINED_RESPONSE_UNKNOWN_V1, true, Optional.of(exactTicketed)));
            }
            return external.handle((result, failure) -> new ExternalAttempt(result, failure))
                    .thenCompose(attempt -> finishOrRetain(decoded.authority().authorityKey(), ticket, attempt));
        });
    }

    private CompletionStage<GuardResultV1> finishOrRetain(
            String authorityKey, ProofBoundWriterTicketV1 ticket, ExternalAttempt attempt) {
        if (attempt.failure() != null
                || attempt.result() == null
                || attempt.result().outcome() == ExternalMutationOutcomeV1.RESPONSE_UNKNOWN_V1) {
            return coordinator
                    .inspect(authorityKey)
                    .thenApply(observed -> new GuardResultV1(
                            GuardOutcomeV1.TICKET_RETAINED_RESPONSE_UNKNOWN_V1,
                            true,
                            observed.map(M5TargetDeleteStoredValueV2::exactStoredValue)));
        }
        Sha256Digest proof = attempt.result().reconciledProofSnapshotDigest().orElseThrow();
        return coordinator.inspect(authorityKey).thenCompose(observed -> {
            if (observed.isEmpty()) {
                return CompletableFuture.completedFuture(
                        new GuardResultV1(GuardOutcomeV1.QUARANTINED_V1, true, Optional.empty()));
            }
            var current = observed.orElseThrow();
            if (current.state() != TargetDeleteAuthorityStateV1.OPEN_V1
                    || current.fullAuthority().orElseThrow().activeWriterTickets().stream()
                            .noneMatch(ticket::equals)) {
                return CompletableFuture.completedFuture(new GuardResultV1(
                        GuardOutcomeV1.QUARANTINED_V1, true, Optional.of(current.exactStoredValue())));
            }
            return coordinator
                    .completeWriterTicket(current.exactStoredValue(), ticket.operationIdSha256(), proof)
                    .thenApply(finished -> finishResult(finished));
        });
    }

    private static GuardResultV1 finishResult(MutationResultV1 finished) {
        if (finished.exactCandidateIsAuthoritative()) {
            return new GuardResultV1(GuardOutcomeV1.COMPLETED_RECONCILED_V1, true, finished.observed());
        }
        return new GuardResultV1(
                finished.outcome() == Outcome.QUARANTINED
                        ? GuardOutcomeV1.QUARANTINED_V1
                        : GuardOutcomeV1.TICKET_RETAINED_CONCURRENT_AUTHORITY_CHANGE_V1,
                true,
                finished.observed());
    }
}
