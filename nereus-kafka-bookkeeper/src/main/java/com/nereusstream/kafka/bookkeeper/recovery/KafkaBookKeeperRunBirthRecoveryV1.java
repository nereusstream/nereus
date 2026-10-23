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

package com.nereusstream.kafka.bookkeeper.recovery;

import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1;
import com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Reads one closed owner's exact run births before a successor can start another native run. */
public final class KafkaBookKeeperRunBirthRecoveryV1 {
    @FunctionalInterface
    public interface UnselectedFence {
        CompletionStage<Void> fence(
                KafkaOwnerAdmissionV1 closed, KafkaRunRootRecordV2 record, RunLedgerHandleV1 handle);

        /** A concrete native owner can use its independently protected recovery session. */
        default CompletionStage<Optional<RunLedgerRecoveryProofV1>> recoverNative(
                KafkaOwnerAdmissionV1 closed,
                KafkaRunRootRecordV2 record,
                BookKeeperCellSession borrowedSession,
                RunLedgerHandleV1 handle) {
            return KafkaBookKeeperRunBirthRecoveryV1.recoverNative(borrowedSession, handle);
        }

        /** A native task fence plus metadata-specific absence can settle an unknown physical create. */
        default CompletionStage<Void> reconcileAbsent(
                KafkaOwnerAdmissionV1 closed, KafkaRunRootRecordV2 record, RunLedgerHandleV1 handle) {
            return CompletableFuture.completedFuture(null);
        }

        /** A native task fence plus exact BK recovery can settle the create without granting orphan eligibility. */
        default CompletionStage<Void> reconcileRecovered(
                KafkaOwnerAdmissionV1 closed,
                KafkaRunRootRecordV2 record,
                RunLedgerHandleV1 handle,
                RunLedgerRecoveryProofV1 proof) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("unselected physical birth has no writer ticket reconciliation"));
        }
    }

    public enum Kind {
        SELECTED,
        RETIRED_SELECTED,
        NEVER_CREATED,
        PHYSICALLY_CREATED_UNSELECTED
    }

    public record Observation(
            KafkaRunRootRecordV2 record,
            RunLedgerHandleV1 handle,
            Kind kind,
            java.util.Optional<RunLedgerRecoveryProofV1> recoveryProof) {
        public Observation {
            Objects.requireNonNull(record, "record");
            Objects.requireNonNull(handle, "handle");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(recoveryProof, "recoveryProof");
            if (!record.root().runId().equals(handle.runId())
                    || !record.root().ledgerIdentity().equals(handle.ledgerIdentity())
                    || recoveryProof
                            .map(proof -> !proof.handle().equals(handle))
                            .orElse(false)
                    || (kind == Kind.PHYSICALLY_CREATED_UNSELECTED) != recoveryProof.isPresent()) {
                throw new IllegalArgumentException("run birth observation changed its exact physical identity");
            }
        }
    }

    private final BookKeeperCellSession session;
    private final KafkaBookKeeperOwnerAuthorityV1 authority;
    private final UnselectedFence unselectedFence;

    public KafkaBookKeeperRunBirthRecoveryV1(
            BookKeeperCellSession session, KafkaBookKeeperOwnerAuthorityV1 authority, UnselectedFence unselectedFence) {
        this.session = Objects.requireNonNull(session, "session");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.unselectedFence = Objects.requireNonNull(unselectedFence, "unselectedFence");
    }

    public CompletionStage<List<Observation>> observe(KafkaOwnerAdmissionV1 closed) {
        Objects.requireNonNull(closed, "closed");
        if (!closed.closed()) {
            throw new IllegalArgumentException("run birth recovery requires a closed owner");
        }
        return authority.readRunBirths(closed).thenCompose(inventory -> {
            var records = List.copyOf(inventory.orElseThrow(
                    () -> new IllegalStateException("closed owner has no discoverable run birth inventory")));
            var links = new HashSet<KafkaRunRootRecordV2.Link>();
            for (var record : records) {
                links.add(record.initialLink());
            }
            if (!links.containsAll(closed.runs())) {
                throw new IllegalStateException("closed owner selected a run missing from its birth inventory");
            }
            CompletionStage<List<Observation>> observations = CompletableFuture.completedFuture(new ArrayList<>());
            for (var record : records) {
                observations = observations.thenCompose(
                        found -> observeOne(closed, record).thenApply(value -> {
                            found.add(value);
                            return found;
                        }));
            }
            return observations.thenCompose(
                    found -> authority.readRunBirths(closed).thenApply(after -> {
                        if (!after.equals(Optional.of(records))) {
                            throw new IllegalStateException("closed owner run births changed after native fencing");
                        }
                        return List.copyOf(found);
                    }));
        });
    }

    private CompletionStage<Observation> observeOne(KafkaOwnerAdmissionV1 closed, KafkaRunRootRecordV2 record) {
        var root = record.root();
        var handle = new RunLedgerHandleV1(
                root.providerScopeId(),
                root.runId(),
                root.ledgerIdentity(),
                session.capabilitySnapshot().configurationDigest());
        if (closed.runs().contains(record.initialLink())) {
            return CompletableFuture.completedFuture(new Observation(
                    record,
                    handle,
                    record.retired() ? Kind.RETIRED_SELECTED : Kind.SELECTED,
                    java.util.Optional.empty()));
        }
        if (record.admitted()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("unselected run birth contradicts its admitted root: " + root.runId()));
        }
        return unselectedFence
                .fence(closed, record, handle)
                .thenCompose(ignored -> unselectedFence.recoverNative(closed, record, session, handle))
                .thenCompose(recovered -> {
                    if (recovered.isEmpty()) {
                        return unselectedFence
                                .reconcileAbsent(closed, record, handle)
                                .thenApply(ignored -> new Observation(
                                        record, handle, Kind.NEVER_CREATED, java.util.Optional.empty()));
                    }
                    var proof = recovered.orElseThrow();
                    if (!proof.handle().equals(handle)) {
                        throw new IllegalStateException("unselected physical run has no exact fenced recovery");
                    }
                    return unselectedFence
                            .reconcileRecovered(closed, record, handle, proof)
                            .thenApply(ignored -> new Observation(
                                    record, handle, Kind.PHYSICALLY_CREATED_UNSELECTED, Optional.of(proof)));
                });
    }

    /** Metadata-specific absence only; a present ledger must return its exact native fenced recovery. */
    public static CompletionStage<Optional<RunLedgerRecoveryProofV1>> recoverNative(
            BookKeeperCellSession session, RunLedgerHandleV1 handle) {
        return session.openRunLedger(handle).thenCompose(open -> {
            if (open.outcome() == RunLedgerOpenOutcomeV1.ABSENT) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            if (open.outcome() != RunLedgerOpenOutcomeV1.OPENED_EXACT
                    || !open.exactHandle().equals(Optional.of(handle))) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                        "unselected run birth physical identity is unresolved: " + open.outcome()));
            }
            return session.fenceAndRecoverRunLedger(handle).thenApply(recovered -> {
                if (recovered.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT
                        || recovered.exactProof().isEmpty()
                        || !recovered.exactProof().orElseThrow().handle().equals(handle)) {
                    throw new IllegalStateException("unselected physical run has no exact fenced recovery");
                }
                return recovered.exactProof();
            });
        });
    }
}
