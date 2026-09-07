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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BatchAuthoritySlotV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BatchMetadataStateV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredSourceRetirementBatchTombstoneV1;
import java.io.ByteArrayOutputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Prewrites one bounded immutable history path, then atomically folds one terminal slot at the selector key. */
public final class M5RetiredBatchHistoryCoordinatorV2 {
    public enum Outcome {
        APPLIED_EXACT,
        EXISTING_TERMINAL,
        DEFINITIVELY_NOT_APPLIED,
        RETRY_STALE,
        RESPONSE_UNKNOWN,
        RETAIN,
        QUARANTINED
    }

    private final ExactMetadataTransactionStoreV1 metadata;
    private final M5RetiredHistoryWriteBudgetV2 budget;

    public M5RetiredBatchHistoryCoordinatorV2(
            ExactMetadataTransactionStoreV1 metadata, M5RetiredHistoryWriteBudgetV2 budget) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.budget = Objects.requireNonNull(budget, "budget");
    }

    public CompletionStage<Outcome> fold(VersionedValue predecessor, Sha256Digest batchId) {
        Objects.requireNonNull(predecessor, "predecessor");
        Objects.requireNonNull(batchId, "batchId");
        var current = M5BindingAuthorityCodecV1.decodeAuthority(predecessor.canonicalStoredBytes());
        BatchAuthoritySlotV1 terminal = current.slot(batchId)
                .filter(slot -> slot.state() == BatchMetadataStateV1.RETIRED_V1)
                .orElseThrow(() -> new IllegalArgumentException("history fold target is not a resident terminal slot"));
        if (current.state() != BindingAuthorityStateV1.OPEN_V1) {
            return CompletableFuture.completedFuture(Outcome.RETAIN);
        }
        var history = new M5RetiredBatchHistoryV2(current.binding());
        Sha256Digest attempt = attempt(predecessor, batchId);
        return metadata.read(predecessor.key()).thenCompose(observed -> {
            if (!observed.equals(Optional.of(predecessor))) {
                return reconcileTerminal(observed, terminal.retiredTombstone().orElseThrow())
                        .thenApply(outcome -> {
                            if (outcome != Outcome.RESPONSE_UNKNOWN) {
                                budget.reconciled(attempt);
                            }
                            return outcome;
                        });
            }
            return history.readProofAsync(current.retiredHistory(), batchId, this::readBytes)
                    .thenCompose(proof -> {
                        var insertion = history.insert(
                                current.retiredHistory(),
                                terminal.retiredTombstone().orElseThrow(),
                                proof);
                        var candidate = M5BindingAuthorityCodecV1.encodeAuthority(
                                M5BindingAuthorityCodecV1.fold(current, terminal, insertion));
                        var reservation = budget.reserve(attempt, insertion);
                        if (reservation.isEmpty() || !reservation.orElseThrow().tryBegin()) {
                            return CompletableFuture.completedFuture(Outcome.RETAIN);
                        }
                        CompletionStage<Boolean> writes = CompletableFuture.completedFuture(true);
                        for (var node : insertion.nodes()) {
                            writes = writes.thenCompose(exact -> exact
                                    ? prewrite(history, node, reservation.orElseThrow())
                                    : CompletableFuture.completedFuture(false));
                        }
                        return writes.thenCompose(exact -> {
                                    if (!exact) {
                                        return CompletableFuture.completedFuture(Outcome.RESPONSE_UNKNOWN);
                                    }
                                    return metadata.compareAndSet(
                                                    Optional.of(predecessor), predecessor.key(), candidate)
                                            .thenCompose(mutation -> metadata.read(predecessor.key())
                                                    .thenCompose(after -> {
                                                        if (after.filter(value -> value.canonicalStoredBytes()
                                                                        .equals(candidate))
                                                                .isPresent()) {
                                                            budget.reconciled(attempt);
                                                            return CompletableFuture.completedFuture(
                                                                    mutation == MutationOutcome.APPLIED_EXACT
                                                                            ? Outcome.APPLIED_EXACT
                                                                            : Outcome.EXISTING_TERMINAL);
                                                        }
                                                        if (after.equals(Optional.of(predecessor))) {
                                                            // Keep the same reservation so a response-unknown retry
                                                            // cannot
                                                            // bypass its quota.
                                                            return CompletableFuture.completedFuture(
                                                                    mutation == MutationOutcome.RESPONSE_UNKNOWN
                                                                            ? Outcome.RESPONSE_UNKNOWN
                                                                            : Outcome.DEFINITIVELY_NOT_APPLIED);
                                                        }
                                                        return reconcileTerminal(
                                                                        after,
                                                                        terminal.retiredTombstone()
                                                                                .orElseThrow())
                                                                .thenApply(outcome -> {
                                                                    if (outcome != Outcome.RESPONSE_UNKNOWN) {
                                                                        budget.reconciled(attempt);
                                                                    }
                                                                    return outcome;
                                                                });
                                                    }));
                                })
                                .whenComplete((outcome, failure) ->
                                        reservation.orElseThrow().endAttempt());
                    });
        });
    }

    /** Reconcile exact terminal identity against a later root; unrelated selector fields may have changed. */
    public CompletionStage<Outcome> reconcileTerminal(
            Optional<VersionedValue> observed, RetiredSourceRetirementBatchTombstoneV1 expected) {
        if (observed.isEmpty()) {
            return CompletableFuture.completedFuture(Outcome.QUARANTINED);
        }
        BindingRetirementAuthorityV1 current =
                M5BindingAuthorityCodecV1.decodeAuthority(observed.orElseThrow().canonicalStoredBytes());
        if (!current.binding().equals(expected.binding())) {
            return CompletableFuture.completedFuture(Outcome.QUARANTINED);
        }
        var resident = current.slot(expected.batchIdSha256());
        if (resident.isPresent()) {
            return CompletableFuture.completedFuture(
                    resident.orElseThrow().retiredTombstone().equals(Optional.of(expected))
                            ? Outcome.RETRY_STALE
                            : Outcome.QUARANTINED);
        }
        var history = new M5RetiredBatchHistoryV2(current.binding());
        return history.readProofAsync(current.retiredHistory(), expected.batchIdSha256(), this::readBytes)
                .thenApply(proof -> proof.tombstone().equals(Optional.of(expected))
                        ? Outcome.EXISTING_TERMINAL
                        : Outcome.QUARANTINED);
    }

    private CompletionStage<Boolean> prewrite(
            M5RetiredBatchHistoryV2 history,
            M5RetiredBatchHistoryV2.NodeWrite node,
            M5RetiredHistoryWriteBudgetV2.Reservation reservation) {
        String key = history.key(node.root().sha256());
        return metadata.read(key).thenCompose(before -> {
            if (before.isPresent()) {
                requireExactNode(before.orElseThrow(), node);
                return CompletableFuture.completedFuture(true);
            }
            reservation.beforeNodeDispatch(node);
            return metadata.compareAndSet(Optional.empty(), key, node.bytes())
                    .thenCompose(ignored -> metadata.read(key).thenApply(after -> {
                        if (after.isEmpty()) {
                            return false;
                        }
                        requireExactNode(after.orElseThrow(), node);
                        return true;
                    }));
        });
    }

    private CompletionStage<Optional<CanonicalBytes>> readBytes(String key) {
        return metadata.read(key).thenApply(value -> value.map(VersionedValue::canonicalStoredBytes));
    }

    private static void requireExactNode(VersionedValue observed, M5RetiredBatchHistoryV2.NodeWrite expected) {
        if (!observed.canonicalStoredBytes().equals(expected.bytes())) {
            throw new IllegalStateException("immutable retired history prewrite conflicts at its content address");
        }
    }

    private static Sha256Digest attempt(VersionedValue predecessor, Sha256Digest batchId) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.writeBytes(predecessor.key().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        bytes.writeBytes(predecessor.canonicalStoredSha256().bytes().toByteArray());
        bytes.writeBytes(batchId.bytes().toByteArray());
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
    }
}
