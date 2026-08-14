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

package com.nereusstream.pulsar.offload;

import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.MetadataSnapshot;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.MetadataSupplier;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.DeleteState;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** Pin-drain, revalidation, native CAS, and physical-delete ordering for one ManagedLedger ledger. */
public final class PulsarBookKeeperDeletionCoordinatorV1 {
    @FunctionalInterface
    public interface ObjectRevalidator {
        CompletionStage<Void> revalidate(MetadataSnapshot expected);
    }

    public interface NativeMetadataCas {
        /** Completes only after response-uncertain CAS resolution; exceptional completion is definitive no-write. */
        CompletionStage<MetadataSnapshot> publishIntent(MetadataSnapshot expectedNone);

        CompletionStage<MetadataSnapshot> publishDone(MetadataSnapshot expectedIntent);
    }

    @FunctionalInterface
    public interface BookKeeperDeleter {
        /** Success means physical deletion or authoritative BKNoSuchLedgerExists. */
        CompletionStage<Void> deleteAndProveAbsent(long ledgerId);
    }

    private final MetadataSupplier metadataSupplier;
    private final PulsarDualSourceReadHandleV1 dualSourceHandle;
    private final ObjectRevalidator objectRevalidator;
    private final NativeMetadataCas metadataCas;
    private final BookKeeperDeleter bookKeeperDeleter;
    private final long pinDrainTimeoutMillis;

    public PulsarBookKeeperDeletionCoordinatorV1(
            MetadataSupplier metadataSupplier,
            PulsarDualSourceReadHandleV1 dualSourceHandle,
            ObjectRevalidator objectRevalidator,
            NativeMetadataCas metadataCas,
            BookKeeperDeleter bookKeeperDeleter) {
        this(metadataSupplier, dualSourceHandle, objectRevalidator, metadataCas, bookKeeperDeleter, 30_000);
    }

    public PulsarBookKeeperDeletionCoordinatorV1(
            MetadataSupplier metadataSupplier,
            PulsarDualSourceReadHandleV1 dualSourceHandle,
            ObjectRevalidator objectRevalidator,
            NativeMetadataCas metadataCas,
            BookKeeperDeleter bookKeeperDeleter,
            long pinDrainTimeoutMillis) {
        this.metadataSupplier = Objects.requireNonNull(metadataSupplier, "metadataSupplier");
        this.dualSourceHandle = Objects.requireNonNull(dualSourceHandle, "dualSourceHandle");
        this.objectRevalidator = Objects.requireNonNull(objectRevalidator, "objectRevalidator");
        this.metadataCas = Objects.requireNonNull(metadataCas, "metadataCas");
        this.bookKeeperDeleter = Objects.requireNonNull(bookKeeperDeleter, "bookKeeperDeleter");
        if (pinDrainTimeoutMillis <= 0) {
            throw new IllegalArgumentException("pin drain timeout must be positive");
        }
        this.pinDrainTimeoutMillis = pinDrainTimeoutMillis;
    }

    public CompletionStage<MetadataSnapshot> deleteOrReconcile() {
        MetadataSnapshot snapshot;
        try {
            snapshot = Objects.requireNonNull(metadataSupplier.current(), "metadata snapshot");
            requireEligible(snapshot);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return switch (snapshot.deleteState()) {
            case BK_DELETE_NONE -> beginDeletion(snapshot);
            case BK_DELETE_INTENT -> finishIntent(snapshot);
            case BK_DELETE_DONE -> CompletableFuture.completedFuture(snapshot);
        };
    }

    private CompletionStage<MetadataSnapshot> beginDeletion(MetadataSnapshot none) {
        CompletionStage<Void> rawDrain = dualSourceHandle.fenceBookKeeper(none);
        CompletionStage<MetadataSnapshot> intentAttempt = bounded(rawDrain, pinDrainTimeoutMillis)
                .thenCompose(ignored -> callStage(() -> objectRevalidator.revalidate(none)))
                .thenCompose(ignored -> callStage(() -> metadataCas.publishIntent(none)));
        CompletableFuture<MetadataSnapshot> resolvedIntent = new CompletableFuture<>();
        intentAttempt.whenComplete((intent, failure) -> {
            if (failure != null) {
                if (!dualSourceHandle.unfenceBookKeeper(none)) {
                    rawDrain.whenComplete((ignored, drainFailure) -> dualSourceHandle.unfenceBookKeeper(none));
                }
                resolvedIntent.completeExceptionally(unwrap(failure));
                return;
            }
            try {
                validateTransition(none, intent, DeleteState.BK_DELETE_INTENT);
                resolvedIntent.complete(intent);
            } catch (Throwable invalidIntent) {
                resolvedIntent.completeExceptionally(invalidIntent);
            }
        });
        return resolvedIntent.thenCompose(this::finishIntent);
    }

    private CompletionStage<MetadataSnapshot> finishIntent(MetadataSnapshot intent) {
        if (intent.deleteState() != DeleteState.BK_DELETE_INTENT) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("delete reconciliation requires INTENT"));
        }
        return dualSourceHandle
                .fenceBookKeeper(intent)
                .thenCompose(ignored -> dualSourceHandle.closeBookKeeperAfterIntent())
                .thenCompose(ignored -> callStage(() -> bookKeeperDeleter.deleteAndProveAbsent(intent.ledgerId())))
                .thenCompose(ignored -> callStage(() -> metadataCas.publishDone(intent)))
                .thenApply(done -> {
                    validateTransition(intent, done, DeleteState.BK_DELETE_DONE);
                    return done;
                });
    }

    private static void requireEligible(MetadataSnapshot snapshot) {
        if (!snapshot.offloadComplete()
                || snapshot.attemptUuid() == null
                || snapshot.retentionClass() != RetentionClass.DELETE_AFTER_VERIFIED) {
            throw new IllegalStateException("native metadata is not eligible for verified BK deletion");
        }
    }

    private static void validateTransition(
            MetadataSnapshot previous, MetadataSnapshot next, DeleteState expectedState) {
        if (next == null
                || next.ledgerId() != previous.ledgerId()
                || !Objects.equals(next.attemptUuid(), previous.attemptUuid())
                || next.version() <= previous.version()
                || !next.offloadComplete()
                || next.retentionClass() != previous.retentionClass()
                || next.deleteState() != expectedState
                || !next.bookkeeperDeleted()) {
            throw new IllegalStateException("native delete-state CAS returned a different attempt or transition");
        }
    }

    private static <T> CompletionStage<T> callStage(Supplier<CompletionStage<T>> call) {
        try {
            CompletionStage<T> stage = call.get();
            return stage == null
                    ? CompletableFuture.failedFuture(new NullPointerException("native delete operation returned null"))
                    : stage;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static <T> CompletionStage<T> bounded(CompletionStage<T> source, long timeoutMillis) {
        CompletableFuture<T> result = new CompletableFuture<>();
        source.whenComplete((value, failure) -> {
            if (failure == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(unwrap(failure));
            }
        });
        CompletableFuture.delayedExecutor(timeoutMillis, TimeUnit.MILLISECONDS)
                .execute(() ->
                        result.completeExceptionally(new TimeoutException("BookKeeper source-pin drain timed out")));
        return result;
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }
}
