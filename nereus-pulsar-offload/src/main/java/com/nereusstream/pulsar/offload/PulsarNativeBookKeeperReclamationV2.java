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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.pulsar.offload.PulsarNativeMaterializationTaskV2.RetentionRoots;
import com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReclamationReason;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1;
import com.nereusstream.storage.object.retention.M5PulsarLogicalTrimCoordinatorV2;
import com.nereusstream.storage.object.retention.M5SingleAuthorityLogicalTrimCoordinatorV2.Outcome;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/** One quiescent native replacement's original scan, exact RELEASED receipt, grace and physical GC. */
public final class PulsarNativeBookKeeperReclamationV2 {
    private final PulsarNativeBindingV1 binding;
    private final PulsarNativeM4ControlV1 control;
    private final M5PulsarLogicalTrimCoordinatorV2 trim;
    private final M5PulsarAggregateRetirementCoordinatorV1 aggregate;
    private final BookKeeperLedger resource;
    private final PulsarNativeM4FallbackRetirementV1.Result release;
    private final M5ClosedWriterRegistryV1 writers;
    private final Executor worker;
    private final Sha256Digest attempt =
            Sha256Digest.hash(CanonicalBytes.copyOf(UUID.randomUUID().toString().getBytes(StandardCharsets.US_ASCII)));
    private VersionedValue scan;
    private String unresolvedScanKey;
    private PulsarNativeCursorFloorScanV3 cursors;
    private RetentionRoots tasks;
    private PulsarNativeGcScopeV1 gc;
    private CompletableFuture<Void> opening;
    private CompletableFuture<VersionedValue> reclaiming;
    private CompletableFuture<Void> closing;

    public PulsarNativeBookKeeperReclamationV2(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4ControlV1 control,
            BookKeeperLedger resource,
            PulsarNativeM4FallbackRetirementV1.Result release,
            M5PulsarAggregateRetirementCoordinatorV1 aggregate,
            M5ClosedWriterRegistryV1 writers,
            Executor worker) {
        this.binding = binding;
        this.control = control;
        this.trim = binding.logicalTrimCoordinator();
        this.aggregate = aggregate;
        this.resource = resource;
        this.release = release;
        this.writers = writers;
        this.worker = worker;
    }

    /** The caller retains its exclusive, drained Cell and all backend connections through the original operation. */
    public synchronized CompletableFuture<Void> open(
            VersionedValue predecessor,
            MetadataStoreExtended metadata,
            ManagedLedgerConfig config,
            M5BookKeeperNamespaceAuthorityV2 originalBookKeeper,
            NativePhysicalNamespaceAuthorityV2 object,
            PulsarNativeReadCellBudgetV1 budget) {
        if (opening != null || closing != null) {
            throw new IllegalStateException("Native reclamation already owns its original scan");
        }
        opening = binding.revalidate()
                .thenCompose(unused -> {
                    unresolvedScanKey = predecessor.key();
                    return trim.beginSourceScan(predecessor, attempt);
                })
                .thenCompose(result -> {
                    if (result.outcome() != Outcome.RESPONSE_UNKNOWN) {
                        unresolvedScanKey = null;
                    }
                    return result.outcome() == Outcome.RESPONSE_UNKNOWN
                            ? resolveOriginalScan()
                            : CompletableFuture.completedFuture(result.exactAuthority()
                                    .orElseThrow(() ->
                                            new IllegalStateException("Original native source scan was not applied")));
                })
                .thenCompose(exact -> {
                    scan = exact;
                    return PulsarNativeCursorFloorScanV3.capture(
                            binding, metadata, originalBookKeeper, scan, budget, config, worker);
                })
                .thenCompose(captured -> {
                    cursors = captured;
                    return control.captureMaterializationTaskRoots(scan, budget, worker, originalBookKeeper, object);
                })
                .thenCompose(captured -> {
                    tasks = captured;
                    return PulsarNativeGcScopeV1.openBookKeeper(
                            binding, resource, budget, cursors, scan, originalBookKeeper, worker);
                })
                .thenAccept(opened -> gc = opened)
                .toCompletableFuture();
        return opening.copy();
    }

    private CompletableFuture<VersionedValue> resolveOriginalScan() {
        return aggregate
                .read(unresolvedScanKey)
                .thenApply(found -> {
                    var actual = found.filter(value -> M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                            value.canonicalStoredBytes())
                                    .logicalTrimSourceScan()
                                    .filter(original ->
                                            original.attemptIdSha256().equals(attempt))
                                    .isPresent())
                            .orElseThrow(
                                    () -> new IllegalStateException("Original native source scan remains unresolved"));
                    unresolvedScanKey = null;
                    return actual;
                })
                .toCompletableFuture();
    }

    /** Waits at least the admitted grace and lets the original GC scope prove actual elapsed time and absence. */
    public synchronized CompletableFuture<VersionedValue> reclaim(long graceMillis) {
        if (opening == null || closing != null || graceMillis <= 0) {
            throw new IllegalStateException("Original native reclamation is not open");
        }
        if (reclaiming == null || reclaiming.isCompletedExceptionally()) {
            reclaiming = opening.thenCompose(unused -> gc.mark(control, tasks, writers, worker))
                    .thenCompose(unused -> CompletableFuture.runAsync(
                            () -> {}, CompletableFuture.delayedExecutor(graceMillis, TimeUnit.MILLISECONDS, worker)))
                    .thenCompose(unused -> gc.requireElapsed())
                    .thenCompose(unused -> reconcileOriginalDelete());
        }
        return reclaiming.copy();
    }

    private CompletableFuture<VersionedValue> reconcileOriginalDelete() {
        return gc.reclaimBookKeeperAfterElapsed(
                        control, tasks, writers, release, ReclamationReason.REPLACED_REPRESENTATION)
                .exceptionallyCompose(failure -> {
                    Throwable cause = failure;
                    while (cause instanceof CompletionException && cause.getCause() != null) {
                        cause = cause.getCause();
                    }
                    if (!(cause instanceof PulsarNativeBookKeeperDeleteV1.OriginalDeleteUnsettledException)) {
                        return CompletableFuture.failedFuture(failure);
                    }
                    // The GC scope keeps its original target and only observes the retained reservation.
                    return CompletableFuture.completedFuture(null)
                            .thenComposeAsync(
                                    unused -> reconcileOriginalDelete(),
                                    CompletableFuture.delayedExecutor(250, TimeUnit.MILLISECONDS, worker));
                });
    }

    /** Releases the source scan only after every original physical and metadata scope confirms its terminal. */
    public synchronized CompletableFuture<Void> closeAsync() {
        if (reclaiming != null && !reclaiming.isDone()) {
            return reclaiming.thenCompose(unused -> closeAsync());
        }
        if (reclaiming != null && reclaiming.isCompletedExceptionally()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Original reclamation must reconcile before releasing its scopes"));
        }
        if (closing == null || closing.isCompletedExceptionally()) {
            var original = reclaiming == null ? opening : reclaiming;
            closing = (original == null
                            ? CompletableFuture.<Void>completedFuture(null)
                            : original.handle((unused, failure) -> (Void) null))
                    .thenComposeAsync(
                            unused -> gc == null ? CompletableFuture.completedFuture(null) : gc.closeAsync(), worker)
                    .thenCompose(unused -> tasks == null ? CompletableFuture.completedFuture(null) : tasks.closeAsync())
                    .thenCompose(
                            unused -> cursors == null ? CompletableFuture.completedFuture(null) : cursors.closeAsync())
                    .thenCompose(unused -> unresolvedScanKey == null
                            ? CompletableFuture.completedFuture(scan)
                            : resolveOriginalScan().thenApply(exact -> scan = exact))
                    .thenCompose(exact -> exact == null
                            ? CompletableFuture.completedFuture(null)
                            : trim.abortSourceScan(exact, attempt).thenAccept(result -> {
                                if (result.exactAuthority().isEmpty()) {
                                    throw new IllegalStateException("Original native source-scan close is unresolved");
                                }
                            }));
        }
        return closing.copy();
    }
}
