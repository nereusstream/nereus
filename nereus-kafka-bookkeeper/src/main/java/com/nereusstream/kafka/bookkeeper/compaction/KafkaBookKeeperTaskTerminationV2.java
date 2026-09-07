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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperTaskTerminalV2.DrainedPart;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.retention.M5TaskSelectionCoordinatorV2;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/** Closes native creation, resolves selection, recovers old native writers and persists the cancelled physical cut. */
public final class KafkaBookKeeperTaskTerminationV2 {
    public enum Outcome {
        TERMINATED_UNPUBLISHED,
        SELECTED_VETO,
        RETAIN_UNKNOWN
    }

    public record Result(Outcome outcome, Optional<KafkaBookKeeperTaskTerminalV2> terminal) {
        public Result {
            Objects.requireNonNull(outcome, "outcome");
            terminal = Objects.requireNonNull(terminal, "terminal");
            if ((outcome == Outcome.TERMINATED_UNPUBLISHED) != terminal.isPresent()) {
                throw new IllegalArgumentException("only terminated unpublished result has a physical cut");
            }
        }
    }

    private final CanonicalControlMetadataStore metadata;
    private final int shardId;
    private final KafkaBookKeeperInventoryV2.Task task;
    private final M5BookKeeperNativeCreateClientV2 nativeClient;
    private final Executor owner;

    public KafkaBookKeeperTaskTerminationV2(
            CanonicalControlMetadataStore rawMetadata,
            int shardId,
            KafkaBookKeeperInventoryV2.Task task,
            M5BookKeeperNativeCreateClientV2 nativeClient,
            Executor owner) {
        this.metadata = Objects.requireNonNull(rawMetadata, "rawMetadata");
        this.shardId = shardId;
        this.task = Objects.requireNonNull(task, "task");
        this.nativeClient = Objects.requireNonNull(nativeClient, "nativeClient");
        this.owner = Objects.requireNonNull(owner, "owner");
        KafkaBookKeeperTaskTerminalV2.requireNativeScope(task, nativeClient.spec());
    }

    /** A veto/unknown result retains all protections; this operation never deletes or releases a GC ticket. */
    public CompletionStage<Result> terminate() {
        return nativeClient.fenceCreates().thenComposeAsync(ignored -> afterCreateFence(), owner);
    }

    private CompletionStage<Result> afterCreateFence() {
        if (!metadata.get(KafkaBookKeeperInventoryV2.taskKey(task.taskIdSha256()))
                .equals(Optional.of(KafkaBookKeeperInventoryCodecV2.encodeTask(task)))) {
            throw new IllegalStateException("BK termination lacks its exact immutable task registration");
        }
        var source = M5MaterializationCodecV1.decodeSourceCut(task.sourceCut());
        var decisions = new M5TaskSelectionCoordinatorV2(
                metadata, shardId, source.identity().binding());
        var decision = decisions.readDecision(task.taskIdSha256());
        if (decision.isEmpty()) {
            var attempt = decisions.prepare(task.taskIdSha256());
            if (attempt.isEmpty()) {
                return completed(Outcome.RETAIN_UNKNOWN);
            }
            decision = attempt.orElseThrow().cancelSelection(source.predecessorSelector());
        }
        if (decision.isEmpty()) {
            return completed(Outcome.RETAIN_UNKNOWN);
        }
        if (decision.orElseThrow().outcome() == M5TaskSelectionDecisionV2.Outcome.SELECTED) {
            return completed(Outcome.SELECTED_VETO);
        }
        if (!decisions.archiveCurrentDecision(task.taskIdSha256())) {
            return completed(Outcome.RETAIN_UNKNOWN);
        }
        var existing = metadata.get(KafkaBookKeeperTaskTerminalV2.key(task.taskIdSha256()));
        if (existing.isPresent()) {
            var terminal = KafkaBookKeeperTaskTerminalV2.decode(existing.orElseThrow());
            if (!terminal.task().equals(task)
                    || !terminal.nativeCreateScope().equals(nativeClient.spec())
                    || !terminal.selection().equals(decision.orElseThrow())) {
                throw new IllegalStateException("BK terminal conflicts with the exact cancelled native task");
            }
            return CompletableFuture.completedFuture(new Result(Outcome.TERMINATED_UNPUBLISHED, Optional.of(terminal)));
        }
        var inventory = new KafkaBookKeeperInventoryV2(metadata, owner);
        // Physical create is strictly after durable part registration. The native fence therefore fixes this cut.
        List<Optional<KafkaBookKeeperInventoryV2.Part>> cut = new ArrayList<>();
        for (int ordinal = 0; ordinal < task.parts().size(); ordinal++) {
            cut.add(inventory.readPart(task, ordinal));
        }
        var session = nativeClient.newSession();
        CompletionStage<List<DrainedPart>> drained = CompletableFuture.completedFuture(new ArrayList<>());
        for (var part : cut) {
            drained = drained.thenComposeAsync(
                    parts -> drain(part, session).thenApply(value -> {
                        parts.add(value);
                        return parts;
                    }),
                    owner);
        }
        var exactDecision = decision.orElseThrow();
        var settled = drained.handle((parts, failure) -> new DrainResult(parts, failure))
                .thenCompose(result -> session.closeAsync().thenApply(ignored -> result));
        return settled.thenApplyAsync(
                result -> {
                    if (result.failure() != null) {
                        return new Result(Outcome.RETAIN_UNKNOWN, Optional.empty());
                    }
                    var terminal =
                            new KafkaBookKeeperTaskTerminalV2(task, nativeClient.spec(), exactDecision, result.parts());
                    String key = KafkaBookKeeperTaskTerminalV2.key(task.taskIdSha256());
                    metadata.putIfAbsent(key, terminal.encode());
                    var observed = metadata.get(key);
                    if (observed.isEmpty()) {
                        return new Result(Outcome.RETAIN_UNKNOWN, Optional.empty());
                    }
                    var recorded = KafkaBookKeeperTaskTerminalV2.decode(observed.orElseThrow());
                    if (!recorded.equals(terminal)) {
                        throw new IllegalStateException(
                                "BK terminal reconciliation differs from the drained physical cut");
                    }
                    return new Result(Outcome.TERMINATED_UNPUBLISHED, Optional.of(recorded));
                },
                owner);
    }

    private CompletionStage<DrainedPart> drain(
            Optional<KafkaBookKeeperInventoryV2.Part> possible, RealBookKeeperCellSessionV1 session) {
        if (possible.isEmpty()) {
            return CompletableFuture.completedFuture(new DrainedPart(Optional.empty(), Optional.empty()));
        }
        var handle = possible.orElseThrow().handle();
        return session.openRunLedger(handle).thenCompose(open -> {
            if (open.outcome() == RunLedgerOpenOutcomeV1.ABSENT) {
                return CompletableFuture.completedFuture(
                        new DrainedPart(Optional.of(handle.ledgerIdentity()), Optional.empty()));
            }
            if (open.outcome() != RunLedgerOpenOutcomeV1.OPENED_EXACT) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("BK task drain cannot identify its ledger"));
            }
            return session.fenceAndRecoverRunLedger(handle).thenCompose(recovery -> {
                if (recovery.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("BK task writer drain remains unknown"));
                }
                return nativeClient.captureExactTarget(handle).thenApply(capture -> {
                    if (capture.outcome() != CaptureOutcome.EXACT_TARGET
                            || capture.exactTarget().orElseThrow().sealedLastEntryId()
                                    != recovery.exactProof().orElseThrow().lastAddConfirmed()) {
                        throw new IllegalStateException("BK task drain lacks its exact recovered native seal");
                    }
                    return new DrainedPart(Optional.of(handle.ledgerIdentity()), capture.exactTarget());
                });
            });
        });
    }

    private record DrainResult(List<DrainedPart> parts, Throwable failure) {}

    private static CompletionStage<Result> completed(Outcome outcome) {
        return CompletableFuture.completedFuture(new Result(outcome, Optional.empty()));
    }
}
