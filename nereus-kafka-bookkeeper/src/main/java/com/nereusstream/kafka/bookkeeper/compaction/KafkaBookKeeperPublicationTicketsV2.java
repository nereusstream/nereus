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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Completion;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/** Concrete BK publication tickets; input membership must be supplied by the admitted source owner. */
public final class KafkaBookKeeperPublicationTicketsV2 {
    /**
     * Verify the complete plan input list against native source bytes and resolve every frozen logical source to all
     * physical members, under its existing source protection. The protocol owner must derive this mapping from actual
     * immutable source metadata; source extents alone do not prove the plan supplied every batch in exact order.
     * Missing native source adapters must fail rather than return an empty or guessed physical membership.
     */
    @FunctionalInterface
    public interface InputMembership {
        CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolve(CompactionPlan plan);
    }

    private final M5TargetDeleteMultiWriterGuardV2 guard;
    private final InputMembership inputMembership;
    private final Executor owner;

    public KafkaBookKeeperPublicationTicketsV2(
            M5TargetDeleteMultiWriterGuardV2 guard, InputMembership inputMembership, Executor owner) {
        this.guard = Objects.requireNonNull(guard, "guard");
        this.inputMembership = Objects.requireNonNull(inputMembership, "inputMembership");
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    CompletionStage<PublicationOutcome> publish(
            CompactionPlan plan,
            KafkaSealedBookKeeperDescriptorV2 descriptor,
            Supplier<CompletionStage<PublicationOutcome>> publication,
            Supplier<Optional<M5TaskSelectionDecisionV2>> currentDecision) {
        if (!plan.sourceCut().equals(descriptor.sourceCut())) {
            throw new IllegalArgumentException("publication input plan differs from the descriptor source cut");
        }
        return inputMembership.resolve(plan).thenCompose(membership -> {
            var resources = targets(descriptor, membership);
            var context = context(descriptor, resources);
            return CompletableFuture.supplyAsync(() -> terminal(descriptor, currentDecision.get()), owner)
                    .thenCompose(known -> {
                        if (known.isPresent()) {
                            // This native irreversible decision also makes unfinished invocations from an old JVM
                            // harmless.
                            return guard.reconcileTerminal(
                                            resources,
                                            context,
                                            known.orElseThrow().proof())
                                    .thenApply(left -> left.isEmpty()
                                            ? known.orElseThrow().outcome()
                                            : PublicationOutcome.OUTCOME_UNKNOWN);
                        }
                        return guard.execute(resources, context, () -> CompletableFuture.supplyAsync(publication, owner)
                                        .thenCompose(stage -> stage)
                                        .handle((outcome, failure) ->
                                                failure == null ? outcome : PublicationOutcome.OUTCOME_UNKNOWN)
                                        .thenApplyAsync(
                                                outcome -> new Completion<>(
                                                        outcome,
                                                        terminal(descriptor, currentDecision.get())
                                                                .map(Terminal::proof)),
                                                owner))
                                .thenCompose(result -> {
                                    if (!result.mutationInvoked()) {
                                        return CompletableFuture.completedFuture(PublicationOutcome.CONFLICT);
                                    }
                                    // A winning native selection also makes older concurrent invocations harmless.
                                    // Reconcile their nonces only after reading that irreversible decision again.
                                    return CompletableFuture.supplyAsync(
                                                    () -> terminal(descriptor, currentDecision.get()), owner)
                                            .thenCompose(selected -> selected.isEmpty()
                                                    ? CompletableFuture.completedFuture(
                                                            PublicationOutcome.OUTCOME_UNKNOWN)
                                                    : guard.reconcileTerminal(
                                                                    resources,
                                                                    context,
                                                                    selected.orElseThrow()
                                                                            .proof())
                                                            .thenApply(left -> left.isEmpty()
                                                                    ? result.value()
                                                                            .filter(value -> value
                                                                                    == PublicationOutcome.APPLIED_EXACT)
                                                                            .orElse(selected.orElseThrow()
                                                                                    .outcome())
                                                                    : PublicationOutcome.OUTCOME_UNKNOWN))
                                            .exceptionally(failure -> PublicationOutcome.OUTCOME_UNKNOWN);
                                });
                    });
        });
    }

    static List<PhysicalResourceIdV2> targets(
            KafkaSealedBookKeeperDescriptorV2 descriptor, Map<Sha256Digest, List<PhysicalResourceIdV2>> membership) {
        Objects.requireNonNull(membership, "membership");
        var expected = descriptor.sourceCut().sources().stream()
                .map(source -> source.sourceIdentitySha256())
                .collect(java.util.stream.Collectors.toSet());
        if (!membership.keySet().equals(expected)) {
            throw new IllegalArgumentException(
                    "publication input membership is missing or has foreign logical sources");
        }
        var result = new ArrayList<PhysicalResourceIdV2>();
        int targetCount = descriptor.sealedParts().size();
        if (targetCount > M5TargetDeleteMultiWriterGuardV2.MAX_TARGETS) {
            throw new IllegalArgumentException("publication physical target count exceeds the bound");
        }
        for (var source : descriptor.sourceCut().sources()) {
            var members = Objects.requireNonNull(membership.get(source.sourceIdentitySha256()), "members");
            if (members.isEmpty()) {
                throw new IllegalArgumentException("publication input source has no physical members");
            }
            if (members.size() > M5TargetDeleteMultiWriterGuardV2.MAX_TARGETS - targetCount) {
                throw new IllegalArgumentException("publication physical target count exceeds the bound");
            }
            targetCount += members.size();
            result.addAll(List.copyOf(members));
        }
        descriptor
                .sealedParts()
                .forEach(seal -> result.add(new PhysicalResourceIdV2.BookKeeperLedger(
                        descriptor.task().namespace(),
                        seal.handle().ledgerIdentity().ledgerId())));
        return M5TargetDeleteMultiWriterGuardV2.canonicalTargets(result);
    }

    static Context context(KafkaSealedBookKeeperDescriptorV2 descriptor, List<PhysicalResourceIdV2> resources) {
        var targets = M5TargetDeleteMultiWriterGuardV2.canonicalTargets(resources);
        int length = 40;
        for (var resource : targets) {
            length += Integer.BYTES + resource.canonicalBytes().length();
        }
        var facts = ByteBuffer.allocate(length)
                .putInt(0x4d355054)
                .put(descriptor.descriptorSha256().bytes().toByteArray())
                .putInt(targets.size());
        for (var resource : targets) {
            facts.putInt(resource.canonicalBytes().length())
                    .put(resource.canonicalBytes().toByteArray());
        }
        return new Context(
                ProofBoundWriterClassV1.MANIFEST_SELECTOR_GENERATION_REPRESENTATION_V1,
                descriptor.task().capability().configurationDigest(),
                Sha256Digest.hash(M5MaterializationCodecV1.encodeSourceCut(descriptor.sourceCut())),
                Sha256Digest.hash(CanonicalBytes.copyOf(facts.array())));
    }

    private static Optional<Terminal> terminal(
            KafkaSealedBookKeeperDescriptorV2 descriptor, Optional<M5TaskSelectionDecisionV2> decision) {
        if (decision.isEmpty()) {
            return Optional.empty();
        }
        var exact = decision.orElseThrow();
        if (!exact.taskId().equals(descriptor.task().taskIdSha256())) {
            throw new IllegalArgumentException("publication native terminal has another task");
        }
        if (exact.outcome() == M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED) {
            return Optional.of(new Terminal(PublicationOutcome.CANCELLED_STALE, Sha256Digest.hash(exact.encode())));
        }
        return exact.selectedOutput().equals(Optional.of(descriptor.descriptorSha256()))
                ? Optional.of(new Terminal(PublicationOutcome.EXISTING_EXACT, Sha256Digest.hash(exact.encode())))
                : Optional.empty();
    }

    private record Terminal(PublicationOutcome outcome, Sha256Digest proof) {}
}
