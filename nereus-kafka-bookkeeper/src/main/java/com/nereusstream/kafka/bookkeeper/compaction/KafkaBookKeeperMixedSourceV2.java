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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.InputBatch;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * One selected BK generation plus sealed raw runs in the exact source-cut order. Each existing reader owns its
 * physical tickets and native session; the shared Cell budget keeps the selected result charged through raw
 * verification. Protocol-owner admission and M4 RELEASED remain separate requirements.
 */
public final class KafkaBookKeeperMixedSourceV2 implements KafkaBookKeeperPublicationTicketsV2.InputMembership {
    private final KafkaBookKeeperReadCellBudgetV2 budget;
    private final KafkaBookKeeperSelectedSourceV2 selected;
    private final KafkaBookKeeperRunSourceV2 raw;

    public KafkaBookKeeperMixedSourceV2(
            KafkaBookKeeperReadCellBudgetV2 budget,
            KafkaBookKeeperSelectedSourceV2 selected,
            KafkaBookKeeperRunSourceV2 raw) {
        this.budget = Objects.requireNonNull(budget, "budget");
        this.selected = Objects.requireNonNull(selected, "selected");
        this.raw = Objects.requireNonNull(raw, "raw");
        if (!selected.usesBudget(budget) || !raw.usesBudget(budget)) {
            throw new IllegalArgumentException("mixed BK source readers require one Cell budget owner");
        }
    }

    @Override
    public CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolve(CompactionPlan plan) {
        Objects.requireNonNull(plan, "plan");
        final Expected expected;
        final KafkaBookKeeperReadCellBudgetV2.Reservation retained;
        try {
            expected = expected(plan);
            retained = budget.reserveRetained(
                    new CellProviderScopeId(plan.sourceCut().identity().providerScopeSha256()),
                    plan.sourceCut().identity().binding(),
                    selected.retainedResultAllowance());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> work;
        try {
            work = selected.capture().thenCompose(snapshot -> {
                if (!snapshot.extent().equals(expected.selected())
                        || !snapshot.selector().equals(plan.sourceCut().predecessorSelector())
                        || !snapshot.view()
                                .descriptor()
                                .task()
                                .capability()
                                .providerScopeId()
                                .digest()
                                .equals(plan.sourceCut().identity().providerScopeSha256())
                        || !snapshot.batches().equals(expected.selectedBatches())) {
                    return CompletableFuture.failedFuture(
                            new IllegalArgumentException("selected BK source differs from complete mixed input plan"));
                }
                return raw.resolveRawSubset(plan.sourceCut(), expected.rawSources(), expected.rawBatches())
                        .thenCompose(
                                rawMembers -> selected.verifyCurrent(snapshot).thenApply(ignored -> {
                                    var namespace = snapshot.resources().get(0).namespace();
                                    Map<Sha256Digest, List<PhysicalResourceIdV2>> members = new LinkedHashMap<>();
                                    members.put(snapshot.extent().sourceIdentitySha256(), snapshot.resources());
                                    for (var source : expected.rawSources()) {
                                        var resource = rawMembers.get(source.sourceIdentitySha256());
                                        if (resource == null
                                                || resource.isEmpty()
                                                || resource.stream().anyMatch(value -> !value.namespace()
                                                        .equals(namespace))) {
                                            throw new IllegalArgumentException(
                                                    "mixed BK sources belong to different physical namespaces");
                                        }
                                        if (members.put(source.sourceIdentitySha256(), resource) != null) {
                                            throw new IllegalArgumentException(
                                                    "duplicate mixed native source identity");
                                        }
                                    }
                                    return Map.copyOf(members);
                                }));
            });
        } catch (Throwable failure) {
            retained.releaseBeforeSession();
            return CompletableFuture.failedFuture(failure);
        }
        // An observer cannot release retained bytes while native reads or ticket cleanup are still running.
        return work.whenComplete((value, failure) -> retained.releaseBeforeSession())
                .thenApply(value -> value);
    }

    private static Expected expected(CompactionPlan plan) {
        var cut = plan.sourceCut();
        if (!M5MaterializationCodecV1.calculateSourceSetSha256(cut.sources()).equals(cut.sourceSetSha256())) {
            throw new IllegalArgumentException("mixed BK source set differs from its canonical digest");
        }
        var identities = new HashSet<Sha256Digest>();
        var rawSources = new ArrayList<SourceExtent>();
        var rawBatches = new ArrayList<InputBatch>();
        var selectedBatches = new ArrayList<InputBatch>();
        SourceExtent selected = null;
        int cursor = 0;
        for (var source : cut.sources()) {
            if (!identities.add(source.sourceIdentitySha256())) {
                throw new IllegalArgumentException("duplicate mixed native source identity");
            }
            List<InputBatch> target;
            if (source.kind() == SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2 && selected == null) {
                selected = source;
                target = selectedBatches;
            } else if (source.kind() == SourceKind.BOOKKEEPER_LEDGER) {
                rawSources.add(source);
                target = rawBatches;
            } else {
                throw new IllegalArgumentException("mixed BK input requires one selected generation and raw runs");
            }
            while (cursor < plan.inputBatches().size()
                    && plan.inputBatches().get(cursor).sourceIdentitySha256().equals(source.sourceIdentitySha256())) {
                target.add(plan.inputBatches().get(cursor++));
            }
        }
        if (selected == null
                || rawSources.isEmpty()
                || cursor != plan.inputBatches().size()) {
            throw new IllegalArgumentException("mixed BK inputs are incomplete or outside source-cut order");
        }
        return new Expected(selected, List.copyOf(selectedBatches), List.copyOf(rawSources), List.copyOf(rawBatches));
    }

    private record Expected(
            SourceExtent selected,
            List<InputBatch> selectedBatches,
            List<SourceExtent> rawSources,
            List<InputBatch> rawBatches) {}
}
