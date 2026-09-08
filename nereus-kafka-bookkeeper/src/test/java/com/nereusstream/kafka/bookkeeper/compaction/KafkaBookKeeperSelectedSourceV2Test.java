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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureResult;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationPlannerV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/** Contract and decoding tests with synthetic provider metadata. Native tests supply transport proof separately. */
class KafkaBookKeeperSelectedSourceV2Test {
    @Test
    void selectedRecoverySharesDecodedCountAndByteLimitsAndAllowsIndexOnlyZeroBudget() {
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(false);
        assertThatThrownBy(() -> reader(fixture, 0, 10000)
                        .recover(fixture.descriptor)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("Kafka decoded record budget exhausted");
        assertThatThrownBy(() -> reader(fixture, 100, 0)
                        .recover(fixture.descriptor)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("Kafka decoded record budget exhausted");
        assertThat(reader(fixture, 1, 10000)
                        .recover(fixture.descriptor)
                        .toCompletableFuture()
                        .join()
                        .parsedBatches())
                .hasSize(1);
        var empty = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(true);
        assertThat(reader(empty, 0, 0)
                        .recover(empty.descriptor)
                        .toCompletableFuture()
                        .join()
                        .parsedBatches())
                .isEmpty();
    }

    @Test
    void typedEmptyGenerationRecompactsAsIndexesAndCannotEnterObjectMaterialization() {
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(true);
        var source = extent(fixture, 0, true, Optional.empty());
        var plan = plan(fixture.input.plan(), source, List.of());
        var encoded = M5MaterializationCodecV1.encodeSourceCut(plan.sourceCut());
        assertThat(M5MaterializationCodecV1.decodeSourceCut(encoded)).isEqualTo(plan.sourceCut());
        var semantic = new KafkaSemanticCompactorV1().compileSemantic(plan);
        assertThatThrownBy(() -> new KafkaSemanticCompactorV1().compact(plan))
                .hasMessage("compacted BK generation input requires the BookKeeper output path");
        assertThatThrownBy(() -> fixture.publication.publish(
                        plan, semantic, fixture.descriptor, List.of(), () -> new KafkaCompactionPublicationFenceV1()
                                .expected(plan)))
                .hasMessage("compacted generation input requires native physical input verification");
        assertThat(semantic.outputBatches()).isEmpty();
        assertThat(semantic.gaps()).containsExactly(new KafkaCompactionRecordsV1.Gap(0, 1));
        assertThat(semantic.indexes()).hasSize(8);
        var layout = KafkaBookKeeperCompactionLayoutV2.plan(
                plan,
                semantic,
                fixture.descriptor.task().namespace(),
                fixture.descriptor.task().capability(),
                2,
                512,
                1024);
        assertThat(layout.task().parts()).allMatch(part -> part.kind() == KafkaBookKeeperInventoryV2.PartKind.INDEX);
        var digest = source.bodySha256();
        var policy = new M5MaterializationPlannerV1.PlanningPolicy(
                M5MaterializationRecordsV1.PayloadKind.KAFKA_BATCH_PRESERVING_V1,
                true,
                digest,
                digest,
                digest,
                List.of(M5MaterializationRecordsV1.IndexKind.OFFSET_OR_POSITION),
                "cell/selected-source-test");
        assertThatThrownBy(() -> new M5MaterializationPlannerV1().plan(plan.sourceCut(), policy))
                .hasMessage("compacted BK generation input requires the M5-B semantic path");
    }

    @Test
    void generationCannotMasqueradeAsOneLedgerOrOmitIndexesAndNonemptyInputs() {
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(false);
        assertThat(SourceKind.BOOKKEEPER_LEDGER.ordinal()).isEqualTo(1);
        assertThat(SourceKind.PULSAR_NPO1_ROOT.ordinal()).isEqualTo(3);
        assertThat(SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2.ordinal()).isEqualTo(4);
        assertThatThrownBy(() -> extent(fixture, 1, true, Optional.of(fixture.descriptor.descriptorSha256())))
                .hasMessage("non-BookKeeper source carries a ledger identity");
        assertThatThrownBy(() -> extent(fixture, 1, false, Optional.empty()))
                .hasMessage("compacted BK generation requires complete single-Binding indexes");
        assertThatThrownBy(() -> plan(fixture.input.plan(), extent(fixture, 1, true, Optional.empty()), List.of()))
                .hasMessage("M5-B input batch count exceeds its cap");
        assertThatThrownBy(() -> plan(
                        fixture.input.plan(),
                        fixture.input.plan().sourceCut().sources().get(0),
                        List.of()))
                .hasMessage("M5-B input batch count exceeds its cap");
    }

    private static KafkaSealedBookKeeperReaderV2 reader(
            KafkaSealedBookKeeperDescriptorV2Test.Fixture fixture, int records, long bytes) {
        return new KafkaSealedBookKeeperReaderV2(
                fixture.session,
                handle -> CompletableFuture.completedFuture(new CaptureResult(
                        CaptureOutcome.EXACT_TARGET,
                        Optional.of(fixture.seals.get(handle.ledgerIdentity().ledgerId())))),
                1000000,
                new KafkaSealedBookKeeperReaderV2.DecodingBounds(records, bytes),
                Runnable::run);
    }

    private static SourceExtent extent(
            KafkaSealedBookKeeperDescriptorV2Test.Fixture fixture,
            int records,
            boolean indexes,
            Optional<com.nereusstream.domain.bytes.Sha256Digest> ledger) {
        var descriptor = fixture.descriptor;
        var hash = descriptor.descriptorSha256();
        return new SourceExtent(
                SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2,
                hash,
                descriptor.sourceCut().coverage(),
                KafkaBookKeeperCompactionPublicationV2.descriptorKey(hash),
                descriptor.encode().length(),
                records,
                records == 0 ? -1 : 1,
                records == 0 ? -1 : 2,
                hash,
                Optional.empty(),
                ledger,
                hash,
                hash,
                true,
                indexes,
                List.of(descriptor.sourceCut().identity().binding().bindingId().digest()));
    }

    private static KafkaCompactionRecordsV1.CompactionPlan plan(
            KafkaCompactionRecordsV1.CompactionPlan previous,
            SourceExtent source,
            List<KafkaCompactionRecordsV1.InputBatch> batches) {
        var old = previous.sourceCut();
        var sources = List.of(source);
        var cut = new M5MaterializationRecordsV1.MaterializationSourceCut(
                old.identity(),
                old.predecessorSelector(),
                old.predecessorSelectorValueSha256(),
                old.predecessorViewSha256(),
                old.coverage(),
                old.durableFrontier(),
                old.logEndFrontier(),
                old.highWatermark(),
                old.lastStableFrontier(),
                old.trimFrontier(),
                old.protocolStateRootSha256(),
                old.recoveryCheckpointRootSha256(),
                old.materializationPolicySha256(),
                old.outputFormatPolicySha256(),
                M5MaterializationCodecV1.calculateSourceSetSha256(sources),
                sources);
        return new KafkaCompactionRecordsV1.CompactionPlan(
                cut,
                previous.policy(),
                previous.frontiers(),
                previous.protocolRoots(),
                batches,
                previous.keyProofs(),
                previous.transactions(),
                previous.leaderEpochs(),
                previous.undecidableOffsets(),
                previous.recoveryRequiredOffsets());
    }
}
