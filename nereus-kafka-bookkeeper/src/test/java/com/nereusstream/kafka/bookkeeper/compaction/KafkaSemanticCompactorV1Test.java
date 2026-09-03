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
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Caps;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CleanupPolicy;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Disposition;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Frontiers;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.InputBatch;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.LatestKeyProof;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.LeaderEpochRange;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Policy;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.ProtocolRoots;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.TransactionOutcome;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.TransactionRange;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.MaterializationDataReader;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.VerifiedObjectRead;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class KafkaSemanticCompactorV1Test {
    @Test
    void persistedCellAdmissionReservesAllCompactionResourcesBeforeDispatch() {
        CanonicalBytes body = records(80, 12, Compression.NONE, new SimpleRecord(80, bytes("admission"), bytes("one")));
        Fixture fixture = new Fixture(List.of(body), 80, 81);
        CompactionPlan plan = fixture.plan(
                List.of(proof("admission", 80, 1_000, false)), List.of(), List.of(), 100, CompressionType.NONE);
        KafkaCompactionAdmissionV1 admission =
                new KafkaCompactionAdmissionV1(new Store(), fixture.envelope.protocolCellSha256());
        KafkaCompactionAdmissionV1.CellCaps caps = new KafkaCompactionAdmissionV1.CellCaps(
                1, 1_000_000, 10, 100, 100, 1_000, 1_000_000, 1_000_000, 100, 100, 10_000, 1_000_000, 100, 100, 100,
                100);
        assertThat(admission.install(caps)).isEqualTo(KafkaCompactionAdmissionV1.Outcome.APPLIED_EXACT);
        assertThat(admission.reserve(plan, 1_000, 1_000, 5, 0, 9, 1, 9, 2))
                .isEqualTo(KafkaCompactionAdmissionV1.Outcome.APPLIED_EXACT);
        assertThat(admission.reserve(plan, 1_000, 1_000, 5, 0, 9, 1, 9, 2))
                .isEqualTo(KafkaCompactionAdmissionV1.Outcome.EXISTING_EXACT);
        assertThat(KafkaCompactionAdmissionV1.decode(
                        KafkaCompactionAdmissionV1.encode(admission.read().orElseThrow())))
                .isEqualTo(admission.read().orElseThrow());
        assertThat(admission.reserve(plan, 2_000, 1_000, 5, 0, 9, 1, 9, 2))
                .isEqualTo(KafkaCompactionAdmissionV1.Outcome.CONFLICT);
        Sha256Digest task = admission.read().orElseThrow().reservations().get(0).compactionTaskIdSha256();
        assertThat(admission.release(task)).isEqualTo(KafkaCompactionAdmissionV1.Outcome.APPLIED_EXACT);
        assertThat(admission.read().orElseThrow().reservations()).isEmpty();
    }

    @Test
    void acceptsKafkaProducedEmptyBatchAndEmitsAuthoritativeNoDataGap() {
        CanonicalBytes empty = emptyBatch(70, 11);
        assertThat(KafkaRecordBatchCodecV1.parse(empty).records()).isEmpty();
        Fixture fixture = new Fixture(List.of(empty), 70, 71);
        CompactionPlan plan = fixture.plan(List.of(), List.of(), List.of(), 100, CompressionType.NONE);

        KafkaSemanticCompactorV1.Result result = new KafkaSemanticCompactorV1().compact(plan);
        assertThat(result.candidate().outputBatches()).isEmpty();
        assertThat(result.candidate().nms1().payload()).isEqualTo(CanonicalBytes.empty());
        assertThat(result.candidate().gaps()).singleElement().satisfies(gap -> {
            assertThat(gap.inclusiveStart()).isEqualTo(70);
            assertThat(gap.exclusiveEnd()).isEqualTo(71);
        });
        assertThat(result.candidate()
                        .indexes()
                        .get(IndexKind.CHECKSUM_COVERAGE.ordinal())
                        .rows())
                .singleElement()
                .satisfies(row -> assertThat(row.flags() & KafkaCompactionIndexV1.FLAG_GAP)
                        .isNotZero());
    }

    @Test
    void compactsAcrossBatchesWithSparseAndNoDataCoverageAtExactTombstoneBoundary() {
        CanonicalBytes first = records(
                0,
                3,
                Compression.NONE,
                new SimpleRecord(10, bytes("a"), bytes("a-old")),
                new SimpleRecord(11, null, bytes("null-key")),
                new SimpleRecord(12, bytes("b"), bytes("b-old")));
        CanonicalBytes second = records(
                3,
                3,
                Compression.gzip().build(),
                new SimpleRecord(13, bytes("a"), bytes("a-new")),
                new SimpleRecord(14, bytes("b"), null),
                new SimpleRecord(15, bytes("c"), null));
        CanonicalBytes emptyOutput = records(
                6,
                3,
                Compression.NONE,
                new SimpleRecord(16, bytes("d"), bytes("d-old")),
                new SimpleRecord(17, bytes("e"), null));
        Fixture fixture = new Fixture(List.of(first, second, emptyOutput), 0, 8);
        CompactionPlan plan = fixture.plan(
                List.of(
                        proof("a", 3, 100, false),
                        proof("b", 4, 100, false),
                        proof("c", 5, 101, false),
                        proof("d", 9, 200, false),
                        proof("e", 7, 100, false)),
                List.of(),
                List.of(),
                100,
                CompressionType.NONE);

        KafkaSemanticCompactorV1.Result result = new KafkaSemanticCompactorV1().compact(plan);

        assertThat(result.candidate().outputBatches())
                .flatExtracting(batch ->
                        batch.records().stream().map(record -> record.offset()).toList())
                .containsExactly(1L, 3L, 5L);
        assertThat(result.candidate().dispositions())
                .extracting(row -> row.offset() + ":" + row.disposition())
                .containsExactly(
                        "0:DROP_SUPERSEDED_VALUE",
                        "1:KEEP_NULL_KEY",
                        "2:DROP_SUPERSEDED_VALUE",
                        "3:KEEP_KEY_LATEST",
                        "4:DROP_EXPIRED_TOMBSTONE",
                        "5:KEEP_TOMBSTONE_WITHIN_RETENTION",
                        "6:DROP_SUPERSEDED_VALUE",
                        "7:DROP_EXPIRED_TOMBSTONE");
        assertThat(result.candidate().gaps())
                .extracting(gap -> gap.inclusiveStart() + ":" + gap.exclusiveEnd())
                .containsExactly("0:1", "2:3", "4:5", "6:8");
        assertThat(result.candidate().batchOutputs().get(2).output()).isEmpty();
        assertThat(result.candidate().indexes())
                .extracting(KafkaCompactionIndexV1::kind)
                .containsExactly(IndexKind.values());
        assertThat(result.candidate()
                        .indexes()
                        .get(IndexKind.OFFSET_OR_POSITION.ordinal())
                        .lookup(2))
                .contains(result.candidate().indexes().get(0).rows().get(1));
        assertThat(result.candidate()
                        .indexes()
                        .get(IndexKind.OFFSET_OR_POSITION.ordinal())
                        .lookup(6))
                .isEmpty();
        assertThat(result.candidate()
                        .indexes()
                        .get(IndexKind.TIMESTAMP.ordinal())
                        .listOffset(13))
                .hasValue(3);

        KafkaCompactionSuppressionV1 suppression =
                new KafkaCompactionSuppressionV1(result.candidate().dispositions());
        assertThat(suppression.rootSha256()).isEqualTo(result.semanticProof().compactionSuppressionRootSha256());
        assertThat(suppression.filterFallback(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L), value -> value))
                .containsExactly(1L, 3L, 5L);
    }

    @Test
    void preservesIdempotentProducerSequencesAcrossPartialSparseRewrite() {
        CanonicalBytes producer = idempotentRecords(
                20,
                4,
                91,
                (short) 3,
                10,
                new SimpleRecord(20, bytes("p"), bytes("old")),
                new SimpleRecord(21, null, bytes("keep")),
                new SimpleRecord(22, bytes("p"), bytes("latest")));
        Fixture fixture = new Fixture(List.of(producer), 20, 23);
        CompactionPlan plan =
                fixture.plan(List.of(proof("p", 22, 1_000, false)), List.of(), List.of(), 100, CompressionType.GZIP);

        KafkaSemanticCompactorV1.Result result = new KafkaSemanticCompactorV1().compact(plan);
        var output = result.candidate().outputBatches().get(0);
        assertThat(output.records()).extracting(record -> record.offset()).containsExactly(21L, 22L);
        assertThat(output.records()).extracting(record -> record.sequence()).containsExactly(11, 12);
        assertThat(output.producerId()).isEqualTo(91);
        assertThat(output.producerEpoch()).isEqualTo((short) 3);
        assertThat(output.baseSequence()).isEqualTo(10);
        assertThat(output.compressionType()).isEqualTo(CompressionType.GZIP);
    }

    @Test
    void retainsTransactionalDataAndExactControlMarkerWithAbortedAndLeaderEpochIndexes() {
        CanonicalBytes transactional =
                transactionalRecords(30, 7, 55, (short) 2, 40, new SimpleRecord(30, bytes("tx"), bytes("one")));
        CanonicalBytes abort = control(31, 7, 55, (short) 2, ControlRecordType.ABORT, 9);
        Fixture fixture = new Fixture(List.of(transactional, abort), 30, 32);
        List<TransactionRange> transactions = List.of(new TransactionRange(55, 30, 32, TransactionOutcome.ABORTED, 9));
        CompactionPlan plan = fixture.plan(
                List.of(proof("tx", 30, 1_000, false)), transactions, List.of(), 100, CompressionType.GZIP);

        KafkaSemanticCompactorV1.Result result = new KafkaSemanticCompactorV1().compact(plan);
        assertThat(result.candidate().dispositions())
                .extracting(row -> row.disposition())
                .containsOnly(Disposition.KEEP_TRANSACTION_OR_CONTROL);
        assertThat(result.candidate().outputBatches().get(1).canonicalBody()).isEqualTo(abort);
        assertThat(result.candidate()
                        .indexes()
                        .get(IndexKind.TRANSACTION.ordinal())
                        .rows())
                .hasSize(2);
        assertThat(result.candidate()
                        .indexes()
                        .get(IndexKind.ABORTED_TRANSACTION.ordinal())
                        .rows())
                .hasSize(2);
        assertThat(result.candidate()
                        .indexes()
                        .get(IndexKind.LEADER_EPOCH.ordinal())
                        .rows())
                .allMatch(row -> row.leaderEpoch() == 7);
    }

    @Test
    void semanticProofFeedsM5PublicationEnvelopeAndBindsSuppressionRoot() throws Exception {
        CanonicalBytes body = records(
                40,
                8,
                Compression.NONE,
                new SimpleRecord(40, bytes("x"), bytes("old")),
                new SimpleRecord(41, bytes("x"), bytes("new")));
        Fixture fixture = new Fixture(List.of(body), 40, 42);
        CompactionPlan plan =
                fixture.plan(List.of(proof("x", 41, 1_000, false)), List.of(), List.of(), 100, CompressionType.NONE);
        KafkaSemanticCompactorV1.Result result = new KafkaSemanticCompactorV1().compact(plan);
        var created = new KafkaM5CompactionBridgeV1.CreatedGeneration(
                List.of(result.candidate().payloadObject()), result.candidate().indexObjects());
        Map<String, CanonicalBytes> objects = new LinkedHashMap<>();
        objects.put(
                result.candidate().payloadObject().identity().key(),
                result.candidate().payloadCandidate().canonicalBody());
        for (var candidate : result.candidate().indexCandidates()) {
            objects.put(candidate.descriptor().identity().key(), candidate.canonicalBody());
        }
        MaterializationDataReader reader = new Reader(objects, fixture.sourceBody, fixture.source);

        var validated = new KafkaM5CompactionBridgeV1()
                .validateForPublication(
                        plan,
                        result,
                        created,
                        fixture.selector,
                        List.of(fixture.protection),
                        reader,
                        () -> new KafkaCompactionPublicationFenceV1().expected(plan));
        assertThat(validated.generation().semanticProofRootSha256())
                .contains(result.semanticProof().semanticValidationRootSha256());
        assertThat(validated.manifestView().compactionSuppressionRootSha256())
                .contains(result.semanticProof().compactionSuppressionRootSha256());
        assertThat(validated.generation().protocolStateRootSha256())
                .isEqualTo(result.semanticProof().protocolStateRootSha256());

        KafkaCompactionPublicationFenceV1.Snapshot expected = new KafkaCompactionPublicationFenceV1().expected(plan);
        assertThatThrownBy(() -> new KafkaM5CompactionBridgeV1()
                        .validateForPublication(
                                plan,
                                result,
                                created,
                                fixture.selector,
                                List.of(fixture.protection),
                                reader,
                                () -> new KafkaCompactionPublicationFenceV1.Snapshot(
                                        expected.compactionPlanRootSha256(),
                                        expected.protocolStateRootSha256(),
                                        expected.policyGeneration() + 1,
                                        expected.frontiers())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stale");
    }

    @Test
    void failsClosedForOpenTransactionsCapsAndMutatedRebuiltIndexes() {
        CanonicalBytes body = records(
                50,
                9,
                Compression.NONE,
                new SimpleRecord(50, bytes("z"), bytes("old")),
                new SimpleRecord(51, bytes("z"), bytes("new")));
        Fixture fixture = new Fixture(List.of(body), 50, 52);
        assertThatThrownBy(() -> fixture.plan(
                        List.of(proof("z", 51, 1_000, false)),
                        List.of(new TransactionRange(1, 50, 52, TransactionOutcome.OPEN, 0)),
                        List.of(),
                        100,
                        CompressionType.NONE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("open transaction");

        CompactionPlan capped = fixture.plan(
                List.of(proof("z", 51, 1_000, false)),
                List.of(),
                List.of(),
                100,
                CompressionType.NONE,
                new Caps(1, 8, 100, 100, 1_000, 1_000_000, 1_000_000, 100, 100));
        assertThatThrownBy(() -> new KafkaSemanticCompactorV1().compact(capped))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dirty bytes");

        KafkaSemanticCompactorV1.Result result = new KafkaSemanticCompactorV1()
                .compact(fixture.plan(
                        List.of(proof("z", 51, 1_000, false)), List.of(), List.of(), 100, CompressionType.NONE));
        byte[] corrupt = result.candidate().indexBodies().get(0).toByteArray();
        corrupt[0] ^= 1;
        assertThatThrownBy(() -> KafkaCompactionIndexV1.decode(CanonicalBytes.copyOf(corrupt)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void outputAndAllRootsAreDeterministic() {
        CanonicalBytes body = records(
                60,
                10,
                Compression.gzip().build(),
                new SimpleRecord(60, bytes("q"), bytes("old")),
                new SimpleRecord(61, bytes("q"), bytes("new")));
        Fixture fixture = new Fixture(List.of(body), 60, 62);
        CompactionPlan plan =
                fixture.plan(List.of(proof("q", 61, 1_000, false)), List.of(), List.of(), 100, CompressionType.GZIP);
        KafkaSemanticCompactorV1.Result first = new KafkaSemanticCompactorV1().compact(plan);
        KafkaSemanticCompactorV1.Result second = new KafkaSemanticCompactorV1().compact(plan);
        assertThat(first).isEqualTo(second);
        assertThat(first.candidate().payloadCandidate().canonicalBody())
                .isEqualTo(second.candidate().payloadCandidate().canonicalBody());
    }

    private static final class Fixture {
        private final List<CanonicalBytes> batches;
        private final CanonicalBytes sourceBody;
        private final BindingIdentity binding = new BindingIdentity(
                new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage-epoch"));
        private final CapabilityBinding capability = new CapabilityBinding(1, digest("capability"));
        private final BindingReadSelector selector;
        private final IdentityEnvelope envelope;
        private final SourceExtent source;
        private final SourceProtectionIdentity protection;
        private final MaterializationSourceCut cut;
        private final long start;
        private final long end;

        private Fixture(List<CanonicalBytes> batches, long start, long end) {
            this.batches = List.copyOf(batches);
            this.start = start;
            this.end = end;
            this.sourceBody = concatenate(batches);
            this.selector = new BindingReadSelector(
                    binding,
                    digest("predecessor"),
                    1,
                    1,
                    7,
                    SelectorMode.PREFERRED_ONLY,
                    AdmissionState.ADMITTING,
                    Optional.empty(),
                    capability,
                    List.of(),
                    List.of());
            this.envelope = new IdentityEnvelope(digest("cell"), digest("provider"), binding, 1, 1, 1, capability);
            List<KafkaCompactionRecordsV1.ParsedBatch> parsed =
                    batches.stream().map(KafkaRecordBatchCodecV1::parse).toList();
            int recordCount =
                    parsed.stream().mapToInt(batch -> batch.records().size()).sum();
            long minTimestamp = parsed.stream()
                    .flatMap(batch -> batch.records().stream())
                    .mapToLong(record -> record.timestamp())
                    .min()
                    .orElse(-1);
            long maxTimestamp = parsed.stream()
                    .flatMap(batch -> batch.records().stream())
                    .mapToLong(record -> record.timestamp())
                    .max()
                    .orElse(-1);
            this.source = new SourceExtent(
                    SourceKind.BOOKKEEPER_LEDGER,
                    digest("source-" + start),
                    new ProtocolCoverage(PositionDomain.KAFKA_OFFSET, start, end),
                    "cell/source/ledger-1",
                    sourceBody.length(),
                    recordCount,
                    minTimestamp,
                    maxTimestamp,
                    Sha256Digest.hash(sourceBody),
                    Optional.empty(),
                    Optional.of(digest("ledger")),
                    digest("nbke2-root"),
                    digest("encryption"),
                    false,
                    false,
                    List.of(binding.bindingId().digest()));
            this.protection = new SourceProtectionIdentity(source.sourceIdentitySha256(), 1, 1, 7, capability);
            this.cut = new MaterializationSourceCut(
                    envelope,
                    selector,
                    Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selector)),
                    selector.selectedViewSha256(),
                    source.coverage(),
                    end,
                    end,
                    end,
                    end,
                    start,
                    digest("protocol-state"),
                    digest("recovery"),
                    digest("materialization-policy"),
                    digest("output-policy"),
                    com.nereusstream.storage.object.materialization.M5MaterializationCodecV1.calculateSourceSetSha256(
                            List.of(source)),
                    List.of(source));
        }

        private CompactionPlan plan(
                List<LatestKeyProof> proofs,
                List<TransactionRange> transactions,
                List<Long> undecidable,
                long capturedNow,
                CompressionType compression) {
            return plan(
                    proofs,
                    transactions,
                    undecidable,
                    capturedNow,
                    compression,
                    new Caps(
                            64 * 1024 * 1024,
                            128,
                            10_000,
                            10_000,
                            16 * 1024 * 1024,
                            64 * 1024 * 1024,
                            64 * 1024 * 1024,
                            1_000,
                            10_000));
        }

        private CompactionPlan plan(
                List<LatestKeyProof> proofs,
                List<TransactionRange> transactions,
                List<Long> undecidable,
                long capturedNow,
                CompressionType compression,
                Caps caps) {
            List<InputBatch> inputs = new ArrayList<>();
            for (int index = 0; index < batches.size(); index++) {
                inputs.add(new InputBatch(source.sourceIdentitySha256(), index, batches.get(index)));
            }
            proofs = proofs.stream()
                    .sorted(Comparator.comparing(value -> value.key().toHex()))
                    .toList();
            List<LeaderEpochRange> epochs = batches.stream()
                    .map(KafkaRecordBatchCodecV1::parse)
                    .map(batch -> batch.partitionLeaderEpoch())
                    .distinct()
                    .sorted()
                    .map(epoch -> new LeaderEpochRange(epoch, start, end))
                    .toList();
            return new CompactionPlan(
                    cut,
                    new Policy(
                            1,
                            (byte) 2,
                            CleanupPolicy.COMPACT_DELETE,
                            500_000,
                            1_000,
                            capturedNow,
                            1,
                            "nereus-m5-b-v1",
                            "apache-kafka-3.9.0",
                            compression,
                            caps),
                    new Frontiers(start, end, end, end, end, start, end),
                    new ProtocolRoots(
                            digest("producer"),
                            digest("speculative"),
                            digest("transactions"),
                            digest("aborted"),
                            digest("leader-epoch"),
                            digest("timestamp"),
                            digest("recovery"),
                            digest("active-tail"),
                            digest("key-domain")),
                    inputs,
                    proofs,
                    transactions,
                    epochs,
                    undecidable.stream().sorted().toList(),
                    List.of());
        }
    }

    private record Reader(Map<String, CanonicalBytes> objects, CanonicalBytes sourceBody, SourceExtent source)
            implements MaterializationDataReader {
        @Override
        public VerifiedObjectRead readObject(ObjectIdentity identity) {
            return new VerifiedObjectRead(objects.get(identity.key()), Optional.empty());
        }

        @Override
        public CanonicalBytes readSource(SourceExtent requested) {
            assertThat(requested).isEqualTo(source);
            return sourceBody;
        }
    }

    private static final class Store implements CanonicalControlMetadataStore {
        private final Map<String, CanonicalBytes> values = new LinkedHashMap<>();

        @Override
        public Optional<CanonicalBytes> get(String key) {
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes exactValue) {
            return values.putIfAbsent(key, exactValue) == null
                    ? ControlMutationOutcome.APPLIED
                    : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }

        @Override
        public ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes exactCandidate) {
            CanonicalBytes current = values.get(key);
            if (!Objects.equals(current, exactExpected.orElse(null))) {
                return ControlMutationOutcome.DEFINITIVE_CONFLICT;
            }
            values.put(key, exactCandidate);
            return ControlMutationOutcome.APPLIED;
        }
    }

    private static LatestKeyProof proof(String key, long latest, long deadline, boolean olderMayReappear) {
        return new LatestKeyProof(bytesValue(key), latest, true, true, olderMayReappear, deadline);
    }

    private static CanonicalBytes records(
            long baseOffset, int leaderEpoch, Compression compression, SimpleRecord... records) {
        return canonical(MemoryRecords.withRecords(
                RecordBatch.MAGIC_VALUE_V2,
                baseOffset,
                compression,
                TimestampType.CREATE_TIME,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                leaderEpoch,
                false,
                records));
    }

    private static CanonicalBytes idempotentRecords(
            long baseOffset,
            int leaderEpoch,
            long producerId,
            short producerEpoch,
            int baseSequence,
            SimpleRecord... records) {
        return canonical(MemoryRecords.withIdempotentRecords(
                RecordBatch.MAGIC_VALUE_V2,
                baseOffset,
                Compression.NONE,
                producerId,
                producerEpoch,
                baseSequence,
                leaderEpoch,
                records));
    }

    private static CanonicalBytes transactionalRecords(
            long baseOffset,
            int leaderEpoch,
            long producerId,
            short producerEpoch,
            int baseSequence,
            SimpleRecord... records) {
        return canonical(MemoryRecords.withTransactionalRecords(
                RecordBatch.MAGIC_VALUE_V2,
                baseOffset,
                Compression.NONE,
                producerId,
                producerEpoch,
                baseSequence,
                leaderEpoch,
                records));
    }

    private static CanonicalBytes control(
            long offset,
            int leaderEpoch,
            long producerId,
            short producerEpoch,
            ControlRecordType type,
            int coordinatorEpoch) {
        return canonical(MemoryRecords.withEndTransactionMarker(
                offset, 100, leaderEpoch, producerId, producerEpoch, new EndTransactionMarker(type, coordinatorEpoch)));
    }

    private static CanonicalBytes emptyBatch(long offset, int leaderEpoch) {
        ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        DefaultRecordBatch.writeEmptyHeader(
                buffer,
                RecordBatch.MAGIC_VALUE_V2,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                offset,
                offset,
                leaderEpoch,
                TimestampType.CREATE_TIME,
                100,
                false,
                false);
        buffer.flip();
        byte[] value = new byte[buffer.remaining()];
        buffer.get(value);
        return CanonicalBytes.copyOf(value);
    }

    private static CanonicalBytes canonical(MemoryRecords records) {
        ByteBuffer buffer = records.buffer().duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return CanonicalBytes.copyOf(bytes);
    }

    private static CanonicalBytes concatenate(List<CanonicalBytes> values) {
        int size = values.stream().mapToInt(CanonicalBytes::length).sum();
        byte[] result = new byte[size];
        int offset = 0;
        for (CanonicalBytes value : values) {
            value.copyTo(result, offset);
            offset += value.length();
        }
        return CanonicalBytes.copyOf(result);
    }

    private static byte[] bytes(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    private static CanonicalBytes bytesValue(String value) {
        return CanonicalBytes.copyOf(bytes(value));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(bytesValue(value));
    }
}
