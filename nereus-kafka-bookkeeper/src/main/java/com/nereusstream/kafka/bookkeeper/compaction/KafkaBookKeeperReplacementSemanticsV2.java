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
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointCodecV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticTransfer;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable comparison made under publication tickets after native input verification and independent output reads.
 * This is semantic evidence only: selection, current native authority, references and grace remain GC prerequisites.
 */
public record KafkaBookKeeperReplacementSemanticsV2(
        Sha256Digest taskId,
        Sha256Digest descriptorId,
        Sha256Digest sourceCutSha256,
        Sha256Digest publicationTargetsSha256,
        List<Comparison> comparisons) {
    private static final int MAGIC = 0x4b425253; // KBRS
    private static final int VERSION = 1;
    private static final int ENCODED_BYTES =
            12 + 5 * Sha256Digest.LENGTH + SemanticAspect.values().length * (Integer.BYTES + 2 * Sha256Digest.LENGTH);

    public record Comparison(SemanticAspect aspect, Sha256Digest priorRoot, Sha256Digest replacementRoot) {
        public Comparison {
            Objects.requireNonNull(aspect, "aspect");
            KafkaBookKeeperInventoryV2.requireDigest(priorRoot);
            KafkaBookKeeperInventoryV2.requireDigest(replacementRoot);
            if (!priorRoot.equals(replacementRoot)) {
                throw new IllegalArgumentException("replacement changed Kafka semantics: " + aspect);
            }
        }
    }

    public KafkaBookKeeperReplacementSemanticsV2 {
        KafkaBookKeeperInventoryV2.requireDigest(taskId);
        KafkaBookKeeperInventoryV2.requireDigest(descriptorId);
        KafkaBookKeeperInventoryV2.requireDigest(sourceCutSha256);
        KafkaBookKeeperInventoryV2.requireDigest(publicationTargetsSha256);
        comparisons = List.copyOf(comparisons);
        if (!comparisons.stream().map(Comparison::aspect).toList().equals(List.of(SemanticAspect.values()))) {
            throw new IllegalArgumentException("replacement comparison must cover every Kafka semantic aspect once");
        }
    }

    /** Only the ticketed publisher calls this, while the original physical membership remains protected. */
    static KafkaBookKeeperReplacementSemanticsV2 verify(
            CompactionPlan plan,
            KafkaCompactionSemanticOutputV2 semantic,
            KafkaSealedBookKeeperDescriptorV2 descriptor,
            KafkaCompactionRecoveryStateV1 priorState,
            KafkaBookKeeperReadViewV2 recovered,
            KafkaBookKeeperPublicationTargetsV2 original) {
        original.requireDescriptor(descriptor);
        priorState.requirePlan(plan);
        if (!descriptor.equals(recovered.descriptor())
                || !descriptor
                        .semanticProof()
                        .equals(new KafkaCompactionSemanticValidatorV1().validateSemantic(plan, semantic))) {
            throw new IllegalArgumentException("replacement comparison differs from the verified compaction plan");
        }
        var replacementState = recovered
                .recoveryState()
                .orElseThrow(() ->
                        new IllegalArgumentException("replacement lacks independently stored Kafka recovery state"));
        var priorSections = KafkaProtocolCheckpointCodecV1.encodeComponents(priorState.components());
        var replacementSections = KafkaProtocolCheckpointCodecV1.encodeComponents(replacementState.components());
        var rows = new ArrayList<Comparison>();
        rows.add(compare(
                SemanticAspect.READ_RESULTS,
                List.of(
                        KafkaCompactionCanonicalV1.outputRecordRoot(semantic.outputBatches())
                                .bytes(),
                        KafkaBookKeeperArtifactAssemblerV2.gapRoot(semantic.gaps())
                                .bytes()),
                List.of(
                        KafkaCompactionCanonicalV1.outputRecordRoot(recovered.parsedBatches())
                                .bytes(),
                        KafkaBookKeeperArtifactAssemblerV2.gapRoot(recovered.gaps())
                                .bytes())));
        rows.add(compare(SemanticAspect.RECOVERY, List.of(priorState.encode()), List.of(replacementState.encode())));
        rows.add(compare(
                SemanticAspect.PRODUCER_STATE,
                List.of(priorSections.producerState()),
                List.of(replacementSections.producerState())));
        rows.add(compare(
                SemanticAspect.TRANSACTIONS,
                List.of(priorSections.transactionIndex()),
                List.of(replacementSections.transactionIndex())));
        rows.add(compare(
                SemanticAspect.LEADER_EPOCH,
                List.of(priorSections.leaderEpochIndex()),
                List.of(replacementSections.leaderEpochIndex())));
        rows.add(compare(
                SemanticAspect.TIMESTAMP_LOOKUP,
                List.of(semantic.indexes().get(IndexKind.TIMESTAMP.ordinal()).encode()),
                List.of(recovered.index(IndexKind.TIMESTAMP).encode())));
        rows.add(compare(
                SemanticAspect.INDEX_COVERAGE,
                semantic.indexBodies(),
                recovered.artifacts().indexes()));
        var replacements = descriptor.sealedParts().stream()
                .map(seal -> (PhysicalResourceIdV2) new PhysicalResourceIdV2.BookKeeperLedger(
                        descriptor.task().namespace(),
                        seal.handle().ledgerIdentity().ledgerId()))
                .toList();
        var inputs = original.resources().stream()
                .filter(resource -> !replacements.contains(resource))
                .toList();
        if (inputs.isEmpty() || !original.resources().containsAll(replacements)) {
            throw new IllegalArgumentException(
                    "replacement comparison lacks original physical input or output members");
        }
        var replacementScopes = descriptor.sealedParts().stream()
                .map(seal -> seal.handle().providerScopeId().digest())
                .distinct()
                .toList();
        if (replacementScopes.size() != 1) {
            throw new IllegalArgumentException("replacement native parts differ in provider scope");
        }
        rows.add(compare(
                SemanticAspect.NATIVE_SOURCE_AUTHORITY,
                nativeScope(inputs, plan.sourceCut().identity().providerScopeSha256()),
                nativeScope(replacements, replacementScopes.get(0))));
        return new KafkaBookKeeperReplacementSemanticsV2(
                descriptor.task().taskIdSha256(),
                descriptor.descriptorSha256(),
                M5MaterializationCodecV1.sourceCutSha256(descriptor.sourceCut()),
                Sha256Digest.hash(original.encode()),
                rows);
    }

    private static List<CanonicalBytes> nativeScope(List<PhysicalResourceIdV2> resources, Sha256Digest scope) {
        var namespaces = resources.stream()
                .map(resource -> {
                    if (!(resource instanceof PhysicalResourceIdV2.BookKeeperLedger)) {
                        throw new IllegalArgumentException("Kafka BK replacement has a non-native physical source");
                    }
                    // Ledger IDs change during rewrite; namespace incarnation and provider scope must remain identical.
                    return new PhysicalResourceIdV2.BookKeeperLedger(resource.namespace(), 0).canonicalBytes();
                })
                .distinct()
                .toList();
        if (namespaces.size() != 1) {
            throw new IllegalArgumentException("Kafka BK replacement spans different native namespaces");
        }
        return List.of(namespaces.get(0), scope.bytes());
    }

    private static Comparison compare(
            SemanticAspect aspect, List<CanonicalBytes> prior, List<CanonicalBytes> replacement) {
        return new Comparison(aspect, semanticRoot(aspect, prior), semanticRoot(aspect, replacement));
    }

    private static Sha256Digest semanticRoot(SemanticAspect aspect, List<CanonicalBytes> values) {
        int length = Math.addExact(
                3 * Integer.BYTES, Math.multiplyExact(values.size(), Integer.BYTES + Sha256Digest.LENGTH));
        var out = ByteBuffer.allocate(length)
                .putInt(MAGIC)
                .putInt(aspect.ordinal())
                .putInt(values.size());
        for (var value : values) {
            out.putInt(value.length()).put(Sha256Digest.hash(value).bytes().toByteArray());
        }
        return Sha256Digest.hash(CanonicalBytes.copyOf(out.array()));
    }

    public static String key(Sha256Digest descriptorId) {
        KafkaBookKeeperInventoryV2.requireDigest(descriptorId);
        return "v2/kafka-bk-compaction-v2/views/" + descriptorId.toHex() + "/replacement-semantics";
    }

    public void requireDescriptor(KafkaSealedBookKeeperDescriptorV2 descriptor) {
        if (!taskId.equals(descriptor.task().taskIdSha256())
                || !descriptorId.equals(descriptor.descriptorSha256())
                || !sourceCutSha256.equals(M5MaterializationCodecV1.sourceCutSha256(descriptor.sourceCut()))) {
            throw new IllegalArgumentException("replacement semantics belong to another descriptor or source cut");
        }
    }

    /** The enclosing GC capture must read and subsequently revalidate this exact metadata fact. */
    public List<SemanticTransfer> transfers(KafkaSealedBookKeeperDescriptorV2 descriptor, AuthorityFactV1 fact) {
        requireDescriptor(descriptor);
        if (!fact.key().equals(key(descriptorId)) || !fact.valueSha256().equals(Sha256Digest.hash(encode()))) {
            throw new IllegalArgumentException("replacement verifier fact does not name these exact comparison bytes");
        }
        return comparisons.stream()
                .map(row -> new SemanticTransfer(
                        row.aspect(), descriptor.sourceCut().coverage(), row.priorRoot(), row.replacementRoot(), fact))
                .toList();
    }

    /** Per-resource semantic evidence; full reference/floor/grace admission is still required for deletion. */
    public record SourceEvidence(
            PhysicalResourceIdV2 resource,
            Sha256Digest sourceIdentity,
            ProtocolCoverage coverage,
            List<SemanticTransfer> transfers) {
        public SourceEvidence {
            Objects.requireNonNull(resource, "resource");
            KafkaBookKeeperInventoryV2.requireDigest(sourceIdentity);
            Objects.requireNonNull(coverage, "coverage");
            transfers = List.copyOf(transfers);
            if (!transfers.stream().map(SemanticTransfer::aspect).toList().equals(List.of(SemanticAspect.values()))
                    || transfers.stream().anyMatch(row -> !row.coverage().equals(coverage))) {
                throw new IllegalArgumentException("source evidence lacks its complete scoped semantic transfers");
            }
        }
    }

    /** Binds every original raw ledger or predecessor Part to the exact source extent that was replaced. */
    public List<SourceEvidence> sources(
            KafkaSealedBookKeeperDescriptorV2 descriptor,
            AuthorityFactV1 fact,
            CanonicalBytes originalTargets,
            Optional<KafkaSealedBookKeeperDescriptorV2> predecessor) {
        transfers(descriptor, fact);
        var original = KafkaBookKeeperPublicationTargetsV2.decode(originalTargets);
        original.requireDescriptor(descriptor);
        if (!publicationTargetsSha256.equals(Sha256Digest.hash(originalTargets))) {
            throw new IllegalArgumentException("source evidence differs from original publication membership");
        }
        com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1.SelectedPrefix
                .requirePredecessor(descriptor, predecessor);
        var outputs = descriptor.sealedParts().stream()
                .map(part -> (PhysicalResourceIdV2) new PhysicalResourceIdV2.BookKeeperLedger(
                        descriptor.task().namespace(),
                        part.handle().ledgerIdentity().ledgerId()))
                .sorted()
                .toList();
        if (!original.resources().containsAll(outputs)) {
            throw new IllegalArgumentException("source evidence omits an actual replacement output Part");
        }
        var inputs = original.resources().stream()
                .filter(resource -> !outputs.contains(resource))
                .toList();
        var evidence = new ArrayList<SourceEvidence>();
        for (var source : descriptor.sourceCut().sources()) {
            List<PhysicalResourceIdV2> resources;
            if (source.kind() == SourceKind.BOOKKEEPER_LEDGER) {
                resources = inputs.stream()
                        .filter(resource -> source.ledgerIdentitySha256().equals(Optional.of(resource.sha256())))
                        .toList();
                if (resources.size() != 1) {
                    throw new IllegalArgumentException("raw source lacks its exact original physical member");
                }
            } else if (source.kind() == SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2) {
                resources = predecessor.orElseThrow().sealedParts().stream()
                        .map(part -> (PhysicalResourceIdV2) new PhysicalResourceIdV2.BookKeeperLedger(
                                descriptor.task().namespace(),
                                part.handle().ledgerIdentity().ledgerId()))
                        .toList();
            } else {
                throw new IllegalArgumentException("source evidence requires a native BK source");
            }
            var rows = comparisons.stream()
                    .map(row -> new SemanticTransfer(
                            row.aspect(), source.coverage(), row.priorRoot(), row.replacementRoot(), fact))
                    .toList();
            for (var resource : resources) {
                if (!inputs.contains(resource)
                        || !resource.namespace().equals(descriptor.task().namespace())) {
                    throw new IllegalArgumentException("source evidence has a foreign physical member");
                }
                evidence.add(new SourceEvidence(resource, source.sourceIdentitySha256(), source.coverage(), rows));
            }
        }
        var members = evidence.stream().map(SourceEvidence::resource).sorted().toList();
        if (!members.equals(inputs) || members.stream().distinct().count() != members.size()) {
            throw new IllegalArgumentException("source evidence omits, repeats or adds an original physical member");
        }
        return evidence.stream()
                .sorted(java.util.Comparator.comparing(SourceEvidence::resource))
                .toList();
    }

    public CanonicalBytes encode() {
        var out = ByteBuffer.allocate(ENCODED_BYTES)
                .putInt(MAGIC)
                .putInt(VERSION)
                .put(taskId.bytes().toByteArray())
                .put(descriptorId.bytes().toByteArray())
                .put(sourceCutSha256.bytes().toByteArray())
                .put(publicationTargetsSha256.bytes().toByteArray())
                .putInt(comparisons.size());
        for (var row : comparisons) {
            out.putInt(row.aspect().ordinal())
                    .put(row.priorRoot().bytes().toByteArray())
                    .put(row.replacementRoot().bytes().toByteArray());
        }
        out.put(Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position())))
                .bytes()
                .toByteArray());
        return CanonicalBytes.copyOf(out.array());
    }

    public static KafkaBookKeeperReplacementSemanticsV2 decode(CanonicalBytes bytes) {
        if (bytes.length() != ENCODED_BYTES) {
            throw new IllegalArgumentException("replacement semantic comparison length is invalid");
        }
        var in = ByteBuffer.wrap(bytes.toByteArray());
        if (in.getInt() != MAGIC || in.getInt() != VERSION) {
            throw new IllegalArgumentException("replacement semantic comparison wire is unknown");
        }
        var task = digest(in);
        var descriptor = digest(in);
        var cut = digest(in);
        var targets = digest(in);
        if (in.getInt() != SemanticAspect.values().length) {
            throw new IllegalArgumentException("replacement semantic comparison is incomplete");
        }
        var rows = new ArrayList<Comparison>();
        for (var aspect : SemanticAspect.values()) {
            if (in.getInt() != aspect.ordinal()) {
                throw new IllegalArgumentException("replacement semantic aspect order is invalid");
            }
            rows.add(new Comparison(aspect, digest(in), digest(in)));
        }
        var value = new KafkaBookKeeperReplacementSemanticsV2(task, descriptor, cut, targets, rows);
        if (!value.encode().equals(bytes)) {
            throw new IllegalArgumentException("replacement semantic comparison is noncanonical or corrupt");
        }
        return value;
    }

    private static Sha256Digest digest(ByteBuffer input) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        input.get(bytes);
        return Sha256Digest.copyOf(bytes);
    }
}
