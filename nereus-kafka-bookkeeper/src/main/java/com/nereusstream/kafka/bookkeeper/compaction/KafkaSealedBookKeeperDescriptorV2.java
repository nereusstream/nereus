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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionLayoutV2.Layout;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionWriterV2.VerifiedPart;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.PartKind;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Task;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.SemanticValidationProof;
import java.util.List;
import java.util.Objects;

/** Immutable SEALED_BK_COMPACTED_RUN_V2 value. Only an exact M4 selector may grant read-selection authority. */
public record KafkaSealedBookKeeperDescriptorV2(
        Task task,
        long sourceGeneration,
        int batchCount,
        Sha256Digest dispositionRootSha256,
        Sha256Digest gapRootSha256,
        SemanticValidationProof semanticProof,
        List<BookKeeperDeleteTargetV1> sealedParts,
        List<IndexLocator> indexes) {
    public static final int MAX_ENCODED_BYTES = 1_048_576;
    public static final String KIND = "SEALED_BK_COMPACTED_RUN_V2";

    /** Chunks are contiguous in ordered part/entry traversal, even when an index crosses multiple ledgers. */
    public record IndexLocator(
            int firstPart, int firstEntry, int chunkCount, int artifactLength, Sha256Digest artifactSha256) {
        public IndexLocator {
            KafkaBookKeeperInventoryV2.requireDigest(artifactSha256);
            if (firstPart < 0
                    || firstPart >= KafkaBookKeeperInventoryV2.MAX_PARTS
                    || firstEntry < 0
                    || firstEntry >= KafkaBookKeeperInventoryV2.MAX_PART_ENTRIES
                    || chunkCount <= 0
                    || chunkCount > KafkaBookKeeperInventoryV2.MAX_PART_ENTRIES
                    || artifactLength <= 0
                    || artifactLength > KafkaBookKeeperCompactionLayoutV2.MAX_ARTIFACT_BYTES) {
                throw new IllegalArgumentException("sealed BK index locator exceeds bound");
            }
        }
    }

    public KafkaSealedBookKeeperDescriptorV2 {
        Objects.requireNonNull(task, "task");
        KafkaBookKeeperInventoryV2.requireDigest(dispositionRootSha256);
        KafkaBookKeeperInventoryV2.requireDigest(gapRootSha256);
        Objects.requireNonNull(semanticProof, "semanticProof");
        sealedParts = List.copyOf(Objects.requireNonNull(sealedParts, "sealedParts"));
        indexes = List.copyOf(Objects.requireNonNull(indexes, "indexes"));
        var cut = M5MaterializationCodecV1.decodeSourceCut(task.sourceCut());
        if (sourceGeneration != Math.addExact(cut.predecessorSelector().sourceGeneration(), 1)
                || batchCount < 0
                || batchCount > KafkaCompactionRecordsV1.MAX_BATCHES
                || sealedParts.size() != task.parts().size()
                || indexes.size() != 8
                || !semanticProof.taskIdSha256().equals(M5MaterializationCodecV1.calculateTaskId(cut))
                || !semanticProof.outputIdentitySha256().equals(task.semanticOutputSha256())
                || !semanticProof.sourceSetSha256().equals(cut.sourceSetSha256())
                || !semanticProof.semanticValidationRootSha256().equals(task.semanticValidationRootSha256())) {
            throw new IllegalArgumentException(
                    "sealed BK descriptor differs from its task, source cut or semantic proof");
        }
        if (sealedParts.stream()
                        .map(value -> value.handle().ledgerIdentity())
                        .distinct()
                        .count()
                != sealedParts.size()) {
            throw new IllegalArgumentException("sealed BK descriptor repeats a native ledger identity");
        }
        boolean indexPhase = false;
        boolean data = false;
        for (int ordinal = 0; ordinal < sealedParts.size(); ordinal++) {
            var plan = task.parts().get(ordinal);
            var seal = sealedParts.get(ordinal);
            var configuration = task.configuration(ordinal);
            var capability = task.capability();
            if (plan.kind() == PartKind.INDEX) {
                indexPhase = true;
            } else {
                if (indexPhase) {
                    throw new IllegalArgumentException("sealed BK data part follows index parts");
                }
                data = true;
            }
            if (!seal.handle().providerScopeId().equals(configuration.providerScopeId())
                    || !seal.handle().runId().equals(configuration.runId())
                    || !seal.handle().configurationDigest().equals(configuration.configurationDigest())
                    || seal.sealedLastEntryId() != plan.entryCount() - 1L
                    || seal.sealedLength() != plan.length()
                    || seal.ensembleSize() != capability.ensembleSize()
                    || seal.writeQuorumSize() != capability.writeQuorumSize()
                    || seal.ackQuorumSize() != capability.ackQuorumSize()
                    || seal.digestType() != capability.digestType()
                    || !seal.passwordCredentialIdentityVersion().equals(capability.credentialIdentityVersion())) {
                throw new IllegalArgumentException("sealed BK part metadata differs from the registered task");
            }
        }
        if (data != (batchCount > 0) || !indexPhase) {
            throw new IllegalArgumentException("sealed BK descriptor has an inconsistent empty output");
        }
        for (IndexLocator index : indexes) {
            if (index.firstPart() >= task.parts().size()
                    || task.parts().get(index.firstPart()).kind() != PartKind.INDEX
                    || index.firstEntry() >= task.parts().get(index.firstPart()).entryCount()) {
                throw new IllegalArgumentException("sealed BK index starts outside an exact index part");
            }
        }
    }

    /** Builds from independently validated semantics and exact physical writer results; it does not publish. */
    public static KafkaSealedBookKeeperDescriptorV2 create(
            CompactionPlan plan, KafkaCompactionSemanticOutputV2 semantic, Layout layout, List<VerifiedPart> verified) {
        var proof = new KafkaCompactionSemanticValidatorV1().validateSemantic(plan, semantic);
        Task task = layout.task();
        if (!task.sourceCut().equals(M5MaterializationCodecV1.encodeSourceCut(plan.sourceCut()))
                || !task.compactionPlanRootSha256().equals(semantic.compactionPlanRootSha256())
                || !task.semanticOutputSha256().equals(semantic.outputIdentitySha256())
                || !task.semanticValidationRootSha256().equals(proof.semanticValidationRootSha256())
                || verified.size() != task.parts().size()) {
            throw new IllegalArgumentException("sealed BK descriptor inputs belong to different compaction tasks");
        }
        for (int ordinal = 0; ordinal < verified.size(); ordinal++) {
            var part = verified.get(ordinal).part();
            if (part.ordinal() != ordinal
                    || !part.taskIdSha256().equals(task.taskIdSha256())
                    || !part.resource().namespace().equals(task.namespace())) {
                throw new IllegalArgumentException(
                        "sealed BK writer result differs from the ordered physical inventory");
            }
        }
        var artifacts = KafkaBookKeeperArtifactAssemblerV2.assemble(task, layout.parts());
        requireBodies(semantic, artifacts);
        return new KafkaSealedBookKeeperDescriptorV2(
                task,
                Math.addExact(plan.sourceCut().predecessorSelector().sourceGeneration(), 1),
                artifacts.batches().size(),
                KafkaCompactionCanonicalV1.dispositionRoot(semantic.dispositions()),
                KafkaBookKeeperArtifactAssemblerV2.gapRoot(semantic.gaps()),
                proof,
                verified.stream().map(VerifiedPart::sealedMetadata).toList(),
                artifacts.indexLocators());
    }

    static void requireBodies(
            KafkaCompactionSemanticOutputV2 expected, KafkaBookKeeperArtifactAssemblerV2.Artifacts actual) {
        if (!actual.batches()
                        .equals(expected.batchOutputs().stream()
                                .flatMap(value -> value.outputBody().stream())
                                .toList())
                || !actual.indexes().equals(expected.indexBodies())) {
            throw new IllegalStateException(
                    "sealed BK artifacts differ from the complete independently validated semantics");
        }
    }

    public MaterializationSourceCut sourceCut() {
        return M5MaterializationCodecV1.decodeSourceCut(task.sourceCut());
    }

    public CanonicalBytes encode() {
        return KafkaSealedBookKeeperDescriptorCodecV2.encode(this);
    }

    public Sha256Digest descriptorSha256() {
        return Sha256Digest.hash(encode());
    }
}
