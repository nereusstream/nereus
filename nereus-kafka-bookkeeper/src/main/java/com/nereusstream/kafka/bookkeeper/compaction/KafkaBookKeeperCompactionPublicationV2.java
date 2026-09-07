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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionPublicationFenceV1.CurrentStateReader;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Publishes one complete BK descriptor through the existing M4 selector CAS, preserving its M5 authority envelope.
 * The enclosing coordinator owns native task/writer admission, source pins and current protocol-state production.
 */
public final class KafkaBookKeeperCompactionPublicationV2 {
    private final CanonicalControlMetadataStore metadata;
    private final BindingIdentity binding;
    private final M4ReadControlCoordinatorV1 m4;
    private final KafkaSealedBookKeeperReaderV2 reader;
    private final Executor controlExecutor;

    public KafkaBookKeeperCompactionPublicationV2(
            CanonicalControlMetadataStore metadata,
            int shardId,
            BindingIdentity binding,
            KafkaSealedBookKeeperReaderV2 reader) {
        this(metadata, shardId, binding, reader, Runnable::run);
    }

    /** Native immutable publication and selector CAS run on the owner's bounded control executor. */
    public KafkaBookKeeperCompactionPublicationV2(
            CanonicalControlMetadataStore metadata,
            int shardId,
            BindingIdentity binding,
            KafkaSealedBookKeeperReaderV2 reader,
            Executor controlExecutor) {
        this.controlExecutor = Objects.requireNonNull(controlExecutor, "controlExecutor");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.binding = Objects.requireNonNull(binding, "binding");
        this.reader = Objects.requireNonNull(reader, "reader");
        this.m4 = new M4ReadControlCoordinatorV1(metadata, shardId, binding);
    }

    /** Rechecks exact semantics and all native stored bytes before creating immutable values and selecting once. */
    public CompletionStage<PublicationOutcome> publish(
            CompactionPlan plan,
            KafkaCompactionSemanticOutputV2 semantic,
            KafkaSealedBookKeeperDescriptorV2 descriptor,
            List<SourceProtectionIdentity> exactFallbackSources,
            CurrentStateReader currentCompactionState) {
        requireIdentity(plan, semantic, descriptor);
        List<SourceProtectionIdentity> sources = List.copyOf(exactFallbackSources);
        M5MaterializationValidatorV1.requireFallbackProtections(plan.sourceCut(), sources);
        return reader.recover(descriptor)
                .thenApplyAsync(
                        recovered -> {
                            KafkaSealedBookKeeperDescriptorV2.requireBodies(semantic, recovered.artifacts());
                            Sha256Digest identity = descriptor.descriptorSha256();
                            PublicationOutcome immutable = createExact(descriptorKey(identity), descriptor.encode());
                            if (immutable != PublicationOutcome.EXISTING_EXACT) {
                                return immutable;
                            }
                            immutable =
                                    createExact(candidateKey(descriptor.task().taskIdSha256()), identity.bytes());
                            if (immutable != PublicationOutcome.EXISTING_EXACT) {
                                return immutable;
                            }
                            new KafkaCompactionPublicationFenceV1().requireCurrent(plan, currentCompactionState);
                            BindingReadSelector expected = plan.sourceCut().predecessorSelector();
                            Optional<BindingReadSelector> current = m4.readSelector();
                            if (current.isPresent()
                                    && current.orElseThrow()
                                            .selectedViewSha256()
                                            .equals(identity)
                                    && current.orElseThrow().sourceGeneration() == descriptor.sourceGeneration()) {
                                return PublicationOutcome.EXISTING_EXACT;
                            }
                            if (!current.equals(Optional.of(expected))) {
                                return PublicationOutcome.CANCELLED_STALE;
                            }
                            M4ReadControlCoordinatorV1.Outcome outcome = expected.mode() == SelectorMode.PREFERRED_ONLY
                                    ? m4.introduceFallback(expected, identity, descriptor.sourceGeneration(), sources)
                                    : m4.updateMembershipNeutralView(
                                            expected, identity, descriptor.sourceGeneration(), sources);
                            return switch (outcome) {
                                case APPLIED -> PublicationOutcome.APPLIED_EXACT;
                                case EXISTING_EXACT -> PublicationOutcome.EXISTING_EXACT;
                                case RETRY_EXACT_PREDECESSOR -> PublicationOutcome.DEFINITIVELY_NOT_APPLIED;
                                case RETAIN -> PublicationOutcome.OUTCOME_UNKNOWN;
                                default -> PublicationOutcome.CONFLICT;
                            };
                        },
                        controlExecutor);
    }

    /**
     * Loads only the descriptor named by an admitted current selector. The caller retains its M4 read/source-plan pin
     * through this returned stage; this method does not create native broker read admission on its own.
     */
    public CompletionStage<KafkaBookKeeperReadViewV2> recoverSelected(BindingReadSelector admittedSelector) {
        if (!admittedSelector.binding().equals(binding)
                || admittedSelector.admissionState() != AdmissionState.ADMITTING
                || !m4.readSelector().equals(Optional.of(admittedSelector))) {
            throw new IllegalStateException("BK descriptor read lacks its exact admitted current selector");
        }
        CanonicalBytes bytes = metadata.get(descriptorKey(admittedSelector.selectedViewSha256()))
                .orElseThrow(() -> new IllegalStateException("selected BK descriptor is missing"));
        if (!Sha256Digest.hash(bytes).equals(admittedSelector.selectedViewSha256())) {
            throw new IllegalStateException("selected BK descriptor digest differs from its selector");
        }
        var descriptor = KafkaSealedBookKeeperDescriptorCodecV2.decode(bytes);
        if (!descriptor.sourceCut().identity().binding().equals(binding)
                || descriptor.sourceGeneration() > admittedSelector.sourceGeneration()) {
            throw new IllegalStateException("selected BK descriptor belongs to another Binding or future generation");
        }
        return reader.recover(descriptor);
    }

    private void requireIdentity(
            CompactionPlan plan,
            KafkaCompactionSemanticOutputV2 semantic,
            KafkaSealedBookKeeperDescriptorV2 descriptor) {
        var proof = new KafkaCompactionSemanticValidatorV1().validateSemantic(plan, semantic);
        var task = descriptor.task();
        if (!plan.sourceCut().identity().binding().equals(binding)
                || !task.sourceCut().equals(M5MaterializationCodecV1.encodeSourceCut(plan.sourceCut()))
                || !task.compactionPlanRootSha256().equals(semantic.compactionPlanRootSha256())
                || !task.semanticOutputSha256().equals(semantic.outputIdentitySha256())
                || !descriptor.semanticProof().equals(proof)
                || !descriptor
                        .dispositionRootSha256()
                        .equals(KafkaCompactionCanonicalV1.dispositionRoot(semantic.dispositions()))
                || !descriptor.gapRootSha256().equals(KafkaBookKeeperArtifactAssemblerV2.gapRoot(semantic.gaps()))
                || !metadata.get(KafkaBookKeeperInventoryV2.taskKey(task.taskIdSha256()))
                        .equals(Optional.of(KafkaBookKeeperInventoryCodecV2.encodeTask(task)))) {
            throw new IllegalStateException("BK publication differs from its registered exact task and semantic proof");
        }
        var inventory = new KafkaBookKeeperInventoryV2(metadata);
        for (int ordinal = 0; ordinal < task.parts().size(); ordinal++) {
            var part = inventory
                    .readPart(task, ordinal)
                    .orElseThrow(() -> new IllegalStateException("BK publication lacks a complete exact inventory"));
            if (!part.handle().equals(descriptor.sealedParts().get(ordinal).handle())) {
                throw new IllegalStateException("BK descriptor differs from its complete physical inventory");
            }
        }
    }

    private PublicationOutcome createExact(String key, CanonicalBytes bytes) {
        metadata.putIfAbsent(key, bytes);
        Optional<CanonicalBytes> observed = metadata.get(key);
        return observed.isEmpty()
                ? PublicationOutcome.OUTCOME_UNKNOWN
                : observed.orElseThrow().equals(bytes)
                        ? PublicationOutcome.EXISTING_EXACT
                        : PublicationOutcome.CONFLICT;
    }

    public static String descriptorKey(Sha256Digest digest) {
        KafkaBookKeeperInventoryV2.requireDigest(digest);
        return "v2/kafka-bk-compaction-v2/views/" + digest.toHex() + "/descriptor";
    }

    public static String candidateKey(Sha256Digest taskId) {
        return KafkaBookKeeperInventoryV2.taskKey(taskId) + "/candidate";
    }
}
