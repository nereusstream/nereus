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

package com.nereusstream.metadata.oxia.v2.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.metadata.oxia.v2.compaction.OxiaKafkaBookKeeperRecordStoreV2;
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5BindingBootstrapV1;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerCoordinatorV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.M4ReleaseBindingV1;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import io.oxia.client.api.AsyncOxiaClient;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletionStage;

/** Exact native Kafka Binding route into the namespace-bound M4 selector and capability records. */
public final class OxiaKafkaBookKeeperM4ReadRouteV2 {
    private final M4ReadControlCoordinatorV1 control;
    private final BindingIdentity binding;
    private final M5MaterializationWorkerCoordinatorV1 workers;
    private final AsyncOxiaClient client;
    private final MetadataNamespaceIdentityV2 namespace;
    private final String cellAuthorityRoot;
    private final CanonicalControlMetadataStore rawBindingControl;
    private final OxiaBindingLifecycleMetadataStoreV2 bindingRoute;
    private final M4ReadControlKeysV1 keys;
    private final String selectorKey;
    private final int partition;

    public OxiaKafkaBookKeeperM4ReadRouteV2(
            AsyncOxiaClient client, MetadataNamespaceIdentityV2 namespace, KafkaRunRootRecordV2.Scope scope) {
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(scope, "scope");
        this.client = Objects.requireNonNull(client, "client");
        partition = scope.partition();
        cellAuthorityRoot = "/nereus/cells/kafka_bk_" + namespace.instanceId().toHex();
        binding = new BindingIdentity(
                scope.bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(scope.topic())),
                scope.storageEpoch().digest());
        bindingRoute = new OxiaBindingLifecycleMetadataStoreV2(client, cellAuthorityRoot, scope.partition(), binding);
        keys = new M4ReadControlKeysV1(scope.partition(), binding);
        selectorKey = keys.selector();
        rawBindingControl = bindingRoute.rawControlMetadata();
        control = new M4ReadControlCoordinatorV1(rawBindingControl, scope.partition(), binding);
        workers = new M5MaterializationWorkerCoordinatorV1(rawBindingControl, scope.partition(), binding);
    }

    public Optional<BindingReadSelector> readSelector() {
        return control.readSelector();
    }

    /** Actual authoritative reread for native completion callbacks; never blocks an Oxia callback thread. */
    public CompletionStage<Optional<BindingReadSelector>> readOpenSelectorAsync() {
        return readOpenSelectorFact().thenApply(value -> value.map(SelectorFact::selector));
    }

    public record SelectorFact(BindingReadSelector selector, VersionedValue stored) {
        public SelectorFact {
            Objects.requireNonNull(selector, "selector");
            Objects.requireNonNull(stored, "stored");
        }
    }

    /** Retains the exact OPEN authority envelope revision as well as its unchanged selector projection. */
    public CompletionStage<Optional<SelectorFact>> readOpenSelectorFact() {
        return bindingRoute
                .read(selectorKey)
                .thenApply(observed -> observed.map(value -> {
                    var bytes = value.canonicalStoredBytes();
                    BindingReadSelector selector;
                    if (M5BindingAuthorityCodecV1.isAuthorityValue(bytes)) {
                        var authority = M5BindingAuthorityCodecV1.decodeAuthority(bytes);
                        if (authority.state() != BindingAuthorityStateV1.OPEN_V1) {
                            throw new IllegalStateException(
                                    "native recovery birth is blocked by the Binding reference scan");
                        }
                        selector = authority.selectorProjection();
                    } else {
                        selector = M4ReadControlCodecV1.decodeSelector(bytes);
                    }
                    if (!selector.binding().equals(binding)) {
                        throw new IllegalStateException("native selector differs from its exact Binding route");
                    }
                    return new SelectorFact(selector, value);
                }));
    }

    /** Missing/unreadable/foreign protection facts cannot establish RELEASED qualification. */
    public CompletionStage<Optional<VersionedValue>> readProtectionFact(Sha256Digest source, long generation) {
        String key = keys.protection(source, generation);
        return bindingRoute
                .read(key)
                .thenApply(observed -> observed.map(value -> {
                    var protection = M4ReadControlCodecV1.decodeProtection(value.canonicalStoredBytes());
                    if (!protection.binding().equals(binding)
                            || !protection.identity().sourceIdentitySha256().equals(source)
                            || protection.identity().protectionGeneration() != generation) {
                        throw new IllegalStateException("native protection differs from its exact Binding route");
                    }
                    return value;
                }));
    }

    /** Actual complete read-epoch closure facts for released sources; call on the owned control executor. */
    public List<VersionedValue> readQuiescenceFacts(SelectorFact selector, List<M4ReleaseBindingV1> releases) {
        if (!selector.selector().binding().equals(binding)
                || selector.selector().mode() != SelectorMode.PREFERRED_ONLY
                || releases.isEmpty()
                || releases.size() > M4ReadControlRecordsV1.MAX_SOURCES_PER_BATCH) {
            throw new IllegalArgumentException("native quiescence capture lacks its exact released Binding");
        }
        var facts = new TreeMap<String, VersionedValue>();
        var headFact = captureFact(facts, keys.proofHead());
        var head = M4ReadControlCodecV1.decodeHead(headFact.canonicalStoredBytes());
        for (var release : releases) {
            var protection = M4ReadControlCodecV1.decodeProtection(release.canonicalProtectionBytes());
            var source = protection.identity();
            var stored =
                    captureFact(facts, keys.protection(source.sourceIdentitySha256(), source.protectionGeneration()));
            if (!protection.binding().equals(binding)
                    || !stored.key().equals(release.protectionAuthority().key())
                    || !stored.metadataVersion()
                            .equals(release.protectionAuthority().metadataVersion())
                    || !stored.canonicalStoredSha256()
                            .equals(release.protectionAuthority().valueSha256())
                    || !stored.canonicalStoredBytes().equals(release.canonicalProtectionBytes())) {
                throw new IllegalStateException("native quiescence source differs from its exact RELEASED fact");
            }
            var batch = selector.selector().activeBatches().stream()
                    .filter(value -> value.batchIdSha256().equals(release.releasedByBatchSha256()))
                    .findFirst()
                    .orElseThrow(() ->
                            new IllegalStateException("native released source lacks its complete retirement batch"));
            if (!batch.sources().contains(source)) {
                throw new IllegalStateException("native released source differs from its exact retirement batch");
            }
            var epochs = new TreeSet<Long>();
            long first = source.firstFallbackCapableReadAdmissionEpoch();
            long last = batch.sharedLastFallbackCapableReadAdmissionEpoch();
            if (first <= 0 || last < first || last - first >= M4ReadControlRecordsV1.MAX_PROOF_INTERVAL_EPOCHS) {
                throw new IllegalStateException("native released source exceeds its bounded read-epoch interval");
            }
            for (long offset = 0; offset <= last - first; offset++) {
                epochs.add(first + offset);
            }
            for (var fold : head.folds()) {
                if (fold.lastEpoch() < first || fold.firstEpoch() > last) {
                    continue;
                }
                for (long offset = 0; offset <= fold.lastEpoch() - fold.firstEpoch(); offset++) {
                    epochs.add(fold.firstEpoch() + offset);
                }
            }
            for (long epoch : epochs) {
                var proof = M4ReadControlCodecV1.decodeProof(
                        captureFact(facts, keys.proof(epoch)).canonicalStoredBytes());
                captureFact(facts, keys.terminal(epoch));
                captureFact(facts, keys.capability(proof.capability().generation()));
            }
            // Validate after capture so exact version rereads also bind the records used by verification.
            if (!control.verifyInterval(head, first, last)) {
                throw new IllegalStateException(
                        "native released source lacks its complete verified read-epoch interval");
            }
        }
        for (var fact : facts.values()) {
            if (!bindingRoute.read(fact.key()).toCompletableFuture().join().equals(Optional.of(fact))) {
                throw new IllegalStateException("native quiescence authority changed during fact capture");
            }
        }
        return List.copyOf(facts.values());
    }

    private VersionedValue captureFact(Map<String, VersionedValue> facts, String key) {
        var observed = bindingRoute
                .read(key)
                .toCompletableFuture()
                .join()
                .orElseThrow(() -> new IllegalStateException("native quiescence fact is missing: " + key));
        var previous = facts.putIfAbsent(key, observed);
        if (previous != null && !previous.equals(observed)) {
            throw new IllegalStateException("native quiescence authority changed during fact capture");
        }
        if (facts.size() > M5RetentionRecordsV1.MAX_REFERENCE_ROWS) {
            throw new IllegalStateException("native quiescence capture exceeds its bounded reference inventory");
        }
        return observed;
    }

    public M4ReadControlCoordinatorV1 control() {
        return control;
    }

    /** The native owner still supplies its verified source cut and actual storage epoch before admission. */
    public M5MaterializationWorkerCoordinatorV1 workers() {
        return workers;
    }

    /** Kafka's typed task route composes this exact raw Binding authority with its immutable records. */
    public CanonicalControlMetadataStore rawBindingControl() {
        return rawBindingControl;
    }

    /** Keeps the native Binding and admits only its canonical M5 task keys and exact Cell admission key. */
    public CanonicalControlMetadataStore materializationMetadata(Sha256Digest protocolCellSha256) {
        return materializationLifecycle(protocolCellSha256).rawControlMetadata();
    }

    public CompletionStage<Void> reconcileMaterializationPendingOutput(
            Sha256Digest protocolCellSha256,
            com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2 original) {
        return materializationLifecycle(protocolCellSha256).reconcileAppliedPendingOutput(original);
    }

    private OxiaBindingLifecycleMetadataStoreV2 materializationLifecycle(Sha256Digest protocolCellSha256) {
        return new OxiaBindingLifecycleMetadataStoreV2(
                client, cellAuthorityRoot, partition, binding, protocolCellSha256, namespace);
    }

    public com.nereusstream.storage.object.retention.M5BindingLogicalTrimCoordinatorV2 logicalTrimCoordinator() {
        return new com.nereusstream.storage.object.retention.M5BindingLogicalTrimCoordinatorV2(
                bindingRoute, selectorKey);
    }

    public CompletionStage<Optional<VersionedValue>> readBindingAuthority() {
        return bindingRoute.read(selectorKey);
    }

    public CompletionStage<VersionedValue> nativeBindingFact(VersionedValue exact) {
        if (!exact.key().equals(selectorKey)) {
            throw new IllegalArgumentException("Binding fact belongs to another selector");
        }
        return bindingRoute.nativeFact(exact);
    }

    public CompletionStage<SelectorFact> readSourceScanSelectorFact(VersionedValue exactScan) {
        return logicalTrimCoordinator().requireSourceScanCurrent(exactScan).thenApply(ignored -> {
            var authority = M5BindingAuthorityCodecV1.decodeAuthority(exactScan.canonicalStoredBytes());
            if (!authority.binding().equals(binding)) {
                throw new IllegalArgumentException("Kafka source scan belongs to another Binding");
            }
            return new SelectorFact(authority.selectorProjection(), exactScan);
        });
    }

    /** Immutable transport under the same admitted native namespace; it grants no worker or output authority. */
    public CanonicalControlMetadataStore compactionRecords() {
        return compactionRecordFacts();
    }

    public OxiaKafkaBookKeeperRecordStoreV2 compactionRecordFacts() {
        return new OxiaKafkaBookKeeperRecordStoreV2(client, cellAuthorityRoot);
    }

    /** All native Bindings in this namespace and Protocol Cell share the same persisted capacity key. */
    public CanonicalControlMetadataStore compactionAdmissionMetadata(Sha256Digest protocolCellSha256) {
        return OxiaCanonicalControlMetadataStore.forKafkaCompactionAdmission(
                client, cellAuthorityRoot, protocolCellSha256);
    }

    /** Complete persisted Cell reservation directory, including preparations whose Task record is not yet known. */
    public Optional<VersionedValue> readCompactionAdmissionFact(Sha256Digest protocolCellSha256) {
        var metadata = OxiaCanonicalControlMetadataStore.forKafkaCompactionAdmission(
                client, cellAuthorityRoot, protocolCellSha256);
        String key = "v2/object-wal/cells/" + protocolCellSha256.toHex() + "/read-m5/kafka-compaction-admission";
        return metadata.getVersioned(key);
    }

    /** Immutable decision for an inventoried Task's exact Binding in this same native metadata namespace. */
    public CompletionStage<Optional<VersionedValue>> readTaskDecisionFact(
            BindingIdentity taskBinding, Sha256Digest taskId) {
        var route = new OxiaBindingLifecycleMetadataStoreV2(client, cellAuthorityRoot, partition, taskBinding);
        return route.read(M5TaskSelectionDecisionV2.key(taskBinding, taskId));
    }

    /** Actual key/version of this Task's already observed decision, for cross-route GC freshness checks. */
    public CompletionStage<VersionedValue> nativeTaskDecisionFact(
            BindingIdentity taskBinding, Sha256Digest taskId, VersionedValue expected) {
        if (!expected.key().equals(M5TaskSelectionDecisionV2.key(taskBinding, taskId))) {
            return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalArgumentException("native decision belongs to another Task"));
        }
        return new OxiaBindingLifecycleMetadataStoreV2(client, cellAuthorityRoot, partition, taskBinding)
                .nativeFact(expected);
    }

    /** The caller must first verify the exact current-source isolated conformance input. */
    public BindingReadSelector provisionDurableDrainOnly(
            Sha256Digest adapter,
            Sha256Digest backendConfiguration,
            Sha256Digest readContract,
            Sha256Digest verifier,
            Sha256Digest receiptIdentity,
            Sha256Digest receipt,
            long maximumSourceAccessLifetimeMillis,
            Sha256Digest selectedView,
            long ownerEpoch) {
        return provisionDurableDrainOnly(
                adapter,
                backendConfiguration,
                readContract,
                verifier,
                receiptIdentity,
                receipt,
                maximumSourceAccessLifetimeMillis,
                selectedView,
                ownerEpoch,
                control.readSelector());
    }

    /** Persist initialization before the SDK source is opened. This does not qualify old history as closed. */
    public void prepareSourceBootstrap(
            Sha256Digest adapter,
            Sha256Digest backendConfiguration,
            Sha256Digest readContract,
            Sha256Digest verifier,
            Sha256Digest receiptIdentity,
            Sha256Digest receipt,
            long maximumSourceAccessLifetimeMillis,
            Sha256Digest initialSourceScope) {
        prepareSourceBootstrap(
                adapter,
                backendConfiguration,
                readContract,
                verifier,
                receiptIdentity,
                receipt,
                maximumSourceAccessLifetimeMillis,
                initialSourceScope,
                Optional.empty());
    }

    public void prepareSourceBootstrap(
            Sha256Digest adapter,
            Sha256Digest backendConfiguration,
            Sha256Digest readContract,
            Sha256Digest verifier,
            Sha256Digest receiptIdentity,
            Sha256Digest receipt,
            long maximumSourceAccessLifetimeMillis,
            Sha256Digest initialSourceScope,
            Optional<com.nereusstream.storage.object.retention.M5KafkaBindingPolicyV1> firstPolicy) {
        var capability = admitDurableDrainCapability(
                adapter,
                backendConfiguration,
                readContract,
                verifier,
                receiptIdentity,
                receipt,
                maximumSourceAccessLifetimeMillis);
        var candidate = new M5BindingBootstrapV1(binding, capability, initialSourceScope, firstPolicy).encode();
        var existing = rawBindingControl.get(selectorKey);
        if (existing.isEmpty()) {
            rawBindingControl.putIfAbsent(selectorKey, candidate);
        }
        var stored = rawBindingControl
                .get(selectorKey)
                .orElseThrow(() -> new IllegalStateException("source bootstrap persistence is unresolved"));
        if (M5BindingBootstrapV1.isBootstrap(stored)) {
            // A successor recovers the original source through the native owner/root authority.
            // It must retain the original initialization bytes, including after a lost create reply.
            if (!M5BindingBootstrapV1.decode(stored).capability().equals(capability)) {
                throw new IllegalStateException("source bootstrap capability differs from original initialization");
            }
            requireOriginalKafkaProfile(
                    firstPolicy, M5BindingBootstrapV1.decode(stored).kafkaPolicy());
        } else {
            var selector = M5BindingAuthorityCodecV1.projectSelector(stored);
            if (!selector.capability().equals(capability)) {
                throw new IllegalStateException("source bootstrap capability differs from existing selector");
            }
            if (M5BindingAuthorityCodecV1.isAuthorityValue(stored)
                    && M5BindingAuthorityCodecV1.decodeAuthority(stored).state() != BindingAuthorityStateV1.OPEN_V1) {
                throw new IllegalStateException("source bootstrap is blocked by the current Binding scan fence");
            }
            var admitted = M5BindingAuthorityCodecV1.isAuthorityValue(stored)
                    ? M5BindingAuthorityCodecV1.decodeAuthority(stored).kafkaPolicy()
                    : Optional.<com.nereusstream.storage.object.retention.M5KafkaBindingPolicyV1>empty();
            requireOriginalKafkaProfile(firstPolicy, admitted);
        }
    }

    private static void requireOriginalKafkaProfile(
            Optional<com.nereusstream.storage.object.retention.M5KafkaBindingPolicyV1> requested,
            Optional<com.nereusstream.storage.object.retention.M5KafkaBindingPolicyV1> admitted) {
        if (requested.isPresent()
                && admitted.filter(requested.orElseThrow()::sameAdmittedProfile).isEmpty()) {
            throw new IllegalStateException("Kafka M5 profile was not first admitted or its immutable inputs changed");
        }
    }

    private CapabilityBinding admitDurableDrainCapability(
            Sha256Digest adapter,
            Sha256Digest backendConfiguration,
            Sha256Digest readContract,
            Sha256Digest verifier,
            Sha256Digest receiptIdentity,
            Sha256Digest receipt,
            long maximumSourceAccessLifetimeMillis) {
        var evidence = new CapabilityEvidence(
                binding,
                1,
                1,
                CapabilityKind.DURABLE_DRAIN_ONLY_V1,
                CapabilityState.ADMITTED,
                adapter,
                backendConfiguration,
                readContract,
                verifier,
                receiptIdentity,
                receipt,
                Sha256Digest.hash(
                        CanonicalBytes.copyOf("M4-DURABLE-DRAIN-ONLY-NO-EXPIRY-V1".getBytes(StandardCharsets.UTF_8))),
                maximumSourceAccessLifetimeMillis,
                0,
                0);
        var capabilityResult = control.createCapability(evidence);
        if (capabilityResult != M4ReadControlCoordinatorV1.Outcome.APPLIED
                && capabilityResult != M4ReadControlCoordinatorV1.Outcome.EXISTING_EXACT) {
            throw new IllegalStateException("M4 durable-drain capability admission is unresolved: " + capabilityResult);
        }
        return new CapabilityBinding(1, M4ReadControlCodecV1.capabilityEvidenceSha256(evidence));
    }

    /** Binds every possible source selector mutation to the caller's exact observed predecessor. */
    public BindingReadSelector provisionDurableDrainOnly(
            Sha256Digest adapter,
            Sha256Digest backendConfiguration,
            Sha256Digest readContract,
            Sha256Digest verifier,
            Sha256Digest receiptIdentity,
            Sha256Digest receipt,
            long maximumSourceAccessLifetimeMillis,
            Sha256Digest selectedView,
            long ownerEpoch,
            Optional<BindingReadSelector> expectedPredecessor) {
        Objects.requireNonNull(expectedPredecessor, "expectedPredecessor");
        var capability = admitDurableDrainCapability(
                adapter,
                backendConfiguration,
                readContract,
                verifier,
                receiptIdentity,
                receipt,
                maximumSourceAccessLifetimeMillis);
        var previous = control.readSelector();
        if (!previous.equals(expectedPredecessor)) {
            throw new IllegalStateException("M4 native source predecessor changed before publication");
        }
        if (previous.isEmpty()) {
            var first = new BindingReadSelector(
                    binding,
                    selectedView,
                    ownerEpoch,
                    1,
                    1,
                    SelectorMode.PREFERRED_ONLY,
                    AdmissionState.ADMITTING,
                    Optional.empty(),
                    capability,
                    List.of(),
                    List.of());
            var result = control.createSelector(first);
            if (result != M4ReadControlCoordinatorV1.Outcome.APPLIED
                    && result != M4ReadControlCoordinatorV1.Outcome.EXISTING_EXACT) {
                throw new IllegalStateException("M4 native selector creation is unresolved: " + result);
            }
        } else {
            var old = previous.orElseThrow();
            if (!old.capability().equals(capability)
                    || old.mode() != SelectorMode.PREFERRED_ONLY && old.mode() != SelectorMode.PREFERRED_WITH_FALLBACK
                    || old.admissionState() != AdmissionState.ADMITTING
                    || old.mode() == SelectorMode.PREFERRED_WITH_FALLBACK
                            && !old.selectedViewSha256().equals(selectedView)) {
                throw new IllegalStateException("M4 native selector has another capability or fallback state");
            }
            if (old.ownerEpoch() < ownerEpoch) {
                var result =
                        control.grantTakeover(old, selectedView, ownerEpoch, Math.addExact(old.sourceGeneration(), 1));
                if (result != M4ReadControlCoordinatorV1.Outcome.APPLIED
                        && result != M4ReadControlCoordinatorV1.Outcome.EXISTING_EXACT) {
                    throw new IllegalStateException("M4 native owner takeover is unresolved: " + result);
                }
            }
        }
        var current = control.readSelector()
                .orElseThrow(() -> new IllegalStateException("M4 native selector disappeared after admission"));
        if (!current.capability().equals(capability)
                || current.ownerEpoch() != ownerEpoch
                || !current.selectedViewSha256().equals(selectedView)
                || current.mode() != previous.map(BindingReadSelector::mode).orElse(SelectorMode.PREFERRED_ONLY)
                || current.admissionState() != AdmissionState.ADMITTING) {
            throw new IllegalStateException("M4 native selector differs from exact recovered owner");
        }
        return current;
    }
}
