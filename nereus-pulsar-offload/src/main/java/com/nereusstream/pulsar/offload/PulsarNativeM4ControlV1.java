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
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaBindingLifecycleMetadataStoreV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPulsarMaterializationInputV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2;
import com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationKeysV1;
import com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2;
import com.nereusstream.storage.object.materialization.M5PulsarRecoveryInputV2;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceAdapterResultV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceScanSummaryV1;
import io.oxia.client.api.AsyncOxiaClient;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;

/** Actual Native Binding M4 facts. Stored ADMITTED state alone is not current backend conformance or an owner grant. */
public final class PulsarNativeM4ControlV1 {
    /** Retains only this failed invocation's private cleanup; retry cannot create or reopen a read owner. */
    public static final class OwnerCleanupException extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final Supplier<CompletableFuture<Void>> cleanup;
        private final PulsarNativeReadCellBudgetV1.Reservation reservation;
        private final Optional<BindingReadSelector> unresolvedInitialSelector;
        private CompletableFuture<Void> attempt;

        private OwnerCleanupException(
                Throwable workFailure,
                Throwable cleanupFailure,
                Supplier<CompletableFuture<Void>> cleanup,
                PulsarNativeReadCellBudgetV1.Reservation reservation) {
            this(workFailure, cleanupFailure, cleanup, reservation, Optional.empty());
        }

        private OwnerCleanupException(
                Throwable workFailure,
                Throwable cleanupFailure,
                Supplier<CompletableFuture<Void>> cleanup,
                PulsarNativeReadCellBudgetV1.Reservation reservation,
                Optional<BindingReadSelector> unresolvedInitialSelector) {
            super("Native owner creation failed and its original cleanup remains unresolved", workFailure);
            addSuppressed(cleanupFailure);
            this.cleanup = cleanup;
            this.reservation = reservation;
            this.unresolvedInitialSelector = unresolvedInitialSelector;
            reservation.retainUndeliveredOwner(this::reconcileCleanup, false);
        }

        /** Original create candidate only; equal stored bytes do not prove ownership or an actual actor drain. */
        public Optional<BindingReadSelector> unresolvedInitialSelector() {
            return unresolvedInitialSelector;
        }

        public synchronized CompletableFuture<Void> reconcileCleanup() {
            try {
                if (attempt == null || attempt.isCompletedExceptionally()) {
                    attempt = cleanup.get().thenRun(reservation::confirmUndeliveredOwnerCleanup);
                }
                return attempt.copy();
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }
    }

    /** Exact selector/capability predecessors, still awaiting current Native backend conformance validation. */
    public static final class CapturedFacts {
        private final BindingReadSelector selector;
        private final CapabilityEvidence capability;
        private final VersionedValue selectorFact;
        private final VersionedValue capabilityFact;

        private CapturedFacts(
                BindingReadSelector selector,
                CapabilityEvidence capability,
                VersionedValue selectorFact,
                VersionedValue capabilityFact) {
            this.selector = selector;
            this.capability = capability;
            this.selectorFact = selectorFact;
            this.capabilityFact = capabilityFact;
        }

        public BindingReadSelector selector() {
            return selector;
        }

        public CapabilityEvidence capability() {
            return capability;
        }

        public VersionedValue selectorFact() {
            return selectorFact;
        }

        public VersionedValue capabilityFact() {
            return capabilityFact;
        }
    }

    private final PulsarNativeBindingV1 binding;
    private final AsyncOxiaClient client;
    private final OxiaBindingLifecycleMetadataStoreV2 route;
    private final M4ReadControlKeysV1 keys;
    private final Oxia09ExactMetadataTransactionStoreV1 sourceMaps;
    private final String sourceMapPrefix;
    private final String ownerDrainPrefix;
    private final String coldClaimPrefix;
    private final String initialClaimKey;
    private PulsarNativeControlWriterV2 controlWriter;
    private boolean controlMutationStarted;
    private CompletableFuture<Void> controlWriterOpening;

    PulsarNativeM4ControlV1(
            AsyncOxiaClient client, MetadataNamespaceIdentityV2 namespace, PulsarNativeBindingV1 binding) {
        this.client = client;
        this.binding = binding;
        var cell = binding.authority().aggregate().binding().cellIdentity();
        String cellRoot = "/nereus/cells/pulsar_native_"
                + namespace.instanceId().toHex() + "_"
                + Sha256Digest.hash(ProtocolCellIdentityCodecV1.encode(cell)).toHex();
        // One canonical control shard within this exact P1 Cell; unrelated to Native ledger IDs/BK bookie shards.
        keys = new M4ReadControlKeysV1(0, binding.bindingIdentity());
        route = new OxiaBindingLifecycleMetadataStoreV2(
                client,
                cellRoot,
                0,
                binding.bindingIdentity(),
                Sha256Digest.hash(ProtocolCellIdentityCodecV1.encode(cell)));
        sourceMaps = new Oxia09ExactMetadataTransactionStoreV1(client);
        String selector = route.nativeKey(keys.selector());
        sourceMapPrefix = selector.substring(0, selector.length() - "/selector".length()) + "/native-source-maps/";
        ownerDrainPrefix = selector.substring(0, selector.length() - "/selector".length()) + "/native-owner-drains/";
        coldClaimPrefix = selector.substring(0, selector.length() - "/selector".length()) + "/native-cold-claims/";
        initialClaimKey =
                selector.substring(0, selector.length() - "/selector".length()) + "/native-initial-owner-claim";
        if (Math.max(sourceMapPrefix.length(), Math.max(ownerDrainPrefix.length(), coldClaimPrefix.length())) + 64
                > OxiaBindingLifecycleMetadataStoreV2.MAX_NATIVE_KEY_BYTES) {
            throw new IllegalArgumentException("Native source-map key exceeds its actual route bound");
        }
    }

    /** Exact route for diagnostics/provisioning by the qualified controller; this method creates nothing. */
    public String selectorKey() {
        return route.nativeKey(keys.selector());
    }

    CompletableFuture<Void> installControlWriter(
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry,
            com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1 coordinator,
            String authorityKey,
            PulsarNativeReadCellBudgetV1 budget) {
        try {
            PulsarNativeWriterRegistryV2.requireNativeOwners(registry);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        synchronized (this) {
            if (controlMutationStarted || controlWriterOpening != null) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Native control writer must install before mutation"));
            }
            String expectedKey = new com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys(binding.authorityRoot())
                    .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
            if (!expectedKey.equals(authorityKey)) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException("Native control C authority key differs"));
            }
            controlWriterOpening = binding.revalidate()
                    .thenCompose(unused -> coordinator.read(authorityKey))
                    .thenCompose(value -> {
                        var actual = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                .decodeAuthority(value.orElseThrow().canonicalStoredBytes());
                        if (!actual.originalAggregateSha256()
                                        .equals(binding.authority().aggregate().canonicalStoredDigest())
                                || !actual.writerEnrollment().equals(Optional.of(registry.enrollment()))
                                || !actual.originalAggregateVersion()
                                        .equals(Optional.of(
                                                binding.authority().aggregate().aggregateSourceVersion()))) {
                            throw new IllegalArgumentException("Native control C enrollment/source differs");
                        }
                        var nativeOwner = budget.nativeOwnerIdentity(binding);
                        var lifetime = budget.reserveCursorObservation(binding, 1_048_576);
                        return lifetime.ready()
                                .thenRun(() -> {
                                    lifetime.actorStarting();
                                    synchronized (this) {
                                        controlWriter = new PulsarNativeControlWriterV2(
                                                binding,
                                                budget,
                                                nativeOwner,
                                                lifetime,
                                                route,
                                                registry,
                                                com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1
                                                        .forPulsarAggregate(
                                                                registry,
                                                                coordinator,
                                                                authorityKey,
                                                                actual.originalAggregateSha256()));
                                    }
                                })
                                .exceptionallyCompose(failure -> lifetime.releaseBeforeActor()
                                        .thenCompose(unused -> CompletableFuture.failedFuture(failure)));
                    })
                    .toCompletableFuture();
            return controlWriterOpening.copy();
        }
    }

    CompletableFuture<Void> closeControlWriter() {
        final CompletableFuture<Void> opening;
        synchronized (this) {
            opening = controlWriterOpening;
        }
        return opening == null
                ? CompletableFuture.completedFuture(null)
                : opening.handle((unused, failure) -> null)
                        .thenCompose(unused -> controlWriter == null
                                ? CompletableFuture.completedFuture(null)
                                : controlWriter.close());
    }

    CompletableFuture<Void> reconcileControlWriterCleanup() {
        synchronized (this) {
            return controlWriter == null ? CompletableFuture.completedFuture(null) : controlWriter.reconcileCleanup();
        }
    }

    private com.nereusstream.storage.object.control.CanonicalControlMetadataStore referenceControlMetadata() {
        return referenceControlMetadata(
                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1.SOURCE_PROTECTION);
    }

    private com.nereusstream.storage.object.control.CanonicalControlMetadataStore referenceControlMetadata(
            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1 defaultKind) {
        return referenceControlMetadata(defaultKind, Optional.empty());
    }

    private com.nereusstream.storage.object.control.CanonicalControlMetadataStore referenceControlMetadata(
            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1 defaultKind,
            Optional<PulsarNativeControlWriterV2.PhysicalWriter> physicalAdmission) {
        var original = route.rawControlMetadata();
        return new com.nereusstream.storage.object.control.CanonicalControlMetadataStore() {
            public Optional<CanonicalBytes> get(String key) {
                return original.get(key);
            }

            public com.nereusstream.storage.object.control.ControlMutationOutcome putIfAbsent(
                    String key, CanonicalBytes value) {
                return compareAndSet(key, Optional.empty(), value);
            }

            public com.nereusstream.storage.object.control.ControlMutationOutcome compareAndSet(
                    String key, Optional<CanonicalBytes> expected, CanonicalBytes candidate) {
                final PulsarNativeControlWriterV2 writer;
                synchronized (PulsarNativeM4ControlV1.this) {
                    controlMutationStarted = true;
                    writer = controlWriter;
                    if (writer == null && controlWriterOpening != null) {
                        return com.nereusstream.storage.object.control.ControlMutationOutcome.RESPONSE_UNKNOWN;
                    }
                }
                if (writer == null) {
                    if (physicalAdmission.isPresent()) {
                        return com.nereusstream.storage.object.control.ControlMutationOutcome.RESPONSE_UNKNOWN;
                    }
                    requireLegacyControlMutation();
                    return original.compareAndSet(key, expected, candidate);
                }
                var kind = key.equals(keys.selector())
                        ? com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                .MANIFEST_SELECTED
                        : defaultKind;
                return writer.mutate(kind, key, expected, candidate, physicalAdmission)
                        .join();
            }
        };
    }

    private void requireLegacyControlMutation() {
        String key = new com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys(binding.authorityRoot())
                .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
        var actual = sourceMaps
                .read(key)
                .toCompletableFuture()
                .join()
                .orElseThrow(() -> new IllegalStateException("Native control mutation P1 authority is absent"));
        var bytes = actual.canonicalStoredBytes();
        if (com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.isAuthorityValue(bytes)) {
            var authority =
                    com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(bytes);
            if (authority.writerEnrollment().isPresent()
                    || authority.logicalTrim().isPresent()
                    || authority.state()
                            != com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1
                                    .PulsarAggregateAuthorityStateV1.OPEN_V1) {
                throw new IllegalStateException("Native enrolled control mutation requires its original P1 writer");
            }
        }
        if (!com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.projectAggregate(bytes)
                .equals(binding.authority().aggregate().canonicalStoredBytes())) {
            throw new IllegalStateException("Native control mutation P1 Binding changed");
        }
    }

    /** Exact native selection receipt; it creates no local reader or recovered actor. */
    CompletableFuture<VersionedValue> confirmMaterializationSelector(BindingReadSelector expected) {
        return binding.revalidate()
                .thenCompose(ignored -> sourceMaps.read(selectorKey()))
                .thenCompose(observed -> {
                    var fact = observed.orElseThrow(
                            () -> new IllegalStateException("Native publication selector is absent"));
                    if (!readSelector(fact).equals(expected)) {
                        throw new IllegalStateException(
                                "Native publication selector differs from its original candidate");
                    }
                    return route.read(keys.selector()).thenCompose(projected -> {
                        if (projected.isEmpty()
                                || !projected
                                        .orElseThrow()
                                        .canonicalStoredBytes()
                                        .equals(fact.canonicalStoredBytes())
                                || !readSelector(projected.orElseThrow()).equals(expected)) {
                            throw new IllegalStateException("Native publication selector projection differs");
                        }
                        return sourceMaps.read(selectorKey()).thenCompose(current -> {
                            if (!current.equals(Optional.of(fact))) {
                                throw new IllegalStateException(
                                        "Native publication selector changed during confirmation");
                            }
                            return binding.revalidate().thenApply(done -> fact);
                        });
                    });
                })
                .toCompletableFuture();
    }

    /** Original-opening identity only; this record is not a read grant or drain proof. */
    public String initialOwnerClaimKey() {
        return initialClaimKey;
    }

    public String capabilityKey(long generation) {
        return route.nativeKey(keys.capability(generation));
    }

    public String sourceMapKey(Sha256Digest selectedView) {
        if (selectedView.isZero()) {
            throw new IllegalArgumentException("Native source-map view digest is zero");
        }
        return sourceMapPrefix + selectedView.toHex();
    }

    java.util.concurrent.CompletionStage<VersionedValue> readRetirementFact(String key) {
        return sourceMaps
                .read(key)
                .thenApply(value -> value.orElseThrow(
                        () -> new IllegalStateException("Original Native M4 retirement fact is absent: " + key)));
    }

    String proofHeadKey() {
        return route.nativeKey(keys.proofHead());
    }

    java.util.concurrent.CompletionStage<Void> requireCurrentSelector(BindingReadSelector expected) {
        return readRetirementFact(selectorKey()).thenAccept(value -> {
            if (!readSelector(value).equals(expected)) {
                throw new IllegalStateException("Native original delete owner is no longer selected");
            }
        });
    }

    String materializationInputKey(Sha256Digest protocolStateRoot) {
        return route.nativeKey(new com.nereusstream.storage.object.materialization.M5MaterializationKeysV1(
                        0, binding.bindingIdentity())
                .nativeInput(protocolStateRoot));
    }

    String materializationPlanKey(Sha256Digest task) {
        return route.nativeKey(new com.nereusstream.storage.object.materialization.M5MaterializationKeysV1(
                        0, binding.bindingIdentity())
                .plan(task));
    }

    String materializationTaskKey(Sha256Digest task) {
        return route.nativeKey(new com.nereusstream.storage.object.materialization.M5MaterializationKeysV1(
                        0, binding.bindingIdentity())
                .task(task));
    }

    String materializationGenerationKey(Sha256Digest generation) {
        return route.nativeKey(new com.nereusstream.storage.object.materialization.M5MaterializationKeysV1(
                        0, binding.bindingIdentity())
                .generation(generation));
    }

    String materializationManifestKey(Sha256Digest manifest) {
        return route.nativeKey(new com.nereusstream.storage.object.materialization.M5MaterializationKeysV1(
                        0, binding.bindingIdentity())
                .manifest(manifest));
    }

    String materializationValidationKey(Sha256Digest validation) {
        return route.nativeKey(new com.nereusstream.storage.object.materialization.M5MaterializationKeysV1(
                        0, binding.bindingIdentity())
                .validation(validation));
    }

    String materializationPendingOutputsKey(Sha256Digest task) {
        return route.nativeKey(new com.nereusstream.storage.object.materialization.M5MaterializationKeysV1(
                        0, binding.bindingIdentity())
                .pendingOutputs(task));
    }

    String materializationAdmissionKey(Sha256Digest cell) {
        return route.nativeKey(
                com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1.stateKey(cell));
    }

    /** Actual immutable input on this original route; no Task/output grant is produced. */
    CompletableFuture<com.nereusstream.metadata.oxia.v2.retention.OxiaPulsarMaterializationInputV2>
            persistMaterializationInput(
                    PulsarNativeBindingReadViewV1 view,
                    com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 coverage,
                    java.util.function.Consumer<
                                    com.nereusstream.metadata.oxia.v2.retention.OxiaPulsarMaterializationInputV2>
                            originalCandidate) {
        try {
            if (!view.isCurrent()
                    || view.binding() != binding
                    || !coverage.chainAuthorityRootSha256().equals(view.sha256())) {
                throw new IllegalArgumentException("Native input is not this original Binding/view");
            }
            var p1 = binding.authority();
            var p1Keys = new com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys(binding.authorityRoot());
            var incarnation = p1.aggregate().binding().incarnationIdentity();
            var pulsar = (com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity) incarnation;
            return binding.revalidate()
                    .thenCompose(ignored -> sourceMaps.read(sourceMapKey(view.sha256())))
                    .thenCompose(stored -> {
                        var fact = stored.orElseThrow(
                                () -> new IllegalStateException("Native input original source-map is absent"));
                        var input = new com.nereusstream.metadata.oxia.v2.retention.OxiaPulsarMaterializationInputV2(
                                binding.metadataNamespace(),
                                binding.authorityRoot(),
                                p1Keys.aggregateKey(incarnation),
                                com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper.toOxia(
                                        p1.aggregate().aggregateSourceVersion()),
                                p1.aggregate().canonicalStoredBytes(),
                                p1Keys.selectorKey(pulsar.persistenceName()),
                                com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper.toOxia(
                                        p1.selector().metadataVersion()),
                                p1.selector().value().canonicalStoredBytes(),
                                fact.key(),
                                com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper.toOxia(
                                        fact.metadataVersion()),
                                fact.canonicalStoredSha256(),
                                coverage,
                                view.materializationRecoveryInput().sha256());
                        var reconstructed = input.verifySourceView(fact);
                        if (!reconstructed.equals(view.materializationRecoveryInput())) {
                            throw new IllegalStateException("Native input recovery snapshot differs from original SDK");
                        }
                        originalCandidate.accept(input); // Retained before any input metadata dispatch.
                        return confirmMaterializationInput(input)
                                .thenCompose(
                                        unused -> sourceMaps.read(fact.key()).thenCompose(current -> {
                                            if (!current.equals(Optional.of(fact)) || !view.isCurrent()) {
                                                throw new IllegalStateException(
                                                        "Native input source changed during persistence");
                                            }
                                            return binding.revalidate().thenApply(done -> input);
                                        }));
                    })
                    .toCompletableFuture();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** Exact original candidate reconciliation. Existing historical bytes settle input I/O, not a new grant. */
    CompletableFuture<Void> confirmMaterializationInput(
            com.nereusstream.metadata.oxia.v2.retention.OxiaPulsarMaterializationInputV2 input) {
        try {
            if (!input.binding().equals(binding.bindingIdentity())) {
                throw new IllegalArgumentException("Native original input belongs to another Binding");
            }
            String key = new com.nereusstream.storage.object.materialization.M5MaterializationKeysV1(
                            0, binding.bindingIdentity())
                    .nativeInput(input.protocolStateRootSha256());
            var candidate = input.encode();
            return route.read(key)
                    .thenCompose(existing -> {
                        if (existing.isPresent()) {
                            if (!existing.orElseThrow().canonicalStoredBytes().equals(candidate)) {
                                throw new IllegalStateException("Native immutable input conflicts");
                            }
                            return CompletableFuture.completedFuture(null);
                        }
                        return route.compareAndSet(Optional.empty(), key, candidate)
                                .thenApply(outcome -> null);
                    })
                    .thenCompose(unused -> route.read(key))
                    .thenAccept(confirmed -> {
                        if (!confirmed.map(VersionedValue::canonicalStoredBytes).equals(Optional.of(candidate))) {
                            throw new IllegalStateException("Native input persistence is unresolved");
                        }
                    })
                    .toCompletableFuture();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** Low-frequency publication before selector CAS; UNKNOWN never authorizes that CAS. */
    void publishSourceMap(PulsarNativeBindingReadViewV1 view) {
        publishSourceMap(view, Optional.empty());
    }

    void publishSourceMap(
            PulsarNativeBindingReadViewV1 view, Optional<PulsarNativeControlWriterV2.PhysicalWriter> physical) {
        if (!view.bindingIdentity().equals(binding.bindingIdentity()) || !view.isCurrent()) {
            throw new IllegalArgumentException("Native source-map publication requires its current actual Binding");
        }
        var map = PulsarNativeM4SourceMapV1.capture(view);
        var value = map.encode();
        String key = sourceMapKey(view.sha256());
        var existing = sourceMaps.read(key).toCompletableFuture().join();
        if (existing.isEmpty()) {
            final PulsarNativeControlWriterV2 writer;
            synchronized (this) {
                controlMutationStarted = true;
                writer = controlWriter;
                if (writer == null && (physical.isPresent() || controlWriterOpening != null)) {
                    throw new IllegalStateException("Native source-map writer is not admitted");
                }
            }
            if (writer != null) {
                if (writer.createSourceMap(map, key, physical, sourceMaps).join()
                        != com.nereusstream.storage.object.control.ControlMutationOutcome.APPLIED) {
                    throw new IllegalStateException("original Native source-map C/D mutation remains unresolved");
                }
            } else {
                sourceMaps
                        .compareAndSet(Optional.empty(), key, value)
                        .toCompletableFuture()
                        .join();
            }
        }
        var confirmed = sourceMaps.read(key).toCompletableFuture().join();
        if (confirmed
                .filter(observed -> observed.canonicalStoredBytes().equals(value))
                .isEmpty()) {
            throw new IllegalStateException("Native source-map publication remains unknown or conflicts");
        }
    }

    /** Reads the actual selected immutable bytes, with selector and source-map exact-version rereads. */
    public CompletableFuture<PulsarNativeM4SourceMapV1> readSelectedSourceMap() {
        return captureSelectedSourceMap().thenApply(value -> {
            if (value.manifest()) {
                throw new IllegalStateException("Native selected source is a materialization manifest");
            }
            return value.map().orElseThrow(() -> new IllegalStateException("Native source selector is absent"));
        });
    }

    /** Same-response versions and full immutable map bytes, including a reread of actual selector absence. */
    record SelectedSourceSnapshot(
            Optional<VersionedValue> selector, Optional<VersionedValue> source, boolean manifest) {
        SelectedSourceSnapshot {
            Objects.requireNonNull(selector, "actual selector observation");
            Objects.requireNonNull(source, "actual selected source observation");
            if (selector.isPresent() != source.isPresent() || selector.isEmpty() && manifest) {
                throw new IllegalArgumentException("Native selection snapshot is incomplete");
            }
        }

        Optional<PulsarNativeM4SourceMapV1> map() {
            if (manifest) {
                return Optional.empty();
            }
            return source.map(fact -> {
                var bytes = selector.orElseThrow().canonicalStoredBytes();
                var selected = M5BindingAuthorityCodecV1.isAuthorityValue(bytes)
                        ? M5BindingAuthorityCodecV1.decodeAuthority(bytes).selectorProjection()
                        : M4ReadControlCodecV1.decodeSelector(bytes);
                return PulsarNativeM4SourceMapV1.decode(fact.canonicalStoredBytes(), selected.selectedViewSha256());
            });
        }
    }

    CompletableFuture<SelectedSourceSnapshot> captureSelectedSourceMap() {
        try {
            return binding.revalidate()
                    .thenCompose(ignored -> route.read(keys.selector()))
                    .thenCompose(selected -> {
                        if (selected.isEmpty()) {
                            return route.read(keys.selector()).thenCompose(current -> {
                                if (current.isPresent()) {
                                    throw new IllegalStateException("Native selector changed during absence capture");
                                }
                                return binding.revalidate()
                                        .thenApply(done ->
                                                new SelectedSourceSnapshot(Optional.empty(), Optional.empty(), false));
                            });
                        }
                        var selectorFact = selected.orElseThrow();
                        var selector = readSelector(selectorFact);
                        String key = sourceMapKey(selector.selectedViewSha256());
                        return sourceMaps.read(key).thenCompose(observed -> {
                            if (observed.isEmpty()) {
                                return captureSelectedManifest(selectorFact, selector);
                            }
                            var mapFact = observed.orElseThrow();
                            PulsarNativeM4SourceMapV1.decode(
                                    mapFact.canonicalStoredBytes(), selector.selectedViewSha256());
                            return route.read(keys.selector()).thenCompose(last -> {
                                if (last.filter(selectorFact::equals).isEmpty()) {
                                    throw new IllegalStateException(
                                            "Native selector changed during source-map capture");
                                }
                                return sourceMaps.read(key).thenCompose(current -> {
                                    if (current.filter(mapFact::equals).isEmpty()) {
                                        throw new IllegalStateException(
                                                "Native source-map predecessor changed during capture");
                                    }
                                    return binding.revalidate()
                                            .thenApply(done -> new SelectedSourceSnapshot(
                                                    Optional.of(selectorFact), Optional.of(mapFact), false));
                                });
                            });
                        });
                    })
                    .toCompletableFuture()
                    .copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<SelectedSourceSnapshot> captureSelectedManifest(
            VersionedValue selectorFact, BindingReadSelector selector) {
        var key = new M5MaterializationKeysV1(0, binding.bindingIdentity()).manifest(selector.selectedViewSha256());
        return route.read(key)
                .thenCompose(value -> {
                    var fact = value.orElseThrow(
                            () -> new IllegalStateException("Native selected source map and manifest are absent"));
                    var manifest = M5MaterializationCodecV1.decodeManifest(fact.canonicalStoredBytes());
                    if (!manifest.identity().binding().equals(binding.bindingIdentity())
                            || !M5MaterializationCodecV1.manifestSha256(manifest)
                                    .equals(selector.selectedViewSha256())) {
                        throw new IllegalStateException(
                                "Native selected manifest differs from its actual Binding/selector");
                    }
                    return route.read(keys.selector()).thenCompose(current -> {
                        if (!current.equals(Optional.of(selectorFact))) {
                            throw new IllegalStateException("Native selector changed during manifest capture");
                        }
                        return route.read(key).thenCompose(last -> {
                            if (!last.equals(Optional.of(fact))) {
                                throw new IllegalStateException("Native selected manifest changed during capture");
                            }
                            return binding.revalidate()
                                    .thenApply(done -> new SelectedSourceSnapshot(
                                            Optional.of(selectorFact), Optional.of(fact), true));
                        });
                    });
                })
                .toCompletableFuture();
    }

    CompletableFuture<Void> requireSelectedSourceSnapshot(SelectedSourceSnapshot expected) {
        return captureSelectedSourceMap().thenAccept(current -> {
            if (!current.equals(expected)) {
                throw new IllegalStateException("actual Native source selection changed before dispatch");
            }
        });
    }

    /** Immutable recovery inputs only. Neither these stored facts nor their decoding grants a Native read actor. */
    public record SelectedMaterialization(
            BindingReadSelector selector,
            VersionedValue selectorFact,
            Sha256Digest materializedManifestSha256,
            PulsarNativeMaterializationReadMapV2 readMap,
            OxiaPulsarMaterializationInputV2 nativeInput,
            M5PulsarRecoveryInputV2 recoveryInput,
            M5CellProviderProfileV2 providerProfile) {}

    /** Follows the sole selected M5 manifest; never substitutes a current Object HEAD or a caller-supplied map. */
    public CompletableFuture<SelectedMaterialization> readSelectedMaterialization(Executor controlExecutor) {
        return readSelectedMaterializationSnapshot(controlExecutor)
                .thenApply(SelectedMaterializationSnapshot::selected);
    }

    /** All materialization roots, including unpublished and terminal tasks; no other lifecycle class is implied. */
    public CompletableFuture<PulsarNativeMaterializationTaskV2.RetentionRoots> captureMaterializationTaskRoots(
            VersionedValue exactScan, PulsarNativeReadCellBudgetV1 budget, Executor executor) {
        return captureMaterializationTaskRoots(
                exactScan,
                budget,
                executor,
                new com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2[0]);
    }

    /** Adds observed metadata from actual assigned physical routes, under the same original workspace. */
    public CompletableFuture<PulsarNativeMaterializationTaskV2.RetentionRoots> captureMaterializationTaskRoots(
            VersionedValue exactScan,
            PulsarNativeReadCellBudgetV1 budget,
            Executor executor,
            com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2... physicalBackends) {
        final var backends = List.of(physicalBackends);
        final PulsarNativeReadCellBudgetV1.Reservation workspace;
        try {
            workspace = budget.reserveCursorObservation(binding, PulsarNativeCursorMetadataV2.MAX_BYTES);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        var started = new java.util.concurrent.atomic.AtomicBoolean();
        var observer = new CompletableFuture<PulsarNativeMaterializationTaskV2.RetentionRoots>();
        workspace
                .ready()
                .thenApplyAsync(
                        unused -> {
                            if (observer.isDone()) {
                                throw new java.util.concurrent.CancellationException(
                                        "Native task-root observer ended before dispatch");
                            }
                            workspace.sourceStarting();
                            started.set(true);
                            return PulsarNativeMaterializationTaskV2.captureRetentionRoots(
                                    binding, this, exactScan, workspace, backends);
                        },
                        executor)
                .whenComplete((roots, failure) -> {
                    java.util.function.Supplier<CompletableFuture<Void>> cleanup =
                            () -> started.get() ? workspace.releaseAfterDrain() : workspace.releaseBeforeActor();
                    if (failure == null) {
                        if (!observer.complete(roots)) {
                            roots.closeAsync();
                        }
                    } else {
                        cleanup.get().whenComplete((unused, cleanupFailure) -> {
                            if (cleanupFailure != null) {
                                workspace.retainUndeliveredSdk(cleanup, false);
                            }
                            observer.completeExceptionally(cleanupFailure == null ? failure : cleanupFailure);
                        });
                    }
                });
        return observer;
    }

    /** Actual selected/read-generation graph inside the P1 scan fence; missing graph/proofs veto the floor. */
    public CompletableFuture<
                    com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1>
            captureGenerationFloor(
                    VersionedValue exactScan,
                    com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 directory,
                    PulsarNativeReadCellBudgetV1 budget,
                    Executor executor) {
        if (!directory.binding().equals(binding.bindingIdentity())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Native floor directory Binding differs"));
        }
        final PulsarNativeReadCellBudgetV1.Reservation workspace;
        try {
            workspace = budget.reserveCursorObservation(binding, PulsarNativeCursorMetadataV2.MAX_BYTES);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        var started = new java.util.concurrent.atomic.AtomicBoolean();
        var observer = new CompletableFuture<
                com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1>();
        workspace
                .ready()
                .thenApplyAsync(
                        unused -> {
                            workspace.sourceStarting();
                            started.set(true);
                            return captureGenerationFloor(exactScan, directory);
                        },
                        executor)
                .whenComplete((floor, failure) -> {
                    java.util.function.Supplier<CompletableFuture<Void>> cleanup =
                            () -> started.get() ? workspace.releaseAfterDrain() : workspace.releaseBeforeActor();
                    cleanup.get().whenComplete((unused, cleanupFailure) -> {
                        if (cleanupFailure != null) {
                            workspace.retainUndeliveredSdk(cleanup, false);
                            observer.completeExceptionally(cleanupFailure);
                        } else if (failure != null) {
                            observer.completeExceptionally(failure);
                        } else {
                            observer.complete(floor);
                        }
                    });
                });
        return observer;
    }

    /**
     * Collects the complete closed Native physical-reference inventory from its original producers. The caller
     * retains the cursor/task captures through consumption. PRESENT rows remain visible; this is neither a
     * reference-free proof nor elapsed-grace qualification. Cancellation never releases either original capture.
     */
    /** All ten floor classes from the original native producers; unresolved Task/shared domains veto. */
    public CompletableFuture<
                    List<com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1>>
            captureRetentionFloors(
                    VersionedValue exactScan,
                    PulsarNativeCursorFloorScanV3 cursors,
                    PulsarNativeMaterializationTaskV2.RetentionRoots tasks,
                    M5ClosedWriterRegistryV1 registry,
                    com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2 inventory,
                    PhysicalResourceIdV2 target,
                    PulsarNativeReadCellBudgetV1 budget,
                    Executor executor) {
        return cursors.retainObservation(() -> {
            var coverage = cursors.coverage();
            var work = captureGenerationFloor(exactScan, coverage, budget, executor)
                    .thenApply(
                            generation -> {
                                var values = new ArrayList<
                                        com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1
                                                .FloorAdapterResultV1>();
                                values.add(generation);
                                for (var row : List.of(cursors.floor(), cursors.bindingRetentionFloor())) {
                                    values.add(new com.nereusstream.storage.object.retention
                                            .M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1(
                                            row.floorClass(),
                                            1,
                                            cursors.witness().length(),
                                            List.of(row)));
                                }
                                return values;
                            })
                    .thenCompose(values -> tasks.refreshReferenceReservations().thenApply(ignored -> {
                        values.add(tasks.closedTaskFloor(registry, target, cursors));
                        return values;
                    }))
                    .thenCompose(values -> binding.captureExclusivePhysicalFloor(
                                    registry, exactScan, inventory, target, coverage)
                            .thenApply(shared -> {
                                values.add(shared);
                                return values;
                            }))
                    .thenCompose(values -> binding.captureNativeOnlyKafkaFloors(registry, exactScan)
                            .thenApply(kafka -> {
                                for (var row : kafka.rows()) {
                                    values.add(new com.nereusstream.storage.object.retention
                                            .M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1(
                                            row.floorClass(),
                                            1,
                                            exactScan.canonicalStoredBytes().length(),
                                            List.of(row)));
                                }
                                return values;
                            }))
                    .thenCompose(values -> binding.captureAccessProjectionFloor(registry, exactScan)
                            .thenApply(projection -> {
                                values.add(projection);
                                return values;
                            }))
                    .thenCompose(values -> binding.captureAuditFloor(registry, exactScan, coverage)
                            .thenApply(audit -> {
                                values.add(audit);
                                return values;
                            }));
            return work.thenCompose(values -> {
                        var kinds =
                                values.stream().map(value -> value.floorClass()).toList();
                        if (kinds.size()
                                        != com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1
                                                .values()
                                                .length
                                || kinds.stream().distinct().count() != kinds.size()) {
                            throw new IllegalStateException(
                                    "Native floor composition differs from the closed ten-class inventory");
                        }
                        return new com.nereusstream.storage.object.retention.M5ReferenceFreshnessVerifierV1(sourceMaps)
                                .requireFreshFacts(values.stream()
                                        .flatMap(value -> value.observations().stream())
                                        .map(row -> row.authority())
                                        .toList())
                                .thenCompose(ignored ->
                                        binding.logicalTrimCoordinator().requireSourceScanCurrent(exactScan))
                                .thenApply(ignored -> values.stream()
                                        .sorted(java.util.Comparator.comparing(value -> value.floorClass()))
                                        .toList());
                    })
                    .toCompletableFuture();
        });
    }

    public CompletableFuture<List<ReferenceAdapterResultV1>> capturePhysicalReferences(
            VersionedValue exactScan,
            PulsarNativeCursorFloorScanV3 cursors,
            PulsarNativeMaterializationTaskV2.RetentionRoots tasks,
            M5ClosedWriterRegistryV1 registry,
            com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2 inventory,
            PhysicalResourceIdV2 target,
            PulsarNativeReadCellBudgetV1 budget,
            Executor executor) {
        var coverage = cursors.coverage();
        var work = binding.captureExcludedPhysicalReferences(registry, exactScan, inventory, target, coverage)
                .thenApply(values -> new java.util.ArrayList<>(values))
                .thenCompose(values -> binding.captureAuditReferences(registry, exactScan, inventory, target, coverage)
                        .thenApply(audit -> {
                            values.add(audit);
                            return values;
                        }))
                .thenCompose(values -> binding.captureSharedPhysicalReferences(
                                registry, exactScan, inventory, target, coverage)
                        .thenApply(shared -> {
                            values.add(shared);
                            return values;
                        }));
        for (var kind : List.of(
                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                        .PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR,
                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                        .RECOVERY_CHECKPOINT_OR_SNAPSHOT)) {
            work = work.thenCompose(values -> cursors.scanReferences(registry, kind, target, executor)
                    .thenApply(cursor -> {
                        values.add(cursor);
                        return values;
                    }));
        }
        var original = work.thenCompose(values -> captureGenerationReferences(
                                exactScan, cursors, registry, target, budget, executor, Optional.of(tasks))
                        .thenApply(generations -> {
                            values.addAll(generations);
                            var sourceProtection = generations.stream()
                                    .filter(result -> result.referenceKind() == ReferenceKindV1.SOURCE_PROTECTION)
                                    .findFirst()
                                    .orElseThrow();
                            // The same actual M4 batch/protection enumeration and old-owner drain also
                            // exclude unresolved source-retirement ownership. Permanent RELEASED history
                            // is not a new readable reference to the old resource.
                            var retirementKind = ReferenceKindV1.RETIREMENT_OR_DELETE_RECONCILIATION;
                            var retirementRows = sourceProtection.observations().stream()
                                    .map(row -> new ReferenceObservationV1(
                                            retirementKind,
                                            row.authority(),
                                            row.targetIdentitySha256(),
                                            row.coverage(),
                                            row.disposition(),
                                            row.enumerationComplete()))
                                    .toList();
                            values.add(new ReferenceAdapterResultV1(
                                    retirementKind,
                                    new ReferenceScanSummaryV1(
                                            retirementKind,
                                            retirementRows.size(),
                                            sourceProtection.summary().pageCount(),
                                            sourceProtection.summary().scannedBytes(),
                                            sourceProtection.summary().complete()),
                                    retirementRows));

                            return values;
                        }))
                .thenCompose(values -> tasks.refreshReferenceReservations().thenApply(ignored -> values))
                .thenApply(values -> {
                    values.add(tasks.closedTaskReferences(registry, target, cursors));
                    var kinds = values.stream()
                            .map(ReferenceAdapterResultV1::referenceKind)
                            .toList();
                    if (kinds.size()
                                    != com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                            .values()
                                            .length
                            || kinds.stream().distinct().count() != kinds.size()) {
                        throw new IllegalStateException(
                                "Native physical-reference inventory is incomplete or duplicated");
                    }
                    values.sort(java.util.Comparator.comparing(ReferenceAdapterResultV1::referenceKind));
                    return List.copyOf(values);
                })
                .thenCompose(values ->
                        binding.requirePhysicalReferenceFactsCurrent(values).thenApply(ignored -> values))
                .thenCompose(values -> binding.logicalTrimCoordinator()
                        .requireSourceScanCurrent(exactScan)
                        .thenApply(ignored -> values))
                .toCompletableFuture();
        return original.thenApply(values -> values);
    }

    /** Actual selected/fallback/protection references and closed registered read actors under the same P1 scan. */
    public CompletableFuture<List<ReferenceAdapterResultV1>> captureGenerationReferences(
            VersionedValue exactScan,
            PulsarNativeCursorFloorScanV3 cursors,
            M5ClosedWriterRegistryV1 registry,
            PhysicalResourceIdV2 target,
            PulsarNativeReadCellBudgetV1 budget,
            Executor executor) {
        return captureGenerationReferences(exactScan, cursors, registry, target, budget, executor, Optional.empty());
    }

    private CompletableFuture<List<ReferenceAdapterResultV1>> captureGenerationReferences(
            VersionedValue exactScan,
            PulsarNativeCursorFloorScanV3 cursors,
            M5ClosedWriterRegistryV1 registry,
            PhysicalResourceIdV2 target,
            PulsarNativeReadCellBudgetV1 budget,
            Executor executor,
            Optional<PulsarNativeMaterializationTaskV2.RetentionRoots> tasks) {
        final PulsarNativeReadCellBudgetV1.Reservation workspace;
        try {
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(executor, "executor");
            PulsarNativeWriterRegistryV2.requireNativeOwners(registry);
            var authority = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(exactScan.canonicalStoredBytes());
            if (authority.retentionPolicy().isEmpty()
                    || authority.firstNativeFactoryScope().isEmpty()
                    || !authority.writerEnrollment().equals(Optional.of(registry.enrollment()))
                    || !cursors.coverage().binding().equals(binding.bindingIdentity())) {
                throw new IllegalStateException("Native generation references lack original closed writer admission");
            }
            workspace = budget.reserveCursorObservation(binding, PulsarNativeCursorMetadataV2.MAX_BYTES);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        var started = new java.util.concurrent.atomic.AtomicBoolean();
        var observer = new CompletableFuture<List<ReferenceAdapterResultV1>>();
        workspace
                .ready()
                .thenApplyAsync(
                        unused -> {
                            workspace.sourceStarting();
                            started.set(true);
                            return captureGenerationReferences(exactScan, cursors, target, workspace, tasks);
                        },
                        executor)
                .whenComplete((references, failure) -> {
                    Supplier<CompletableFuture<Void>> cleanup =
                            () -> started.get() ? workspace.releaseAfterDrain() : workspace.releaseBeforeActor();
                    cleanup.get().whenComplete((unused, cleanupFailure) -> {
                        if (cleanupFailure != null) {
                            workspace.retainUndeliveredSdk(cleanup, false);
                            observer.completeExceptionally(cleanupFailure);
                        } else if (failure != null) {
                            observer.completeExceptionally(failure);
                        } else {
                            observer.complete(references);
                        }
                    });
                });
        return observer;
    }

    private List<ReferenceAdapterResultV1> captureGenerationReferences(
            VersionedValue exactScan,
            PulsarNativeCursorFloorScanV3 cursors,
            PhysicalResourceIdV2 target,
            PulsarNativeReadCellBudgetV1.Reservation workspace,
            Optional<PulsarNativeMaterializationTaskV2.RetentionRoots> tasks) {
        var cursorOperation = cursors.referenceScope(exactScan, workspace);
        var directory = cursors.coverage();
        // NPOD plus every pending anchor's original terminal/proof qualifies the registered actor drain.
        var floor = captureGenerationFloor(exactScan, directory);
        var reservations = workspace.captureReservations().join();
        var taskOperation = tasks.map(value -> value.referenceScope(exactScan, workspace));
        if (reservations.otherReservations().keySet().stream()
                .anyMatch(id -> !id.equals(cursorOperation) && !taskOperation.equals(Optional.of(id)))) {
            throw new IllegalStateException("Native generation references have unresolved Cell reservations");
        }
        var selection = captureSelectedSourceMap().join();
        var materialized = selection.manifest()
                || selection
                        .map()
                        .filter(map -> PulsarNativeMaterializedReadV2.isRecipe(map.actualViewBytes()))
                        .isPresent();
        var selected = materialized ? captureSelectedMaterialization().selected() : null;
        var nativeMap = materialized ? null : selection.map().orElseThrow();
        var map = selected == null ? null : selected.readMap();
        var physical = map == null ? nativeMap.floorDirectory(binding) : map.originalNativeDirectory(binding);
        if (!target.namespace().equals(physical.bookKeeper().physicalNamespace())
                && !target.namespace().equals(physical.objects().physicalNamespace())) {
            throw new IllegalArgumentException("Native generation target belongs to another physical namespace");
        }
        var selector = selected == null ? readSelector(selection.selector().orElseThrow()) : selected.selector();
        var preferred = new java.util.TreeSet<PhysicalResourceIdV2>();
        var fallback = new java.util.TreeSet<PhysicalResourceIdV2>();
        var protectedSources = new java.util.TreeSet<PhysicalResourceIdV2>();
        var chain = map == null
                ? physical.chain()
                : map.plan()
                        .sourceCut()
                        .coverage()
                        .pulsarCoverage()
                        .orElseThrow()
                        .chain();
        for (var ledger : chain) {
            preferred.addAll((map == null
                            ? nativeMap.physicalLedgerReferences(
                                    binding,
                                    physical.bookKeeper(),
                                    physical.objects(),
                                    ledger.ledgerId(),
                                    ledger.entryEndExclusive())
                            : map.physicalLedgerReferences(
                                    binding,
                                    physical.bookKeeper(),
                                    physical.objects(),
                                    ledger.ledgerId(),
                                    ledger.entryEndExclusive()))
                    .orElseThrow(() -> new IllegalStateException("Native selected physical source is unresolved")));
        }
        if (map != null) {
            map.readableOutputs()
                    .forEach(credit -> preferred.add(credit.resource().orElseThrow()));
        }
        if (selector.mode() == SelectorMode.PREFERRED_WITH_FALLBACK) {
            if (map == null) {
                throw new IllegalStateException("Native plain-view fallback references are unresolved");
            }
            fallback.addAll(map.fallbackPhysicalReferences(binding, physical.bookKeeper(), physical.objects()));
        }
        var protections = scanGenerationProtections();
        for (var fact : protections.values()) {
            var protection = M4ReadControlCodecV1.decodeProtection(fact.canonicalStoredBytes());
            if (protection.state() == ProtectionState.RELEASED) {
                continue;
            }
            if (map != null) {
                protectedSources.addAll(map.protectedPhysicalReferences(
                        binding,
                        physical.bookKeeper(),
                        physical.objects(),
                        protection.identity().sourceIdentitySha256()));
            } else {
                var ledger = chain.stream()
                        .filter(row -> row.sourceIdentitySha256()
                                .equals(protection.identity().sourceIdentitySha256()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("Native protected physical source is unresolved"));
                protectedSources.addAll(nativeMap
                        .physicalLedgerReferences(
                                binding,
                                physical.bookKeeper(),
                                physical.objects(),
                                ledger.ledgerId(),
                                ledger.entryEndExclusive())
                        .orElseThrow());
            }
        }
        long bytes = floor.scannedBytes();
        for (var references : List.of(preferred, fallback, protectedSources)) {
            for (var resource : references) {
                bytes = Math.addExact(
                        bytes, PhysicalResourceIdCodecV2.encode(resource).length());
            }
        }
        if (bytes > PulsarNativeCursorMetadataV2.MAX_BYTES) {
            throw new IllegalArgumentException("Native generation references exceed the original metadata workspace");
        }
        // P1 freezes new actor/source admission. Cell-head bytes are a live admission check, not a durable
        // reference root: this metadata observer's own eventual release necessarily advances that head.
        if (!workspace.captureReservations().join().equals(reservations)) {
            throw new IllegalStateException("Native generation reservation inventory changed during capture");
        }
        for (var row : floor.observations()) {
            var fact = sourceMaps
                    .read(row.authority().key())
                    .toCompletableFuture()
                    .join()
                    .orElseThrow();
            if (!fact.metadataVersion().equals(row.authority().metadataVersion())
                    || !fact.canonicalStoredSha256().equals(row.authority().valueSha256())) {
                throw new IllegalStateException("Native generation reference authority changed during capture");
            }
        }
        if (!scanGenerationProtections().equals(protections)) {
            throw new IllegalStateException("Native generation reference protections changed during capture");
        }
        requireSelectedSourceSnapshot(selection).join();
        binding.logicalTrimCoordinator()
                .requireSourceScanCurrent(exactScan)
                .toCompletableFuture()
                .join();
        cursors.referenceScope(exactScan, workspace);
        tasks.ifPresent(value -> value.referenceScope(exactScan, workspace));
        var results = new ArrayList<ReferenceAdapterResultV1>();
        for (var kind : List.of(
                ReferenceKindV1.MANIFEST_SELECTED,
                ReferenceKindV1.MANIFEST_FALLBACK,
                ReferenceKindV1.SOURCE_PROTECTION,
                ReferenceKindV1.READ_GENERATION_PIN_OR_OPEN_HANDLE)) {
            boolean present =
                    switch (kind) {
                        case MANIFEST_SELECTED -> preferred.contains(target);
                        case MANIFEST_FALLBACK -> fallback.contains(target);
                        case SOURCE_PROTECTION -> protectedSources.contains(target);
                        default -> false; // Exact original registered actor and every older anchor were drained above.
                    };
            var rows = floor.observations().stream()
                    .map(row -> new ReferenceObservationV1(
                            kind,
                            row.authority(),
                            target.sha256(),
                            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage
                                    .pulsar(directory),
                            present ? ReferenceDispositionV1.PRESENT : ReferenceDispositionV1.ABSENT,
                            true))
                    .toList();
            results.add(new ReferenceAdapterResultV1(
                    kind, new ReferenceScanSummaryV1(kind, rows.size(), 1, bytes, true), rows));
        }
        return List.copyOf(results);
    }

    private com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1
            captureGenerationFloor(
                    VersionedValue exactScan,
                    com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 directory) {
        binding.logicalTrimCoordinator()
                .requireSourceScanCurrent(exactScan)
                .toCompletableFuture()
                .join();
        var facts = new LinkedHashMap<String, VersionedValue>();
        var selectionFact = captureNativeFact(selectorKey(), facts);
        var selector = readSelector(selectionFact);
        String originalSourceKey = sourceMapKey(selector.selectedViewSha256());
        var originalSource =
                sourceMaps.read(originalSourceKey).toCompletableFuture().join();
        var nativeMap = originalSource.map(
                value -> PulsarNativeM4SourceMapV1.decode(value.canonicalStoredBytes(), selector.selectedViewSha256()));
        final List<com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.ChainLedger> retained;
        final List<Sha256Digest> sourceIdentities;
        if (nativeMap.isPresent()
                && !PulsarNativeMaterializedReadV2.isRecipe(
                        nativeMap.orElseThrow().actualViewBytes())) {
            facts.put(originalSourceKey, originalSource.orElseThrow());
            retained = nativeMap.orElseThrow().floorChain(binding);
            sourceIdentities =
                    retained.stream().map(value -> value.sourceIdentitySha256()).toList();
        } else {
            var captured = captureSelectedMaterialization();
            var selected = captured.selected();
            if (!selected.selectorFact().equals(selectionFact)) {
                throw new IllegalStateException("Native generation floor selection changed during capture");
            }
            facts.putAll(captured.nativeFacts());
            for (var entry : captured.immutableFacts().entrySet()) {
                var raw = captureNativeFact(route.nativeKey(entry.getKey()), facts);
                if (!raw.canonicalStoredBytes().equals(entry.getValue().canonicalStoredBytes())) {
                    throw new IllegalStateException("Native generation floor immutable source changed");
                }
            }
            retained = selected.readMap()
                    .plan()
                    .sourceCut()
                    .coverage()
                    .pulsarCoverage()
                    .orElseThrow()
                    .chain();
            sourceIdentities = selected.readMap().plan().sourceCut().sources().stream()
                    .map(value -> value.sourceIdentitySha256())
                    .toList();
        }
        var capability = M4ReadControlCodecV1.decodeCapability(
                captureNativeFact(capabilityKey(selector.capability().generation()), facts)
                        .canonicalStoredBytes());
        if (!capability.binding().equals(binding.bindingIdentity())
                || capability.generation() != selector.capability().generation()
                || capability.kind() != M4ReadControlRecordsV1.CapabilityKind.DURABLE_DRAIN_ONLY_V1
                || !M4ReadControlCodecV1.capabilityEvidenceSha256(capability)
                        .equals(selector.capability().evidenceSha256())) {
            throw new IllegalStateException("Native generation floor capability differs");
        }
        requireGenerationOwnerDrain(selector, capability, captureNativeFact(ownerDrainKey(selector), facts));
        var protections = scanGenerationProtections();
        facts.putAll(protections);
        for (var protectionFact : protections.values()) {
            var protection = M4ReadControlCodecV1.decodeProtection(protectionFact.canonicalStoredBytes());
            if (!protection.binding().equals(binding.bindingIdentity())
                    || !protectionKey(protection.identity()).equals(protectionFact.key())) {
                throw new IllegalStateException("Native generation floor protection key/Binding differs");
            }
            if (protection.state() != M4ReadControlRecordsV1.ProtectionState.RELEASED
                    && !sourceIdentities.contains(protection.identity().sourceIdentitySha256())) {
                throw new IllegalStateException("Native generation floor has an unresolved older protected source");
            }
        }
        for (var batch : selector.activeBatches()) {
            for (var source : batch.sources()) {
                var protection = M4ReadControlCodecV1.decodeProtection(
                        captureNativeFact(protectionKey(source), facts).canonicalStoredBytes());
                if (!protection.binding().equals(binding.bindingIdentity())
                        || !protection.identity().equals(source)) {
                    throw new IllegalStateException("Native generation floor protection differs");
                }
            }
        }
        // An unresolved old epoch may still own a read view; never replace its missing proof with absence.
        for (var anchor : selector.pendingAnchors()) {
            var oldDrain = captureNativeFact(
                    ownerDrainPrefix + anchor.predecessorSelectorCoreSha256().toHex(), facts);
            var originalSelector = generationDrainSelector(oldDrain.canonicalStoredBytes());
            if (!M4ReadControlCodecV1.selectorCoreSha256(originalSelector)
                            .equals(anchor.predecessorSelectorCoreSha256())
                    || originalSelector.readAdmissionEpoch() != anchor.closedReadAdmissionEpoch()
                    || originalSelector.ownerEpoch() != anchor.ownerEpoch()) {
                throw new IllegalStateException("Native old-epoch drain differs from its exact anchor");
            }
            var oldCapability = M4ReadControlCodecV1.decodeCapability(captureNativeFact(
                            capabilityKey(originalSelector.capability().generation()), facts)
                    .canonicalStoredBytes());
            requireGenerationOwnerDrain(originalSelector, oldCapability, oldDrain);
            var terminal = M4ReadControlCodecV1.decodeTerminal(
                    captureNativeFact(terminalKey(anchor.closedReadAdmissionEpoch()), facts)
                            .canonicalStoredBytes());
            var proof = M4ReadControlCodecV1.decodeProof(
                    captureNativeFact(proofKey(anchor.closedReadAdmissionEpoch()), facts)
                            .canonicalStoredBytes());
            if (!terminal.binding().equals(binding.bindingIdentity())
                    || !proof.binding().equals(binding.bindingIdentity())
                    || !terminal.closureAnchorSha256().equals(M4ReadControlCodecV1.anchorSha256(anchor))
                    || terminal.readAdmissionEpoch() != anchor.closedReadAdmissionEpoch()
                    || terminal.ownerEpoch() != anchor.ownerEpoch()
                    || !terminal.capability().equals(anchor.capability())
                    || terminal.kind() != M4ReadControlRecordsV1.TerminalKind.PLANNED_DRAIN
                    || proof.readAdmissionEpoch() != terminal.readAdmissionEpoch()
                    || !proof.terminalCutSha256().equals(M4ReadControlCodecV1.terminalSha256(terminal))
                    || !proof.capability().equals(anchor.capability())
                    || proof.kind() != terminal.kind()
                    || proof.safeAfterAuthorityTimeMillis() < terminal.safeAfterAuthorityTimeMillis()
                    || proof.drainedThroughReadViewGeneration() < terminal.lastAdmittedAndDrainedReadViewGeneration()) {
                throw new IllegalStateException("Native generation floor lacks an exact closed-epoch proof");
            }
        }
        var last = retained.stream()
                .filter(ledger -> ledger.entryEndExclusive() > 0)
                .reduce((left, right) -> right)
                .orElseThrow(() -> new IllegalStateException("Native selected generation has no floor"));
        // Every admitted actor is drained; future actors must acquire P1 and capture its committed frontier.
        var safe = new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                last.ledgerId(), last.entryEndExclusive());
        directory.compare(safe, safe); // Validates the actual directory; never clamps missing retired lineage.
        facts.put(exactScan.key(), exactScan);
        long bytes = 0;
        for (var fact : facts.values()) {
            bytes = Math.addExact(bytes, fact.canonicalStoredBytes().length());
            if (bytes > PulsarNativeCursorMetadataV2.MAX_BYTES) {
                throw new IllegalArgumentException("Native generation floor exceeds its admitted metadata workspace");
            }
            if (!sourceMaps.read(fact.key()).toCompletableFuture().join().equals(Optional.of(fact))) {
                throw new IllegalStateException("Native generation floor version/value changed during capture");
            }
        }
        binding.logicalTrimCoordinator()
                .requireSourceScanCurrent(exactScan)
                .toCompletableFuture()
                .join();
        if (!scanGenerationProtections().equals(protections)) {
            throw new IllegalStateException("Native generation protection inventory changed during capture");
        }
        var kind = com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1.GENERATION_READ;
        var rows = facts.values().stream()
                .sorted(java.util.Comparator.comparing(VersionedValue::key))
                .map(fact ->
                        new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorObservationV1(
                                kind,
                                new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                                        fact.key(), fact.metadataVersion(), fact.canonicalStoredSha256()),
                                com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1
                                        .PositionDomain.PULSAR_ENTRY,
                                -1,
                                true,
                                true,
                                Optional.of(safe)))
                .toList();
        return new com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1(
                kind, 1, bytes, rows);
    }

    private static BindingReadSelector generationDrainSelector(CanonicalBytes bytes) {
        var input = ByteBuffer.wrap(bytes.toByteArray());
        if (input.remaining() < 140 || input.getInt() != 0x4e504f44 || input.getInt() != 1) {
            throw new IllegalStateException("Native generation floor needs a registered actor NPOD drain");
        }
        int length = input.getInt();
        if (length < 1 || length != input.remaining() - 128) {
            throw new IllegalStateException("Native generation floor drain length differs");
        }
        byte[] selected = new byte[length];
        input.get(selected);
        return M4ReadControlCodecV1.decodeSelector(CanonicalBytes.copyOf(selected));
    }

    private static void requireGenerationOwnerDrain(
            BindingReadSelector selector, CapabilityEvidence capability, VersionedValue drain) {
        var original = generationDrainSelector(drain.canonicalStoredBytes());
        if (!M4ReadControlCodecV1.selectorCoreSha256(original)
                .equals(M4ReadControlCodecV1.selectorCoreSha256(selector))) {
            throw new IllegalStateException("Native generation floor drain belongs to another actor/view");
        }
        // Metadata normalization may fold anchors/batches without changing this actor's read-admission core.
        byte[] selected = M4ReadControlCodecV1.encodeSelector(original).toByteArray();
        var expected = CanonicalBytes.copyOf(ByteBuffer.allocate(12 + selected.length + 128)
                .putInt(0x4e504f44)
                .putInt(1)
                .putInt(selected.length)
                .put(selected)
                .put(capability.conformanceReceiptSha256().bytes().toByteArray())
                .put(capability.conformanceReceiptIdentitySha256().bytes().toByteArray())
                .put(capability.backendAdapterSha256().bytes().toByteArray())
                .put(capability.backendProtocolConfigurationSha256().bytes().toByteArray())
                .array());
        if (!capability.binding().equals(selector.binding())
                || capability.generation() != selector.capability().generation()
                || !M4ReadControlCodecV1.capabilityEvidenceSha256(capability)
                        .equals(selector.capability().evidenceSha256())
                || capability.kind() != CapabilityKind.DURABLE_DRAIN_ONLY_V1
                || !capability.readAdmissionContractSha256().equals(capability.backendAdapterSha256())
                || !capability.verifierSha256().equals(capability.backendAdapterSha256())
                || !drain.canonicalStoredBytes().equals(expected)) {
            throw new IllegalStateException("Native generation floor lacks an exact qualified original actor drain");
        }
    }

    Map<String, VersionedValue> scanMaterializationNamespace() {
        var root = materializationNamespaceRoot();
        // Oxia orders keys hierarchically, not by a raw byte prefix. The checked route permits ASCII
        // kind names and these two record depths; scan each level without crossing a sibling Binding.
        var facts = new LinkedHashMap<>(scanNativeNamespace(root + "/!/", root + "/~/", root + "/"));
        var retained = scanNativeNamespace(root + "/retention/!/", root + "/retention/~/", root + "/retention/");
        for (var fact : retained.values()) {
            if (facts.putIfAbsent(fact.key(), fact) != null) {
                throw new IllegalStateException("Native materialization scan repeated a key across levels");
            }
        }
        if (facts.size() > com.nereusstream.storage.object.retention.M5RetentionRecordsV1.MAX_FLOOR_ROWS) {
            throw new IllegalArgumentException("Native materialization root inventory exceeds its row bound");
        }
        return Map.copyOf(facts);
    }

    Map<String, VersionedValue> captureMaterializationControls() {
        var facts = new LinkedHashMap<String, VersionedValue>();
        var selector = captureNativeFact(selectorKey(), facts);
        readSelector(selector);
        var cell = Sha256Digest.hash(ProtocolCellIdentityCodecV1.encode(
                binding.authority().aggregate().binding().cellIdentity()));
        sourceMaps
                .read(materializationAdmissionKey(cell))
                .toCompletableFuture()
                .join()
                .ifPresent(fact -> facts.put(fact.key(), fact));
        return Map.copyOf(facts);
    }

    void requireMaterializationControlsCurrent(Map<String, VersionedValue> captured) {
        if (!captureMaterializationControls().equals(captured)) {
            throw new IllegalStateException("Native materialization worker/admission version changed during capture");
        }
    }

    void requireMaterializationNamespaceCurrent(Map<String, VersionedValue> captured) {
        for (var fact : captured.values()) {
            if (!sourceMaps.read(fact.key()).toCompletableFuture().join().equals(Optional.of(fact))) {
                throw new IllegalStateException("Native materialization root version/value changed during capture");
            }
        }
        if (!scanMaterializationNamespace().equals(captured)) {
            throw new IllegalStateException("Native materialization root inventory changed during capture");
        }
    }

    String materializationNamespaceRoot() {
        var keys = new M5MaterializationKeysV1(0, binding.bindingIdentity());
        var leaf = keys.plan(binding.authority().aggregate().canonicalStoredDigest());
        var nativeLeaf = route.nativeKey(leaf);
        return nativeLeaf.substring(
                0, nativeLeaf.length() - leaf.length() + keys.root().length());
    }

    private Map<String, VersionedValue> scanGenerationProtections() {
        String selector = selectorKey();
        String root = selector.substring(0, selector.lastIndexOf('/'));
        return scanNativeNamespace(root + "/protections/", root + "/protections//", root + "/protections/");
    }

    private Map<String, VersionedValue> scanNativeNamespace(String first, String end, String prefix) {
        var result = new CompletableFuture<Map<String, VersionedValue>>();
        client.rangeScan(first, end, new io.oxia.client.api.RangeScanConsumer() {
            private final Map<String, VersionedValue> found = new java.util.TreeMap<>();
            private long bytes;
            private Throwable failure;

            public boolean onNext(io.oxia.client.api.GetResult value) {
                try {
                    if (!value.key().startsWith(prefix)
                            || found.size()
                                    >= com.nereusstream.storage.object.retention.M5RetentionRecordsV1.MAX_FLOOR_ROWS
                            || (bytes = Math.addExact(bytes, value.value().length))
                                    > PulsarNativeCursorMetadataV2.MAX_BYTES) {
                        throw new IllegalArgumentException("Native metadata scan exceeds its exact scope/bounds");
                    }
                    var fact = VersionedValue.of(
                            value.key(),
                            CanonicalBytes.copyOf(value.value()),
                            com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper.fromOxia(
                                    value.version().versionId()));
                    if (found.putIfAbsent(fact.key(), fact) != null) {
                        throw new IllegalStateException("Native metadata scan repeated a key");
                    }
                    return true;
                } catch (Throwable error) {
                    failure = error;
                    return false;
                }
            }

            public void onError(Throwable error) {
                result.completeExceptionally(error);
            }

            public void onCompleted() {
                if (failure == null) {
                    result.complete(Map.copyOf(found));
                } else {
                    result.completeExceptionally(failure);
                }
            }
        });
        return result.join();
    }

    record SelectedMaterializationSnapshot(
            SelectedMaterialization selected,
            Map<String, VersionedValue> immutableFacts,
            Map<String, VersionedValue> nativeFacts) {
        SelectedMaterializationSnapshot {
            immutableFacts = Map.copyOf(immutableFacts);
            nativeFacts = Map.copyOf(nativeFacts);
        }
    }

    CompletableFuture<SelectedMaterializationSnapshot> readSelectedMaterializationSnapshot(Executor controlExecutor) {
        try {
            return binding.revalidate()
                    .thenApplyAsync(ignored -> captureSelectedMaterialization(), controlExecutor)
                    .toCompletableFuture()
                    .copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** Runs on the caller's already admitted metadata worker; decoding never grants an SDK actor. */
    SelectedMaterializationSnapshot captureSelectedMaterialization() {
        var immutableFacts = new LinkedHashMap<String, VersionedValue>();
        var nativeFacts = new LinkedHashMap<String, VersionedValue>();
        var selectorFact = captureNativeFact(selectorKey(), nativeFacts);
        var selected = readSelector(selectorFact);
        var projected = route.read(keys.selector()).toCompletableFuture().join().orElseThrow();
        if (!projected.canonicalStoredBytes().equals(selectorFact.canonicalStoredBytes())) {
            throw new IllegalStateException("Native selected materialization projection differs");
        }
        var materialization = new M5MaterializationKeysV1(0, binding.bindingIdentity());
        var manifestSha = selected.selectedViewSha256();
        PulsarNativeMaterializedReadV2.Recipe recipe = null;
        if (route.read(materialization.manifest(manifestSha))
                .toCompletableFuture()
                .join()
                .isEmpty()) {
            var selectedMap = PulsarNativeM4SourceMapV1.decode(
                    captureNativeFact(sourceMapKey(manifestSha), nativeFacts).canonicalStoredBytes(), manifestSha);
            recipe = PulsarNativeMaterializedReadV2.decodeRecipe(selectedMap.actualViewBytes());
            manifestSha = recipe.manifestSha256();
        }
        var manifest = M5MaterializationCodecV1.decodeManifest(
                captureMaterializationFact(materialization.manifest(manifestSha), immutableFacts)
                        .canonicalStoredBytes());
        var generation = M5MaterializationCodecV1.decodeGeneration(captureMaterializationFact(
                        materialization.generation(manifest.preferredGenerationSha256()), immutableFacts)
                .canonicalStoredBytes());
        var plan = M5MaterializationCodecV1.decodePlan(
                captureMaterializationFact(materialization.plan(generation.taskIdSha256()), immutableFacts)
                        .canonicalStoredBytes());
        var cut = plan.sourceCut();
        var validation = M5MaterializationCodecV1.decodeValidationRoot(captureMaterializationFact(
                        materialization.validation(generation.validationRootSha256()), immutableFacts)
                .canonicalStoredBytes());
        if (!cut.identity().binding().equals(binding.bindingIdentity())
                || !manifest.identity().equals(cut.identity())
                || !generation.identity().equals(cut.identity())
                || !manifest.coverage().equals(cut.coverage())
                || !generation.coverage().equals(cut.coverage())
                || !manifest.exactPredecessorViewSha256().equals(cut.predecessorViewSha256())
                || !manifest.exactPredecessorSelectorValueSha256().equals(cut.predecessorSelectorValueSha256())
                || !generation.predecessorSelectedViewSha256().equals(cut.predecessorViewSha256())
                || generation.sourceGeneration()
                        != Math.addExact(cut.predecessorSelector().sourceGeneration(), 1)
                || selected.sourceGeneration() < generation.sourceGeneration()
                || selected.ownerEpoch() < cut.identity().ownerEpoch()
                || selected.mode() == SelectorMode.PREFERRED_WITH_FALLBACK
                        && !selected.fallbackSetSha256().equals(generation.fallbackSetSha256())
                || !generation.outputIdentitySha256().equals(plan.outputIdentitySha256())
                || !generation.sourceSetSha256().equals(cut.sourceSetSha256())
                || !generation.protocolStateRootSha256().equals(cut.protocolStateRootSha256())
                || generation.payloadKind() != plan.payloadKind()
                || generation.representationMode() != plan.representationMode()
                || !validation.taskIdSha256().equals(plan.taskIdSha256())
                || !validation.outputIdentitySha256().equals(plan.outputIdentitySha256())
                || !validation.sourceSetSha256().equals(cut.sourceSetSha256())) {
            throw new IllegalStateException(
                    "Native selected manifest/generation/validation differs from its full plan");
        }
        var input = OxiaPulsarMaterializationInputV2.decode(
                captureMaterializationFact(materialization.nativeInput(cut.protocolStateRootSha256()), immutableFacts)
                        .canonicalStoredBytes());
        if (!input.binding().equals(binding.bindingIdentity())
                || !input.metadataNamespace().equals(binding.metadataNamespace())
                || !input.coverage().equals(cut.coverage().pulsarCoverage().orElseThrow())
                || !input.recoveryInputSha256().equals(cut.recoveryCheckpointRootSha256())
                || input.storageFence() != cut.identity().storageFence()
                || !input.sourceViewKey().equals(sourceMapKey(cut.predecessorViewSha256()))) {
            throw new IllegalStateException("Native selected input/recovery/source route differs from its cut");
        }
        input.requireCurrentP1(
                captureNativeFact(input.aggregateKey(), nativeFacts),
                captureNativeFact(input.selectorKey(), nativeFacts));
        var predecessorFact = captureNativeFact(input.sourceViewKey(), nativeFacts);
        var recovery = input.verifySourceView(predecessorFact);
        var predecessor =
                PulsarNativeM4SourceMapV1.decode(predecessorFact.canonicalStoredBytes(), cut.predecessorViewSha256());
        var pending = M5PendingOutputInventoryV2.decode(
                captureMaterializationFact(materialization.pendingOutputs(plan.taskIdSha256()), immutableFacts)
                        .canonicalStoredBytes());
        pending.requirePlan(plan);
        if (pending.state() != M5PendingOutputInventoryV2.State.PREPARED
                || !plan.encryptionGenerationSha256()
                        .equals(Sha256Digest.hash(pending.profile().encode()))) {
            throw new IllegalStateException("Native selected output profile differs from its original plan");
        }
        var physical = pending.profile().physicalBinding();
        var layout =
                M5GcQuotaRecordsV2.Layout.withPhysicalMembers(physical.authorityRoot(), physical.physicalNamespace());
        var credits = new ArrayList<M5ObjectBirthCreditV3>(pending.outputs().size());
        for (var slot : pending.outputs()) {
            String key = M5ObjectBirthCreditV3.key(layout.scope(), pending.worker(), slot);
            var credit = M5ObjectBirthCreditV3.decode(
                    captureNativeFact(layout.nativeKey(key), nativeFacts).canonicalStoredBytes());
            if (!credit.key().equals(key)) {
                throw new IllegalStateException("Native selected birth-credit key/body differs");
            }
            credits.add(credit);
        }
        var map = PulsarNativeMaterializationReadMapV2.restore(predecessor, plan, credits, generation);
        for (var credit : map.readableOutputs()) {
            if (!M5ObjectBirthCreditV3.decode(captureNativeFact(layout.nativeKey(credit.key()), nativeFacts)
                            .canonicalStoredBytes())
                    .equals(credit)) {
                throw new IllegalStateException("Native retained mapped output changed its permanent credit");
            }
        }
        if (recipe != null && !recipe.map().encode().equals(map.encode())) {
            throw new IllegalStateException("Native recovered read recipe changed its selected materialization");
        }
        for (var fact : immutableFacts.entrySet()) {
            if (!route.read(fact.getKey()).toCompletableFuture().join().equals(Optional.of(fact.getValue()))) {
                throw new IllegalStateException("Native selected materialization metadata changed during capture");
            }
        }
        for (var fact : nativeFacts.entrySet()) {
            var reread = sourceMaps.read(fact.getKey()).toCompletableFuture().join();
            if (!reread.equals(Optional.of(fact.getValue()))) {
                var failure = new IllegalStateException(
                        "Native selected materialization native version changed during capture");
                failure.addSuppressed(new IllegalStateException("Changed Native authority: " + fact.getKey()
                        + " captured=" + fact.getValue().canonicalStoredSha256().toHex()
                        + " reread="
                        + reread.map(value -> value.canonicalStoredSha256().toHex())
                                .orElse("absent")));
                throw failure;
            }
        }
        binding.revalidate().toCompletableFuture().join();
        return new SelectedMaterializationSnapshot(
                new SelectedMaterialization(
                        selected, selectorFact, manifestSha, map, input, recovery, pending.profile()),
                immutableFacts,
                nativeFacts);
    }

    CompletableFuture<Void> requireSelectedMaterializationSnapshot(
            SelectedMaterializationSnapshot expected, Executor controlExecutor) {
        return readSelectedMaterializationSnapshot(controlExecutor).thenAccept(current -> {
            var oldNative = new java.util.HashMap<>(expected.nativeFacts());
            var newNative = new java.util.HashMap<>(current.nativeFacts());
            String aggregateKey = expected.selected().nativeInput().aggregateKey();
            // The original C ticket changes this physical envelope, while current P1 validation above still
            // requires the exact immutable NTA bytes/source version and ACTIVE selector. Other facts stay exact.
            oldNative.remove(aggregateKey);
            newNative.remove(aggregateKey);
            if (!current.immutableFacts().equals(expected.immutableFacts())
                    || !newNative.equals(oldNative)
                    || !current.selected()
                            .readMap()
                            .encode()
                            .equals(expected.selected().readMap().encode())) {
                throw new IllegalStateException("actual Native selected materialization changed before dispatch");
            }
        });
    }

    private VersionedValue captureMaterializationFact(String key, Map<String, VersionedValue> captured) {
        var fact = route.read(key)
                .toCompletableFuture()
                .join()
                .orElseThrow(
                        () -> new IllegalStateException("Native selected materialization metadata is absent: " + key));
        captured.put(key, fact);
        return fact;
    }

    private VersionedValue captureNativeFact(String key, Map<String, VersionedValue> captured) {
        var fact = sourceMaps
                .read(key)
                .toCompletableFuture()
                .join()
                .orElseThrow(() ->
                        new IllegalStateException("Native selected materialization source/version is absent: " + key));
        captured.put(key, fact);
        return fact;
    }

    public String terminalKey(long epoch) {
        return route.nativeKey(keys.terminal(epoch));
    }

    public String ownerDrainKey(BindingReadSelector selector) {
        if (!selector.binding().equals(binding.bindingIdentity())) {
            throw new IllegalArgumentException("Native drain belongs to another Binding");
        }
        return ownerDrainPrefix
                + M4ReadControlCodecV1.selectorCoreSha256(selector).toHex();
    }

    CompletableFuture<Void> persistOwnerDrain(
            PulsarNativeM4ReadOwnerV1 owner, PulsarNativeM4DrainAdmissionV1 admission, Executor executor) {
        return binding.revalidate()
                .thenRunAsync(() -> persistConfirmedOwnerDrain(owner, admission), executor)
                .toCompletableFuture();
    }

    /** Already on the original blocking control worker; never queue onto that same worker and wait for it. */
    void persistMaterializationOwnerDrain(PulsarNativeM4ReadOwnerV1 owner, PulsarNativeM4DrainAdmissionV1 admission) {
        binding.revalidate().toCompletableFuture().join();
        persistConfirmedOwnerDrain(owner, admission);
    }

    private void persistConfirmedOwnerDrain(PulsarNativeM4ReadOwnerV1 owner, PulsarNativeM4DrainAdmissionV1 admission) {
        owner.requireConfirmedDrain(this);
        if (!coordinator().readSelector().filter(owner.selector()::equals).isPresent()) {
            throw new IllegalStateException("Native drained owner is no longer the exact selector");
        }
        publishExact(ownerDrainKey(owner.selector()), drainBytes(owner.selector(), admission));
    }

    /** Captures the actual selected versions under a source preparation; this creates no cold read grant. */
    public CompletableFuture<PulsarNativeSourcePreparationV1> preparePublishedReadSources(
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            PulsarNativeReadCellBudgetV1 budget) {
        try {
            return readSelectedSourceMap()
                    .thenCompose(map -> binding.prepareReadSources(
                            ledger, bookKeeper, offloader, objectNamespace, budget, true, map));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** Actual SDK preparation for the complete selected rewrite; fully replaced Native handles are not opened. */
    public CompletableFuture<PulsarNativeSourcePreparationV1> prepareMaterializedReadSources(
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            PulsarNativeReadCellBudgetV1 budget,
            Executor controlWorker) {
        return readSelectedMaterialization(controlWorker)
                .thenCompose(selected -> {
                    CompletableFuture<PulsarNativeM4SourceMapV1> published =
                            selected.selector().selectedViewSha256().equals(selected.materializedManifestSha256())
                                    ? CompletableFuture.completedFuture(
                                            selected.readMap().predecessor())
                                    : readSelectedSourceMap();
                    return published.thenCompose(map -> binding.revalidate()
                            .thenCompose(ignored -> ledger.captureNativeReadView())
                            .thenApply(scope -> new PulsarNativeSourcePreparationV1(
                                    binding,
                                    scope,
                                    ledger,
                                    bookKeeper,
                                    offloader,
                                    objectNamespace,
                                    budget,
                                    true,
                                    map,
                                    selected,
                                    controlWorker)));
                })
                .toCompletableFuture()
                .copy();
    }

    /** A published Task can transfer only its actual drained predecessor, never an assumed successor actor. */
    public CompletableFuture<PulsarNativeM4ColdRecoveryV1> prepareMaterializedReadOwner(
            PulsarNativeSourcePreparationV1 preparation,
            PulsarNativeM4DrainAdmissionV1 admission,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Executor controlWorker) {
        return readSelectedMaterialization(controlWorker)
                .thenCompose(selected -> preparation
                        .view()
                        .thenApplyAsync(
                                view -> {
                                    admission.requireConfiguration(view, bookKeeper, offloader);
                                    requireCurrent(view);
                                    requireCurrentCapability(selected.selector(), admission);
                                    if (!PulsarNativeMaterializedReadV2.isRecipe(view.canonicalBytes())
                                            || !PulsarNativeMaterializedReadV2.decodeRecipe(view.canonicalBytes())
                                                    .map()
                                                    .encode()
                                                    .equals(selected.readMap().encode())) {
                                        throw new IllegalStateException(
                                                "Native materialized preparation changed selected mapping");
                                    }
                                    var ownDrain = sourceMaps
                                            .read(ownerDrainKey(selected.selector()))
                                            .toCompletableFuture()
                                            .join();
                                    boolean initial = ownDrain.isEmpty()
                                            && selected.selector()
                                                    .selectedViewSha256()
                                                    .equals(selected.materializedManifestSha256());
                                    if (initial
                                            && sourceMaps
                                                    .read(materializedInitialClaimKey(selected.selector()))
                                                    .toCompletableFuture()
                                                    .join()
                                                    .isPresent()) {
                                        throw new IllegalStateException(
                                                "Native materialized initial actor is not drained");
                                    }
                                    var predecessor = initial
                                            ? selected.readMap()
                                                    .plan()
                                                    .sourceCut()
                                                    .predecessorSelector()
                                            : selected.selector();
                                    var drain = sourceMaps
                                            .read(ownerDrainKey(predecessor))
                                            .toCompletableFuture()
                                            .join();
                                    if (drain.isEmpty()
                                            || !isConfirmedDrain(
                                                    drain.orElseThrow().canonicalStoredBytes(),
                                                    predecessor,
                                                    admission,
                                                    preparation.budget())) {
                                        throw new IllegalStateException(
                                                "Native materialized predecessor has no actual durable drain");
                                    }
                                    if (!confirmMaterializationSelector(selected.selector())
                                            .join()
                                            .equals(selected.selectorFact())) {
                                        throw new IllegalStateException(
                                                "Native materialized selector changed before owner grant");
                                    }
                                    view.requirePreparationBudget(preparation.budget());
                                    var recovery = new PulsarNativeM4ColdRecoveryV1(
                                            this, view, selected.selector(), admission, initial);
                                    recovery.requireBookKeeper(bookKeeper);
                                    return recovery;
                                },
                                controlWorker))
                .toCompletableFuture()
                .copy();
    }

    /** Only the original actor's durable confirmed drain permits a grant from actual prepared published sources. */
    public CompletableFuture<PulsarNativeM4ColdRecoveryV1> prepareColdReadOwner(
            PulsarNativeSourcePreparationV1 preparation,
            PulsarNativeM4DrainAdmissionV1 admission,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Executor controlExecutor) {
        try {
            Objects.requireNonNull(admission, "current Native conformance admission");
            Objects.requireNonNull(preparation, "actual published source preparation");
            return readSelectedSourceMap()
                    .thenCompose(map -> preparation
                            .view()
                            .thenApplyAsync(
                                    view -> {
                                        admission.requireConfiguration(view, bookKeeper, offloader);
                                        requireCurrent(view);
                                        var selector =
                                                coordinator().readSelector().orElseThrow();
                                        if (selector.mode() != SelectorMode.PREFERRED_ONLY
                                                || selector.admissionState() != AdmissionState.ADMITTING
                                                || !selector.selectedViewSha256()
                                                        .equals(map.selectedViewSha256())) {
                                            throw new IllegalStateException(
                                                    "Native cold recovery requires its exact PO selector");
                                        }
                                        requireCurrentCapability(selector, admission);
                                        var drained = sourceMaps
                                                .read(ownerDrainKey(selector))
                                                .toCompletableFuture()
                                                .join();
                                        if (drained.isEmpty()
                                                || !isConfirmedDrain(
                                                        drained.orElseThrow().canonicalStoredBytes(),
                                                        selector,
                                                        admission,
                                                        preparation.budget())) {
                                            throw new IllegalStateException(
                                                    "Native current owner has no confirmed durable read drain");
                                        }
                                        preparation.requirePublished(map);
                                        view.requirePreparationBudget(preparation.budget());
                                        var storedMap = sourceMaps
                                                .read(sourceMapKey(map.selectedViewSha256()))
                                                .toCompletableFuture()
                                                .join()
                                                .orElseThrow();
                                        if (!storedMap.canonicalStoredBytes().equals(map.encode())) {
                                            throw new IllegalStateException(
                                                    "Native published source map changed before recovery");
                                        }
                                        var recovery =
                                                new PulsarNativeM4ColdRecoveryV1(this, view, selector, admission);
                                        recovery.requireBookKeeper(bookKeeper);
                                        return recovery;
                                    },
                                    controlExecutor))
                    .toCompletableFuture()
                    .copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    void publishColdClaim(BindingReadSelector predecessor, CanonicalBytes claim) {
        publishExact(coldClaimKey(predecessor), claim);
    }

    private String coldClaimKey(BindingReadSelector selector) {
        return coldClaimPrefix
                + M4ReadControlCodecV1.selectorCoreSha256(selector).toHex();
    }

    void publishMaterializedInitialClaim(BindingReadSelector selector, CanonicalBytes original) {
        publishExact(materializedInitialClaimKey(selector), original);
    }

    private String materializedInitialClaimKey(BindingReadSelector selector) {
        // The first actor of an already-published epoch is distinct from the later takeover out of that epoch.
        var core = M4ReadControlCodecV1.selectorCoreSha256(selector).bytes().toByteArray();
        return coldClaimPrefix
                + Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(4 + core.length)
                                .putInt(0x4e504d49)
                                .put(core)
                                .array()))
                        .toHex();
    }

    record DrainedOwnerProof(
            M4ReadControlRecordsV1.ReadAdmissionEpochTerminalCut terminal,
            M4ReadControlRecordsV1.ReadQuiescenceProof proof) {}

    /** The original cold attempt retains this exact SDK-drain candidate through UNKNOWN. */
    DrainedOwnerProof captureRecoveredOwnerProof(
            BindingReadSelector predecessor, BindingReadSelector successor, PulsarNativeM4DrainAdmissionV1 admission) {
        var drained = sourceMaps
                .read(ownerDrainKey(predecessor))
                .toCompletableFuture()
                .join()
                .orElseThrow();
        if (!drained.canonicalStoredBytes().equals(drainBytes(predecessor, admission))) {
            throw new IllegalStateException("Native takeover lacks exact original actor SDK drain");
        }
        var anchor = successor.pendingAnchors().stream()
                .filter(value -> value.closedReadAdmissionEpoch() == predecessor.readAdmissionEpoch())
                .findFirst()
                .orElseThrow();
        if (!anchor.predecessorSelectorCoreSha256().equals(M4ReadControlCodecV1.selectorCoreSha256(predecessor))
                || !anchor.successorSelectorCoreSha256().equals(M4ReadControlCodecV1.selectorCoreSha256(successor))) {
            throw new IllegalStateException("Native takeover drain anchor differs from its exact selectors");
        }
        var facts = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(4 + 32 * 4)
                .putInt(0x4e504450)
                .put(drained.canonicalStoredSha256().bytes().toByteArray())
                .put(Sha256Digest.hash(drained.metadataVersion().value())
                        .bytes()
                        .toByteArray())
                .put(M4ReadControlCodecV1.selectorCoreSha256(predecessor)
                        .bytes()
                        .toByteArray())
                .put(M4ReadControlCodecV1.selectorCoreSha256(successor).bytes().toByteArray())
                .array()));
        var terminal = new M4ReadControlRecordsV1.ReadAdmissionEpochTerminalCut(
                predecessor.binding(),
                M4ReadControlCodecV1.anchorSha256(anchor),
                predecessor.readAdmissionEpoch(),
                predecessor.ownerEpoch(),
                predecessor.sourceGeneration(),
                Math.max(1, System.currentTimeMillis()),
                predecessor.capability(),
                M4ReadControlRecordsV1.TerminalKind.PLANNED_DRAIN,
                facts,
                facts,
                0,
                0,
                predecessor.ownerEpoch());
        var terminalSha = M4ReadControlCodecV1.terminalSha256(terminal);
        var draft = new M4ReadControlRecordsV1.ReadQuiescenceProof(
                predecessor.binding(),
                predecessor.readAdmissionEpoch(),
                terminalSha,
                predecessor.sourceGeneration(),
                terminal.safeAfterAuthorityTimeMillis(),
                predecessor.capability(),
                M4ReadControlRecordsV1.TerminalKind.PLANNED_DRAIN,
                terminalSha);
        var proof = new M4ReadControlRecordsV1.ReadQuiescenceProof(
                draft.binding(),
                draft.readAdmissionEpoch(),
                draft.terminalCutSha256(),
                draft.drainedThroughReadViewGeneration(),
                draft.safeAfterAuthorityTimeMillis(),
                draft.capability(),
                draft.kind(),
                M4ReadControlCodecV1.calculateProofIdentity(draft));
        return new DrainedOwnerProof(terminal, proof);
    }

    void publishRecoveredOwnerProof(DrainedOwnerProof original) {
        requireAppliedOrExact(coordinator().publishTerminal(original.terminal()));
        requireAppliedOrExact(coordinator().publishProof(original.proof()));
    }

    private void publishExact(String key, CanonicalBytes bytes) {
        var stored = sourceMaps.read(key).toCompletableFuture().join();
        if (stored.isEmpty()) {
            sourceMaps
                    .compareAndSet(Optional.empty(), key, bytes)
                    .toCompletableFuture()
                    .join();
        }
        if (sourceMaps
                .read(key)
                .toCompletableFuture()
                .join()
                .filter(value -> value.canonicalStoredBytes().equals(bytes))
                .isEmpty()) {
            throw new IllegalStateException("Native recovery publication is unknown or belongs to another attempt");
        }
    }

    void requireCurrentCapability(BindingReadSelector selector, PulsarNativeM4DrainAdmissionV1 admission) {
        var observed = route.read(keys.capability(selector.capability().generation()))
                .toCompletableFuture()
                .join();
        var capability =
                M4ReadControlCodecV1.decodeCapability(observed.orElseThrow().canonicalStoredBytes());
        if (!capability.binding().equals(binding.bindingIdentity())
                || capability.generation() != selector.capability().generation()
                || capability.kind() != CapabilityKind.DURABLE_DRAIN_ONLY_V1
                || capability.state() != CapabilityState.ADMITTED
                || !M4ReadControlCodecV1.capabilityEvidenceSha256(capability)
                        .equals(selector.capability().evidenceSha256())
                || !capability.conformanceReceiptSha256().equals(admission.receipt())
                || !capability.conformanceReceiptIdentitySha256().equals(admission.inputs())
                || !capability.backendAdapterSha256().equals(admission.runtime())
                || !capability.readAdmissionContractSha256().equals(admission.runtime())
                || !capability.verifierSha256().equals(admission.runtime())
                || !capability.backendProtocolConfigurationSha256().equals(admission.configuration())) {
            throw new IllegalStateException("Native cold capability differs from current actual conformance");
        }
    }

    private static CanonicalBytes drainBytes(BindingReadSelector selector, PulsarNativeM4DrainAdmissionV1 admission) {
        byte[] selected = M4ReadControlCodecV1.encodeSelector(selector).toByteArray();
        return CanonicalBytes.copyOf(ByteBuffer.allocate(12 + selected.length + 128)
                .putInt(0x4e504f44)
                .putInt(1)
                .putInt(selected.length)
                .put(selected)
                .put(admission.receipt().bytes().toByteArray())
                .put(admission.inputs().bytes().toByteArray())
                .put(admission.runtime().bytes().toByteArray())
                .put(admission.configuration().bytes().toByteArray())
                .array());
    }

    private static CanonicalBytes unregisteredDrainBytes(
            BindingReadSelector selector, PulsarNativeM4DrainAdmissionV1 admission, VersionedValue claim) {
        byte[] original = drainBytes(selector, admission).toByteArray();
        var stored = claim.canonicalStoredBytes();
        var version = claim.metadataVersion().value();
        return CanonicalBytes.copyOf(ByteBuffer.allocate(20 + original.length + stored.length() + version.length())
                .putInt(0x4e505544)
                .putInt(1)
                .putInt(original.length)
                .put(original)
                .putInt(stored.length())
                .put(stored.toByteArray())
                .putInt(version.length())
                .put(version.toByteArray())
                .array());
    }

    private boolean isConfirmedDrain(
            CanonicalBytes stored,
            BindingReadSelector selector,
            PulsarNativeM4DrainAdmissionV1 admission,
            PulsarNativeReadCellBudgetV1 budget) {
        if (stored.equals(drainBytes(selector, admission))) {
            return true;
        }
        // This distinct record is written only after the original SDK preparation proves no owner was installed.
        var claim = PulsarNativeInitialOwnerClaimV1.readActiveIdentity(
                sourceMaps,
                initialClaimKey,
                budget.nativeOwnerIdentity(binding),
                binding.bindingIdentity(),
                selector.selectedViewSha256(),
                admission.receipt());
        return stored.equals(unregisteredDrainBytes(selector, admission, claim));
    }

    private void requireUnregisteredGrant(
            PulsarNativeBindingReadViewV1 view,
            BindingReadSelector selector,
            PulsarNativeM4DrainAdmissionV1 admission,
            PulsarNativeInitialOwnerClaimV1 claim) {
        claim.requireActive();
        if (!coordinator().readSelector().filter(selector::equals).isPresent()) {
            throw new IllegalStateException("Original unregistered grant is absent or changed");
        }
        requireCurrentCapability(selector, admission);
        var expectedMap = PulsarNativeM4SourceMapV1.capture(view).encode();
        if (sourceMaps
                .read(sourceMapKey(view.sha256()))
                .toCompletableFuture()
                .join()
                .filter(actual -> actual.canonicalStoredBytes().equals(expectedMap))
                .isEmpty()) {
            throw new IllegalStateException("Original unregistered grant source map is absent or changed");
        }
    }

    private CompletableFuture<Void> reconcileUnregisteredGrant(
            PulsarNativeBindingReadViewV1 view,
            BindingReadSelector selector,
            PulsarNativeM4DrainAdmissionV1 admission,
            PulsarNativeInitialOwnerClaimV1 claim,
            PulsarNativeReadCellBudgetV1.Reservation reservation,
            Executor executor) {
        return binding.revalidate()
                .thenRunAsync(() -> requireUnregisteredGrant(view, selector, admission, claim), executor)
                .thenCompose(ignored -> view.closeUnregisteredPreparation())
                .thenCompose(ignored -> binding.revalidate())
                .thenRunAsync(
                        () -> {
                            view.requireUnregisteredPreparationDrain();
                            requireUnregisteredGrant(view, selector, admission, claim);
                            publishExact(
                                    ownerDrainKey(selector),
                                    unregisteredDrainBytes(selector, admission, claim.activeIdentity()));
                            requireUnregisteredGrant(view, selector, admission, claim);
                        },
                        executor)
                .thenCompose(ignored -> reservation.releaseBeforeActor())
                .toCompletableFuture();
    }

    /** Actual current published versions; returns a source candidate, never a reconstructed owner grant. */
    public CompletableFuture<PulsarNativeBindingReadViewV1> capturePublishedPreferredView(
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace) {
        try {
            return readSelectedSourceMap().thenCompose(map -> binding.capturePublishedPreferredView(
                            ledger, bookKeeper, offloader, objectNamespace, map)
                    .thenCompose(candidate -> readSelectedSourceMap().thenApply(current -> {
                        if (!current.actualViewBytes().equals(map.actualViewBytes()) || !candidate.isCurrent()) {
                            throw new IllegalStateException("published Native source changed during cold capture");
                        }
                        return candidate;
                    })));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    public String proofKey(long epoch) {
        return route.nativeKey(keys.proof(epoch));
    }

    public String protectionKey(SourceProtectionIdentity source) {
        return route.nativeKey(keys.protection(source.sourceIdentitySha256(), source.protectionGeneration()));
    }

    M4ReadControlCoordinatorV1 coordinator() {
        return new M4ReadControlCoordinatorV1(referenceControlMetadata(), 0, binding.bindingIdentity());
    }

    M4ReadControlCoordinatorV1 coordinator(PulsarNativeControlWriterV2.PhysicalWriter physicalAdmission) {
        return new M4ReadControlCoordinatorV1(
                referenceControlMetadata(
                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                .SOURCE_PROTECTION,
                        Optional.of(physicalAdmission)),
                0,
                binding.bindingIdentity());
    }

    com.nereusstream.storage.object.retention.M5MaterializationWorkerCoordinatorV1 materializationWorkers() {
        return new com.nereusstream.storage.object.retention.M5MaterializationWorkerCoordinatorV1(
                referenceControlMetadata(
                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                .MATERIALIZATION_OR_COMPACTION_TASK),
                0,
                binding.bindingIdentity());
    }

    com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1 materializationTasks() {
        return new com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1(
                referenceControlMetadata(
                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                .MATERIALIZATION_OR_COMPACTION_TASK),
                0,
                binding.bindingIdentity());
    }

    /** This original Task's stop/dispatch boundary wraps only its sole mutable selector publication. */
    com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1 materializationTasks(
            Runnable beforeSelectorDispatch) {
        var raw = referenceControlMetadata(
                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                        .MATERIALIZATION_OR_COMPACTION_TASK);
        return new com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1(
                new com.nereusstream.storage.object.control.CanonicalControlMetadataStore() {
                    public Optional<CanonicalBytes> get(String key) {
                        return raw.get(key);
                    }

                    public com.nereusstream.storage.object.control.ControlMutationOutcome putIfAbsent(
                            String key, CanonicalBytes value) {
                        return raw.putIfAbsent(key, value);
                    }

                    public com.nereusstream.storage.object.control.ControlMutationOutcome compareAndSet(
                            String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
                        if (key.equals(keys.selector())) {
                            beforeSelectorDispatch.run();
                        }
                        return raw.compareAndSet(key, expected, value);
                    }
                },
                0,
                binding.bindingIdentity());
    }

    com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1 materializationAdmission(
            Sha256Digest cell) {
        return new com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1(
                referenceControlMetadata(
                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                .MATERIALIZATION_OR_COMPACTION_TASK),
                cell);
    }

    CompletableFuture<Void> revalidateBinding() {
        return binding.revalidate().toCompletableFuture();
    }

    PulsarNativeReadCellBudgetV1.Reservation reserveOwnerBudget(
            PulsarNativeBindingReadViewV1 view, PulsarNativeReadCellBudgetV1 budget, int capacity) {
        if (!view.bindingIdentity().equals(binding.bindingIdentity())) {
            throw new IllegalArgumentException("Native owner view belongs to another Binding");
        }
        view.requirePreparationBudget(budget);
        return Objects.requireNonNull(budget, "admitted Cell budget").reserve(binding, view, capacity);
    }

    CompletableFuture<PulsarNativeM4ReadOwnerV1> openSuccessorReadHold(
            PulsarNativeBindingReadViewV1 view,
            BindingReadSelector selector,
            PulsarNativeReadCellBudgetV1 budget,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            java.util.function.Function<PulsarNativeControlWriterV2.ReadHold, PulsarNativeM4ReadOwnerV1> start,
            Executor executor) {
        if (!view.materialized()) {
            return openNativeReadHold(view, start, executor);
        }
        final PulsarNativeControlWriterV2 writer;
        synchronized (this) {
            writer = controlWriter;
        }
        if (writer == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Native materialized read writer is not installed"));
        }
        return CompletableFuture.supplyAsync(
                        () -> {
                            requireCurrent(view);
                            return new PulsarNativePhysicalReferenceSetV2(
                                    view, view, budget, bookKeeper, selector, true);
                        },
                        executor)
                .thenCompose(physical -> writer.openReadHold(physical, start, executor));
    }

    /** Exact retries use the same owned retirement, including after an UNKNOWN metadata response. */
    public CompletableFuture<PulsarNativeM4FallbackRetirementV1.Result> retireFallback(
            PulsarNativeM4ReadOwnerV1 owner,
            PulsarNativeBindingReadViewV1 preferred,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Executor controlExecutor) {
        try {
            var retirement = owner.prepareRetirement(this, preferred);
            retirement.requireConfiguration(bookKeeper, offloader);
            return retirement.retire(controlExecutor);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<PulsarNativeM4ReadOwnerV1> openInitialReadHold(
            PulsarNativeBindingReadViewV1 view,
            java.util.function.Supplier<PulsarNativeM4ReadOwnerV1> start,
            Executor executor) {
        return openNativeReadHold(
                view,
                hold -> {
                    var owner = start.get();
                    if (hold != null) {
                        owner.attachReadHold(hold);
                    }
                    return owner;
                },
                executor);
    }

    private CompletableFuture<PulsarNativeM4ReadOwnerV1> openNativeReadHold(
            PulsarNativeBindingReadViewV1 view,
            java.util.function.Function<PulsarNativeControlWriterV2.ReadHold, PulsarNativeM4ReadOwnerV1> start,
            Executor executor) {
        final PulsarNativeControlWriterV2 writer;
        synchronized (this) {
            writer = controlWriter;
            if (writer == null && controlWriterOpening != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Native control writer is not ready"));
            }
        }
        if (writer == null) {
            String p1Key = new com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys(binding.authorityRoot())
                    .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
            return sourceMaps
                    .read(p1Key)
                    .thenCompose(value -> {
                        var actual = value.orElseThrow(
                                () -> new IllegalStateException("Native P1 read authority is absent"));
                        if (com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                .isAuthorityValue(actual.canonicalStoredBytes())) {
                            var authority = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                    .decodeAuthority(actual.canonicalStoredBytes());
                            if (authority.writerEnrollment().isPresent()
                                    || authority.logicalTrim().isPresent()) {
                                throw new IllegalStateException(
                                        "Enrolled Native read owner requires its original control writer");
                            }
                        }
                        return CompletableFuture.supplyAsync(() -> start.apply(null), executor);
                    })
                    .toCompletableFuture();
        }
        return writer.openReadHold(
                view.sha256(),
                hold -> {
                    hold.requireTrimDirectory(view);
                    return start.apply(hold);
                },
                executor);
    }

    /** Initial grant only. Keep the supplied control executor alive until the original opening/cleanup terminates. */
    public CompletableFuture<PulsarNativeM4ReadOwnerV1> createReadOwner(
            PulsarNativeBindingReadViewV1 view,
            PulsarNativeM4DrainAdmissionV1 admission,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Executor controlExecutor,
            Executor ownerEventLoop,
            int capacity,
            PulsarNativeReadCellBudgetV1 cellBudget) {
        PulsarNativeReadCellBudgetV1.Reservation reserved = null;
        var createdOwner = new AtomicReference<PulsarNativeM4ReadOwnerV1>();
        var attemptedSelector = new AtomicReference<BindingReadSelector>();
        var initialClaim = new AtomicReference<PulsarNativeInitialOwnerClaimV1>();
        var observer = new CompletableFuture<PulsarNativeM4ReadOwnerV1>();
        try {
            Objects.requireNonNull(admission, "current Native conformance admission");
            admission.requireConfiguration(view, bookKeeper, offloader);
            if (capacity <= 0 || capacity > 65_536) {
                throw new IllegalArgumentException("Native M4 owner capacity is outside its hard bound");
            }
            if (view.fallbackSourceIdentities().size() > M4ReadControlRecordsV1.MAX_SOURCES_PER_BATCH) {
                throw new IllegalArgumentException("Native M4 fallback inventory exceeds its hard count bound");
            }
            reserved = reserveOwnerBudget(view, cellBudget, capacity);
            var reservation = reserved;
            initialClaim.set(new PulsarNativeInitialOwnerClaimV1(
                    sourceMaps,
                    initialClaimKey,
                    cellBudget.nativeOwnerIdentity(binding),
                    binding.bindingIdentity(),
                    reservation.operationId(),
                    view.sha256(),
                    admission.receipt()));
            var opening = reservation
                    .ready()
                    .thenCompose(unused -> binding.revalidate())
                    .thenCompose(ignored -> openInitialReadHold(
                            view,
                            () -> {
                                if (observer.isDone()) {
                                    throw new java.util.concurrent.CancellationException(
                                            "Native owner opening observer ended before grant dispatch");
                                }
                                requireCurrent(view);
                                var control = new M4ReadControlCoordinatorV1(
                                        referenceControlMetadata(), 0, binding.bindingIdentity());
                                if (control.readSelector().isPresent()) {
                                    throw new IllegalStateException(
                                            "Native M4 selector already has an owner or unresolved grant");
                                }
                                initialClaim.get().acquire();
                                publishSourceMap(view);
                                // Initial M4 counters belong to this newly created durable Binding selector, not SDK
                                // metadata versions.
                                var evidence = new CapabilityEvidence(
                                        binding.bindingIdentity(),
                                        1,
                                        1,
                                        CapabilityKind.DURABLE_DRAIN_ONLY_V1,
                                        CapabilityState.ADMITTED,
                                        admission.runtime(),
                                        admission.configuration(),
                                        admission.runtime(),
                                        admission.runtime(),
                                        admission.inputs(),
                                        admission.receipt(),
                                        PulsarNativeM4DrainAdmissionV1.digest(
                                                Map.of("time", "DURABLE_DRAIN_ONLY_NO_EXPIRY")),
                                        bookKeeper.capability().timeoutClass().readMillis(),
                                        0,
                                        0);
                                requireAppliedOrExact(control.createCapability(evidence));
                                var capability = new CapabilityBinding(
                                        1, M4ReadControlCodecV1.capabilityEvidenceSha256(evidence));
                                var sources = view.fallbackSourceIdentities().stream()
                                        .map(source -> new SourceProtectionIdentity(source, 1, 1, 1, capability))
                                        .sorted(Comparator.comparing(source ->
                                                source.sourceIdentitySha256().toHex()))
                                        .toList();
                                for (var source : sources) {
                                    requireAppliedOrExact(control.createProtection(new SourceProtection(
                                            binding.bindingIdentity(),
                                            source,
                                            ProtectionState.PROTECTED,
                                            Optional.empty(),
                                            Optional.empty())));
                                }
                                var selector = new BindingReadSelector(
                                        binding.bindingIdentity(),
                                        view.sha256(),
                                        1,
                                        1,
                                        1,
                                        sources.isEmpty()
                                                ? SelectorMode.PREFERRED_ONLY
                                                : SelectorMode.PREFERRED_WITH_FALLBACK,
                                        AdmissionState.ADMITTING,
                                        sources.isEmpty()
                                                ? Optional.empty()
                                                : Optional.of(M4ReadControlCodecV1.calculateFallbackSetSha256(sources)),
                                        capability,
                                        List.of(),
                                        List.of());
                                initialClaim.get().requireActive();
                                attemptedSelector.set(selector);
                                var created = control.createSelector(selector);
                                if (created != M4ReadControlCoordinatorV1.Outcome.APPLIED) {
                                    throw new IllegalStateException(
                                            "Native M4 initial owner grant was not uniquely applied: " + created);
                                }
                                var owner = new PulsarNativeM4ReadOwnerV1(
                                        view,
                                        selector,
                                        ownerEventLoop,
                                        capacity,
                                        this,
                                        admission,
                                        cellBudget,
                                        reservation);
                                createdOwner.set(owner);
                                return owner;
                            },
                            controlExecutor))
                    .thenCompose(owner -> readFacts(view).thenApply(facts -> {
                        var captured = facts.orElseThrow(
                                () -> new IllegalStateException("Native M4 granted selector disappeared"));
                        if (!captured.selector().equals(owner.selector())
                                || !captured.capability()
                                        .conformanceReceiptSha256()
                                        .equals(admission.receipt())) {
                            throw new IllegalStateException("Native M4 grant changed before registration");
                        }
                        view.installReadAdmission(owner);
                        return owner;
                    }))
                    .handle((owner, failure) -> failure == null
                            ? CompletableFuture.completedFuture(owner)
                            : cleanupFailedInitialOwner(
                                    failure,
                                    reservation,
                                    createdOwner.get(),
                                    controlExecutor,
                                    attemptedSelector.get(),
                                    initialClaim.get(),
                                    view,
                                    admission))
                    .thenCompose(value -> value)
                    .toCompletableFuture();
            opening.whenComplete((actual, failure) -> {
                if (failure != null) {
                    observer.completeExceptionally(failure);
                } else if (!observer.complete(actual)) {
                    reservation.retainUndeliveredOwner(() -> actual.fenceUndelivered(controlExecutor), true);
                }
            });
            return observer;
        } catch (Throwable failure) {
            return reserved == null
                    ? CompletableFuture.failedFuture(failure)
                    : cleanupFailedInitialOwner(
                                    failure,
                                    reserved,
                                    createdOwner.get(),
                                    controlExecutor,
                                    attemptedSelector.get(),
                                    initialClaim.get(),
                                    view,
                                    admission)
                            .copy();
        }
    }

    private CompletableFuture<PulsarNativeM4ReadOwnerV1> cleanupFailedInitialOwner(
            Throwable failure,
            PulsarNativeReadCellBudgetV1.Reservation reservation,
            PulsarNativeM4ReadOwnerV1 actor,
            Executor controlExecutor,
            BindingReadSelector attempted,
            PulsarNativeInitialOwnerClaimV1 claim,
            PulsarNativeBindingReadViewV1 view,
            PulsarNativeM4DrainAdmissionV1 admission) {
        if (actor != null) {
            return cleanupFailedOwner(failure, reservation, actor, controlExecutor);
        }
        if (attempted == null) {
            Supplier<CompletableFuture<Void>> cleanup = claim == null
                    ? reservation::releaseBeforeActor
                    : () -> claim.abortBeforeGrant(controlExecutor)
                            .thenCompose(ignored -> reservation.releaseBeforeActor());
            return failAfterCleanup(failure, reservation, cleanup);
        }
        Supplier<CompletableFuture<Void>> cleanup = claim == null
                ? () -> CompletableFuture.failedFuture(new IllegalStateException(
                        "Native initial selector create lacks a confirmed original actor drain"))
                : () -> reconcileUnregisteredGrant(view, attempted, admission, claim, reservation, controlExecutor);
        return failAfterCleanup(failure, reservation, cleanup, Optional.of(attempted));
    }

    CompletableFuture<PulsarNativeM4ReadOwnerV1> cleanupFailedOwner(
            Throwable failure,
            PulsarNativeReadCellBudgetV1.Reservation reservation,
            PulsarNativeM4ReadOwnerV1 actor,
            Executor controlExecutor) {
        Supplier<CompletableFuture<Void>> cleanup =
                actor == null ? reservation::releaseBeforeActor : () -> actor.fenceUndelivered(controlExecutor);
        return failAfterCleanup(failure, reservation, cleanup);
    }

    private CompletableFuture<PulsarNativeM4ReadOwnerV1> failAfterCleanup(
            Throwable failure,
            PulsarNativeReadCellBudgetV1.Reservation reservation,
            Supplier<CompletableFuture<Void>> cleanup) {
        return failAfterCleanup(failure, reservation, cleanup, Optional.empty());
    }

    private CompletableFuture<PulsarNativeM4ReadOwnerV1> failAfterCleanup(
            Throwable failure,
            PulsarNativeReadCellBudgetV1.Reservation reservation,
            Supplier<CompletableFuture<Void>> cleanup,
            Optional<BindingReadSelector> initialSelector) {
        return cleanup.get().handle((ignored, cleanupFailure) -> {
            if (cleanupFailure != null) {
                throw new OwnerCleanupException(failure, cleanupFailure, cleanup, reservation, initialSelector);
            }
            throw new CompletionException(failure);
        });
    }

    private static void requireAppliedOrExact(M4ReadControlCoordinatorV1.Outcome outcome) {
        if (outcome != M4ReadControlCoordinatorV1.Outcome.APPLIED
                && outcome != M4ReadControlCoordinatorV1.Outcome.EXISTING_EXACT) {
            throw new IllegalStateException("Native M4 admission metadata is unresolved: " + outcome);
        }
    }

    /** Namespace/P1, selector, capability, exact predecessor rereads, then Namespace/P1 revalidation. */
    public CompletableFuture<Optional<CapturedFacts>> readFacts(PulsarNativeBindingReadViewV1 view) {
        try {
            Objects.requireNonNull(view, "view");
            if (!view.bindingIdentity().equals(binding.bindingIdentity())) {
                throw new IllegalArgumentException("native M4 view belongs to another Binding");
            }
            if (!view.isCurrent()) {
                throw new IllegalStateException("native M4 capture requires a current Native view");
            }
            return binding.revalidate()
                    .thenCompose(ignored -> route.read(keys.selector()))
                    .thenCompose(observed -> {
                        if (observed.isEmpty()) {
                            return binding.revalidate().thenApply(ignored -> {
                                requireCurrent(view);
                                return Optional.<CapturedFacts>empty();
                            });
                        }
                        var selectorFact = observed.orElseThrow();
                        var selector = readSelector(selectorFact);
                        if (selector.admissionState() != AdmissionState.ADMITTING) {
                            throw new IllegalStateException("native M4 selector does not admit the captured view");
                        }
                        view.requireSelectedRoot(selector);
                        String capabilityKey =
                                keys.capability(selector.capability().generation());
                        return route.read(capabilityKey).thenCompose(current -> {
                            var capabilityFact = current.orElseThrow(
                                    () -> new IllegalStateException("native M4 capability is absent"));
                            var capability =
                                    M4ReadControlCodecV1.decodeCapability(capabilityFact.canonicalStoredBytes());
                            if (!capability.binding().equals(binding.bindingIdentity())
                                    || capability.generation()
                                            != selector.capability().generation()
                                    || capability.state() != CapabilityState.ADMITTED
                                    || !M4ReadControlCodecV1.capabilityEvidenceSha256(capability)
                                            .equals(selector.capability().evidenceSha256())) {
                                throw new IllegalStateException(
                                        "native M4 capability does not match admitted selector evidence");
                            }
                            return route.read(keys.selector()).thenCompose(lastSelector -> {
                                if (lastSelector.filter(selectorFact::equals).isEmpty()) {
                                    throw new IllegalStateException(
                                            "native M4 selector predecessor changed during capture");
                                }
                                return route.read(capabilityKey).thenCompose(lastCapability -> {
                                    if (lastCapability
                                            .filter(capabilityFact::equals)
                                            .isEmpty()) {
                                        throw new IllegalStateException(
                                                "native M4 capability predecessor changed during capture");
                                    }
                                    return binding.revalidate().thenApply(ignored -> {
                                        requireCurrent(view);
                                        return Optional.of(
                                                new CapturedFacts(selector, capability, selectorFact, capabilityFact));
                                    });
                                });
                            });
                        });
                    })
                    .toCompletableFuture()
                    .copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static void requireCurrent(PulsarNativeBindingReadViewV1 view) {
        if (!view.isCurrent()) {
            throw new IllegalStateException("native M4 view changed during control capture");
        }
    }

    private BindingReadSelector readSelector(VersionedValue fact) {
        var bytes = fact.canonicalStoredBytes();
        BindingReadSelector selector;
        if (M5BindingAuthorityCodecV1.isAuthorityValue(bytes)) {
            var envelope = M5BindingAuthorityCodecV1.decodeAuthority(bytes);
            if (envelope.state() != BindingAuthorityStateV1.OPEN_V1) {
                throw new IllegalStateException("native M4 selector is blocked by the Binding reference scan");
            }
            selector = envelope.selectorProjection();
        } else {
            selector = M4ReadControlCodecV1.decodeSelector(bytes);
        }
        if (!selector.binding().equals(binding.bindingIdentity())) {
            throw new IllegalStateException("native M4 selector belongs to another Binding");
        }
        return selector;
    }
}
