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

import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.metadata.oxia.v2.capability.OxiaPulsarTopicGenerationSelectorStore;
import com.nereusstream.metadata.oxia.v2.capability.OxiaTopicBindingAggregateReader;
import com.nereusstream.metadata.oxia.v2.capability.PulsarActiveTopicAuthority;
import com.nereusstream.metadata.oxia.v2.codec.OxiaV2CodecSet;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectIdentityKind;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1.Outcome;
import com.nereusstream.storage.object.retention.M5PulsarAuditDomainV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceAdapterResultV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceScanSummaryV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorObservationV1;
import io.oxia.client.api.AsyncOxiaClient;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedObjectReadIdentity;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedReadMemoryBounds;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedReadSourceIdentity;
import org.apache.bookkeeper.mledger.impl.LedgerMetadataUtils;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedNativeReadView;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedOffloadReadSource;
import org.apache.bookkeeper.versioning.LongVersion;

/** Actual P1/native/Object source binding. This alone grants no M4 selector, global owner drain or RELEASED. */
public final class PulsarNativeBindingV1 {
    private static final String PROPERTY_PREFIX = "nereus.native.pulsar.binding-v1.";

    /** Constructed after P1 reread, actual stored Native metadata and this provider's physical marker verification. */
    public static final class CapturedSource {
        private final PulsarNativeBindingV1 binding;
        private final CapturedOffloadReadSource nativeSource;
        private final OffloadedReadSourceIdentity objects;
        private final PhysicalNamespaceAuthorityBindingV2 physicalBinding;
        private final BookKeeperSource bookKeeper;
        private final PhysicalNamespaceAuthorityBindingV2 bookKeeperBinding;
        private final List<PhysicalResourceIdV2> members;
        private final PulsarNativeReadViewV1 readView;
        private final OffloadedReadMemoryBounds memoryBounds;

        private CapturedSource(
                PulsarNativeBindingV1 binding,
                CapturedOffloadReadSource nativeSource,
                OffloadedReadSourceIdentity objects,
                OffloadedReadMemoryBounds memoryBounds,
                PhysicalNamespaceAuthorityBindingV2 physicalBinding,
                BookKeeperSource bookKeeper) {
            this(binding, nativeSource, objects, memoryBounds, physicalBinding, bookKeeper, bookKeeper.binding());
        }

        private CapturedSource(
                PulsarNativeBindingV1 binding,
                CapturedOffloadReadSource nativeSource,
                OffloadedReadSourceIdentity objects,
                OffloadedReadMemoryBounds memoryBounds,
                PhysicalNamespaceAuthorityBindingV2 physicalBinding,
                BookKeeperSource bookKeeper,
                PhysicalNamespaceAuthorityBindingV2 bookKeeperBinding) {
            this.binding = binding;
            this.nativeSource = nativeSource;
            this.objects = objects;
            this.memoryBounds = Objects.requireNonNull(memoryBounds, "actual opened memory bounds");
            this.physicalBinding = physicalBinding;
            this.bookKeeper = bookKeeper;
            this.bookKeeperBinding = bookKeeperBinding;
            if (nativeSource.isObjectOnly() != (bookKeeper == null)) {
                throw new IllegalStateException("native source plan and BK observation differ");
            }
            members = nativeSource.isObjectOnly()
                    ? List.of(
                            objectVersion(physicalBinding.physicalNamespace(), objects.root()),
                            objectVersion(physicalBinding.physicalNamespace(), objects.data()))
                    : List.of(
                            bookKeeper.member(),
                            objectVersion(physicalBinding.physicalNamespace(), objects.root()),
                            objectVersion(physicalBinding.physicalNamespace(), objects.data()));
            readView = PulsarNativeReadViewV1.fromCapturedSource(this);
        }

        public PulsarNativeBindingV1 binding() {
            return binding;
        }

        public CapturedOffloadReadSource nativeSource() {
            return nativeSource;
        }

        OffloadedReadMemoryBounds memoryBounds() {
            return memoryBounds;
        }

        public OffloadedReadSourceIdentity objects() {
            return objects;
        }

        public PhysicalNamespaceAuthorityBindingV2 physicalBinding() {
            return physicalBinding;
        }

        public PhysicalNamespaceAuthorityBindingV2 bookKeeperBinding() {
            return bookKeeperBinding;
        }

        public long bookKeeperMetadataVersion() {
            if (bookKeeper == null) {
                throw new IllegalStateException("Object-only source has no selected BK metadata");
            }
            return bookKeeper.metadataVersion();
        }

        public boolean preferredOnly() {
            return nativeSource.isObjectOnly();
        }

        public List<PhysicalResourceIdV2> members() {
            return members;
        }

        /** One sealed source cut, not a complete Binding publication or an M4 admission grant. */
        public PulsarNativeReadViewV1 readView() {
            return readView;
        }
    }

    private record BookKeeperSource(
            PhysicalNamespaceAuthorityBindingV2 binding,
            PhysicalResourceIdV2.BookKeeperLedger member,
            long metadataVersion) {}

    private final AsyncOxiaClient client;
    private final MetadataNamespaceIdentityV2 metadataNamespace;
    private final String authorityRoot;
    private final PulsarTopicIncarnationIdentity incarnation;
    private final PulsarActiveTopicAuthority authority;
    private final BindingIdentity bindingIdentity;
    private final Map<String, String> nativeProperties;

    private PulsarNativeBindingV1(
            AsyncOxiaClient client,
            MetadataNamespaceIdentityV2 metadataNamespace,
            String authorityRoot,
            PulsarTopicIncarnationIdentity incarnation,
            PulsarActiveTopicAuthority authority,
            java.util.function.Predicate<StorageProfileV1> acceptsProfile) {
        this.client = client;
        this.metadataNamespace = metadataNamespace;
        this.authorityRoot = authorityRoot;
        this.incarnation = incarnation;
        this.authority = authority;
        if (!authority.aggregate().binding().incarnationIdentity().equals(incarnation)
                || !acceptsProfile.test(authority.aggregate().initialEpoch().storageProfile())
                || authority.selector().value().generation().value()
                        != incarnation.bindingGeneration().value()) {
            throw new IllegalArgumentException("native Binding differs from its P1 incarnation/profile");
        }
        bindingIdentity = new BindingIdentity(
                authority.aggregate().binding().bindingId(),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(incarnation)),
                authority.aggregate().initialEpoch().storageEpochId().digest());
        nativeProperties = Map.of(
                PROPERTY_PREFIX + "bindingId",
                        bindingIdentity.bindingId().digest().toHex(),
                PROPERTY_PREFIX + "incarnation",
                        bindingIdentity.incarnationSha256().toHex(),
                PROPERTY_PREFIX + "storageEpoch",
                        bindingIdentity.storageEpochSha256().toHex(),
                PROPERTY_PREFIX + "aggregate",
                        authority.aggregate().canonicalStoredDigest().toHex(),
                PROPERTY_PREFIX + "metadataNamespace",
                        HexFormat.of().formatHex(metadataNamespace.encode().toByteArray()));
    }

    /** Explicit first creation only; existing policy-less P1 aggregates are never converted. */
    public static CompletionStage<PulsarNativeBindingV1> createNativeOnly(
            AsyncOxiaClient client,
            MetadataNamespaceIdentityV2 metadataNamespace,
            String authorityRoot,
            com.nereusstream.metadata.spi.model.AggregatePublicationCandidate candidate,
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry) {
        return createNativeOnly(
                client,
                metadataNamespace,
                authorityRoot,
                candidate,
                registry,
                com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarRetentionPolicyV1
                        .NATIVE_PULSAR_ONLY_V1);
    }

    /** Explicit first-admission runtime exclusion; it cannot be attached to an existing Binding. */
    public static CompletionStage<PulsarNativeBindingV1> createNativeOnly(
            AsyncOxiaClient client,
            MetadataNamespaceIdentityV2 metadataNamespace,
            String authorityRoot,
            com.nereusstream.metadata.spi.model.AggregatePublicationCandidate candidate,
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry,
            com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarRetentionPolicyV1
                    policy) {
        return createNativeOnly(
                client, metadataNamespace, authorityRoot, candidate, registry, policy, Optional.empty());
    }

    public static CompletionStage<PulsarNativeBindingV1> createNativeOnly(
            AsyncOxiaClient client,
            MetadataNamespaceIdentityV2 metadataNamespace,
            String authorityRoot,
            com.nereusstream.metadata.spi.model.AggregatePublicationCandidate candidate,
            M5ClosedWriterRegistryV1 registry,
            com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarRetentionPolicyV1
                    policy,
            Optional<M5PulsarAuditDomainV1> initialAudits) {
        try {
            PulsarNativeWriterRegistryV2.requireNativeOwners(registry);
            com.nereusstream.domain.aggregate.TopicBindingAggregateValidatorV1.validate(candidate.aggregate());
            if (!(candidate.aggregate().binding().incarnationIdentity()
                            instanceof PulsarTopicIncarnationIdentity incarnation)
                    || !isNativeBookKeeperProfile(
                            candidate.aggregate().initialEpoch().storageProfile())
                    || !com.nereusstream.domain.codec.Nta1CodecV1.encode(candidate.aggregate())
                            .equals(candidate.canonicalStoredBytes())
                    || !Sha256Digest.hash(candidate.canonicalStoredBytes()).equals(candidate.canonicalStoredDigest())) {
                throw new IllegalArgumentException(
                        "Native-only creation requires one exact canonical Pulsar P1 candidate");
            }
            var keys = new OxiaV2AuthorityKeys(authorityRoot);
            var coordinator = new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1(
                    new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(client));
            var conditional = new AsyncOxiaConditionalClient(client);
            var codecs = OxiaV2CodecSet.productionP1();
            var mutations = new ConditionalMutationEngine(conditional, new MutationFailureClassifier());
            var selectors = new OxiaPulsarTopicGenerationSelectorStore(
                    () -> {}, keys, codecs.selector(), conditional, mutations);
            var publisher = new com.nereusstream.metadata.oxia.v2.capability.PulsarTopicAuthorityCoordinator(
                    proposed -> publishNativeOnlyAggregate(
                            coordinator,
                            keys.aggregateKey(incarnation),
                            incarnation,
                            proposed,
                            registry,
                            policy,
                            initialAudits,
                            codecs.aggregate()),
                    new OxiaTopicBindingAggregateReader(() -> {}, keys, codecs.aggregate(), conditional),
                    selectors);
            return OxiaPhysicalMetadataNamespaceV2.connect(client, metadataNamespace)
                    .thenCompose(namespace -> namespace.requireCurrent())
                    .thenCompose(ignored -> publisher.activate(candidate))
                    .thenCompose(ignored -> read(client, metadataNamespace, authorityRoot, incarnation));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static boolean isNativeBookKeeperProfile(StorageProfileV1 profile) {
        return profile == StorageProfileV1.BOOKKEEPER_WAL_ONLY
                || profile == StorageProfileV1.BOOKKEEPER_WAL_ASYNC_OBJECT;
    }

    private static CompletionStage<
                    com.nereusstream.metadata.spi.model.CreateMutationResult<
                            com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot>>
            publishNativeOnlyAggregate(
                    com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1 coordinator,
                    String key,
                    PulsarTopicIncarnationIdentity incarnation,
                    com.nereusstream.metadata.spi.model.AggregatePublicationCandidate candidate,
                    com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry,
                    com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1
                                    .PulsarRetentionPolicyV1
                            policy,
                    Optional<M5PulsarAuditDomainV1> initialAudits,
                    com.nereusstream.metadata.oxia.v2.codec.AggregateAuthorityCodec codec) {
        var request = new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                .NativeOnlyCreationRequest(
                key, candidate.canonicalStoredBytes(), registry.enrollment(), policy, initialAudits);
        return coordinator.createNativeOnly(request).thenCompose(outcome -> {
            if (outcome
                            != com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                    .Outcome.APPLIED_EXACT
                    && outcome
                            != com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                    .Outcome.EXISTING_EXACT) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Native-only P1 creation remains closed: " + outcome));
            }
            return coordinator.read(key).thenApply(observed -> {
                var stored = observed.orElseThrow();
                var snapshot = codec.decode(key, incarnation, stored.canonicalStoredBytes(), stored.metadataVersion());
                return outcome
                                == com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1
                                        .Outcome.APPLIED_EXACT
                        ? com.nereusstream.metadata.spi.model.CreateMutationResult.created(snapshot)
                        : com.nereusstream.metadata.spi.model.CreateMutationResult.existingExact(snapshot);
            });
        });
    }

    /** Reads through one actual Oxia client and its permanent namespace marker; never accepts a caller snapshot. */
    public static CompletionStage<PulsarNativeBindingV1> read(
            AsyncOxiaClient client,
            MetadataNamespaceIdentityV2 metadataNamespace,
            String authorityRoot,
            PulsarTopicIncarnationIdentity incarnation) {
        return readExact(
                client,
                metadataNamespace,
                authorityRoot,
                incarnation,
                PulsarNativeBindingV1::isNativeBookKeeperProfile);
    }

    /** Object WAL reuses the same exact P1 Binding capture and admits only its own Storage Epoch profile. */
    public static CompletionStage<PulsarNativeBindingV1> readObjectWal(
            AsyncOxiaClient client,
            MetadataNamespaceIdentityV2 metadataNamespace,
            String authorityRoot,
            PulsarTopicIncarnationIdentity incarnation) {
        return readExact(
                client,
                metadataNamespace,
                authorityRoot,
                incarnation,
                profile -> profile == StorageProfileV1.OBJECT_WAL);
    }

    private static CompletionStage<PulsarNativeBindingV1> readExact(
            AsyncOxiaClient client,
            MetadataNamespaceIdentityV2 metadataNamespace,
            String authorityRoot,
            PulsarTopicIncarnationIdentity incarnation,
            java.util.function.Predicate<StorageProfileV1> acceptsProfile) {
        try {
            Objects.requireNonNull(client, "client");
            Objects.requireNonNull(incarnation, "incarnation");
            var keys = new OxiaV2AuthorityKeys(authorityRoot);
            var codecs = OxiaV2CodecSet.productionP1();
            var conditional = new AsyncOxiaConditionalClient(client);
            var mutations = new ConditionalMutationEngine(conditional, new MutationFailureClassifier());
            var selectors = new OxiaPulsarTopicGenerationSelectorStore(
                    () -> {}, keys, codecs.selector(), conditional, mutations);
            var aggregates = new OxiaTopicBindingAggregateReader(() -> {}, keys, codecs.aggregate(), conditional);
            return OxiaPhysicalMetadataNamespaceV2.connect(client, metadataNamespace)
                    .thenCompose(namespace -> selectors
                            .readSelector(incarnation.persistenceName())
                            .thenCompose(first -> {
                                var selected = first.orElseThrow(
                                        () -> new IllegalStateException("native P1 selector is absent"));
                                return aggregates.readAggregate(incarnation).thenCompose(observed -> {
                                    var aggregate = observed.orElseThrow(
                                            () -> new IllegalStateException("native P1 aggregate is absent"));
                                    var active = new PulsarActiveTopicAuthority(selected, aggregate);
                                    return selectors
                                            .readSelector(incarnation.persistenceName())
                                            .thenCompose(last -> {
                                                if (last.filter(selected::equals)
                                                        .isEmpty()) {
                                                    return CompletableFuture.failedFuture(new IllegalStateException(
                                                            "native P1 selection changed during capture"));
                                                }
                                                return namespace
                                                        .requireCurrent()
                                                        .thenApply(ignored -> new PulsarNativeBindingV1(
                                                                client,
                                                                metadataNamespace,
                                                                authorityRoot,
                                                                incarnation,
                                                                active,
                                                                acceptsProfile));
                                            });
                                });
                            }));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    final class RetentionInstallation {
        private final org.apache.bookkeeper.mledger.ManagedLedgerConfig config;
        private final Optional<com.nereusstream.storage.object.retention.M5PulsarRetentionConfigurationV1> expected;
        private final Sha256Digest factoryScope;

        private RetentionInstallation(
                org.apache.bookkeeper.mledger.ManagedLedgerConfig config,
                Optional<com.nereusstream.storage.object.retention.M5PulsarRetentionConfigurationV1> expected,
                Sha256Digest factoryScope) {
            this.config = config;
            this.expected = expected;
            this.factoryScope = factoryScope;
        }

        org.apache.bookkeeper.mledger.ManagedLedgerConfig config() {
            return config;
        }

        void requireRequested(org.apache.bookkeeper.mledger.ManagedLedgerConfig requested) {
            if (expected.isPresent()
                    && (!java.time.Clock.systemUTC().equals(requested.getClock())
                            || !expected.orElseThrow()
                                    .sameValues(
                                            requested.getRetentionTimeMillis(), requested.getRetentionSizeInMB()))) {
                throw new IllegalArgumentException("Native retention change requires drained P1 policy update");
            }
        }

        CompletionStage<Void> installed(org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl ledger) {
            if (expected.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            if (ledger.getConfig() != config) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Native SDK installed another configuration"));
            }
            var coordinator =
                    new M5PulsarAggregateRetirementCoordinatorV1(new Oxia09ExactMetadataTransactionStoreV1(client));
            var key = new OxiaV2AuthorityKeys(authorityRoot).aggregateKey(incarnation);
            return coordinator.read(key).thenCompose(actual -> {
                var value = actual.orElseThrow();
                var current = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(value.canonicalStoredBytes())
                        .nativeRetention()
                        .orElseThrow();
                var original = expected.orElseThrow();
                if (current.generation() != original.generation()
                        || !current.sameValues(config.getRetentionTimeMillis(), config.getRetentionSizeInMB())) {
                    throw new IllegalStateException("Native retention changed during SDK installation");
                }
                return coordinator
                        .confirmNativeRetention(value, current.installed(factoryScope))
                        .thenApply(outcome -> {
                            requireNativePolicyApplied(outcome);
                            return null;
                        });
            });
        }
    }

    private static org.apache.bookkeeper.mledger.ManagedLedgerConfig frozenRetention(
            org.apache.bookkeeper.mledger.ManagedLedgerConfig configured) {
        if (configured.getClass() != org.apache.bookkeeper.mledger.ManagedLedgerConfig.class
                || !java.time.Clock.systemUTC().equals(configured.getClock())) {
            throw new IllegalArgumentException("Native retention requires concrete SDK config and system UTC clock");
        }
        return configured.copyWithFrozenRetention();
    }

    private static void requireNativePolicyApplied(M5PulsarAggregateRetirementCoordinatorV1.Outcome outcome) {
        if (outcome != M5PulsarAggregateRetirementCoordinatorV1.Outcome.APPLIED_EXACT
                && outcome != M5PulsarAggregateRetirementCoordinatorV1.Outcome.EXISTING_EXACT) {
            throw new IllegalStateException("Native retention authority remains unresolved: " + outcome);
        }
    }

    CompletionStage<RetentionInstallation> prepareNativeRetention(
            PulsarNativeBookKeeperCreateV1 creation,
            org.apache.bookkeeper.mledger.impl.MetaStoreImpl nativeStore,
            org.apache.bookkeeper.mledger.ManagedLedgerConfig configured) {
        var coordinator =
                new M5PulsarAggregateRetirementCoordinatorV1(new Oxia09ExactMetadataTransactionStoreV1(client));
        var key = new OxiaV2AuthorityKeys(authorityRoot).aggregateKey(incarnation);
        return coordinator.read(key).thenCompose(actual -> {
            var predecessor = actual.orElseThrow();
            var value = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(predecessor.canonicalStoredBytes());
            if (value.retentionPolicy().isEmpty()) {
                return CompletableFuture.completedFuture(new RetentionInstallation(
                        configured, Optional.empty(), creation.spec().sha256()));
            }
            var copy = frozenRetention(configured);
            if (value.nativeRetention().isPresent()) {
                var existing = value.nativeRetention().orElseThrow();
                if (!existing.sameValues(copy.getRetentionTimeMillis(), copy.getRetentionSizeInMB())) {
                    throw new IllegalArgumentException("Native retention change requires drained P1 policy update");
                }
                return CompletableFuture.completedFuture(new RetentionInstallation(
                        copy, Optional.of(existing), creation.spec().sha256()));
            }
            return nativeStore
                    .readStoredMutationValue(managedLedgerName(), "/managed-ledgers/" + managedLedgerName(), 8_388_608)
                    .thenCompose(parent -> {
                        if (parent.isPresent()) {
                            // Historical SDK reads remain available, but cannot import missing policy history.
                            return CompletableFuture.completedFuture(new RetentionInstallation(
                                    configured,
                                    Optional.empty(),
                                    creation.spec().sha256()));
                        }
                        if (!value.firstNativeFactoryScope()
                                .equals(Optional.of(creation.spec().sha256()))) {
                            throw new IllegalStateException(
                                    "Native retention lacks its original missing-parent admission");
                        }
                        var pending = new com.nereusstream.storage.object.retention.M5PulsarRetentionConfigurationV1(
                                copy.getRetentionTimeMillis(),
                                copy.getRetentionSizeInMB(),
                                1,
                                creation.spec().sha256(),
                                Optional.empty());
                        return coordinator
                                .prepareNativeRetention(predecessor, pending)
                                .thenApply(outcome -> {
                                    requireNativePolicyApplied(outcome);
                                    return new RetentionInstallation(
                                            copy,
                                            Optional.of(pending),
                                            creation.spec().sha256());
                                });
                    });
        });
    }

    /** Explicit operator update; pending policy cannot qualify floors until an actual SDK installation confirms it. */
    public CompletionStage<Outcome> updateNativeRetention(org.apache.bookkeeper.mledger.ManagedLedgerConfig desired) {
        try {
            var copy = frozenRetention(desired);
            var coordinator =
                    new M5PulsarAggregateRetirementCoordinatorV1(new Oxia09ExactMetadataTransactionStoreV1(client));
            var key = new OxiaV2AuthorityKeys(authorityRoot).aggregateKey(incarnation);
            return coordinator.read(key).thenCompose(actual -> {
                var predecessor = actual.orElseThrow();
                var previous = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(predecessor.canonicalStoredBytes())
                        .nativeRetention()
                        .orElseThrow();
                return coordinator.updateNativeRetention(
                        predecessor,
                        new com.nereusstream.storage.object.retention.M5PulsarRetentionConfigurationV1(
                                copy.getRetentionTimeMillis(),
                                copy.getRetentionSizeInMB(),
                                Math.addExact(previous.generation(), 1),
                                previous.originalFactoryScope(),
                                Optional.empty()));
            });
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    CompletionStage<Void> claimFirstNativeFactory(
            PulsarNativeBookKeeperCreateV1 creation, org.apache.bookkeeper.mledger.impl.MetaStoreImpl nativeStore) {
        var coordinator = new com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1(
                new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(client));
        String key = new OxiaV2AuthorityKeys(authorityRoot).aggregateKey(incarnation);
        return coordinator.read(key).thenCompose(observed -> {
            var captured = observed.orElseThrow();
            var current = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                    captured.canonicalStoredBytes());
            if (current.retentionPolicy().isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            return nativeStore
                    .readStoredMutationValue(managedLedgerName(), "/managed-ledgers/" + managedLedgerName(), 8_388_608)
                    .thenCompose(parent -> {
                        if (current.firstNativeFactoryScope().isPresent()) {
                            if (parent.isEmpty()
                                    && !current.firstNativeFactoryScope()
                                            .equals(Optional.of(creation.spec().sha256()))) {
                                return CompletableFuture.failedFuture(
                                        new IllegalStateException("prior Native factory parent is absent"));
                            }
                            return CompletableFuture.completedFuture(null);
                        }
                        if (parent.isPresent()) {
                            return CompletableFuture.failedFuture(new IllegalStateException(
                                    "Native-only first admission cannot adopt existing parent metadata"));
                        }
                        return coordinator
                                .claimFirstNativeFactory(
                                        key, captured, creation.spec().sha256())
                                .thenCompose(outcome -> {
                                    if (outcome
                                                    != com.nereusstream.storage.object.retention
                                                            .M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                                            .APPLIED_EXACT
                                            && outcome
                                                    != com.nereusstream.storage.object.retention
                                                            .M5PulsarAggregateRetirementCoordinatorV1.Outcome
                                                            .EXISTING_EXACT) {
                                        return CompletableFuture.failedFuture(new IllegalStateException(
                                                "Native first-factory claim remains unresolved: " + outcome));
                                    }
                                    return CompletableFuture.completedFuture(null);
                                });
                    });
        });
    }

    CompletionStage<Void> requireFirstNativeFactory(PulsarNativeBookKeeperCreateV1 creation) {
        String key = new OxiaV2AuthorityKeys(authorityRoot).aggregateKey(incarnation);
        var exact = new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(client);
        return revalidate().thenCompose(ignored -> exact.read(key)).thenAccept(observed -> {
            var value = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                    observed.orElseThrow().canonicalStoredBytes());
            if (value.retentionPolicy().isEmpty()
                    || !value.firstNativeFactoryScope()
                            .equals(Optional.of(creation.spec().sha256()))
                    || !value.originalAggregateSha256()
                            .equals(authority.aggregate().canonicalStoredDigest())) {
                throw new IllegalStateException("missing Native parent is not this original first-factory admission");
            }
        });
    }

    /** Three explicit policy rows, still tied to the caller's original durable P1 source-scan revision. */
    public static final class NativeOnlyKafkaFloors {
        private final Sha256Digest policyRoot;
        private final List<com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorObservationV1>
                rows;

        private NativeOnlyKafkaFloors(
                Sha256Digest policyRoot,
                List<com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorObservationV1> rows) {
            this.policyRoot = policyRoot;
            this.rows = List.copyOf(rows);
        }

        public Sha256Digest policyRoot() {
            return policyRoot;
        }

        public List<com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorObservationV1> rows() {
            return rows;
        }
    }

    /** Policy bytes alone are insufficient: verify actual closed enrollment, identity and the physical P1 version. */
    public CompletionStage<NativeOnlyKafkaFloors> captureNativeOnlyKafkaFloors(
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue exactScan) {
        try {
            PulsarNativeWriterRegistryV2.requireNativeOwners(registry);
            var coordinator = logicalTrimCoordinator();
            return coordinator
                    .requireSourceScanCurrent(exactScan)
                    .thenCompose(ignored -> revalidate())
                    .thenApply(ignored -> {
                        var value = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                .decodeAuthority(exactScan.canonicalStoredBytes());
                        if (value.retentionPolicy().isEmpty()
                                || value.firstNativeFactoryScope().isEmpty()
                                || !value.canonicalAggregateBytes()
                                        .equals(authority.aggregate().canonicalStoredBytes())
                                || !value.originalAggregateVersion()
                                        .equals(Optional.of(
                                                authority.aggregate().aggregateSourceVersion()))
                                || !value.writerEnrollment().equals(Optional.of(registry.enrollment()))) {
                            throw new IllegalStateException(
                                    "Native-only floor lacks its original P1 policy and closed writer admission");
                        }
                        var root = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1
                                .nativeOnlyPolicyRoot(value);
                        var fact = new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                                exactScan.key(), exactScan.metadataVersion(), exactScan.canonicalStoredSha256());
                        var rows = List.of(
                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1
                                                .KAFKA_CONSUMER_GROUP,
                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1
                                                .KAFKA_PRODUCER_TRANSACTION,
                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1
                                                .KAFKA_REPLICATION_RECOVERY)
                                .stream()
                                .map(
                                        kind -> new com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .RetentionFloorObservationV1(
                                                kind,
                                                fact,
                                                com.nereusstream.storage.object.materialization
                                                        .M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY,
                                                -1,
                                                false,
                                                true))
                                .toList();
                        return new NativeOnlyKafkaFloors(root, rows);
                    })
                    .thenCompose(result -> coordinator
                            .requireSourceScanCurrent(exactScan)
                            .thenCompose(ignored -> revalidate())
                            .thenApply(ignored -> result));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /**
     * Enumerates only this original aggregate's closed Kafka protocol domain. Shared physical sources and
     * other Bindings require their own member scans; these four rows cannot replace the full reference inventory.
     */
    public CompletionStage<
                    List<
                            com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1
                                    .ReferenceAdapterResultV1>>
            captureNativeOnlyKafkaAggregateReferences(
                    com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 registry,
                    com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue exactScan,
                    com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 coverage) {
        if (!coverage.binding().equals(bindingIdentity)) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Native-only reference coverage belongs to another Binding"));
        }
        // The birth policy, exact original P1 version, first-factory admission and production ownership
        // enrollment establish the closed protocol domain. A floor value alone never establishes absence.
        return captureNativeOnlyKafkaFloors(registry, exactScan).thenCompose(ignored -> {
            var value = com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                    exactScan.canonicalStoredBytes());
            var fact = new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                    exactScan.key(), exactScan.metadataVersion(), exactScan.canonicalStoredSha256());
            var results = java.util.Arrays.stream(
                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1.values())
                    .filter(kind -> value.retentionPolicy().orElseThrow().excludesKafkaReference(kind))
                    .map(
                            kind -> {
                                var row = new com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                        .ReferenceObservationV1(
                                        kind,
                                        fact,
                                        value.originalAggregateSha256(),
                                        com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1
                                                .ProtocolCoverage.pulsar(coverage),
                                        com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .ReferenceDispositionV1.ABSENT,
                                        true);
                                return new com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1
                                        .ReferenceAdapterResultV1(
                                        kind,
                                        new com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                .ReferenceScanSummaryV1(
                                                kind,
                                                1,
                                                1,
                                                exactScan.canonicalStoredBytes().length(),
                                                true),
                                        List.of(row));
                            })
                    .toList();
            return logicalTrimCoordinator()
                    .requireSourceScanCurrent(exactScan)
                    .thenCompose(unused -> revalidate())
                    .thenApply(unused -> results);
        });
    }

    /** The ADR0016 exclusion must be persisted before first admission; service absence is not evidence. */
    public CompletionStage<FloorAdapterResultV1> captureAccessProjectionFloor(
            M5ClosedWriterRegistryV1 registry,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue exactScan) {
        return captureNativeOnlyKafkaFloors(registry, exactScan).thenApply(ignored -> {
            var policy = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(exactScan.canonicalStoredBytes())
                    .retentionPolicy()
                    .orElseThrow();
            if (!policy.excludesAccessProjection()) {
                throw new IllegalStateException("Native Binding has no first-admission Access Projection exclusion");
            }
            var kind = FloorClassV1.PROJECTION_MIGRATION;
            var fact = new AuthorityFactV1(
                    exactScan.key(), exactScan.metadataVersion(), exactScan.canonicalStoredSha256());
            var row = new RetentionFloorObservationV1(
                    kind,
                    fact,
                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain
                            .PULSAR_ENTRY,
                    -1,
                    false,
                    true);
            return new FloorAdapterResultV1(
                    kind, 1, exactScan.canonicalStoredBytes().length(), List.of(row));
        });
    }

    /** Actual same-Cell membership and birth policy qualify only the explicitly excluded physical-reference kinds. */
    public CompletionStage<List<ReferenceAdapterResultV1>> captureExcludedPhysicalReferences(
            M5ClosedWriterRegistryV1 registry,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue exactScan,
            com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2 inventory,
            PhysicalResourceIdV2 target,
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 coverage) {
        if (!coverage.binding().equals(bindingIdentity)) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Native excluded-reference coverage belongs to another Binding"));
        }
        return captureNativeOnlyKafkaFloors(registry, exactScan)
                .thenCompose(ignored -> inventory.readFact(target))
                .thenCompose(memberFact -> {
                    var members = memberFact.inventory();
                    var cell = Sha256Digest.hash(com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1.encode(
                            authority.aggregate().binding().cellIdentity()));
                    if (!members.resource().equals(target.sha256())
                            || !members.protocolCell().equals(cell)
                            || members.members().stream()
                                    .anyMatch(member -> !member.binding().equals(bindingIdentity)
                                            || member.domain()
                                                    != com.nereusstream.storage.object.materialization
                                                            .M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY)) {
                        throw new IllegalStateException(
                                "Native excluded-reference target has foreign physical membership");
                    }
                    var policy = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(exactScan.canonicalStoredBytes())
                            .retentionPolicy()
                            .orElseThrow();
                    var facts = List.of(
                                    new AuthorityFactV1(
                                            exactScan.key(),
                                            exactScan.metadataVersion(),
                                            exactScan.canonicalStoredSha256()),
                                    memberFact.authority())
                            .stream()
                            .sorted(java.util.Comparator.comparing(AuthorityFactV1::key))
                            .toList();
                    long bytes = (long) exactScan.canonicalStoredBytes().length()
                            + memberFact.canonicalStoredBytes().length();
                    var results = java.util.Arrays.stream(
                                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                            .values())
                            .filter(policy::excludesReference)
                            .map(kind -> {
                                var rows = facts.stream()
                                        .map(fact -> new ReferenceObservationV1(
                                                kind,
                                                fact,
                                                target.sha256(),
                                                com.nereusstream.storage.object.materialization
                                                        .M5MaterializationRecordsV1.ProtocolCoverage.pulsar(coverage),
                                                ReferenceDispositionV1.ABSENT,
                                                true))
                                        .toList();
                                return new ReferenceAdapterResultV1(
                                        kind, new ReferenceScanSummaryV1(kind, rows.size(), 1, bytes, true), rows);
                            })
                            .toList();
                    return inventory.readFact(target).thenCompose(current -> {
                        if (!current.equals(memberFact)) {
                            return CompletableFuture.failedFuture(new IllegalStateException(
                                    "Native excluded-reference membership changed during capture"));
                        }
                        return logicalTrimCoordinator()
                                .requireSourceScanCurrent(exactScan)
                                .thenCompose(unused -> revalidate())
                                .thenApply(unused -> results);
                    });
                });
    }

    /** Actual P1 retirement closure and the physical resource's complete member inventory. */
    public CompletionStage<ReferenceAdapterResultV1> captureSharedPhysicalReferences(
            M5ClosedWriterRegistryV1 registry,
            VersionedValue exactScan,
            com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2 inventory,
            PhysicalResourceIdV2 target,
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 coverage) {
        var coordinator =
                new M5PulsarAggregateRetirementCoordinatorV1(new Oxia09ExactMetadataTransactionStoreV1(client));
        return coordinator
                .requireNativeRetirementQuiescent(exactScan, registry)
                .thenCompose(
                        ignored -> captureExcludedPhysicalReferences(registry, exactScan, inventory, target, coverage))
                .thenCompose(excluded -> inventory.readFact(target).thenCompose(members -> {
                    var scope = excluded.get(0);
                    if (scope.observations().stream()
                            .noneMatch(row -> row.authority().equals(members.authority()))) {
                        return CompletableFuture.failedFuture(new IllegalStateException(
                                "Native physical membership changed during retirement enumeration"));
                    }
                    var shared = members.inventory().members().size() == 1
                            ? ReferenceDispositionV1.ABSENT
                            : ReferenceDispositionV1.PRESENT;
                    var kind = com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                            .SHARED_PHYSICAL_MEMBER;
                    var rows = scope.observations().stream()
                            .map(row -> new ReferenceObservationV1(
                                    kind, row.authority(), row.targetIdentitySha256(), row.coverage(), shared, true))
                            .toList();
                    var results = new ReferenceAdapterResultV1(
                            kind,
                            new ReferenceScanSummaryV1(
                                    kind, rows.size(), 1, scope.summary().scannedBytes(), true),
                            rows);
                    return coordinator
                            .requireNativeRetirementQuiescent(exactScan, registry)
                            .thenApply(ignored -> results);
                }));
    }

    CompletionStage<FloorAdapterResultV1> captureExclusivePhysicalFloor(
            M5ClosedWriterRegistryV1 registry,
            VersionedValue exactScan,
            com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2 inventory,
            PhysicalResourceIdV2 target,
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 coverage) {
        return captureSharedPhysicalReferences(registry, exactScan, inventory, target, coverage)
                .thenApply(references -> {
                    if (references.observations().stream()
                            .anyMatch(row -> row.disposition() != ReferenceDispositionV1.ABSENT)) {
                        throw new IllegalStateException(
                                "Shared physical floor requires every member's qualified obligations");
                    }
                    var kind = FloorClassV1.SHARED_PHYSICAL_SOURCE;
                    var rows = references.observations().stream()
                            .map(row -> new RetentionFloorObservationV1(
                                    kind,
                                    row.authority(),
                                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1
                                            .PositionDomain.PULSAR_ENTRY,
                                    -1,
                                    false,
                                    true,
                                    Optional.empty()))
                            .toList();
                    return new FloorAdapterResultV1(
                            kind,
                            references.summary().pageCount(),
                            references.summary().scannedBytes(),
                            rows);
                });
    }

    CompletionStage<Void> requirePhysicalReferenceFactsCurrent(List<ReferenceAdapterResultV1> results) {
        return new com.nereusstream.storage.object.retention.M5ReferenceFreshnessVerifierV1(
                        new Oxia09ExactMetadataTransactionStoreV1(client))
                .requireFreshFacts(results.stream()
                        .flatMap(result -> result.observations().stream())
                        .map(ReferenceObservationV1::authority)
                        .toList())
                .thenApply(ignored -> null);
    }

    /** Original audit-registration invocation; observers cannot cancel its durable physical-writer lifetime. */
    public final class AuditRegistration {
        private final M5ClosedWriterRegistryV1 registry;
        private final OxiaQuotaTargetDeleteStoreV2 route;
        private final VersionedValue predecessor;
        private final M5PulsarAuditDomainV1.Hold hold;
        private final M5TargetDeleteMultiWriterGuardV2.Context context;
        private final M5TargetDeleteMultiWriterGuardV2 guard;
        private CompletableFuture<Outcome> original;
        private M5TargetDeleteMultiWriterGuardV2.Result<Outcome> result;
        private volatile Outcome resolved;

        private AuditRegistration(
                M5ClosedWriterRegistryV1 registry,
                OxiaQuotaTargetDeleteStoreV2 route,
                VersionedValue predecessor,
                M5PulsarAuditDomainV1.Hold hold,
                Sha256Digest capability) {
            this.registry = registry;
            this.route = route;
            this.predecessor = predecessor;
            this.hold = hold;
            context = new M5TargetDeleteMultiWriterGuardV2.Context(
                    ProofBoundWriterClassV1.LOGICAL_TRIM_RETENTION_FLOOR_V1,
                    capability,
                    predecessor.canonicalStoredSha256(),
                    Sha256Digest.hash(new M5PulsarAuditDomainV1(List.of(hold)).encode()));
            guard = new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(route));
        }

        public M5PulsarAuditDomainV1.Hold hold() {
            return hold;
        }

        public synchronized CompletableFuture<Outcome> register() {
            if (original == null) {
                var metadata = new Oxia09ExactMetadataTransactionStoreV1(client);
                var coordinator = new M5PulsarAggregateRetirementCoordinatorV1(metadata, (candidate, mutation) -> {
                    if (!candidate.equals(hold)) {
                        return CompletableFuture.failedFuture(new IllegalStateException("audit candidate changed"));
                    }
                    return guard.execute(List.of(hold.resource()), context, () -> revalidate()
                                    .thenCompose(ignored -> mutation.get())
                                    .handle((value, failure) -> value == null ? Outcome.RESPONSE_UNKNOWN : value)
                                    .thenCompose(value -> terminal()
                                            .thenApply(proof -> new M5TargetDeleteMultiWriterGuardV2.Completion<>(
                                                    proof.isPresent()
                                                            ? proof.orElseThrow()
                                                                    .outcome()
                                                            : value,
                                                    proof.map(AuditTerminal::sha256)))))
                            .thenApply(settled -> {
                                result = settled;
                                return settled.unresolvedTargets().isEmpty()
                                        ? resolve(settled.value().orElse(Outcome.DEFINITIVELY_NOT_APPLIED))
                                        : Outcome.RESPONSE_UNKNOWN;
                            });
                });
                original = coordinator
                        .registerAuditHold(
                                predecessor.key(),
                                predecessor,
                                registry,
                                hold.obligationId(),
                                hold.resource(),
                                hold.coverage())
                        .toCompletableFuture();
            }
            return original.thenApply(value -> resolved == null ? resolve(value) : resolved);
        }

        /** A later exact P1 version excludes the original conditional request; retries never dispatch it again. */
        public CompletableFuture<Outcome> reconcile() {
            final CompletableFuture<Outcome> operation;
            synchronized (this) {
                operation = original;
            }
            if (operation == null) {
                return register();
            }
            return operation
                    .thenCompose(value -> {
                        if (resolved != null) {
                            return CompletableFuture.completedFuture(resolved);
                        }
                        if (result == null || result.unresolvedTargets().isEmpty()) {
                            return CompletableFuture.completedFuture(resolve(value));
                        }
                        if (result.terminalCleanup().isPresent()) {
                            return result.terminalCleanup()
                                    .orElseThrow()
                                    .reconcile()
                                    .thenApply(left -> left.isEmpty()
                                            ? resolve(Outcome.DEFINITIVELY_NOT_APPLIED)
                                            : Outcome.RESPONSE_UNKNOWN);
                        }
                        return terminal().thenCompose(proof -> {
                            if (proof.isPresent()) {
                                return finishAudit(proof.orElseThrow());
                            }
                            var metadata = new Oxia09ExactMetadataTransactionStoreV1(client);
                            var coordinator = new M5PulsarAggregateRetirementCoordinatorV1(metadata);
                            return metadata.read(predecessor.key()).thenCompose(current -> {
                                if (current.isEmpty()) {
                                    return CompletableFuture.completedFuture(Outcome.RESPONSE_UNKNOWN);
                                }
                                return coordinator
                                        .fenceAuditAttempt(predecessor.key(), current.orElseThrow(), registry)
                                        .thenCompose(ignored -> terminal())
                                        .thenCompose(fenced -> fenced.isPresent()
                                                ? finishAudit(fenced.orElseThrow())
                                                : CompletableFuture.completedFuture(Outcome.RESPONSE_UNKNOWN));
                            });
                        });
                    })
                    .toCompletableFuture();
        }

        private CompletionStage<Outcome> finishAudit(AuditTerminal proof) {
            return guard.reconcileOperation(List.of(hold.resource()), context, result.operationId(), proof.sha256())
                    .thenApply(left -> left.isEmpty() ? resolve(proof.outcome()) : Outcome.RESPONSE_UNKNOWN);
        }

        private synchronized Outcome resolve(Outcome outcome) {
            if (resolved == null
                    && (outcome == Outcome.APPLIED_EXACT
                            || outcome == Outcome.EXISTING_EXACT
                            || outcome == Outcome.DEFINITIVELY_NOT_APPLIED)) {
                resolved = outcome;
            }
            return resolved == null ? outcome : resolved;
        }

        private CompletionStage<Optional<AuditTerminal>> terminal() {
            return new Oxia09ExactMetadataTransactionStoreV1(client)
                    .read(predecessor.key())
                    .handle((observed, failure) -> {
                        if (failure != null || observed.isEmpty()) {
                            return Optional.empty();
                        }
                        var actual = observed.orElseThrow();
                        if (!M5PulsarAggregateAuthorityCodecV1.isAuthorityValue(actual.canonicalStoredBytes())) {
                            return !actual.metadataVersion().equals(predecessor.metadataVersion())
                                    ? Optional.of(new AuditTerminal(
                                            Outcome.DEFINITIVELY_NOT_APPLIED, actual.canonicalStoredSha256()))
                                    : Optional.empty();
                        }
                        var value = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(actual.canonicalStoredBytes());
                        var found = value.auditDomain().stream()
                                .flatMap(domain -> domain.holds().stream())
                                .anyMatch(hold::equals);
                        if (found) {
                            return Optional.of(
                                    new AuditTerminal(Outcome.APPLIED_EXACT, actual.canonicalStoredSha256()));
                        }
                        if (!actual.metadataVersion().equals(predecessor.metadataVersion())) {
                            return Optional.of(new AuditTerminal(
                                    Outcome.DEFINITIVELY_NOT_APPLIED, actual.canonicalStoredSha256()));
                        }
                        return Optional.empty();
                    });
        }
    }

    private record AuditTerminal(Outcome outcome, Sha256Digest sha256) {}

    public CompletionStage<AuditRegistration> prepareAuditRegistration(
            M5ClosedWriterRegistryV1 registry,
            com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2 backend,
            PhysicalResourceIdV2 resource,
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 coverage,
            Sha256Digest obligationId) {
        PulsarNativeWriterRegistryV2.requireNativeOwners(registry);
        if (!coverage.binding().equals(bindingIdentity) || !resource.namespace().equals(backend.namespace())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("audit target scope differs"));
        }
        var key = new OxiaV2AuthorityKeys(authorityRoot).aggregateKey(incarnation);
        return revalidate()
                .thenCompose(ignored -> materializationQuota(backend))
                .thenCompose(route -> route.members().readFact(resource).thenCompose(members -> {
                    var cell = Sha256Digest.hash(com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1.encode(
                            authority.aggregate().binding().cellIdentity()));
                    if (!members.inventory().protocolCell().equals(cell)
                            || members.inventory().members().stream()
                                    .noneMatch(member -> member.binding().equals(bindingIdentity)
                                            && member.domain()
                                                    == com.nereusstream.storage.object.materialization
                                                            .M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY)) {
                        return CompletableFuture.failedFuture(
                                new IllegalArgumentException("audit resource is not a Native member"));
                    }
                    return new Oxia09ExactMetadataTransactionStoreV1(client)
                            .read(key)
                            .thenCompose(open -> {
                                var predecessor = open.orElseThrow();
                                var value = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                        predecessor.canonicalStoredBytes());
                                requireAuditDomain(value, registry);
                                if (value.state()
                                        != com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1
                                                .PulsarAggregateAuthorityStateV1.OPEN_V1) {
                                    throw new IllegalStateException(
                                            "audit registration is blocked by the current P1 scan");
                                }
                                var hold = value.auditDomain().orElseThrow().holds().stream()
                                        .filter(existing ->
                                                existing.obligationId().equals(obligationId))
                                        .findFirst()
                                        .orElseGet(() -> new M5PulsarAuditDomainV1.Hold(
                                                obligationId,
                                                resource,
                                                coverage,
                                                Math.addExact(value.authorityGeneration(), 1)));
                                if (!hold.resource().equals(resource)
                                        || !hold.coverage().equals(coverage)) {
                                    throw new IllegalArgumentException(
                                            "audit obligation id has another resource or coverage");
                                }
                                return new M5TargetDeleteAuthorityCoordinatorV1(route)
                                        .read(resource.authorityKey())
                                        .thenCompose(target -> revalidate()
                                                .thenApply(ignored -> new AuditRegistration(
                                                        registry,
                                                        route,
                                                        predecessor,
                                                        hold,
                                                        target.orElseThrow()
                                                                .authority()
                                                                .writerEnrollment()
                                                                .capabilitySetRootSha256())));
                            });
                }));
    }

    /** Confirms the original audit enrollment before a Broker or maintenance actor admits source deletion. */
    public CompletionStage<Void> requireBookKeeperDeleteAuditEnrollment(M5ClosedWriterRegistryV1 registry) {
        var key = new OxiaV2AuthorityKeys(authorityRoot).aggregateKey(incarnation);
        return revalidate()
                .thenCompose(unused -> new Oxia09ExactMetadataTransactionStoreV1(client).read(key))
                .thenAccept(found -> requireAuditEnrollment(
                        M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                found.orElseThrow().canonicalStoredBytes()),
                        registry));
    }

    private void requireAuditDomain(
            com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1
                            .PulsarAggregateRetirementAuthorityV1
                    value,
            M5ClosedWriterRegistryV1 registry) {
        requireAuditEnrollment(value, registry);
        if (value.firstNativeFactoryScope().isEmpty()) {
            throw new IllegalStateException("Native audit capture has no first factory scope");
        }
    }

    private void requireAuditEnrollment(
            com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1
                            .PulsarAggregateRetirementAuthorityV1
                    value,
            M5ClosedWriterRegistryV1 registry) {
        if (value.auditDomain().isEmpty()
                || !value.writerEnrollment().equals(Optional.of(registry.enrollment()))
                || !value.canonicalAggregateBytes().equals(authority.aggregate().canonicalStoredBytes())
                || !value.originalAggregateVersion()
                        .equals(Optional.of(authority.aggregate().aggregateSourceVersion()))) {
            throw new IllegalStateException("Native audit requires its first-admission domain and exact enrolled P1");
        }
    }

    /** Business audit enumeration only; elapsed grace still requires the original GC producer's mark/readiness. */
    public CompletionStage<ReferenceAdapterResultV1> captureAuditReferences(
            M5ClosedWriterRegistryV1 registry,
            VersionedValue exactScan,
            com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2 inventory,
            PhysicalResourceIdV2 target,
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 coverage) {
        return captureExcludedPhysicalReferences(registry, exactScan, inventory, target, coverage)
                .thenApply(excluded -> {
                    var value = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(exactScan.canonicalStoredBytes());
                    requireAuditDomain(value, registry);
                    var disposition = value.auditDomain().orElseThrow().holds().stream()
                                    .anyMatch(hold -> hold.resource().equals(target))
                            ? ReferenceDispositionV1.PRESENT
                            : ReferenceDispositionV1.ABSENT;
                    var kind =
                            com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1.AUDIT_GRACE;
                    var scope = excluded.get(0);
                    var rows = scope.observations().stream()
                            .map(row -> new ReferenceObservationV1(
                                    kind, row.authority(), target.sha256(), row.coverage(), disposition, true))
                            .toList();
                    return new ReferenceAdapterResultV1(
                            kind,
                            new ReferenceScanSummaryV1(
                                    kind, rows.size(), 1, scope.summary().scannedBytes(), true),
                            rows);
                });
    }

    public CompletionStage<FloorAdapterResultV1> captureAuditFloor(
            M5ClosedWriterRegistryV1 registry,
            VersionedValue exactScan,
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2 directory) {
        return captureNativeOnlyKafkaFloors(registry, exactScan).thenApply(ignored -> {
            var value = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(exactScan.canonicalStoredBytes());
            requireAuditDomain(value, registry);
            if (!directory.binding().equals(bindingIdentity)) {
                throw new IllegalArgumentException("audit floor Binding differs");
            }
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position minimum = null;
            for (var hold : value.auditDomain().orElseThrow().holds()) {
                if (hold.coverage().chain().stream()
                                .filter(held -> hold.coverage().ranges().stream()
                                        .anyMatch(range -> range.ledgerId() == held.ledgerId()))
                                .anyMatch(held -> directory.chain().stream()
                                        .noneMatch(actual -> actual.ledgerId() == held.ledgerId()
                                                && !actual.active()
                                                && !held.active()
                                                && actual.entryEndExclusive() == held.entryEndExclusive()))
                        || hold.coverage().ranges().stream().anyMatch(wanted -> directory.ranges().stream()
                                .noneMatch(actual -> actual.ledgerId() == wanted.ledgerId()
                                        && actual.startEntryInclusive() <= wanted.startEntryInclusive()
                                        && actual.endEntryExclusive() >= wanted.endEntryExclusive()))) {
                    throw new IllegalStateException("audit hold coverage is outside the current observed directory");
                }
                var first = hold.coverage().ranges().get(0);
                var position = new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                        first.ledgerId(), first.startEntryInclusive());
                if (minimum == null || directory.compare(position, minimum) < 0) {
                    minimum = position;
                }
            }
            var kind = FloorClassV1.AUDIT_GRACE;
            var fact = new AuthorityFactV1(
                    exactScan.key(), exactScan.metadataVersion(), exactScan.canonicalStoredSha256());
            var row = new RetentionFloorObservationV1(
                    kind,
                    fact,
                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain
                            .PULSAR_ENTRY,
                    -1,
                    minimum != null,
                    true,
                    Optional.ofNullable(minimum));
            return new FloorAdapterResultV1(
                    kind, 1, exactScan.canonicalStoredBytes().length(), List.of(row));
        });
    }

    public BindingIdentity bindingIdentity() {
        return bindingIdentity;
    }

    MetadataNamespaceIdentityV2 metadataNamespace() {
        return metadataNamespace;
    }

    String authorityRoot() {
        return authorityRoot;
    }

    PulsarNativeOffloadAttemptStoreV2 nativeOffloadAttempts() {
        return new PulsarNativeOffloadAttemptStoreV2(
                new Oxia09ExactMetadataTransactionStoreV1(client),
                authorityRoot,
                bindingIdentity.bindingId().digest());
    }

    CompletableFuture<com.nereusstream.metadata.oxia.v2.retention.OxiaCellProviderScopeRegistryV2> providerScopes(
            com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2 backend) {
        return revalidate()
                .thenCompose(ignored -> OxiaPhysicalMetadataNamespaceV2.connect(client, metadataNamespace))
                .thenCompose(namespace -> namespace.openCellProviderScopes(backend))
                .toCompletableFuture();
    }

    /** Reopens the original assigned Object quota. Task admission cannot initialize or expand its capacity. */
    CompletableFuture<com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2> materializationQuota(
            com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2 backend) {
        return revalidate()
                .thenCompose(ignored -> OxiaPhysicalMetadataNamespaceV2.connect(client, metadataNamespace))
                .thenCompose(namespace -> namespace.openAuthorityRoute(
                        backend,
                        new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(client)))
                .thenCompose(route -> route.quota().snapshot().thenApply(ignored -> route))
                .toCompletableFuture();
    }

    /** Uses the original P1 aggregate key and Oxia connection; this grants no floor qualification. */
    public com.nereusstream.storage.object.retention.M5PulsarLogicalTrimCoordinatorV2 logicalTrimCoordinator() {
        var key =
                new com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys(authorityRoot).aggregateKey(incarnation);
        return new com.nereusstream.storage.object.retention.M5PulsarLogicalTrimCoordinatorV2(
                new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(client), key);
    }

    public PulsarNativeM4ControlV1 m4Control() {
        return new PulsarNativeM4ControlV1(client, metadataNamespace, this);
    }

    public PulsarActiveTopicAuthority authority() {
        return authority;
    }

    /** One actual namespace/full-Cell allocator; constructing it performs no SDK source or metadata mutation. */
    public PulsarNativeReadCellBudgetV1 readCellBudget(
            PulsarNativeReadCellBudgetV1.Usage limit,
            Map<BindingIdentity, PulsarNativeReadCellBudgetV1.Usage> shares,
            PulsarNativeReadCellBudgetV1.ReadLimits limits) {
        return new PulsarNativeReadCellBudgetV1(client, this, limit, shares, limits);
    }

    String managedLedgerName() {
        return incarnation.persistenceName().value().value();
    }

    void requireStoredMetadataBinding(org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo info) {
        var properties = info.getPropertiesList().stream()
                .collect(java.util.stream.Collectors.toMap(
                        org.apache.bookkeeper.mledger.proto.KeyValue::getKey,
                        org.apache.bookkeeper.mledger.proto.KeyValue::getValue));
        if (!properties.entrySet().containsAll(nativeProperties.entrySet())) {
            throw new IllegalArgumentException("stored Native metadata differs from its actual P1 Binding");
        }
    }

    /** Actual private P1/Cell/namespace route for Native data/cursor birth admission. */
    public CompletableFuture<PulsarNativeBookKeeperCreateV1> openGuardedBookKeeper(
            String metadataServiceUri,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            PulsarNativeReadCellBudgetV1 budget,
            long capacityBytes,
            java.util.concurrent.Executor controlExecutor) {
        return PulsarNativeBookKeeperCreateV1.open(
                client, this, metadataServiceUri, bootstrap, budget, capacityBytes, controlExecutor);
    }

    CompletableFuture<PulsarNativeBookKeeperCreateV1> openFactoryBookKeeper(
            String metadataServiceUri,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            PulsarNativeReadCellBudgetV1 budget,
            long capacityBytes,
            java.util.concurrent.Executor executor) {
        return openFactoryBookKeeper(metadataServiceUri, bootstrap, budget, capacityBytes, executor, Optional.empty());
    }

    CompletableFuture<PulsarNativeBookKeeperCreateV1> openFactoryBookKeeper(
            String metadataServiceUri,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            PulsarNativeReadCellBudgetV1 budget,
            long capacityBytes,
            java.util.concurrent.Executor executor,
            Optional<com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1.BookKeeperGcGraceV1> gcGrace) {
        return PulsarNativeBookKeeperCreateV1.open(
                client,
                this,
                metadataServiceUri,
                bootstrap,
                budget,
                capacityBytes,
                executor,
                Optional.of(java.util.UUID.randomUUID()),
                gcGrace);
    }

    /** Adds exact identity at native creation. Recovery never uses this method to retag an existing ledger. */
    public void configureCreate(ManagedLedgerConfig config) {
        Objects.requireNonNull(config, "config");
        var properties = new HashMap<String, String>();
        if (config.getProperties() != null) {
            properties.putAll(config.getProperties());
        }
        nativeProperties.forEach((key, expected) -> {
            var previous = properties.putIfAbsent(key, expected);
            if (previous != null && !previous.equals(expected)) {
                throw new IllegalArgumentException("native creation properties belong to another Binding");
            }
        });
        config.setProperties(Map.copyOf(properties));
    }

    /** Rechecks the permanent metadata marker and exact ACTIVE generation; no local status substitutes for IO. */
    public CompletionStage<Void> revalidate() {
        return read(client, metadataNamespace, authorityRoot, incarnation).thenApply(current -> {
            if (!authority.selector().equals(current.authority.selector())
                    || !authority
                            .aggregate()
                            .canonicalStoredBytes()
                            .equals(current.authority.aggregate().canonicalStoredBytes())
                    || !authority
                            .aggregate()
                            .aggregateSourceVersion()
                            .equals(current.authority.aggregate().aggregateSourceVersion())) {
                throw new IllegalStateException("native P1 Binding authority changed");
            }
            return null;
        });
    }

    /** One low-frequency exact source cut; ordinary reads still require their M4 owner/admission runtime. */
    public CompletableFuture<CapturedSource> capture(
            ManagedLedgerImpl ledger,
            long ledgerId,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace) {
        return capture(ledger, ledgerId, bookKeeper, offloader, objectNamespace, false);
    }

    private CompletableFuture<CapturedSource> capture(
            ManagedLedgerImpl ledger,
            long ledgerId,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            boolean objectOnly) {
        return capture(ledger, ledgerId, bookKeeper, offloader, objectNamespace, objectOnly, null);
    }

    private CompletableFuture<CapturedSource> capture(
            ManagedLedgerImpl ledger,
            long ledgerId,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            boolean objectOnly,
            PulsarNativeM4SourceMapV1.PublishedObject published) {
        try {
            Objects.requireNonNull(ledger, "ledger");
            Objects.requireNonNull(offloader, "offloader");
            Objects.requireNonNull(bookKeeper, "bookKeeper");
            return revalidate()
                    .thenCompose(ignored -> published != null
                            ? ledger.captureOffloadObjectReadSource(ledgerId, published.identity())
                            : objectOnly
                                    ? ledger.captureOffloadObjectReadSource(ledgerId)
                                    : ledger.captureOffloadReadSource(ledgerId))
                    .thenCompose(source -> {
                        requireNativeBinding(source, bookKeeper, offloader);
                        if (published != null
                                && (source.lastAddConfirmed() != published.lastEntry()
                                        || source.length() != published.logicalLength()
                                        || !source.driverMetadata().equals(published.driverMetadata()))) {
                            throw new IllegalStateException(
                                    "published Object extent/configuration differs from Native source");
                        }
                        if (objectOnly) {
                            return source.verifyStoredMetadata()
                                    .thenCompose(ignored -> observeBookKeeperNamespace(bookKeeper))
                                    .thenCompose(
                                            bk -> captureOpenedFacts(source).thenCompose(facts -> offloader
                                                    .observePhysicalNamespace(objectNamespace, metadataNamespace)
                                                    .thenCompose(physical -> revalidate()
                                                            .thenCompose(
                                                                    ignored -> observeBookKeeperNamespace(bookKeeper))
                                                            .thenCompose(current -> source.verifyStoredMetadata()
                                                                    .thenApply(ignored -> {
                                                                        requireNativeBinding(
                                                                                source, bookKeeper, offloader);
                                                                        if (!bk.equals(current)) {
                                                                            throw new IllegalStateException(
                                                                                    "native BK namespace changed");
                                                                        }
                                                                        return new CapturedSource(
                                                                                this,
                                                                                source,
                                                                                facts.identity(),
                                                                                facts.memory(),
                                                                                physical,
                                                                                null,
                                                                                bk);
                                                                    })))));
                        }
                        return source.verifyStoredMetadata()
                                .thenCompose(ignored -> observeBookKeeper(source, bookKeeper))
                                .thenCompose(bk -> captureOpenedFacts(source).thenCompose(facts -> offloader
                                        .observePhysicalNamespace(objectNamespace, metadataNamespace)
                                        .thenCompose(physical -> revalidate()
                                                .thenCompose(ignored -> observeBookKeeper(source, bookKeeper))
                                                .thenCompose(current -> source.verifyStoredMetadata()
                                                        .thenApply(ignored -> {
                                                            requireNativeBinding(source, bookKeeper, offloader);
                                                            if (!bk.equals(current)) {
                                                                throw new IllegalStateException(
                                                                        "native BookKeeper source changed "
                                                                                + "during capture");
                                                            }
                                                            return new CapturedSource(
                                                                    this,
                                                                    source,
                                                                    facts.identity(),
                                                                    facts.memory(),
                                                                    physical,
                                                                    bk);
                                                        })))));
                    })
                    .toCompletableFuture()
                    .copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private record OpenedFacts(OffloadedReadSourceIdentity identity, OffloadedReadMemoryBounds memory) {}

    private static CompletableFuture<OpenedFacts> captureOpenedFacts(CapturedOffloadReadSource source) {
        return source.captureOffloadedReadSourceIdentity()
                .thenCompose(identity -> source.captureOffloadedReadMemoryBounds()
                        .thenApply(memory -> new OpenedFacts(identity, memory)));
    }

    private CompletableFuture<BookKeeperSource> observeBookKeeper(
            CapturedOffloadReadSource source, M5BookKeeperNamespaceAuthorityV2 namespace) {
        return observeBookKeeper(
                source.ledgerId(),
                source.lastAddConfirmed(),
                source.length(),
                false,
                () -> source.isCurrent() && source.usesBookKeeper(namespace.borrowClient()),
                namespace);
    }

    private CompletableFuture<PhysicalNamespaceAuthorityBindingV2> observeBookKeeperNamespace(
            M5BookKeeperNamespaceAuthorityV2 namespace) {
        return namespace
                .readBinding()
                .thenApply(observed -> {
                    var current = observed.orElseThrow(
                            () -> new IllegalStateException("native BookKeeper namespace is unbound"));
                    if (!current.metadataNamespace().equals(metadataNamespace)) {
                        throw new IllegalStateException(
                                "native BookKeeper namespace belongs to another metadata authority");
                    }
                    return current;
                })
                .toCompletableFuture();
    }

    private CompletableFuture<BookKeeperSource> observeBookKeeper(
            long ledgerId,
            long lastEntry,
            long sealedLength,
            boolean active,
            BooleanSupplier sourceCurrent,
            M5BookKeeperNamespaceAuthorityV2 namespace) {
        try {
            if (!sourceCurrent.getAsBoolean()) {
                throw new IllegalStateException("native source uses another BookKeeper client");
            }
            return namespace
                    .readBinding()
                    .thenCompose(observed -> {
                        var binding = observed.orElseThrow(
                                () -> new IllegalStateException("native BookKeeper namespace is unbound"));
                        if (!binding.physicalNamespace().equals(namespace.namespace())
                                || !binding.metadataNamespace().equals(metadataNamespace)) {
                            throw new IllegalStateException(
                                    "native BookKeeper namespace belongs to another metadata authority");
                        }
                        return namespace
                                .borrowClient()
                                .getLedgerManager()
                                .readLedgerMetadata(ledgerId)
                                .thenApply(stored -> {
                                    var metadata = stored.getValue();
                                    if (!sourceCurrent.getAsBoolean()
                                            || !(stored.getVersion() instanceof LongVersion version)
                                            || metadata.isClosed() == active
                                            || (active
                                                    && metadata.getState()
                                                            != org.apache.bookkeeper.client.api.LedgerMetadata.State
                                                                    .OPEN)
                                            || (!active
                                                    && (metadata.getLastEntryId() != lastEntry
                                                            || metadata.getLength() != sealedLength))
                                            || !Arrays.equals(
                                                    metadata.getCustomMetadata()
                                                            .get(LedgerMetadataUtils.METADATA_PROPERTY_APPLICATION),
                                                    LedgerMetadataUtils.METADATA_PROPERTY_APPLICATION_PULSAR)
                                            || !Arrays.equals(
                                                    metadata.getCustomMetadata()
                                                            .get(LedgerMetadataUtils.METADATA_PROPERTY_COMPONENT),
                                                    LedgerMetadataUtils.METADATA_PROPERTY_COMPONENT_MANAGED_LEDGER)
                                            || metadata.getCustomMetadata()
                                                    .containsKey(LedgerMetadataUtils.METADATA_PROPERTY_CURSOR_NAME)
                                            || !Arrays.equals(
                                                    metadata.getCustomMetadata()
                                                            .get(
                                                                    LedgerMetadataUtils
                                                                            .METADATA_PROPERTY_MANAGED_LEDGER_NAME),
                                                    incarnation
                                                            .persistenceName()
                                                            .value()
                                                            .value()
                                                            .getBytes(StandardCharsets.UTF_8))) {
                                        throw new IllegalStateException(
                                                "native BookKeeper metadata differs from sealed source");
                                    }
                                    return new BookKeeperSource(
                                            binding,
                                            new PhysicalResourceIdV2.BookKeeperLedger(
                                                    binding.physicalNamespace(), ledgerId),
                                            version.getLongVersion());
                                })
                                .thenCompose(
                                        candidate -> namespace.readBinding().thenApply(current -> {
                                            if (current.filter(candidate.binding()::equals)
                                                            .isEmpty()
                                                    || !sourceCurrent.getAsBoolean()) {
                                                throw new IllegalStateException("native BookKeeper namespace changed "
                                                        + "during source observation");
                                            }
                                            return candidate;
                                        }));
                    })
                    .toCompletableFuture()
                    .copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** Captures every current Native data ledger and its confirmed tail; qualification still requires M4. */
    public CompletableFuture<PulsarNativeBindingReadViewV1> captureReadView(
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace) {
        return captureReadView(ledger, bookKeeper, offloader, objectNamespace, false);
    }

    /** Qualified preparation reserves its source cut before Object opens; it supplies no read-owner grant. */
    public CompletableFuture<PulsarNativeSourcePreparationV1> prepareReadSources(
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            PulsarNativeReadCellBudgetV1 budget,
            boolean preferredOnly) {
        return prepareReadSources(ledger, bookKeeper, offloader, objectNamespace, budget, preferredOnly, null);
    }

    CompletableFuture<PulsarNativeSourcePreparationV1> prepareReadSources(
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            PulsarNativeReadCellBudgetV1 budget,
            boolean preferredOnly,
            PulsarNativeM4SourceMapV1 published) {
        try {
            Objects.requireNonNull(budget, "admitted source preparation budget");
            return revalidate()
                    .thenCompose(ignored -> ledger.captureNativeReadView())
                    .thenApply(scope -> {
                        requireNativeBinding(scope, bookKeeper, offloader);
                        return new PulsarNativeSourcePreparationV1(
                                this,
                                scope,
                                ledger,
                                bookKeeper,
                                offloader,
                                objectNamespace,
                                budget,
                                preferredOnly,
                                published);
                    })
                    .toCompletableFuture()
                    .copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** Captures a successor with no offloaded BK source; it grants no selector transition or delete authority. */
    public CompletableFuture<PulsarNativeBindingReadViewV1> capturePreferredOnlyReadView(
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace) {
        return captureReadView(ledger, bookKeeper, offloader, objectNamespace, true);
    }

    CompletableFuture<PulsarNativeBindingReadViewV1> captureMappedPreferredView(
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            PulsarNativeM4SourceMapV1 published,
            PulsarNativeM4ControlV1.SelectedMaterialization selected) {
        return revalidate()
                .thenCompose(ignored -> observeBookKeeperNamespace(bookKeeper))
                .thenCompose(bk -> offloader
                        .observePhysicalNamespace(objectNamespace, metadataNamespace)
                        .thenCompose(objects -> {
                            var sources = published.publishedSources(this, bk, objects);
                            return captureReadView(
                                            ledger,
                                            bookKeeper,
                                            offloader,
                                            objectNamespace,
                                            true,
                                            sources.objects(),
                                            selected)
                                    .thenApply(view -> {
                                        view.requirePublishedPlainCoverage(sources);
                                        return view;
                                    });
                        }))
                .toCompletableFuture();
    }

    private CompletableFuture<PulsarNativeBindingReadViewV1> captureReadView(
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            boolean objectOnly) {
        return captureReadView(ledger, bookKeeper, offloader, objectNamespace, objectOnly, null);
    }

    CompletableFuture<PulsarNativeBindingReadViewV1> capturePublishedPreferredView(
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            PulsarNativeM4SourceMapV1 published) {
        try {
            return revalidate()
                    .thenCompose(ignored -> observeBookKeeperNamespace(bookKeeper))
                    .thenCompose(bk -> offloader
                            .observePhysicalNamespace(objectNamespace, metadataNamespace)
                            .thenCompose(objects -> {
                                var sources = published.publishedSources(this, bk, objects);
                                return captureReadView(
                                                ledger, bookKeeper, offloader, objectNamespace, true, sources.objects())
                                        .thenApply(candidate -> {
                                            candidate.requirePublishedPlainCoverage(sources);
                                            return candidate;
                                        });
                            }))
                    .toCompletableFuture()
                    .copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<PulsarNativeBindingReadViewV1> captureReadView(
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            boolean objectOnly,
            Map<Long, PulsarNativeM4SourceMapV1.PublishedObject> published) {
        return captureReadView(ledger, bookKeeper, offloader, objectNamespace, objectOnly, published, null);
    }

    private CompletableFuture<PulsarNativeBindingReadViewV1> captureReadView(
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            boolean objectOnly,
            Map<Long, PulsarNativeM4SourceMapV1.PublishedObject> published,
            PulsarNativeM4ControlV1.SelectedMaterialization selected) {
        try {
            Objects.requireNonNull(ledger, "ledger");
            Objects.requireNonNull(bookKeeper, "bookKeeper");
            Objects.requireNonNull(offloader, "offloader");
            return revalidate()
                    .thenCompose(ignored -> ledger.captureNativeReadView())
                    .thenCompose(nativeView -> {
                        requireNativeBinding(nativeView, bookKeeper, offloader);
                        if (published != null && !nativeView.ledgerIds().containsAll(published.keySet())) {
                            throw new IllegalStateException("Native cold cut dropped a published Object source");
                        }
                        var slots = new ArrayList<PulsarNativeBindingReadViewV1.Slot>();
                        if (nativeView.ledgerIds().size() > 512) {
                            throw new IllegalStateException("Native source cut exceeds its bounded source-map count");
                        }
                        var headers = new HashMap<Long, BookKeeperSource>();
                        CompletionStage<Void> capture = CompletableFuture.completedFuture(null);
                        // Validate the entire Native/BK cut before any offloaded source can open Object data.
                        for (long id : nativeView.ledgerIds()) {
                            if (selected != null && selected.readMap().fullyReplaces(id, nativeView.lastEntry(id))) {
                                continue;
                            }
                            if (objectOnly && nativeView.offloadComplete(id)) {
                                continue;
                            }
                            capture = capture.thenCompose(ignored -> observeBookKeeper(
                                            id,
                                            nativeView.lastEntry(id),
                                            nativeView.activeTail(id) ? 0 : nativeView.sealedLength(id),
                                            nativeView.activeTail(id),
                                            () -> nativeView.isCurrent()
                                                    && nativeView.usesBookKeeper(bookKeeper.borrowClient()),
                                            bookKeeper)
                                    .thenAccept(header -> headers.put(id, header)));
                        }
                        for (long id : nativeView.ledgerIds()) {
                            if (selected != null && selected.readMap().fullyReplaces(id, nativeView.lastEntry(id))) {
                                continue;
                            }
                            capture = capture.thenCompose(ignored -> {
                                requireNativeBinding(nativeView, bookKeeper, offloader);
                                if (nativeView.offloadComplete(id)) {
                                    if (published != null && !published.containsKey(id)) {
                                        throw new IllegalStateException(
                                                "Native offloaded source is not in the published map");
                                    }
                                    return capture(
                                                    ledger,
                                                    id,
                                                    bookKeeper,
                                                    offloader,
                                                    objectNamespace,
                                                    objectOnly,
                                                    published == null ? null : published.get(id))
                                            .thenAccept(source -> {
                                                if (source.nativeSource()
                                                                        .sourceState()
                                                                        .metadataVersion()
                                                                != nativeView.metadataVersion()
                                                        || !Arrays.equals(
                                                                nativeView.ledgerInfoBytes(id),
                                                                source.nativeSource()
                                                                        .sealedLedgerInfoBytes())) {
                                                    throw new IllegalStateException(
                                                            "native source differs from Binding read cut");
                                                }
                                                var header = headers.get(id);
                                                if (!objectOnly
                                                        && (source.bookKeeperMetadataVersion()
                                                                        != header.metadataVersion()
                                                                || !source.bookKeeperBinding()
                                                                        .equals(header.binding()))) {
                                                    throw new IllegalStateException("native Binding BK header changed");
                                                }
                                                slots.add(PulsarNativeBindingReadViewV1.Slot.offloaded(source));
                                            });
                                }
                                return nativeView.captureBookKeeperSource(id).thenCompose(source -> observeBookKeeper(
                                                id,
                                                source.lastAddConfirmed(),
                                                source.activeTail() ? 0 : source.sealedLength(),
                                                source.activeTail(),
                                                () -> nativeView.isCurrent()
                                                        && nativeView.usesBookKeeper(bookKeeper.borrowClient()),
                                                bookKeeper)
                                        .thenAccept(bk -> {
                                            if (!bk.equals(headers.get(id))) {
                                                throw new IllegalStateException("native Binding BK header changed");
                                            }
                                            slots.add(PulsarNativeBindingReadViewV1.Slot.bookKeeper(
                                                    source, bk.binding(), bk.member(), bk.metadataVersion()));
                                        }));
                            });
                        }
                        return capture.thenCompose(ignored ->
                                        offloader.observePhysicalNamespace(objectNamespace, metadataNamespace))
                                .thenCompose(physical -> revalidate().thenCompose(ignored -> {
                                    CompletionStage<Void> recheck = CompletableFuture.completedFuture(null);
                                    for (var slot : slots) {
                                        if (slot.offloaded() != null
                                                && slot.offloaded().preferredOnly()) {
                                            recheck = recheck.thenCompose(
                                                    nothing -> observeBookKeeperNamespace(bookKeeper)
                                                            .thenAccept(current -> {
                                                                if (!current.equals(slot.bookKeeperBinding())) {
                                                                    throw new IllegalStateException(
                                                                            "native BK namespace changed");
                                                                }
                                                            }));
                                            continue;
                                        }
                                        recheck = recheck.thenCompose(nothing -> observeBookKeeper(
                                                        slot.ledgerId(),
                                                        slot.lastEntry(),
                                                        slot.activeTail() ? 0 : slot.sealedLength(),
                                                        slot.activeTail(),
                                                        () -> nativeView.isCurrent()
                                                                && nativeView.usesBookKeeper(bookKeeper.borrowClient()),
                                                        bookKeeper)
                                                .thenAccept(current -> {
                                                    if (!current.binding().equals(slot.bookKeeperBinding())
                                                            || current.metadataVersion()
                                                                    != slot.bookKeeperVersion()
                                                                            .orElseThrow()
                                                            || !current.member()
                                                                    .equals(slot.bookKeeperMember()
                                                                            .orElseThrow())) {
                                                        throw new IllegalStateException(
                                                                "native Binding BK source changed " + "during capture");
                                                    }
                                                }));
                                    }
                                    return recheck.thenCompose(nothing -> observeBookKeeperNamespace(bookKeeper))
                                            .thenCompose(actualBookKeeper -> nativeView
                                                    .verifyStoredMetadata()
                                                    .thenApply(nothing -> {
                                                        requireNativeBinding(nativeView, bookKeeper, offloader);
                                                        if (selected != null) {
                                                            return PulsarNativeBindingReadViewV1.fromMapped(
                                                                    this,
                                                                    nativeView,
                                                                    physical,
                                                                    actualBookKeeper,
                                                                    slots,
                                                                    bookKeeper
                                                                            .capability()
                                                                            .clientFrameLimitBytes(),
                                                                    selected);
                                                        }
                                                        return PulsarNativeBindingReadViewV1.fromCaptured(
                                                                this,
                                                                nativeView,
                                                                physical,
                                                                actualBookKeeper,
                                                                slots,
                                                                bookKeeper
                                                                        .capability()
                                                                        .clientFrameLimitBytes());
                                                    }));
                                }));
                    })
                    .toCompletableFuture()
                    .copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    CompletableFuture<Void> verifyMaterializationView(
            PulsarNativeBindingReadViewV1 view,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader) {
        if (!view.bindingIdentity().equals(bindingIdentity) || !view.isCurrent() || !view.usesBookKeeper(bookKeeper)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Native materialization owner/source differs"));
        }
        // Snapshot local directory before starting P1 I/O; a stale local view must not orphan an accepted GET.
        var directory = view.materializationDirectory();
        CompletionStage<Void> verification = revalidate();
        for (var slot : directory) {
            verification = verification.thenCompose(ignored -> slot.bookKeeperMember()
                            .isEmpty()
                    ? observeBookKeeperNamespace(bookKeeper).thenAccept(actual -> {
                        if (!actual.equals(slot.bookKeeperBinding())) {
                            throw new IllegalStateException("Native materialization BK namespace changed");
                        }
                    })
                    : observeBookKeeper(
                                    slot.ledgerId(),
                                    slot.lastEntry(),
                                    slot.activeTail() ? 0 : slot.sealedLength(),
                                    slot.activeTail(),
                                    () -> view.isCurrent() && view.usesBookKeeper(bookKeeper),
                                    bookKeeper)
                            .thenAccept(actual -> {
                                if (!actual.binding().equals(slot.bookKeeperBinding())
                                        || actual.metadataVersion()
                                                != slot.bookKeeperVersion().orElseThrow()
                                        || !actual.member()
                                                .equals(slot.bookKeeperMember().orElseThrow())) {
                                    throw new IllegalStateException(
                                            "Native materialization BK source identity changed");
                                }
                            }));
        }
        return verification
                .thenCompose(ignored -> offloader.observePhysicalNamespace(view.objectNamespace(), metadataNamespace))
                .thenCompose(actual -> revalidate().thenRun(() -> {
                    if (!actual.equals(view.objectBinding()) || !view.isCurrent()) {
                        throw new IllegalStateException("Native materialization source changed during revalidation");
                    }
                }))
                .toCompletableFuture();
    }

    private void requireNativeBinding(
            CapturedNativeReadView source,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader) {
        if (!source.isCurrent()
                || !source.usesBookKeeper(bookKeeper.borrowClient())
                || !source.usesOffloader(offloader)
                || !incarnation.persistenceName().value().value().equals(source.managedLedgerName())
                || !source.managedLedgerProperties().entrySet().containsAll(nativeProperties.entrySet())) {
            throw new IllegalStateException("native read view lacks its exact persisted P1 Binding");
        }
    }

    private void requireNativeBinding(
            CapturedOffloadReadSource source,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader) {
        if (!source.isCurrent()
                || !source.usesBookKeeper(bookKeeper.borrowClient())
                || !source.usesOffloader(offloader)
                || !incarnation
                        .persistenceName()
                        .value()
                        .value()
                        .equals(source.driverMetadata().get("ManagedLedgerName"))
                || !source.managedLedgerProperties().entrySet().containsAll(nativeProperties.entrySet())) {
            throw new IllegalStateException("native source lacks its exact persisted P1 Binding");
        }
    }

    private static PhysicalResourceIdV2.ObjectVersion objectVersion(
            Namespace namespace, OffloadedObjectReadIdentity object) {
        String token = object.version();
        if (!token.startsWith("version:")
                || token.length() == 8
                || token.substring(8).equals("null")) {
            throw new IllegalStateException("native M5 Object source requires an immutable version token");
        }
        return new PhysicalResourceIdV2.ObjectVersion(
                namespace,
                CanonicalUtf8.fromString(object.key()),
                ObjectIdentityKind.IMMUTABLE_VERSION,
                CanonicalUtf8.fromString(token.substring(8)));
    }
}
