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

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.aggregate.FrameEncodingPolicyValueV1;
import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.PolicyCatalogDigest;
import com.nereusstream.domain.aggregate.ProfileOriginV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.aggregate.TopicBindingV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.codec.Nta1CodecV1;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5BindingRetirementCoordinatorV1;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1.WriterDeclarationV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.ExternalMutationOutcomeV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.ExternalMutationResultV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.GuardOutcomeV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.GuardedMutationRequestV1;
import com.nereusstream.storage.object.retention.M5RetentionCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BatchMetadataStateV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.M4ReleaseBindingV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.PhysicalCleanupSummaryV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceScanSummaryV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredTopicIncarnationTombstoneV1;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Real source-locked Oxia boundary for the amended M5-C one-key authority protocol. */
class M5RetentionOxiaIntegrationTest {
    private static final CapabilityBinding CAPABILITY = new CapabilityBinding(7, digest("m5-capability"));
    private static final BindingIdentity BINDING = new BindingIdentity(
            new TopicBindingId(digest("m5-binding")), digest("m5-incarnation"), digest("m5-storage-epoch"));
    private static final IdentityEnvelope IDENTITY =
            new IdentityEnvelope(digest("m5-cell"), digest("m5-provider"), BINDING, 11, 13, 17, CAPABILITY);
    private static final ProtocolCoverage COVERAGE = new ProtocolCoverage(PositionDomain.KAFKA_OFFSET, 0, 1_000);
    private static final Sha256Digest PLACEHOLDER = digest("m5-placeholder");

    @Test
    void bindingAuthorityMigratesTicketsFencesRetiresAndSurvivesRestart() throws Exception {
        String root = "/nereus/v2/m5/retention/binding-" + UUID.randomUUID();
        String authorityKey = root + "/selector";
        SourceRetirementBatch batch = batch();
        BindingReadSelector legacySelector = selector(List.of(batch));
        VersionedValue fenced;
        CanonicalBytes retiredBytes;

        try (AsyncOxiaClient client = connect()) {
            Oxia09ExactMetadataTransactionStoreV1 store = new Oxia09ExactMetadataTransactionStoreV1(client);
            VersionedValue legacy = create(store, authorityKey, M4ReadControlCodecV1.encodeSelector(legacySelector));
            List<M4ReleaseBindingV1> releases = releases(store, root, batch);
            M5ClosedWriterRegistryV1 registry = registry();
            M5BindingRetirementCoordinatorV1 coordinator = new M5BindingRetirementCoordinatorV1(store);

            assertThat(coordinator
                            .migrateLegacy(new M5BindingRetirementCoordinatorV1.MigrationRequest(authorityKey, legacy))
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
            VersionedValue open = read(store, authorityKey);
            assertThat(M5BindingAuthorityCodecV1.projectSelector(open.canonicalStoredBytes()))
                    .isEqualTo(legacySelector);
            assertThat(coordinator
                            .enrollWriters(new M5BindingRetirementCoordinatorV1.EnrollmentRequest(
                                    authorityKey, open, registry.enrollment()))
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);

            M5ReferenceMutationGuardV1 guard = M5ReferenceMutationGuardV1.forBindingBatch(
                    registry, coordinator, authorityKey, batch.batchIdSha256());
            Sha256Digest externalRoot = digest("m5-binding-external-root");
            var guarded = guard.execute(
                            new GuardedMutationRequestV1(
                                    registry.writerFor(ReferenceKindV1.MANIFEST_SELECTED),
                                    ReferenceTargetKindV1.RETIREMENT_BATCH,
                                    batch.batchIdSha256(),
                                    digest("m5-binding-operation"),
                                    externalRoot),
                            () -> coordinator.read(authorityKey).thenApply(exact -> {
                                var ticketed = M5BindingAuthorityCodecV1.decodeAuthority(
                                        exact.orElseThrow().canonicalStoredBytes());
                                assertThat(ticketed.referenceMutationTickets()).hasSize(1);
                                return new ExternalMutationResultV1(
                                        ExternalMutationOutcomeV1.APPLIED_EXACT, externalRoot);
                            }))
                    .toCompletableFuture()
                    .join();
            assertThat(guarded.outcome()).isEqualTo(GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED);

            open = read(store, authorityKey);
            assertThat(coordinator
                            .fence(new M5BindingRetirementCoordinatorV1.FenceRequest(
                                    authorityKey, open, batch.batchIdSha256(), digest("m5-binding-fence"), releases))
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
            fenced = read(store, authorityKey);
            ReferenceFreeProofV1 proof = proof(
                    store,
                    root + "/proof",
                    ReferenceTargetKindV1.RETIREMENT_BATCH,
                    batch.batchIdSha256(),
                    fenced,
                    releases,
                    IDENTITY);

            var retirement = new M5BindingRetirementCoordinatorV1.RetirementRequest(authorityKey, fenced, proof);
            assertThat(coordinator.retire(retirement).toCompletableFuture().join())
                    .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
            assertThat(coordinator.retire(retirement).toCompletableFuture().join())
                    .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.EXISTING_EXACT);
            VersionedValue retired = read(store, authorityKey);
            var authority = M5BindingAuthorityCodecV1.decodeAuthority(retired.canonicalStoredBytes());
            assertThat(authority.state()).isEqualTo(BindingAuthorityStateV1.OPEN_V1);
            assertThat(authority.selectorProjection().activeBatches()).isEmpty();
            assertThat(authority.batchSlots()).singleElement().satisfies(slot -> assertThat(slot.state())
                    .isEqualTo(BatchMetadataStateV1.RETIRED_V1));
            assertThat(store.supportsAtomicMultiKeyTransactions()).isFalse();
            retiredBytes = retired.canonicalStoredBytes();
        }

        try (AsyncOxiaClient restarted = connect()) {
            Oxia09ExactMetadataTransactionStoreV1 store = new Oxia09ExactMetadataTransactionStoreV1(restarted);
            assertThat(read(store, authorityKey).canonicalStoredBytes()).isEqualTo(retiredBytes);
            assertThat(store.compareAndSet(Optional.of(fenced), authorityKey, bytes("stale-binding-candidate"))
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(MutationOutcome.DEFINITIVE_CONFLICT);
        }
    }

    @Test
    void pulsarAuthorityMigratesTicketsFencesRetiresAndSurvivesRestart() throws Exception {
        String root = "/nereus/v2/m5/retention/pulsar-" + UUID.randomUUID();
        String aggregateKey = root + "/aggregate";
        String selectorKey = root + "/selector";
        PulsarProtocolCellIdentity cell = new PulsarProtocolCellIdentity(
                new DeploymentId(new Id128(1, 2)),
                new ReservationDomainId(new Id128(3, 4)),
                new PulsarCellId(new Id128(5, 6)));
        PulsarTopicIncarnationIdentity incarnation = new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString("tenant/ns/persistent/m5-orders"),
                PulsarTopicName.fromString("persistent://tenant/ns/m5-orders"),
                new PulsarBindingGeneration(7));
        TopicBindingId bindingId = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
        StorageEpochId epochId = DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 0);
        TopicBindingAggregateV1 aggregate = new TopicBindingAggregateV1(
                TopicBindingAggregateV1.SCHEMA_VERSION,
                new TopicBindingV1(ProtocolKindV1.PULSAR, bindingId, cell, incarnation),
                new InitialStorageEpochV1(
                        epochId,
                        0,
                        StorageProfileV1.BOOKKEEPER_WAL_ONLY,
                        ProfileOriginV1.TOPIC_EXPLICIT,
                        new PolicyCatalogDigest(digest("m5-pulsar-policy")),
                        FrameEncodingPolicyValueV1.none()));
        VersionedValue fenced;
        CanonicalBytes retiredBytes;

        try (AsyncOxiaClient client = connect()) {
            Oxia09ExactMetadataTransactionStoreV1 store = new Oxia09ExactMetadataTransactionStoreV1(client);
            VersionedValue legacyAggregate = create(store, aggregateKey, Nta1CodecV1.encode(aggregate));
            VersionedValue deletedSelector = create(store, selectorKey, bytes("m5-deleted-generation-seven"));
            PulsarTopicGenerationSelectorValueV1 selectorValue = new PulsarTopicGenerationSelectorValueV1(
                    incarnation.persistenceName(),
                    incarnation.bindingGeneration(),
                    PulsarTopicGenerationSelectorStateV1.DELETED,
                    bindingId,
                    legacyAggregate.canonicalStoredSha256(),
                    deletedSelector.canonicalStoredBytes(),
                    deletedSelector.canonicalStoredSha256());
            M5ClosedWriterRegistryV1 registry = registry();
            M5PulsarAggregateRetirementCoordinatorV1 coordinator = new M5PulsarAggregateRetirementCoordinatorV1(store);

            assertThat(coordinator
                            .migrateLegacy(new M5PulsarAggregateRetirementCoordinatorV1.MigrationRequest(
                                    aggregateKey, legacyAggregate, CAPABILITY))
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
            VersionedValue open = read(store, aggregateKey);
            assertThat(M5PulsarAggregateAuthorityCodecV1.projectAggregate(open.canonicalStoredBytes()))
                    .isEqualTo(legacyAggregate.canonicalStoredBytes());
            assertThat(coordinator
                            .enrollWriters(new M5PulsarAggregateRetirementCoordinatorV1.EnrollmentRequest(
                                    aggregateKey, open, registry.enrollment()))
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.APPLIED_EXACT);

            M5ReferenceMutationGuardV1 guard = M5ReferenceMutationGuardV1.forPulsarAggregate(
                    registry, coordinator, aggregateKey, legacyAggregate.canonicalStoredSha256());
            Sha256Digest externalRoot = digest("m5-pulsar-external-root");
            var guarded = guard.execute(
                            new GuardedMutationRequestV1(
                                    registry.writerFor(ReferenceKindV1.PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR),
                                    ReferenceTargetKindV1.PULSAR_AGGREGATE,
                                    legacyAggregate.canonicalStoredSha256(),
                                    digest("m5-pulsar-operation"),
                                    externalRoot),
                            () -> coordinator.read(aggregateKey).thenApply(exact -> {
                                var ticketed = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                        exact.orElseThrow().canonicalStoredBytes());
                                assertThat(ticketed.referenceMutationTickets()).hasSize(1);
                                return new ExternalMutationResultV1(
                                        ExternalMutationOutcomeV1.EXISTING_EXACT, externalRoot);
                            }))
                    .toCompletableFuture()
                    .join();
            assertThat(guarded.outcome()).isEqualTo(GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED);

            open = read(store, aggregateKey);
            assertThat(coordinator
                            .fence(new M5PulsarAggregateRetirementCoordinatorV1.FenceRequest(
                                    aggregateKey,
                                    open,
                                    selectorKey,
                                    deletedSelector,
                                    selectorValue,
                                    digest("m5-pulsar-fence")))
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
            fenced = read(store, aggregateKey);
            BindingIdentity proofBinding =
                    new BindingIdentity(bindingId, digest("m5-pulsar-incarnation"), digest("m5-pulsar-storage"));
            IdentityEnvelope proofIdentity = new IdentityEnvelope(
                    digest("m5-pulsar-cell"), digest("m5-pulsar-provider"), proofBinding, 11, 13, 17, CAPABILITY);
            ReferenceFreeProofV1 proof = proof(
                    store,
                    root + "/proof",
                    ReferenceTargetKindV1.PULSAR_AGGREGATE,
                    legacyAggregate.canonicalStoredSha256(),
                    deletedSelector,
                    List.of(),
                    proofIdentity);
            VersionedValue cleanupRoot = create(store, root + "/cleanup", bytes("m5-delete-done"));
            PhysicalCleanupSummaryV1 cleanup = new PhysicalCleanupSummaryV1(
                    fact(cleanupRoot),
                    incarnation,
                    bindingId,
                    legacyAggregate.canonicalStoredSha256(),
                    CAPABILITY,
                    2,
                    1,
                    1,
                    0);
            var retirement = new M5PulsarAggregateRetirementCoordinatorV1.RetirementRequest(
                    aggregateKey, fenced, proof, cleanup);

            assertThat(coordinator.retire(retirement).toCompletableFuture().join())
                    .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
            assertThat(coordinator.retire(retirement).toCompletableFuture().join())
                    .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.EXISTING_EXACT);
            VersionedValue retired = read(store, aggregateKey);
            RetiredTopicIncarnationTombstoneV1 tombstone =
                    M5RetentionCodecV1.decodeRetiredPulsar(retired.canonicalStoredBytes());
            assertThat(tombstone.incarnation()).isEqualTo(incarnation);
            assertThat(tombstone.originalAggregateSha256()).isEqualTo(legacyAggregate.canonicalStoredSha256());
            assertThat(read(store, selectorKey)).isEqualTo(deletedSelector);
            assertThat(store.supportsAtomicMultiKeyTransactions()).isFalse();
            retiredBytes = retired.canonicalStoredBytes();
        }

        try (AsyncOxiaClient restarted = connect()) {
            Oxia09ExactMetadataTransactionStoreV1 store = new Oxia09ExactMetadataTransactionStoreV1(restarted);
            assertThat(read(store, aggregateKey).canonicalStoredBytes()).isEqualTo(retiredBytes);
            assertThat(store.compareAndSet(Optional.of(fenced), aggregateKey, bytes("stale-pulsar-candidate"))
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(MutationOutcome.DEFINITIVE_CONFLICT);
        }
    }

    private static SourceRetirementBatch batch() {
        SourceProtectionIdentity source = new SourceProtectionIdentity(digest("m5-source"), 3, 5, 7, CAPABILITY);
        List<SourceProtectionIdentity> sources = List.of(source);
        Sha256Digest fallback = M4ReadControlCodecV1.calculateFallbackSetSha256(sources);
        SourceRetirementBatch draft = new SourceRetirementBatch(
                BINDING,
                PLACEHOLDER,
                digest("m5-predecessor-core"),
                digest("m5-successor-core"),
                digest("m5-transition"),
                fallback,
                6,
                5,
                CAPABILITY,
                sources);
        return new SourceRetirementBatch(
                BINDING,
                M4ReadControlCodecV1.calculateBatchId(draft),
                draft.predecessorSelectorCoreSha256(),
                draft.successorSelectorCoreSha256(),
                draft.transitionSha256(),
                draft.fallbackSetSha256(),
                draft.sharedLastFallbackCapableReadAdmissionEpoch(),
                draft.minimumFirstEpochSummary(),
                CAPABILITY,
                sources);
    }

    private static BindingReadSelector selector(List<SourceRetirementBatch> batches) {
        return new BindingReadSelector(
                BINDING,
                digest("m5-selected-view"),
                11,
                13,
                17,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                CAPABILITY,
                List.of(),
                batches);
    }

    private static List<M4ReleaseBindingV1> releases(
            Oxia09ExactMetadataTransactionStoreV1 store, String root, SourceRetirementBatch batch) {
        List<M4ReleaseBindingV1> releases = new ArrayList<>();
        for (int index = 0; index < batch.sources().size(); index++) {
            SourceProtectionIdentity source = batch.sources().get(index);
            Sha256Digest proofHead = digest("m5-proof-head-" + index);
            CanonicalBytes protectionBytes = M4ReadControlCodecV1.encodeProtection(new SourceProtection(
                    BINDING,
                    source,
                    ProtectionState.RELEASED,
                    Optional.of(batch.batchIdSha256()),
                    Optional.of(proofHead)));
            VersionedValue protection = create(store, root + "/protection/" + index, protectionBytes);
            releases.add(new M4ReleaseBindingV1(
                    source.sourceIdentitySha256(),
                    source.protectionGeneration(),
                    fact(protection),
                    protectionBytes,
                    batch.batchIdSha256(),
                    proofHead));
        }
        return List.copyOf(releases);
    }

    private static M5ClosedWriterRegistryV1 registry() {
        return new M5ClosedWriterRegistryV1(List.of(new WriterDeclarationV1(
                "com.nereusstream.m5.RealOxiaClosedWriter",
                CAPABILITY,
                List.of(FloorClassV1.values()),
                List.of(ReferenceKindV1.values()),
                digest("m5-real-oxia-writer-source"))));
    }

    private static ReferenceFreeProofV1 proof(
            Oxia09ExactMetadataTransactionStoreV1 store,
            String prefix,
            ReferenceTargetKindV1 targetKind,
            Sha256Digest target,
            VersionedValue selector,
            List<M4ReleaseBindingV1> releases,
            IdentityEnvelope identity) {
        AuthorityFactV1 manifest = fact(create(store, prefix + "/manifest", bytes("manifest")));
        AuthorityFactV1 trim = fact(create(store, prefix + "/trim", bytes("trim")));
        AuthorityFactV1 owner = fact(create(store, prefix + "/owner", bytes("owner")));
        AuthorityFactV1 worker = fact(create(store, prefix + "/worker", bytes("worker")));
        AuthorityFactV1 storage = fact(create(store, prefix + "/storage", bytes("storage")));
        AuthorityFactV1 provider = fact(create(store, prefix + "/provider", bytes("provider")));
        List<ReferenceObservationV1> observations = new ArrayList<>();
        List<ReferenceScanSummaryV1> summaries = new ArrayList<>();
        for (ReferenceKindV1 kind : ReferenceKindV1.values()) {
            AuthorityFactV1 authority = fact(create(
                    store,
                    prefix + "/references/" + String.format(java.util.Locale.ROOT, "%02d", kind.ordinal()),
                    bytes(kind.name())));
            observations.add(
                    new ReferenceObservationV1(kind, authority, target, COVERAGE, ReferenceDispositionV1.ABSENT, true));
            summaries.add(new ReferenceScanSummaryV1(kind, 1, 1, 64, true));
        }
        observations.sort(Comparator.comparing(ReferenceObservationV1::kind));
        summaries.sort(Comparator.comparing(ReferenceScanSummaryV1::kind));
        ReferenceFreeProofV1 draft = new ReferenceFreeProofV1(
                identity,
                targetKind,
                target,
                COVERAGE,
                fact(selector),
                manifest,
                trim,
                digest(prefix + "/snapshot"),
                M5RetentionCodecV1.calculateObservationsRoot(observations),
                releases,
                owner,
                worker,
                storage,
                provider,
                1_000,
                1_001,
                summaries,
                observations,
                PLACEHOLDER);
        return M5RetentionCodecV1.finalizeProof(draft);
    }

    private static VersionedValue create(
            Oxia09ExactMetadataTransactionStoreV1 store, String key, CanonicalBytes value) {
        assertThat(store.compareAndSet(Optional.empty(), key, value)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        return read(store, key);
    }

    private static VersionedValue read(Oxia09ExactMetadataTransactionStoreV1 store, String key) {
        return store.read(key).toCompletableFuture().join().orElseThrow();
    }

    private static AuthorityFactV1 fact(VersionedValue value) {
        return new AuthorityFactV1(value.key(), value.metadataVersion(), value.canonicalStoredSha256());
    }

    private static AsyncOxiaClient connect() throws Exception {
        String serviceAddress = System.getProperty("nereus.m5.retention.oxia.serviceAddress");
        if (serviceAddress == null || serviceAddress.isBlank() || "UNSET".equals(serviceAddress)) {
            throw new IllegalStateException("missing source-locked M5 retention Oxia service address");
        }
        return OxiaClientBuilder.create(serviceAddress)
                .namespace("default")
                .asyncClient()
                .get(30, TimeUnit.SECONDS);
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(bytes(value));
    }
}
