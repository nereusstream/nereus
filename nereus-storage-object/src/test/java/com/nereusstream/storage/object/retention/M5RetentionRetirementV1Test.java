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

package com.nereusstream.storage.object.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.ExactPut;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.ExactTransaction;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.TransactionOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
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
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceWriterEnrollmentV1;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1.WriterDeclarationV1;
import com.nereusstream.storage.object.retention.M5LogicalTrimCoordinatorV1.Outcome;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.ExternalMutationOutcomeV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.ExternalMutationResultV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.GuardOutcomeV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.GuardedMutationRequestV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BatchMetadataStateV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FullSourceRetirementBatchV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.M4ReleaseBindingV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.PhysicalCleanupSummaryV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceScanSummaryV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredSourceRetirementBatchTombstoneV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredTopicIncarnationTombstoneV1;
import com.nereusstream.storage.object.retention.M5RetirementCoordinatorV1.BatchRetirementRequest;
import com.nereusstream.storage.object.retention.M5RetirementCoordinatorV1.ExternalizationRequest;
import com.nereusstream.storage.object.retention.M5RetirementCoordinatorV1.PulsarRetirementRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class M5RetentionRetirementV1Test {
    private static final CapabilityBinding CAPABILITY = new CapabilityBinding(7, digest("capability"));
    private static final BindingIdentity BINDING =
            new BindingIdentity(new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage-epoch"));
    private static final IdentityEnvelope IDENTITY =
            new IdentityEnvelope(digest("cell"), digest("provider-scope"), BINDING, 11, 13, 17, CAPABILITY);
    private static final ProtocolCoverage COVERAGE = new ProtocolCoverage(PositionDomain.KAFKA_OFFSET, 0, 1_000);
    private static final Sha256Digest PLACEHOLDER = digest("placeholder");

    @Test
    void floorSnapshotIsClosedCanonicalAndSelectsMinimumSafeFloor() {
        InMemoryStore metadata = new InMemoryStore(true);
        RetentionFloorSnapshotV1 snapshot = snapshot(metadata, 0, 100);

        assertThat(snapshot.minimumSafeFloor()).isEqualTo(100);
        assertThat(M5RetentionCodecV1.decodeSnapshot(M5RetentionCodecV1.encodeSnapshot(snapshot)))
                .isEqualTo(snapshot);
        assertThatThrownBy(() -> new RetentionFloorSnapshotV1(
                        snapshot.identity(),
                        snapshot.domain(),
                        snapshot.generation(),
                        snapshot.priorTrimFrontier(),
                        snapshot.retentionPolicyRootSha256(),
                        snapshot.ownerFence(),
                        snapshot.storageFence(),
                        snapshot.pageCount(),
                        snapshot.scannedBytes(),
                        snapshot.rows().subList(1, snapshot.rows().size()),
                        snapshot.snapshotRootSha256()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("omits");
    }

    @Test
    void referenceFreeProofRequiresEveryKindCompleteAndAbsent() {
        InMemoryStore metadata = new InMemoryStore(true);
        VersionedValue selector = metadata.seed("/selector", bytes("selector"));
        ReferenceFreeProofV1 proof = proof(
                metadata, "/proof-a", ReferenceTargetKindV1.PULSAR_AGGREGATE, digest("aggregate"), selector, List.of());

        assertThat(M5RetentionCodecV1.decodeReferenceFreeProof(M5RetentionCodecV1.encodeReferenceFreeProof(proof)))
                .isEqualTo(proof);
        List<ReferenceObservationV1> present = new ArrayList<>(proof.observations());
        ReferenceObservationV1 first = present.get(0);
        present.set(
                0,
                new ReferenceObservationV1(
                        first.kind(),
                        first.authority(),
                        first.targetIdentitySha256(),
                        first.coverage(),
                        ReferenceDispositionV1.PRESENT,
                        true));
        assertThatThrownBy(() -> copyProof(proof, present, proof.scanSummaries()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("present");
        assertThatThrownBy(() -> copyProof(
                        proof,
                        proof.observations().subList(1, proof.observations().size()),
                        proof.scanSummaries()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void freshnessRereadRejectsAnyChangedVersionVectorMember() {
        InMemoryStore metadata = new InMemoryStore(true);
        VersionedValue selector = metadata.seed("/selector", bytes("selector"));
        ReferenceFreeProofV1 proof = proof(
                metadata, "/proof-b", ReferenceTargetKindV1.PULSAR_AGGREGATE, digest("aggregate"), selector, List.of());
        metadata.overwrite(proof.observations().get(7).authority().key(), bytes("changed"));

        assertThatThrownBy(() -> new M5ReferenceFreshnessVerifierV1(metadata)
                        .requireFresh(proof)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(M5ReferenceFreshnessVerifierV1.StaleAuthorityException.class);
    }

    @Test
    void logicalTrimUsesMinimumAndCannotRegress() {
        InMemoryStore metadata = new InMemoryStore(true);
        RetentionFloorSnapshotV1 first = snapshot(metadata, 0, 100);
        M5LogicalTrimCoordinatorV1 coordinator = new M5LogicalTrimCoordinatorV1(metadata);

        metadata.casResponseUnknownWithoutApply = true;
        M5LogicalTrimCoordinatorV1.Result notApplied = coordinator
                .advance("/trim-frontier", first)
                .toCompletableFuture()
                .join();
        assertThat(notApplied.outcome()).isEqualTo(Outcome.DEFINITIVELY_NOT_APPLIED);
        assertThat(metadata.readOptional("/trim-frontier")).isEmpty();

        M5LogicalTrimCoordinatorV1.Result result = coordinator
                .advance("/trim-frontier", first)
                .toCompletableFuture()
                .join();
        assertThat(result.outcome()).isEqualTo(Outcome.APPLIED_EXACT);
        assertThat(result.exactFrontier().orElseThrow().newFrontier()).isEqualTo(100);
        assertThatThrownBy(() -> new M5RetentionPlannerV1().plan(snapshot(metadata, 100, 99), result.exactFrontier()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("regress");
    }

    @Test
    void singleBindingAuthorityMigrationPreservesExactM4Projection() {
        RetirementFixture fixture = retirementFixture(false);
        M5BindingRetirementCoordinatorV1 coordinator = new M5BindingRetirementCoordinatorV1(fixture.metadata);

        assertThat(coordinator
                        .migrateLegacy(new M5BindingRetirementCoordinatorV1.MigrationRequest(
                                fixture.selectorKey, fixture.exactSelector))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);

        VersionedValue stored = fixture.metadata.readNow(fixture.selectorKey);
        BindingRetirementAuthorityV1 authority =
                M5BindingAuthorityCodecV1.decodeAuthority(stored.canonicalStoredBytes());
        assertThat(M4ReadControlCodecV1.encodeSelector(authority.selectorProjection()))
                .isEqualTo(fixture.exactSelector.canonicalStoredBytes());
        assertThat(authority.batchSlots()).singleElement().satisfies(slot -> {
            assertThat(slot.state()).isEqualTo(BatchMetadataStateV1.FULL_V1);
            assertThat(slot.fullBatch()).isEqualTo(fixture.batch);
        });
        assertThat(authority.authorityGeneration()).isEqualTo(1);
        assertThat(authority.predecessorValueSha256()).contains(fixture.exactSelector.canonicalStoredSha256());
        assertThat(authority.writerEnrollment()).isEmpty();
        assertThat(fixture.metadata.transactionCalls).isZero();
    }

    @Test
    void writerEnrollmentRejectsEveryMissingClosedClass() {
        for (FloorClassV1 missing : FloorClassV1.values()) {
            List<FloorClassV1> floors = new ArrayList<>(List.of(FloorClassV1.values()));
            floors.remove(missing);
            assertThatThrownBy(() -> new ReferenceWriterEnrollmentV1(
                            CAPABILITY, floors, List.of(ReferenceKindV1.values()), digest("missing-floor-" + missing)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("closed proof inventory");
        }
        for (ReferenceKindV1 missing : ReferenceKindV1.values()) {
            List<ReferenceKindV1> references = new ArrayList<>(List.of(ReferenceKindV1.values()));
            references.remove(missing);
            assertThatThrownBy(() -> new ReferenceWriterEnrollmentV1(
                            CAPABILITY,
                            List.of(FloorClassV1.values()),
                            references,
                            digest("missing-reference-" + missing)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("closed proof inventory");
        }
    }

    @ParameterizedTest
    @EnumSource(FloorClassV1.class)
    void closedWriterRegistryRejectsEveryMissingFloorOwner(FloorClassV1 missing) {
        assertThatThrownBy(() ->
                        new M5ClosedWriterRegistryV1(closedWriterDeclarations(Optional.of(missing), Optional.empty())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(ReferenceKindV1.class)
    void closedWriterRegistryRejectsEveryMissingReferenceOwner(ReferenceKindV1 missing) {
        assertThatThrownBy(() ->
                        new M5ClosedWriterRegistryV1(closedWriterDeclarations(Optional.empty(), Optional.of(missing))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void closedWriterRegistryRejectsMalformedOrConflictingDeclarations() {
        List<WriterDeclarationV1> duplicateOwnership =
                new ArrayList<>(closedWriterDeclarations(Optional.empty(), Optional.empty()));
        WriterDeclarationV1 last = duplicateOwnership.get(duplicateOwnership.size() - 1);
        duplicateOwnership.set(
                duplicateOwnership.size() - 1,
                new WriterDeclarationV1(
                        last.writerClass(),
                        last.capability(),
                        last.floorClasses(),
                        List.of(last.referenceKinds().get(0), ReferenceKindV1.MANIFEST_SELECTED),
                        last.implementationSourceSha256()));
        assertThatThrownBy(() -> new M5ClosedWriterRegistryV1(duplicateOwnership))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more than one writer owner");

        List<WriterDeclarationV1> mixedCapability =
                new ArrayList<>(closedWriterDeclarations(Optional.empty(), Optional.empty()));
        WriterDeclarationV1 first = mixedCapability.get(0);
        mixedCapability.set(
                0,
                new WriterDeclarationV1(
                        first.writerClass(),
                        new CapabilityBinding(8, digest("different-capability")),
                        first.floorClasses(),
                        first.referenceKinds(),
                        first.implementationSourceSha256()));
        assertThatThrownBy(() -> new M5ClosedWriterRegistryV1(mixedCapability))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mixed capability");

        List<FloorClassV1> nullFloor = new ArrayList<>(List.of(FloorClassV1.values()));
        nullFloor.set(0, null);
        assertThatThrownBy(() -> new WriterDeclarationV1(
                        "malformed-null-floor", CAPABILITY, nullFloor, List.of(), digest("malformed-null-source")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not unique enum members");
        assertThatThrownBy(() -> new WriterDeclarationV1(
                        "x".repeat(M5ClosedWriterRegistryV1.MAX_WRITER_CLASS_BYTES + 1),
                        CAPABILITY,
                        List.of(FloorClassV1.BINDING_EPOCH_POLICY),
                        List.of(),
                        digest("oversized-writer-source")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds its hard cap");
    }

    @ParameterizedTest
    @EnumSource(ReferenceKindV1.class)
    void ticketAndFenceRaceHasOnlyOneWinningAuthorityCas(ReferenceKindV1 referenceKind) {
        RetirementFixture fixture = retirementFixture(false);
        M5BindingRetirementCoordinatorV1 coordinator = new M5BindingRetirementCoordinatorV1(fixture.metadata);
        coordinator
                .migrateLegacy(new M5BindingRetirementCoordinatorV1.MigrationRequest(
                        fixture.selectorKey, fixture.exactSelector))
                .toCompletableFuture()
                .join();
        VersionedValue open = fixture.metadata.readNow(fixture.selectorKey);
        ReferenceMutationTicketV1 ticket = new ReferenceMutationTicketV1(
                ReferenceTargetKindV1.RETIREMENT_BATCH,
                fixture.batch.batchIdSha256(),
                referenceKind,
                CAPABILITY,
                digest("ticket-operation-" + referenceKind),
                digest("ticket-external-root-" + referenceKind));

        assertThat(coordinator
                        .acquireTicket(
                                new M5BindingRetirementCoordinatorV1.TicketRequest(fixture.selectorKey, open, ticket))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.RETAIN);
        assertThat(coordinator
                        .enrollWriters(new M5BindingRetirementCoordinatorV1.EnrollmentRequest(
                                fixture.selectorKey, open, writerEnrollment()))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
        open = fixture.metadata.readNow(fixture.selectorKey);
        assertThat(coordinator
                        .acquireTicket(
                                new M5BindingRetirementCoordinatorV1.TicketRequest(fixture.selectorKey, open, ticket))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
        VersionedValue ticketed = fixture.metadata.readNow(fixture.selectorKey);
        assertThat(coordinator
                        .fence(new M5BindingRetirementCoordinatorV1.FenceRequest(
                                fixture.selectorKey,
                                ticketed,
                                fixture.batch.batchIdSha256(),
                                digest("fence-attempt"),
                                fixture.releases))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.RETAIN);

        assertThat(coordinator
                        .clearTicket(new M5BindingRetirementCoordinatorV1.TicketRequest(
                                fixture.selectorKey, ticketed, ticket))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
        VersionedValue reopened = fixture.metadata.readNow(fixture.selectorKey);
        assertThat(coordinator
                        .fence(new M5BindingRetirementCoordinatorV1.FenceRequest(
                                fixture.selectorKey,
                                reopened,
                                fixture.batch.batchIdSha256(),
                                digest("fence-attempt"),
                                fixture.releases))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
        VersionedValue fenced = fixture.metadata.readNow(fixture.selectorKey);
        assertThat(M5BindingAuthorityCodecV1.decodeAuthority(fenced.canonicalStoredBytes())
                        .state())
                .isEqualTo(BindingAuthorityStateV1.REFERENCE_SCAN_FENCED_V1);
        assertThat(coordinator
                        .acquireTicket(
                                new M5BindingRetirementCoordinatorV1.TicketRequest(fixture.selectorKey, fenced, ticket))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.RETAIN);
        assertThat(fixture.metadata.transactionCalls).isZero();
    }

    @Test
    void fencedBatchRetiresInOneSingleKeyCasAndLostResponseConverges() {
        RetirementFixture fixture = retirementFixture(false);
        M5BindingRetirementCoordinatorV1 coordinator = new M5BindingRetirementCoordinatorV1(fixture.metadata);
        coordinator
                .migrateLegacy(new M5BindingRetirementCoordinatorV1.MigrationRequest(
                        fixture.selectorKey, fixture.exactSelector))
                .toCompletableFuture()
                .join();
        VersionedValue open = fixture.metadata.readNow(fixture.selectorKey);
        coordinator
                .enrollWriters(new M5BindingRetirementCoordinatorV1.EnrollmentRequest(
                        fixture.selectorKey, open, writerEnrollment()))
                .toCompletableFuture()
                .join();
        open = fixture.metadata.readNow(fixture.selectorKey);
        coordinator
                .fence(new M5BindingRetirementCoordinatorV1.FenceRequest(
                        fixture.selectorKey,
                        open,
                        fixture.batch.batchIdSha256(),
                        digest("retirement-attempt"),
                        fixture.releases))
                .toCompletableFuture()
                .join();
        VersionedValue fenced = fixture.metadata.readNow(fixture.selectorKey);
        ReferenceFreeProofV1 proof = proof(
                fixture.metadata,
                "/single-key-proof",
                ReferenceTargetKindV1.RETIREMENT_BATCH,
                fixture.batch.batchIdSha256(),
                fenced,
                fixture.releases);
        M5BindingRetirementCoordinatorV1.RetirementRequest request =
                new M5BindingRetirementCoordinatorV1.RetirementRequest(fixture.selectorKey, fenced, proof);

        fixture.metadata.failReadAfterCas = true;
        assertThat(coordinator.retire(request).toCompletableFuture().join())
                .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.RESPONSE_UNKNOWN);
        assertThat(coordinator.retire(request).toCompletableFuture().join())
                .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.EXISTING_EXACT);

        BindingRetirementAuthorityV1 retired = M5BindingAuthorityCodecV1.decodeAuthority(
                fixture.metadata.readNow(fixture.selectorKey).canonicalStoredBytes());
        assertThat(retired.state()).isEqualTo(BindingAuthorityStateV1.OPEN_V1);
        assertThat(retired.selectorProjection().activeBatches()).isEmpty();
        assertThat(retired.batchSlots()).singleElement().satisfies(slot -> {
            assertThat(slot.state()).isEqualTo(BatchMetadataStateV1.RETIRED_V1);
            assertThat(slot.retiredTombstone()).isPresent();
        });
        assertThat(fixture.metadata.transactionCalls).isZero();
        assertThat(fixture.metadata.readOptional(fixture.batchKey)).isEmpty();
    }

    @Test
    void m4ControlMutationCannotBypassReferenceScanFence() {
        RetirementFixture fixture = retirementFixture(false);
        M5BindingRetirementCoordinatorV1 retirement = new M5BindingRetirementCoordinatorV1(fixture.metadata);
        retirement
                .migrateLegacy(new M5BindingRetirementCoordinatorV1.MigrationRequest(
                        fixture.selectorKey, fixture.exactSelector))
                .toCompletableFuture()
                .join();
        VersionedValue open = fixture.metadata.readNow(fixture.selectorKey);
        retirement
                .enrollWriters(new M5BindingRetirementCoordinatorV1.EnrollmentRequest(
                        fixture.selectorKey, open, writerEnrollment()))
                .toCompletableFuture()
                .join();
        open = fixture.metadata.readNow(fixture.selectorKey);
        retirement
                .fence(new M5BindingRetirementCoordinatorV1.FenceRequest(
                        fixture.selectorKey,
                        open,
                        fixture.batch.batchIdSha256(),
                        digest("m4-blocking-fence"),
                        fixture.releases))
                .toCompletableFuture()
                .join();
        CanonicalBytes exactFenced =
                fixture.metadata.readNow(fixture.selectorKey).canonicalStoredBytes();
        String selectorKey = "/m4-selector-authority";
        ControlStore store = new ControlStore();
        store.values.put(selectorKey, exactFenced);
        M5BindingAuthorityControlMetadataStoreV1 m4Facade =
                new M5BindingAuthorityControlMetadataStoreV1(store, selectorKey);
        BindingReadSelector expected =
                M4ReadControlCodecV1.decodeSelector(m4Facade.get(selectorKey).orElseThrow());
        BindingReadSelector candidate = new BindingReadSelector(
                expected.binding(),
                digest("fenced-successor-view"),
                expected.ownerEpoch() + 1,
                expected.readAdmissionEpoch() + 1,
                expected.sourceGeneration() + 1,
                expected.mode(),
                expected.admissionState(),
                expected.fallbackSetSha256(),
                expected.capability(),
                expected.pendingAnchors(),
                expected.activeBatches());

        assertThat(m4Facade.compareAndSet(
                        selectorKey,
                        Optional.of(M4ReadControlCodecV1.encodeSelector(expected)),
                        M4ReadControlCodecV1.encodeSelector(candidate)))
                .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(store.values.get(selectorKey)).isEqualTo(exactFenced);
    }

    @Test
    void pulsarAggregateMigrationPreservesExactNta1Projection() {
        PulsarAuthorityFixture fixture = pulsarAuthorityFixture();
        M5PulsarAggregateRetirementCoordinatorV1 coordinator =
                new M5PulsarAggregateRetirementCoordinatorV1(fixture.metadata);

        assertThat(coordinator
                        .migrateLegacy(new M5PulsarAggregateRetirementCoordinatorV1.MigrationRequest(
                                fixture.aggregateKey, fixture.exactAggregate, CAPABILITY))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.APPLIED_EXACT);

        VersionedValue stored = fixture.metadata.readNow(fixture.aggregateKey);
        PulsarAggregateRetirementAuthorityV1 authority =
                M5PulsarAggregateAuthorityCodecV1.decodeAuthority(stored.canonicalStoredBytes());
        assertThat(authority.state()).isEqualTo(PulsarAggregateAuthorityStateV1.OPEN_V1);
        assertThat(authority.authorityGeneration()).isEqualTo(1);
        assertThat(authority.originalAggregateSha256()).isEqualTo(fixture.exactAggregate.canonicalStoredSha256());
        assertThat(M5PulsarAggregateAuthorityCodecV1.projectAggregate(stored.canonicalStoredBytes()))
                .isEqualTo(fixture.exactAggregate.canonicalStoredBytes());
        assertThat(Nta1CodecV1.decode(authority.canonicalAggregateBytes())).isEqualTo(fixture.aggregate);
        assertThat(fixture.metadata.transactionCalls).isZero();
    }

    @ParameterizedTest
    @EnumSource(ReferenceKindV1.class)
    void pulsarTicketAndFenceRaceHasOnlyOneWinningAuthorityCas(ReferenceKindV1 referenceKind) {
        PulsarAuthorityFixture fixture = pulsarAuthorityFixture();
        M5PulsarAggregateRetirementCoordinatorV1 coordinator =
                new M5PulsarAggregateRetirementCoordinatorV1(fixture.metadata);
        coordinator
                .migrateLegacy(new M5PulsarAggregateRetirementCoordinatorV1.MigrationRequest(
                        fixture.aggregateKey, fixture.exactAggregate, CAPABILITY))
                .toCompletableFuture()
                .join();
        VersionedValue open = fixture.metadata.readNow(fixture.aggregateKey);
        ReferenceMutationTicketV1 ticket = new ReferenceMutationTicketV1(
                ReferenceTargetKindV1.PULSAR_AGGREGATE,
                fixture.exactAggregate.canonicalStoredSha256(),
                referenceKind,
                CAPABILITY,
                digest("pulsar-operation-" + referenceKind),
                digest("pulsar-external-root-" + referenceKind));
        M5PulsarAggregateRetirementCoordinatorV1.TicketRequest initialTicket =
                new M5PulsarAggregateRetirementCoordinatorV1.TicketRequest(fixture.aggregateKey, open, ticket);

        assertThat(coordinator
                        .acquireTicket(initialTicket)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.RETAIN);
        assertThat(coordinator
                        .enrollWriters(new M5PulsarAggregateRetirementCoordinatorV1.EnrollmentRequest(
                                fixture.aggregateKey, open, writerEnrollment()))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
        open = fixture.metadata.readNow(fixture.aggregateKey);
        assertThat(coordinator
                        .acquireTicket(new M5PulsarAggregateRetirementCoordinatorV1.TicketRequest(
                                fixture.aggregateKey, open, ticket))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
        VersionedValue ticketed = fixture.metadata.readNow(fixture.aggregateKey);
        assertThat(coordinator
                        .fence(fixture.fenceRequest(ticketed, "pulsar-ticketed-fence"))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.RETAIN);
        assertThat(coordinator
                        .clearTicket(new M5PulsarAggregateRetirementCoordinatorV1.TicketRequest(
                                fixture.aggregateKey, ticketed, ticket))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
        VersionedValue reopened = fixture.metadata.readNow(fixture.aggregateKey);
        assertThat(coordinator
                        .fence(fixture.fenceRequest(reopened, "pulsar-winning-fence"))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
        VersionedValue fenced = fixture.metadata.readNow(fixture.aggregateKey);
        assertThat(M5PulsarAggregateAuthorityCodecV1.decodeAuthority(fenced.canonicalStoredBytes())
                        .state())
                .isEqualTo(PulsarAggregateAuthorityStateV1.REFERENCE_SCAN_FENCED_V1);
        assertThat(coordinator
                        .acquireTicket(new M5PulsarAggregateRetirementCoordinatorV1.TicketRequest(
                                fixture.aggregateKey, fenced, ticket))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.RETAIN);
        assertThat(fixture.metadata.transactionCalls).isZero();
    }

    @Test
    void pulsarFenceRejectsSameNameGenerationAba() {
        PulsarAuthorityFixture fixture = pulsarAuthorityFixture();
        M5PulsarAggregateRetirementCoordinatorV1 coordinator =
                new M5PulsarAggregateRetirementCoordinatorV1(fixture.metadata);
        coordinator
                .migrateLegacy(new M5PulsarAggregateRetirementCoordinatorV1.MigrationRequest(
                        fixture.aggregateKey, fixture.exactAggregate, CAPABILITY))
                .toCompletableFuture()
                .join();
        VersionedValue open = fixture.metadata.readNow(fixture.aggregateKey);
        coordinator
                .enrollWriters(new M5PulsarAggregateRetirementCoordinatorV1.EnrollmentRequest(
                        fixture.aggregateKey, open, writerEnrollment()))
                .toCompletableFuture()
                .join();
        open = fixture.metadata.readNow(fixture.aggregateKey);
        PulsarTopicGenerationSelectorValueV1 nextGeneration = new PulsarTopicGenerationSelectorValueV1(
                fixture.selectorValue.persistenceName(),
                new PulsarBindingGeneration(
                        Math.addExact(fixture.selectorValue.generation().value(), 1)),
                PulsarTopicGenerationSelectorStateV1.DELETED,
                fixture.selectorValue.aggregateBindingId(),
                fixture.selectorValue.aggregateCanonicalStoredDigest(),
                fixture.selectorValue.canonicalStoredBytes(),
                fixture.selectorValue.canonicalStoredDigest());

        VersionedValue exactOpen = open;
        assertThatThrownBy(() -> coordinator.fence(new M5PulsarAggregateRetirementCoordinatorV1.FenceRequest(
                        fixture.aggregateKey,
                        exactOpen,
                        fixture.selectorKey,
                        fixture.exactSelector,
                        nextGeneration,
                        digest("pulsar-aba-fence"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DELETED generation selector");
        assertThat(fixture.metadata.readNow(fixture.aggregateKey)).isEqualTo(open);
    }

    @Test
    void fencedPulsarAggregateRetiresInOneSingleKeyCasAfterCleanup() {
        PulsarAuthorityFixture fixture = pulsarAuthorityFixture();
        M5PulsarAggregateRetirementCoordinatorV1 coordinator =
                new M5PulsarAggregateRetirementCoordinatorV1(fixture.metadata);
        coordinator
                .migrateLegacy(new M5PulsarAggregateRetirementCoordinatorV1.MigrationRequest(
                        fixture.aggregateKey, fixture.exactAggregate, CAPABILITY))
                .toCompletableFuture()
                .join();
        VersionedValue open = fixture.metadata.readNow(fixture.aggregateKey);
        coordinator
                .enrollWriters(new M5PulsarAggregateRetirementCoordinatorV1.EnrollmentRequest(
                        fixture.aggregateKey, open, writerEnrollment()))
                .toCompletableFuture()
                .join();
        open = fixture.metadata.readNow(fixture.aggregateKey);
        coordinator
                .fence(fixture.fenceRequest(open, "pulsar-retirement-fence"))
                .toCompletableFuture()
                .join();
        VersionedValue fenced = fixture.metadata.readNow(fixture.aggregateKey);
        ReferenceFreeProofV1 proof = proof(
                fixture.metadata,
                "/pulsar/single-key-proof",
                ReferenceTargetKindV1.PULSAR_AGGREGATE,
                fixture.exactAggregate.canonicalStoredSha256(),
                fixture.exactSelector,
                List.of(),
                fixture.proofIdentity);
        PhysicalCleanupSummaryV1 cleanup = new PhysicalCleanupSummaryV1(
                fact(fixture.metadata.seed("/pulsar/cleanup", bytes("delete-done"))),
                fixture.incarnation,
                fixture.aggregate.binding().bindingId(),
                fixture.exactAggregate.canonicalStoredSha256(),
                CAPABILITY,
                2,
                1,
                1,
                0);
        M5PulsarAggregateRetirementCoordinatorV1.RetirementRequest request =
                new M5PulsarAggregateRetirementCoordinatorV1.RetirementRequest(
                        fixture.aggregateKey, fenced, proof, cleanup);
        int beforeRetirementCas = fixture.metadata.casCalls;
        fixture.metadata.casResponseUnknownAfterApply = true;

        assertThat(coordinator.retire(request).toCompletableFuture().join())
                .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.EXISTING_EXACT);
        assertThat(fixture.metadata.casCalls - beforeRetirementCas).isOne();
        assertThat(coordinator.retire(request).toCompletableFuture().join())
                .isEqualTo(M5PulsarAggregateRetirementCoordinatorV1.Outcome.EXISTING_EXACT);
        RetiredTopicIncarnationTombstoneV1 tombstone = M5RetentionCodecV1.decodeRetiredPulsar(
                fixture.metadata.readNow(fixture.aggregateKey).canonicalStoredBytes());
        assertThat(tombstone.incarnation()).isEqualTo(fixture.incarnation);
        assertThat(tombstone.originalAggregateSha256()).isEqualTo(fixture.exactAggregate.canonicalStoredSha256());
        assertThat(tombstone.aggregatePredecessorValueSha256()).isEqualTo(fenced.canonicalStoredSha256());
        assertThat(fixture.metadata.readNow(fixture.selectorKey)).isEqualTo(fixture.exactSelector);
        assertThat(fixture.metadata.transactionCalls).isZero();
    }

    @ParameterizedTest
    @EnumSource(ReferenceKindV1.class)
    void guardedBindingMutationDispatchesOnlyWithVisibleRegisteredTicket(ReferenceKindV1 referenceKind) {
        RetirementFixture fixture = retirementFixture(false);
        M5ClosedWriterRegistryV1 registry = closedWriterRegistry();
        M5BindingRetirementCoordinatorV1 coordinator = new M5BindingRetirementCoordinatorV1(fixture.metadata);
        coordinator
                .migrateLegacy(new M5BindingRetirementCoordinatorV1.MigrationRequest(
                        fixture.selectorKey, fixture.exactSelector))
                .toCompletableFuture()
                .join();
        VersionedValue open = fixture.metadata.readNow(fixture.selectorKey);
        coordinator
                .enrollWriters(new M5BindingRetirementCoordinatorV1.EnrollmentRequest(
                        fixture.selectorKey, open, registry.enrollment()))
                .toCompletableFuture()
                .join();
        M5ReferenceMutationGuardV1 guard = M5ReferenceMutationGuardV1.forBindingBatch(
                registry, coordinator, fixture.selectorKey, fixture.batch.batchIdSha256());
        Sha256Digest externalRoot = digest("guarded-binding-root-" + referenceKind);
        GuardedMutationRequestV1 request = new GuardedMutationRequestV1(
                registry.writerFor(referenceKind),
                ReferenceTargetKindV1.RETIREMENT_BATCH,
                fixture.batch.batchIdSha256(),
                digest("guarded-binding-operation-" + referenceKind),
                externalRoot);
        AtomicInteger dispatched = new AtomicInteger();
        fixture.metadata.casResponseUnknownAfterApply = true;

        var result = guard.execute(request, () -> {
                    dispatched.incrementAndGet();
                    BindingRetirementAuthorityV1 ticketed = M5BindingAuthorityCodecV1.decodeAuthority(
                            fixture.metadata.readNow(fixture.selectorKey).canonicalStoredBytes());
                    assertThat(ticketed.referenceMutationTickets())
                            .singleElement()
                            .satisfies(
                                    ticket -> assertThat(ticket.referenceKind()).isEqualTo(referenceKind));
                    return CompletableFuture.completedFuture(
                            new ExternalMutationResultV1(ExternalMutationOutcomeV1.APPLIED_EXACT, externalRoot));
                })
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED);
        assertThat(result.externalMutationDispatched()).isTrue();
        assertThat(dispatched).hasValue(1);
        assertThat(M5BindingAuthorityCodecV1.decodeAuthority(
                                fixture.metadata.readNow(fixture.selectorKey).canonicalStoredBytes())
                        .referenceMutationTickets())
                .isEmpty();
    }

    @Test
    void ambiguousGuardedMutationRetainsTicketAndVetoesFence() {
        RetirementFixture fixture = retirementFixture(false);
        M5ClosedWriterRegistryV1 registry = closedWriterRegistry();
        M5BindingRetirementCoordinatorV1 coordinator = new M5BindingRetirementCoordinatorV1(fixture.metadata);
        coordinator
                .migrateLegacy(new M5BindingRetirementCoordinatorV1.MigrationRequest(
                        fixture.selectorKey, fixture.exactSelector))
                .toCompletableFuture()
                .join();
        VersionedValue open = fixture.metadata.readNow(fixture.selectorKey);
        coordinator
                .enrollWriters(new M5BindingRetirementCoordinatorV1.EnrollmentRequest(
                        fixture.selectorKey, open, registry.enrollment()))
                .toCompletableFuture()
                .join();
        M5ReferenceMutationGuardV1 guard = M5ReferenceMutationGuardV1.forBindingBatch(
                registry, coordinator, fixture.selectorKey, fixture.batch.batchIdSha256());
        Sha256Digest externalRoot = digest("ambiguous-external-root");
        GuardedMutationRequestV1 request = new GuardedMutationRequestV1(
                registry.writerFor(ReferenceKindV1.RETIREMENT_OR_DELETE_RECONCILIATION),
                ReferenceTargetKindV1.RETIREMENT_BATCH,
                fixture.batch.batchIdSha256(),
                digest("ambiguous-operation"),
                externalRoot);

        var result = guard.execute(
                        request,
                        () -> CompletableFuture.completedFuture(
                                new ExternalMutationResultV1(ExternalMutationOutcomeV1.RESPONSE_UNKNOWN, externalRoot)))
                .toCompletableFuture()
                .join();
        VersionedValue ticketed = fixture.metadata.readNow(fixture.selectorKey);

        assertThat(result.outcome()).isEqualTo(GuardOutcomeV1.RETAINED_AMBIGUOUS_EXTERNAL_MUTATION);
        assertThat(M5BindingAuthorityCodecV1.decodeAuthority(ticketed.canonicalStoredBytes())
                        .referenceMutationTickets())
                .hasSize(1);
        assertThat(coordinator
                        .fence(new M5BindingRetirementCoordinatorV1.FenceRequest(
                                fixture.selectorKey,
                                ticketed,
                                fixture.batch.batchIdSha256(),
                                digest("ambiguous-veto-fence"),
                                fixture.releases))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.RETAIN);

        var mismatchedRoot = guard.execute(
                        request,
                        () -> CompletableFuture.completedFuture(new ExternalMutationResultV1(
                                ExternalMutationOutcomeV1.APPLIED_EXACT, digest("mismatched-external-root"))))
                .toCompletableFuture()
                .join();
        assertThat(mismatchedRoot.outcome()).isEqualTo(GuardOutcomeV1.RETAINED_AMBIGUOUS_EXTERNAL_MUTATION);
        assertThat(M5BindingAuthorityCodecV1.decodeAuthority(
                                fixture.metadata.readNow(fixture.selectorKey).canonicalStoredBytes())
                        .referenceMutationTickets())
                .hasSize(1);

        var recovered = guard.execute(
                        request,
                        () -> CompletableFuture.completedFuture(
                                new ExternalMutationResultV1(ExternalMutationOutcomeV1.EXISTING_EXACT, externalRoot)))
                .toCompletableFuture()
                .join();
        assertThat(recovered.outcome()).isEqualTo(GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED);
        assertThat(M5BindingAuthorityCodecV1.decodeAuthority(
                                fixture.metadata.readNow(fixture.selectorKey).canonicalStoredBytes())
                        .referenceMutationTickets())
                .isEmpty();
    }

    @Test
    void winningFencePreventsGuardedExternalMutationDispatch() {
        RetirementFixture fixture = retirementFixture(false);
        M5ClosedWriterRegistryV1 registry = closedWriterRegistry();
        M5BindingRetirementCoordinatorV1 coordinator = new M5BindingRetirementCoordinatorV1(fixture.metadata);
        coordinator
                .migrateLegacy(new M5BindingRetirementCoordinatorV1.MigrationRequest(
                        fixture.selectorKey, fixture.exactSelector))
                .toCompletableFuture()
                .join();
        VersionedValue open = fixture.metadata.readNow(fixture.selectorKey);
        coordinator
                .enrollWriters(new M5BindingRetirementCoordinatorV1.EnrollmentRequest(
                        fixture.selectorKey, open, registry.enrollment()))
                .toCompletableFuture()
                .join();
        open = fixture.metadata.readNow(fixture.selectorKey);
        coordinator
                .fence(new M5BindingRetirementCoordinatorV1.FenceRequest(
                        fixture.selectorKey,
                        open,
                        fixture.batch.batchIdSha256(),
                        digest("winning-fence-before-guard"),
                        fixture.releases))
                .toCompletableFuture()
                .join();
        M5ReferenceMutationGuardV1 guard = M5ReferenceMutationGuardV1.forBindingBatch(
                registry, coordinator, fixture.selectorKey, fixture.batch.batchIdSha256());
        AtomicInteger dispatched = new AtomicInteger();
        Sha256Digest externalRoot = digest("blocked-external-root");

        var result = guard.execute(
                        new GuardedMutationRequestV1(
                                registry.writerFor(ReferenceKindV1.MANIFEST_SELECTED),
                                ReferenceTargetKindV1.RETIREMENT_BATCH,
                                fixture.batch.batchIdSha256(),
                                digest("blocked-operation"),
                                externalRoot),
                        () -> {
                            dispatched.incrementAndGet();
                            return CompletableFuture.completedFuture(new ExternalMutationResultV1(
                                    ExternalMutationOutcomeV1.APPLIED_EXACT, externalRoot));
                        })
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(GuardOutcomeV1.RETAINED_BEFORE_EXTERNAL_MUTATION);
        assertThat(result.externalMutationDispatched()).isFalse();
        assertThat(dispatched).hasValue(0);
    }

    @Test
    void pulsarGuardUsesTheSameClosedWriterTicketProtocol() {
        PulsarAuthorityFixture fixture = pulsarAuthorityFixture();
        M5ClosedWriterRegistryV1 registry = closedWriterRegistry();
        M5PulsarAggregateRetirementCoordinatorV1 coordinator =
                new M5PulsarAggregateRetirementCoordinatorV1(fixture.metadata);
        coordinator
                .migrateLegacy(new M5PulsarAggregateRetirementCoordinatorV1.MigrationRequest(
                        fixture.aggregateKey, fixture.exactAggregate, CAPABILITY))
                .toCompletableFuture()
                .join();
        VersionedValue open = fixture.metadata.readNow(fixture.aggregateKey);
        coordinator
                .enrollWriters(new M5PulsarAggregateRetirementCoordinatorV1.EnrollmentRequest(
                        fixture.aggregateKey, open, registry.enrollment()))
                .toCompletableFuture()
                .join();
        M5ReferenceMutationGuardV1 guard = M5ReferenceMutationGuardV1.forPulsarAggregate(
                registry, coordinator, fixture.aggregateKey, fixture.exactAggregate.canonicalStoredSha256());
        Sha256Digest externalRoot = digest("pulsar-guard-root");

        var result = guard.execute(
                        new GuardedMutationRequestV1(
                                registry.writerFor(ReferenceKindV1.PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR),
                                ReferenceTargetKindV1.PULSAR_AGGREGATE,
                                fixture.exactAggregate.canonicalStoredSha256(),
                                digest("pulsar-guard-operation"),
                                externalRoot),
                        () -> CompletableFuture.completedFuture(
                                new ExternalMutationResultV1(ExternalMutationOutcomeV1.EXISTING_EXACT, externalRoot)))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED);
        assertThat(M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                fixture.metadata.readNow(fixture.aggregateKey).canonicalStoredBytes())
                        .referenceMutationTickets())
                .isEmpty();
    }

    @Test
    void externalizationIsOneAtomicTransitionAndResponseLossConverges() {
        RetirementFixture fixture = retirementFixture(true);
        fixture.metadata.responseUnknownAfterApply = true;
        M5RetirementCoordinatorV1 coordinator = new M5RetirementCoordinatorV1(fixture.metadata);

        M5RetirementCoordinatorV1.Outcome outcome = coordinator
                .externalize(fixture.externalizationRequest())
                .toCompletableFuture()
                .join();

        assertThat(outcome).isEqualTo(M5RetirementCoordinatorV1.Outcome.EXISTING_EXACT);
        assertThat(M4ReadControlCodecV1.decodeSelector(
                                fixture.metadata.readNow(fixture.selectorKey).canonicalStoredBytes())
                        .activeBatches())
                .isEmpty();
        assertThat(M5RetentionCodecV1.decodeFullBatch(
                        fixture.metadata.readNow(fixture.batchKey).canonicalStoredBytes()))
                .isEqualTo(fixture.fullBatch);
    }

    @Test
    void externalizationLostResponseWithExactPredecessorsIsDefinitivelyNotApplied() {
        RetirementFixture fixture = retirementFixture(true);
        fixture.metadata.responseUnknownWithoutApply = true;

        assertThat(new M5RetirementCoordinatorV1(fixture.metadata)
                        .externalize(fixture.externalizationRequest())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.DEFINITIVELY_NOT_APPLIED);
        assertThat(fixture.metadata.readOptional(fixture.batchKey)).isEmpty();
        assertThat(M4ReadControlCodecV1.decodeSelector(
                                fixture.metadata.readNow(fixture.selectorKey).canonicalStoredBytes())
                        .activeBatches())
                .containsExactly(fixture.batch);
    }

    @Test
    void failedAuthoritativeReconciliationReadRemainsResponseUnknown() {
        RetirementFixture fixture = retirementFixture(true);
        fixture.metadata.failReadAfterTransaction = true;

        assertThat(new M5RetirementCoordinatorV1(fixture.metadata)
                        .externalize(fixture.externalizationRequest())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.RESPONSE_UNKNOWN);
    }

    @Test
    void retirementUsesFreshProofAndIrreversibleSameKeyTombstone() {
        RetirementFixture fixture = retirementFixture(true);
        M5RetirementCoordinatorV1 coordinator = new M5RetirementCoordinatorV1(fixture.metadata);
        assertThat(coordinator
                        .externalize(fixture.externalizationRequest())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.APPLIED_EXACT);
        VersionedValue successorSelector = fixture.metadata.readNow(fixture.selectorKey);
        ReferenceFreeProofV1 retirementProof = proof(
                fixture.metadata,
                "/retire-proof",
                ReferenceTargetKindV1.RETIREMENT_BATCH,
                fixture.batch.batchIdSha256(),
                successorSelector,
                fixture.releases);
        ReferenceFreeProofV1 alternateProof = proof(
                fixture.metadata,
                "/alternate-retire-proof",
                ReferenceTargetKindV1.RETIREMENT_BATCH,
                fixture.batch.batchIdSha256(),
                successorSelector,
                fixture.releases);
        VersionedValue exactFull = fixture.metadata.readNow(fixture.batchKey);

        fixture.metadata.responseUnknownWithoutApply = true;
        assertThat(coordinator
                        .retireBatch(new BatchRetirementRequest(
                                fixture.partitionKey, fixture.batchKey, exactFull, fixture.fullBatch, retirementProof))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.DEFINITIVELY_NOT_APPLIED);
        assertThat(fixture.metadata.readNow(fixture.batchKey)).isEqualTo(exactFull);
        assertThat(coordinator
                        .retireBatch(new BatchRetirementRequest(
                                fixture.partitionKey, fixture.batchKey, exactFull, fixture.fullBatch, retirementProof))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.APPLIED_EXACT);
        assertThat(M5RetentionCodecV1.decodeRetiredBatch(
                                fixture.metadata.readNow(fixture.batchKey).canonicalStoredBytes())
                        .state())
                .isEqualTo(BatchMetadataStateV1.RETIRED_V1);
        assertThat(coordinator
                        .retireBatch(new BatchRetirementRequest(
                                fixture.partitionKey, fixture.batchKey, exactFull, fixture.fullBatch, retirementProof))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.EXISTING_EXACT);
        assertThat(coordinator
                        .retireBatch(new BatchRetirementRequest(
                                fixture.partitionKey, fixture.batchKey, exactFull, fixture.fullBatch, alternateProof))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.ALREADY_RETIRED);
        CanonicalBytes permanentTombstone =
                fixture.metadata.readNow(fixture.batchKey).canonicalStoredBytes();
        assertThatThrownBy(() -> new M5RetirementCoordinatorV1(fixture.metadata)
                        .externalize(fixture.externalizationRequest())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(M5ReferenceFreshnessVerifierV1.StaleAuthorityException.class);
        assertThat(fixture.metadata.readNow(fixture.batchKey).canonicalStoredBytes())
                .isEqualTo(permanentTombstone);
        assertThatThrownBy(() -> M4ReadControlCodecV1.decodeProtection(permanentTombstone))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> M4ReadControlCodecV1.decodeBatch(permanentTombstone))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void partialOrWrongM4ReleaseCannotExternalize() {
        RetirementFixture fixture = retirementFixture(true);
        M4ReleaseBindingV1 release = fixture.releases.get(0);
        SourceProtectionIdentity wrongSource =
                new SourceProtectionIdentity(digest("wrong-source"), release.protectionGeneration(), 5, 7, CAPABILITY);
        CanonicalBytes wrongProtectionBytes = M4ReadControlCodecV1.encodeProtection(new SourceProtection(
                BINDING,
                wrongSource,
                ProtectionState.RELEASED,
                Optional.of(fixture.batch.batchIdSha256()),
                Optional.of(release.releaseProofHeadSha256())));
        VersionedValue wrongProtection = fixture.metadata.seed("/binding/wrong-protection", wrongProtectionBytes);
        M4ReleaseBindingV1 wrong = new M4ReleaseBindingV1(
                wrongSource.sourceIdentitySha256(),
                release.protectionGeneration(),
                fact(wrongProtection),
                wrongProtectionBytes,
                release.releasedByBatchSha256(),
                release.releaseProofHeadSha256());
        ReferenceFreeProofV1 proof = proof(
                fixture.metadata,
                "/wrong-release",
                ReferenceTargetKindV1.RETIREMENT_BATCH,
                fixture.batch.batchIdSha256(),
                fixture.exactSelector,
                List.of(wrong));
        ExternalizationRequest request = new ExternalizationRequest(
                fixture.partitionKey,
                fixture.selectorKey,
                fixture.batchKey,
                fixture.exactSelector,
                fixture.selectorSuccessorBytes,
                Optional.empty(),
                new FullSourceRetirementBatchV1(
                        BatchMetadataStateV1.FULL_V1,
                        BINDING,
                        fixture.batch.batchIdSha256(),
                        Sha256Digest.hash(M4ReadControlCodecV1.encodeBatch(fixture.batch)),
                        M4ReadControlCodecV1.encodeBatch(fixture.batch),
                        fixture.exactSelector.canonicalStoredSha256(),
                        proof.proofSha256(),
                        CAPABILITY),
                proof);

        assertThatThrownBy(() -> new M5RetirementCoordinatorV1(fixture.metadata).externalize(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lacks exact");

        CanonicalBytes protectedBytes = M4ReadControlCodecV1.encodeProtection(new SourceProtection(
                BINDING,
                fixture.batch.sources().get(0),
                ProtectionState.PROTECTED,
                Optional.empty(),
                Optional.empty()));
        VersionedValue protectedValue = fixture.metadata.seed("/binding/still-protected", protectedBytes);
        assertThatThrownBy(() -> new M4ReleaseBindingV1(
                        fixture.batch.sources().get(0).sourceIdentitySha256(),
                        fixture.batch.sources().get(0).protectionGeneration(),
                        fact(protectedValue),
                        protectedBytes,
                        fixture.batch.batchIdSha256(),
                        digest("proof-head")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RELEASED");
    }

    @Test
    void nMemberReleaseRequiresEveryExactSiblingBinding() {
        RetirementFixture fixture = retirementFixture(true, 3);
        M5RetirementCoordinatorV1 coordinator = new M5RetirementCoordinatorV1(fixture.metadata);

        ReferenceFreeProofV1 partialProof = proof(
                fixture.metadata,
                "/partial-release",
                ReferenceTargetKindV1.RETIREMENT_BATCH,
                fixture.batch.batchIdSha256(),
                fixture.exactSelector,
                fixture.releases.subList(0, 2));
        assertThatThrownBy(() -> coordinator.externalize(externalizationRequest(fixture, partialProof)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("count differs");

        M4ReleaseBindingV1 first = fixture.releases.get(0);
        VersionedValue duplicateAuthority =
                fixture.metadata.seed("/binding/duplicate-release-authority", first.canonicalProtectionBytes());
        M4ReleaseBindingV1 duplicateFirst = new M4ReleaseBindingV1(
                first.sourceIdentitySha256(),
                first.protectionGeneration(),
                fact(duplicateAuthority),
                first.canonicalProtectionBytes(),
                first.releasedByBatchSha256(),
                first.releaseProofHeadSha256());
        List<M4ReleaseBindingV1> missingSibling =
                new ArrayList<>(List.of(first, duplicateFirst, fixture.releases.get(2)));
        missingSibling.sort(Comparator.comparing((M4ReleaseBindingV1 value) ->
                        value.sourceIdentitySha256().toHex())
                .thenComparingLong(M4ReleaseBindingV1::protectionGeneration));
        ReferenceFreeProofV1 siblingProof = proof(
                fixture.metadata,
                "/missing-sibling-release",
                ReferenceTargetKindV1.RETIREMENT_BATCH,
                fixture.batch.batchIdSha256(),
                fixture.exactSelector,
                missingSibling);
        assertThatThrownBy(() -> coordinator.externalize(externalizationRequest(fixture, siblingProof)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lacks exact");
        assertThat(fixture.metadata.transactionCalls).isZero();
        assertThat(fixture.metadata.readNow(fixture.selectorKey)).isEqualTo(fixture.exactSelector);
        assertThat(fixture.metadata.readOptional(fixture.batchKey)).isEmpty();

        assertThat(coordinator
                        .externalize(fixture.externalizationRequest())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.APPLIED_EXACT);
        assertThat(fixture.releases.stream()
                        .map(M4ReleaseBindingV1::sourceIdentitySha256)
                        .toList())
                .isEqualTo(fixture.batch.sources().stream()
                        .map(SourceProtectionIdentity::sourceIdentitySha256)
                        .toList());
        assertThat(fixture.metadata.transactionCalls).isOne();
    }

    @Test
    void unsupportedBackendPerformsNoTransactionAndNeverCreatesSplitState() {
        RetirementFixture fixture = retirementFixture(false);

        assertThat(new M5RetirementCoordinatorV1(fixture.metadata)
                        .externalize(fixture.externalizationRequest())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.UNSUPPORTED);
        assertThat(fixture.metadata.transactionCalls).isZero();
        assertThat(fixture.metadata.readOptional(fixture.batchKey)).isEmpty();
        assertThat(M4ReadControlCodecV1.decodeSelector(
                                fixture.metadata.readNow(fixture.selectorKey).canonicalStoredBytes())
                        .activeBatches())
                .containsExactly(fixture.batch);
    }

    @Test
    void impossibleInlineAndFullSplitIsQuarantined() {
        RetirementFixture fixture = retirementFixture(true);
        VersionedValue existing =
                fixture.metadata.seed(fixture.batchKey, M5RetentionCodecV1.encodeFullBatch(fixture.fullBatch));
        fixture.metadata.responseUnknownWithoutApply = true;
        ExternalizationRequest request = new ExternalizationRequest(
                fixture.partitionKey,
                fixture.selectorKey,
                fixture.batchKey,
                fixture.exactSelector,
                fixture.selectorSuccessorBytes,
                Optional.of(existing),
                fixture.fullBatch,
                fixture.proof);

        assertThat(new M5RetirementCoordinatorV1(fixture.metadata)
                        .externalize(request)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.QUARANTINED);
    }

    @Test
    void everyImpossibleExternalizationSplitStateIsQuarantined() {
        assertThat(reconcileAfterInterference(fixture ->
                        fixture.metadata.seed(fixture.batchKey, M5RetentionCodecV1.encodeFullBatch(fixture.fullBatch))))
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.QUARANTINED);
        assertThat(reconcileAfterInterference(
                        fixture -> fixture.metadata.seed(fixture.batchKey, retiredBatchBytes(fixture, true))))
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.QUARANTINED);
        assertThat(reconcileAfterInterference(
                        fixture -> fixture.metadata.seed(fixture.batchKey, bytes("foreign-batch-state"))))
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.QUARANTINED);
        assertThat(reconcileAfterInterference(
                        fixture -> fixture.metadata.overwrite(fixture.selectorKey, fixture.selectorSuccessorBytes)))
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.QUARANTINED);
        assertThat(reconcileAfterInterference(fixture -> {
                    fixture.metadata.remove(fixture.selectorKey);
                    fixture.metadata.seed(fixture.batchKey, M5RetentionCodecV1.encodeFullBatch(fixture.fullBatch));
                }))
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.QUARANTINED);
    }

    @Test
    void mismatchedExternalizationStatesNeverConvergeAsApplied() {
        assertThat(reconcileAfterInterference(fixture -> {
                    fixture.metadata.overwrite(fixture.selectorKey, fixture.selectorSuccessorBytes);
                    fixture.metadata.seed(fixture.batchKey, bytes("foreign-batch-state"));
                }))
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.CONFLICT);
        assertThat(reconcileAfterInterference(fixture -> {
                    fixture.metadata.overwrite(
                            fixture.selectorKey,
                            M4ReadControlCodecV1.encodeSelector(new BindingReadSelector(
                                    BINDING,
                                    digest("foreign-selected-view"),
                                    11,
                                    13,
                                    17,
                                    SelectorMode.PREFERRED_ONLY,
                                    AdmissionState.ADMITTING,
                                    Optional.empty(),
                                    CAPABILITY,
                                    List.of(),
                                    List.of())));
                    fixture.metadata.seed(fixture.batchKey, M5RetentionCodecV1.encodeFullBatch(fixture.fullBatch));
                }))
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.CONFLICT);
        assertThat(reconcileAfterInterference(fixture -> {
                    fixture.metadata.overwrite(fixture.selectorKey, fixture.selectorSuccessorBytes);
                    fixture.metadata.seed(fixture.batchKey, retiredBatchBytes(fixture, false));
                }))
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.CONFLICT);
    }

    @Test
    void admissionPersistsExactCapsAndRejectsBeforeExceedingThem() {
        InMemoryStore metadata = new InMemoryStore(true);
        M5RetentionAdmissionV1 admission =
                new M5RetentionAdmissionV1(metadata, "/binding/retention-admission", digest("cell"), BINDING);
        M5RetentionAdmissionV1.Caps caps = new M5RetentionAdmissionV1.Caps(values(100));

        assertThat(admission.install(caps).toCompletableFuture().join().outcome())
                .isEqualTo(M5RetentionAdmissionV1.Outcome.INSTALLED);
        M5RetentionAdmissionV1.Reservation first = reservation("first", 60, 11);
        M5RetentionAdmissionV1.Result applied =
                admission.reserve(first).toCompletableFuture().join();
        assertThat(applied.outcome()).isEqualTo(M5RetentionAdmissionV1.Outcome.APPLIED_EXACT);
        assertThat(applied.exactState()
                        .orElseThrow()
                        .usage()
                        .value(M5RetentionAdmissionV1.LimitKindV1.RETAINED_SOURCE_BYTES))
                .isEqualTo(60);
        assertThat(M5RetentionAdmissionV1.decode(
                        M5RetentionAdmissionV1.encode(applied.exactState().orElseThrow())))
                .isEqualTo(applied.exactState().orElseThrow());
        assertThat(admission.reserve(first).toCompletableFuture().join().outcome())
                .isEqualTo(M5RetentionAdmissionV1.Outcome.EXISTING_EXACT);

        M5RetentionAdmissionV1.Result rejected = admission
                .reserve(reservation("second", 50, 17))
                .toCompletableFuture()
                .join();
        assertThat(rejected.outcome()).isEqualTo(M5RetentionAdmissionV1.Outcome.REJECTED_CAP);
        M5RetentionAdmissionV1.AlertV1 alert = rejected.alert().orElseThrow();
        assertThat(alert.limitKind()).isEqualTo(M5RetentionAdmissionV1.LimitKindV1.RETAINED_SOURCE_BYTES);
        assertThat(alert.currentValue()).isEqualTo(60);
        assertThat(alert.reservedValue()).isEqualTo(50);
        assertThat(alert.hardValue()).isEqualTo(100);
        assertThat(admission.read().toCompletableFuture().join().orElseThrow().reservations())
                .containsExactly(first);
    }

    @Test
    void admissionDistinguishesExactNonApplicationFromFailedReconciliationRead() {
        InMemoryStore metadata = new InMemoryStore(true);
        M5RetentionAdmissionV1 admission =
                new M5RetentionAdmissionV1(metadata, "/binding/admission-response", digest("cell"), BINDING);
        assertThat(admission
                        .install(new M5RetentionAdmissionV1.Caps(values(100)))
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(M5RetentionAdmissionV1.Outcome.INSTALLED);
        M5RetentionAdmissionV1.Reservation reservation = reservation("response", 1, 1);

        metadata.casResponseUnknownWithoutApply = true;
        assertThat(admission.reserve(reservation).toCompletableFuture().join().outcome())
                .isEqualTo(M5RetentionAdmissionV1.Outcome.DEFINITIVELY_NOT_APPLIED);
        assertThat(admission.read().toCompletableFuture().join().orElseThrow().reservations())
                .isEmpty();

        metadata.failReadAfterCas = true;
        assertThat(admission.reserve(reservation).toCompletableFuture().join().outcome())
                .isEqualTo(M5RetentionAdmissionV1.Outcome.RESPONSE_UNKNOWN);
        assertThat(admission.read().toCompletableFuture().join().orElseThrow().reservations())
                .containsExactly(reservation);
    }

    @ParameterizedTest
    @EnumSource(M5RetentionAdmissionV1.LimitKindV1.class)
    void everyClosedAdmissionLimitRejectsBeforeItsHardCapIsExceeded(M5RetentionAdmissionV1.LimitKindV1 limitedKind) {
        InMemoryStore metadata = new InMemoryStore(true);
        M5RetentionAdmissionV1 admission = new M5RetentionAdmissionV1(
                metadata, "/binding/admission/" + limitedKind.ordinal(), digest("cell"), BINDING);
        EnumMap<M5RetentionAdmissionV1.LimitKindV1, Long> hardLimits = values(100);
        long hard = limitedKind == M5RetentionAdmissionV1.LimitKindV1.RESERVATIONS
                        || limitedKind == M5RetentionAdmissionV1.LimitKindV1.QUARANTINES
                ? 1
                : 10;
        hardLimits.put(limitedKind, hard);
        assertThat(admission
                        .install(new M5RetentionAdmissionV1.Caps(hardLimits))
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(M5RetentionAdmissionV1.Outcome.INSTALLED);
        long firstAmount = limitedKind == M5RetentionAdmissionV1.LimitKindV1.RESERVATIONS
                        || limitedKind == M5RetentionAdmissionV1.LimitKindV1.QUARANTINES
                ? 1
                : 6;
        long secondAmount = limitedKind == M5RetentionAdmissionV1.LimitKindV1.RETAINED_SOURCE_MAX_AGE_MILLIS
                ? 11
                : limitedKind == M5RetentionAdmissionV1.LimitKindV1.RESERVATIONS
                                || limitedKind == M5RetentionAdmissionV1.LimitKindV1.QUARANTINES
                        ? 1
                        : 5;
        M5RetentionAdmissionV1.Reservation first = reservationForLimit("first", limitedKind, firstAmount);
        assertThat(admission.reserve(first).toCompletableFuture().join().outcome())
                .isEqualTo(M5RetentionAdmissionV1.Outcome.APPLIED_EXACT);

        M5RetentionAdmissionV1.Result rejected = admission
                .reserve(reservationForLimit("second", limitedKind, secondAmount))
                .toCompletableFuture()
                .join();
        assertThat(rejected.outcome()).isEqualTo(M5RetentionAdmissionV1.Outcome.REJECTED_CAP);
        assertThat(rejected.alert().orElseThrow().limitKind()).isEqualTo(limitedKind);
        assertThat(rejected.alert().orElseThrow().hardValue()).isEqualTo(hard);
        assertThat(admission.read().toCompletableFuture().join().orElseThrow().reservations())
                .containsExactly(first);
    }

    @Test
    void pulsarAggregateRetirementInstallsPermanentSameKeyTombstone() {
        InMemoryStore metadata = new InMemoryStore(true);
        String selectorKey = "/pulsar/topic/selector";
        String aggregateKey = "/pulsar/topic/aggregate";
        VersionedValue selector = metadata.seed(selectorKey, bytes("selector-deleted"));
        VersionedValue aggregate = metadata.seed(aggregateKey, bytes("aggregate"));
        PulsarPersistenceName persistenceName = PulsarPersistenceName.fromString("tenant/ns/topic");
        PulsarBindingGeneration generation = new PulsarBindingGeneration(7);
        PulsarTopicGenerationSelectorValueV1 selectorValue = new PulsarTopicGenerationSelectorValueV1(
                persistenceName,
                generation,
                PulsarTopicGenerationSelectorStateV1.DELETED,
                BINDING.bindingId(),
                aggregate.canonicalStoredSha256(),
                selector.canonicalStoredBytes(),
                selector.canonicalStoredSha256());
        ReferenceFreeProofV1 proof = proof(
                metadata,
                "/pulsar-retire-proof",
                ReferenceTargetKindV1.PULSAR_AGGREGATE,
                aggregate.canonicalStoredSha256(),
                selector,
                List.of());
        PulsarTopicIncarnationIdentity incarnation = new PulsarTopicIncarnationIdentity(
                persistenceName, PulsarTopicName.fromString("persistent://tenant/ns/topic"), generation);
        PhysicalCleanupSummaryV1 cleanup = new PhysicalCleanupSummaryV1(
                fact(metadata.seed("/pulsar/topic/cleanup", bytes("cleanup-complete"))),
                incarnation,
                BINDING.bindingId(),
                aggregate.canonicalStoredSha256(),
                CAPABILITY,
                2,
                1,
                1,
                0);
        assertThatThrownBy(() -> new PhysicalCleanupSummaryV1(
                        cleanup.cleanupRoot(),
                        incarnation,
                        BINDING.bindingId(),
                        aggregate.canonicalStoredSha256(),
                        CAPABILITY,
                        2,
                        1,
                        0,
                        1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incomplete");
        RetiredTopicIncarnationTombstoneV1 tombstone =
                M5RetentionCodecV1.finalizeRetiredPulsar(new RetiredTopicIncarnationTombstoneV1(
                        incarnation,
                        BINDING.bindingId(),
                        aggregate.canonicalStoredSha256(),
                        proof.proofSha256(),
                        generation.value(),
                        PulsarTopicGenerationSelectorStateV1.DELETED,
                        selector.metadataVersion(),
                        aggregate.metadataVersion(),
                        aggregate.canonicalStoredSha256(),
                        CAPABILITY,
                        PLACEHOLDER));
        PulsarRetirementRequest staleCleanupRequest = new PulsarRetirementRequest(
                "/pulsar/topic",
                selectorKey,
                selector,
                selectorValue,
                aggregateKey,
                aggregate,
                proof,
                cleanup,
                tombstone);
        ReferenceFreeProofV1 alternateProof = proof(
                metadata,
                "/alternate-pulsar-retire-proof",
                ReferenceTargetKindV1.PULSAR_AGGREGATE,
                aggregate.canonicalStoredSha256(),
                selector,
                List.of());
        RetiredTopicIncarnationTombstoneV1 alternateTombstone =
                M5RetentionCodecV1.finalizeRetiredPulsar(new RetiredTopicIncarnationTombstoneV1(
                        incarnation,
                        BINDING.bindingId(),
                        aggregate.canonicalStoredSha256(),
                        alternateProof.proofSha256(),
                        generation.value(),
                        PulsarTopicGenerationSelectorStateV1.DELETED,
                        selector.metadataVersion(),
                        aggregate.metadataVersion(),
                        aggregate.canonicalStoredSha256(),
                        CAPABILITY,
                        PLACEHOLDER));

        metadata.overwrite(cleanup.cleanupRoot().key(), bytes("cleanup-root-changed"));
        assertThatThrownBy(() -> new M5RetirementCoordinatorV1(metadata)
                        .retirePulsarAggregate(staleCleanupRequest)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(M5ReferenceFreshnessVerifierV1.StaleAuthorityException.class);
        assertThat(metadata.transactionCalls).isZero();
        assertThat(metadata.readNow(aggregateKey)).isEqualTo(aggregate);
        PhysicalCleanupSummaryV1 freshCleanup = new PhysicalCleanupSummaryV1(
                fact(metadata.readNow(cleanup.cleanupRoot().key())),
                incarnation,
                BINDING.bindingId(),
                aggregate.canonicalStoredSha256(),
                CAPABILITY,
                2,
                1,
                1,
                0);
        PulsarRetirementRequest request = new PulsarRetirementRequest(
                "/pulsar/topic",
                selectorKey,
                selector,
                selectorValue,
                aggregateKey,
                aggregate,
                proof,
                freshCleanup,
                tombstone);

        metadata.responseUnknownWithoutApply = true;
        assertThat(new M5RetirementCoordinatorV1(metadata)
                        .retirePulsarAggregate(request)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.DEFINITIVELY_NOT_APPLIED);
        assertThat(metadata.readNow(aggregateKey)).isEqualTo(aggregate);
        assertThat(new M5RetirementCoordinatorV1(metadata)
                        .retirePulsarAggregate(request)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.APPLIED_EXACT);
        assertThat(M5RetentionCodecV1.decodeRetiredPulsar(
                        metadata.readNow(aggregateKey).canonicalStoredBytes()))
                .isEqualTo(tombstone);
        assertThat(new M5RetirementCoordinatorV1(metadata)
                        .retirePulsarAggregate(new PulsarRetirementRequest(
                                "/pulsar/topic",
                                selectorKey,
                                selector,
                                selectorValue,
                                aggregateKey,
                                aggregate,
                                alternateProof,
                                freshCleanup,
                                alternateTombstone))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.ALREADY_RETIRED);
        CanonicalBytes permanentTombstone = metadata.readNow(aggregateKey).canonicalStoredBytes();
        assertThatThrownBy(() -> M4ReadControlCodecV1.decodeProtection(permanentTombstone))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> M4ReadControlCodecV1.decodeBatch(permanentTombstone))
                .isInstanceOf(IllegalArgumentException.class);

        PulsarTopicGenerationSelectorValueV1 sameNameNextGeneration = new PulsarTopicGenerationSelectorValueV1(
                persistenceName,
                new PulsarBindingGeneration(8),
                PulsarTopicGenerationSelectorStateV1.DELETED,
                BINDING.bindingId(),
                aggregate.canonicalStoredSha256(),
                selector.canonicalStoredBytes(),
                selector.canonicalStoredSha256());
        assertThatThrownBy(() -> new M5RetirementCoordinatorV1(metadata)
                        .retirePulsarAggregate(new PulsarRetirementRequest(
                                "/pulsar/topic",
                                selectorKey,
                                selector,
                                sameNameNextGeneration,
                                aggregateKey,
                                aggregate,
                                proof,
                                freshCleanup,
                                tombstone)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("selector");
    }

    private static M5RetirementCoordinatorV1.Outcome reconcileAfterInterference(
            Consumer<RetirementFixture> interference) {
        RetirementFixture fixture = retirementFixture(true);
        fixture.metadata.interfereWithNextTransaction(() -> interference.accept(fixture));
        return new M5RetirementCoordinatorV1(fixture.metadata)
                .externalize(fixture.externalizationRequest())
                .toCompletableFuture()
                .join();
    }

    private static ExternalizationRequest externalizationRequest(
            RetirementFixture fixture, ReferenceFreeProofV1 proof) {
        CanonicalBytes batchBytes = M4ReadControlCodecV1.encodeBatch(fixture.batch);
        FullSourceRetirementBatchV1 full = new FullSourceRetirementBatchV1(
                BatchMetadataStateV1.FULL_V1,
                BINDING,
                fixture.batch.batchIdSha256(),
                Sha256Digest.hash(batchBytes),
                batchBytes,
                fixture.exactSelector.canonicalStoredSha256(),
                proof.proofSha256(),
                CAPABILITY);
        return new ExternalizationRequest(
                fixture.partitionKey,
                fixture.selectorKey,
                fixture.batchKey,
                fixture.exactSelector,
                fixture.selectorSuccessorBytes,
                Optional.empty(),
                full,
                proof);
    }

    private static CanonicalBytes retiredBatchBytes(RetirementFixture fixture, boolean matching) {
        RetiredSourceRetirementBatchTombstoneV1 draft = new RetiredSourceRetirementBatchTombstoneV1(
                BatchMetadataStateV1.RETIRED_V1,
                matching
                        ? BINDING
                        : new BindingIdentity(
                                new TopicBindingId(digest("foreign-binding")),
                                digest("foreign-incarnation"),
                                digest("foreign-storage-epoch")),
                fixture.batch.batchIdSha256(),
                fixture.fullBatch.fullBatchSha256(),
                fixture.proof.proofSha256(),
                fixture.exactSelector.metadataVersion(),
                Sha256Digest.hash(M5RetentionCodecV1.encodeFullBatch(fixture.fullBatch)),
                CAPABILITY,
                PLACEHOLDER);
        return M5RetentionCodecV1.encodeRetiredBatch(M5RetentionCodecV1.finalizeRetiredBatch(draft));
    }

    private static M5RetentionAdmissionV1.Reservation reservation(String id, long bytes, long ageMillis) {
        EnumMap<M5RetentionAdmissionV1.LimitKindV1, Long> reserved = values(0);
        reserved.put(M5RetentionAdmissionV1.LimitKindV1.RESERVATIONS, 1L);
        reserved.put(M5RetentionAdmissionV1.LimitKindV1.RETAINED_SOURCE_BYTES, bytes);
        reserved.put(M5RetentionAdmissionV1.LimitKindV1.RETAINED_SOURCE_MAX_AGE_MILLIS, ageMillis);
        return new M5RetentionAdmissionV1.Reservation(
                digest("reservation-" + id),
                digest("oldest-" + id),
                reserved,
                List.of(ReferenceKindV1.READ_GENERATION_PIN_OR_OPEN_HANDLE),
                Optional.empty());
    }

    private static M5RetentionAdmissionV1.Reservation reservationForLimit(
            String id, M5RetentionAdmissionV1.LimitKindV1 limitedKind, long amount) {
        EnumMap<M5RetentionAdmissionV1.LimitKindV1, Long> reserved = values(0);
        reserved.put(M5RetentionAdmissionV1.LimitKindV1.RESERVATIONS, 1L);
        if (limitedKind != M5RetentionAdmissionV1.LimitKindV1.RESERVATIONS) {
            reserved.put(limitedKind, amount);
        }
        Optional<String> quarantineReason = limitedKind == M5RetentionAdmissionV1.LimitKindV1.QUARANTINES
                ? Optional.of("test quarantine " + id)
                : Optional.empty();
        return new M5RetentionAdmissionV1.Reservation(
                digest("reservation-" + limitedKind + "-" + id),
                digest("oldest-" + limitedKind + "-" + id),
                reserved,
                List.of(ReferenceKindV1.RETIREMENT_OR_DELETE_RECONCILIATION),
                quarantineReason);
    }

    private static EnumMap<M5RetentionAdmissionV1.LimitKindV1, Long> values(long value) {
        EnumMap<M5RetentionAdmissionV1.LimitKindV1, Long> values =
                new EnumMap<>(M5RetentionAdmissionV1.LimitKindV1.class);
        for (M5RetentionAdmissionV1.LimitKindV1 kind : M5RetentionAdmissionV1.LimitKindV1.values()) {
            values.put(kind, value);
        }
        return values;
    }

    private static RetentionFloorSnapshotV1 snapshot(InMemoryStore metadata, long prior, long baseFloor) {
        AuthorityFactV1 owner = fact(metadata.seed("/floors/owner/" + prior + "/" + baseFloor, bytes("owner")));
        AuthorityFactV1 storage = fact(metadata.seed("/floors/storage/" + prior + "/" + baseFloor, bytes("storage")));
        List<RetentionFloorObservationV1> rows = new ArrayList<>();
        for (FloorClassV1 floorClass : FloorClassV1.values()) {
            VersionedValue value = metadata.seed(
                    "/floors/" + prior + "/" + baseFloor + "/" + floorClass.ordinal(), bytes(floorClass.name()));
            rows.add(new RetentionFloorObservationV1(
                    floorClass,
                    fact(value),
                    PositionDomain.KAFKA_OFFSET,
                    baseFloor + floorClass.ordinal(),
                    true,
                    true));
        }
        RetentionFloorSnapshotV1 draft = new RetentionFloorSnapshotV1(
                IDENTITY,
                PositionDomain.KAFKA_OFFSET,
                prior + 1,
                prior,
                digest("policy"),
                owner,
                storage,
                1,
                4_096,
                rows,
                PLACEHOLDER);
        return M5RetentionCodecV1.finalizeSnapshot(draft);
    }

    private static RetirementFixture retirementFixture(boolean transactionSupported) {
        return retirementFixture(transactionSupported, 1);
    }

    private static RetirementFixture retirementFixture(boolean transactionSupported, int sourceCount) {
        if (sourceCount <= 0) {
            throw new IllegalArgumentException("test fixture source count must be positive");
        }
        InMemoryStore metadata = new InMemoryStore(transactionSupported);
        List<SourceProtectionIdentity> sources = new ArrayList<>();
        for (int index = 0; index < sourceCount; index++) {
            sources.add(new SourceProtectionIdentity(
                    digest("source-" + index), 3 + index, 5 + index, 7 + index, CAPABILITY));
        }
        sources.sort(Comparator.comparing(value -> value.sourceIdentitySha256().toHex()));
        Sha256Digest fallback = M4ReadControlCodecV1.calculateFallbackSetSha256(sources);
        SourceRetirementBatch draft = new SourceRetirementBatch(
                BINDING,
                PLACEHOLDER,
                digest("predecessor-core"),
                digest("successor-core"),
                digest("transition"),
                fallback,
                5L + sourceCount,
                5,
                CAPABILITY,
                sources);
        SourceRetirementBatch batch = new SourceRetirementBatch(
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
        BindingReadSelector selector = selector(List.of(batch));
        BindingReadSelector successor = selector(List.of());
        String selectorKey = "/binding/selector";
        String batchKey = "/binding/retirement-batches/" + batch.batchIdSha256().toHex();
        VersionedValue exactSelector = metadata.seed(selectorKey, M4ReadControlCodecV1.encodeSelector(selector));
        List<M4ReleaseBindingV1> releases = new ArrayList<>();
        for (int index = 0; index < sources.size(); index++) {
            SourceProtectionIdentity source = sources.get(index);
            Sha256Digest proofHead = digest("proof-head-" + index);
            CanonicalBytes protectionBytes = M4ReadControlCodecV1.encodeProtection(new SourceProtection(
                    BINDING,
                    source,
                    ProtectionState.RELEASED,
                    Optional.of(batch.batchIdSha256()),
                    Optional.of(proofHead)));
            VersionedValue protection = metadata.seed("/binding/protection/" + index, protectionBytes);
            releases.add(new M4ReleaseBindingV1(
                    source.sourceIdentitySha256(),
                    source.protectionGeneration(),
                    fact(protection),
                    protectionBytes,
                    batch.batchIdSha256(),
                    proofHead));
        }
        ReferenceFreeProofV1 proof = proof(
                metadata,
                "/externalize-proof",
                ReferenceTargetKindV1.RETIREMENT_BATCH,
                batch.batchIdSha256(),
                exactSelector,
                releases);
        CanonicalBytes batchBytes = M4ReadControlCodecV1.encodeBatch(batch);
        FullSourceRetirementBatchV1 full = new FullSourceRetirementBatchV1(
                BatchMetadataStateV1.FULL_V1,
                BINDING,
                batch.batchIdSha256(),
                Sha256Digest.hash(batchBytes),
                batchBytes,
                exactSelector.canonicalStoredSha256(),
                proof.proofSha256(),
                CAPABILITY);
        return new RetirementFixture(
                metadata,
                "/binding",
                selectorKey,
                batchKey,
                exactSelector,
                M4ReadControlCodecV1.encodeSelector(successor),
                batch,
                full,
                releases,
                proof);
    }

    private static PulsarAuthorityFixture pulsarAuthorityFixture() {
        InMemoryStore metadata = new InMemoryStore(false);
        PulsarProtocolCellIdentity cell = new PulsarProtocolCellIdentity(
                new DeploymentId(new Id128(1, 2)),
                new ReservationDomainId(new Id128(3, 4)),
                new PulsarCellId(new Id128(5, 6)));
        PulsarTopicIncarnationIdentity incarnation = new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString("tenant/ns/persistent/orders"),
                PulsarTopicName.fromString("persistent://tenant/ns/orders"),
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
                        new PolicyCatalogDigest(digest("pulsar-policy-catalog")),
                        FrameEncodingPolicyValueV1.none()));
        CanonicalBytes aggregateBytes = Nta1CodecV1.encode(aggregate);
        String aggregateKey = "/pulsar/aggregates/generation-7";
        VersionedValue exactAggregate = metadata.seed(aggregateKey, aggregateBytes);
        String selectorKey = "/pulsar/selectors/orders";
        VersionedValue exactSelector = metadata.seed(selectorKey, bytes("deleted-generation-7"));
        PulsarTopicGenerationSelectorValueV1 selectorValue = new PulsarTopicGenerationSelectorValueV1(
                incarnation.persistenceName(),
                incarnation.bindingGeneration(),
                PulsarTopicGenerationSelectorStateV1.DELETED,
                bindingId,
                exactAggregate.canonicalStoredSha256(),
                exactSelector.canonicalStoredBytes(),
                exactSelector.canonicalStoredSha256());
        BindingIdentity proofBinding =
                new BindingIdentity(bindingId, digest("pulsar-incarnation"), digest("pulsar-storage-epoch"));
        IdentityEnvelope proofIdentity = new IdentityEnvelope(
                digest("pulsar-cell"), digest("pulsar-provider"), proofBinding, 11, 13, 17, CAPABILITY);
        return new PulsarAuthorityFixture(
                metadata,
                aggregateKey,
                selectorKey,
                aggregate,
                incarnation,
                exactAggregate,
                exactSelector,
                selectorValue,
                proofIdentity);
    }

    private static BindingReadSelector selector(List<SourceRetirementBatch> batches) {
        return new BindingReadSelector(
                BINDING,
                digest("selected-view"),
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

    private static ReferenceFreeProofV1 proof(
            InMemoryStore metadata,
            String prefix,
            ReferenceTargetKindV1 targetKind,
            Sha256Digest target,
            VersionedValue selector,
            List<M4ReleaseBindingV1> releases) {
        return proof(metadata, prefix, targetKind, target, selector, releases, IDENTITY);
    }

    private static ReferenceFreeProofV1 proof(
            InMemoryStore metadata,
            String prefix,
            ReferenceTargetKindV1 targetKind,
            Sha256Digest target,
            VersionedValue selector,
            List<M4ReleaseBindingV1> releases,
            IdentityEnvelope identity) {
        AuthorityFactV1 manifest = fact(metadata.seed(prefix + "/manifest", bytes("manifest")));
        AuthorityFactV1 trim = fact(metadata.seed(prefix + "/trim", bytes("trim")));
        AuthorityFactV1 owner = fact(metadata.seed(prefix + "/owner", bytes("owner")));
        AuthorityFactV1 worker = fact(metadata.seed(prefix + "/worker", bytes("worker")));
        AuthorityFactV1 storage = fact(metadata.seed(prefix + "/storage", bytes("storage")));
        AuthorityFactV1 provider = fact(metadata.seed(prefix + "/provider", bytes("provider")));
        List<ReferenceObservationV1> observations = new ArrayList<>();
        List<ReferenceScanSummaryV1> summaries = new ArrayList<>();
        for (ReferenceKindV1 kind : ReferenceKindV1.values()) {
            AuthorityFactV1 authority = fact(metadata.seed(
                    prefix + "/references/" + String.format(java.util.Locale.ROOT, "%02d", kind.ordinal()),
                    bytes(kind.name())));
            observations.add(
                    new ReferenceObservationV1(kind, authority, target, COVERAGE, ReferenceDispositionV1.ABSENT, true));
            summaries.add(new ReferenceScanSummaryV1(kind, 1, 1, 64, true));
        }
        Sha256Digest observationRoot = M5RetentionCodecV1.calculateObservationsRoot(observations);
        ReferenceFreeProofV1 draft = new ReferenceFreeProofV1(
                identity,
                targetKind,
                target,
                COVERAGE,
                fact(selector),
                manifest,
                trim,
                digest(prefix + "/snapshot"),
                observationRoot,
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

    private static ReferenceFreeProofV1 copyProof(
            ReferenceFreeProofV1 proof,
            List<ReferenceObservationV1> observations,
            List<ReferenceScanSummaryV1> summaries) {
        return new ReferenceFreeProofV1(
                proof.identity(),
                proof.targetKind(),
                proof.targetIdentitySha256(),
                proof.coverage(),
                proof.selectorRoot(),
                proof.manifestRoot(),
                proof.trimRoot(),
                proof.retentionSnapshotRootSha256(),
                M5RetentionCodecV1.calculateObservationsRoot(observations),
                proof.m4Releases(),
                proof.ownerFence(),
                proof.workerFence(),
                proof.storageFence(),
                proof.providerFence(),
                proof.auditGraceDeadlineMillis(),
                proof.observedAuthorityTimeMillis(),
                summaries,
                observations,
                PLACEHOLDER);
    }

    private static AuthorityFactV1 fact(VersionedValue value) {
        return new AuthorityFactV1(value.key(), value.metadataVersion(), value.canonicalStoredSha256());
    }

    private static ReferenceWriterEnrollmentV1 writerEnrollment() {
        return new ReferenceWriterEnrollmentV1(
                CAPABILITY,
                List.of(FloorClassV1.values()),
                List.of(ReferenceKindV1.values()),
                digest("ticket-protocol-writer-enrollment"));
    }

    private static M5ClosedWriterRegistryV1 closedWriterRegistry() {
        return new M5ClosedWriterRegistryV1(closedWriterDeclarations(Optional.empty(), Optional.empty()));
    }

    private static List<WriterDeclarationV1> closedWriterDeclarations(
            Optional<FloorClassV1> missingFloor, Optional<ReferenceKindV1> missingReference) {
        List<WriterDeclarationV1> declarations = new ArrayList<>();
        ReferenceKindV1[] references = ReferenceKindV1.values();
        FloorClassV1[] floors = FloorClassV1.values();
        for (int index = 0; index < references.length; index++) {
            ReferenceKindV1 reference = references[index];
            Optional<FloorClassV1> floor = index < floors.length ? Optional.of(floors[index]) : Optional.empty();
            List<ReferenceKindV1> ownedReferences =
                    missingReference.filter(value -> value == reference).isPresent() ? List.of() : List.of(reference);
            List<FloorClassV1> ownedFloors = floor.filter(
                            value -> missingFloor.filter(value::equals).isEmpty())
                    .map(List::of)
                    .orElseGet(List::of);
            declarations.add(new WriterDeclarationV1(
                    String.format(java.util.Locale.ROOT, "proof-bound-writer-%02d", index),
                    CAPABILITY,
                    ownedFloors,
                    ownedReferences,
                    digest("writer-source-" + index)));
        }
        return declarations;
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(bytes(value));
    }

    private record RetirementFixture(
            InMemoryStore metadata,
            String partitionKey,
            String selectorKey,
            String batchKey,
            VersionedValue exactSelector,
            CanonicalBytes selectorSuccessorBytes,
            SourceRetirementBatch batch,
            FullSourceRetirementBatchV1 fullBatch,
            List<M4ReleaseBindingV1> releases,
            ReferenceFreeProofV1 proof) {
        private ExternalizationRequest externalizationRequest() {
            return new ExternalizationRequest(
                    partitionKey,
                    selectorKey,
                    batchKey,
                    exactSelector,
                    selectorSuccessorBytes,
                    Optional.empty(),
                    fullBatch,
                    proof);
        }
    }

    private record PulsarAuthorityFixture(
            InMemoryStore metadata,
            String aggregateKey,
            String selectorKey,
            TopicBindingAggregateV1 aggregate,
            PulsarTopicIncarnationIdentity incarnation,
            VersionedValue exactAggregate,
            VersionedValue exactSelector,
            PulsarTopicGenerationSelectorValueV1 selectorValue,
            IdentityEnvelope proofIdentity) {
        private M5PulsarAggregateRetirementCoordinatorV1.FenceRequest fenceRequest(
                VersionedValue open, String attempt) {
            return new M5PulsarAggregateRetirementCoordinatorV1.FenceRequest(
                    aggregateKey, open, selectorKey, exactSelector, selectorValue, digest(attempt));
        }
    }

    private static final class ControlStore implements CanonicalControlMetadataStore {
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
            Optional<CanonicalBytes> current = Optional.ofNullable(values.get(key));
            if (!current.equals(exactExpected)) {
                return ControlMutationOutcome.DEFINITIVE_CONFLICT;
            }
            values.put(key, exactCandidate);
            return ControlMutationOutcome.APPLIED;
        }
    }

    private static final class InMemoryStore implements ExactMetadataTransactionStoreV1 {
        private final boolean transactionSupported;
        private final Map<String, VersionedValue> values = new LinkedHashMap<>();
        private long version;
        private int transactionCalls;
        private boolean responseUnknownAfterApply;
        private boolean responseUnknownWithoutApply;
        private boolean failReadAfterTransaction;
        private boolean failReadAfterCas;
        private boolean failNextRead;
        private boolean casResponseUnknownWithoutApply;
        private boolean casResponseUnknownAfterApply;
        private int casCalls;
        private Runnable nextTransactionInterference;

        private InMemoryStore(boolean transactionSupported) {
            this.transactionSupported = transactionSupported;
        }

        synchronized VersionedValue seed(String key, CanonicalBytes value) {
            if (values.containsKey(key)) {
                throw new IllegalArgumentException("duplicate test seed: " + key);
            }
            VersionedValue stored = stored(key, value);
            values.put(key, stored);
            return stored;
        }

        synchronized VersionedValue overwrite(String key, CanonicalBytes value) {
            if (!values.containsKey(key)) {
                throw new IllegalArgumentException("missing test value: " + key);
            }
            VersionedValue stored = stored(key, value);
            values.put(key, stored);
            return stored;
        }

        synchronized void remove(String key) {
            values.remove(key);
        }

        synchronized void interfereWithNextTransaction(Runnable interference) {
            if (nextTransactionInterference != null) {
                throw new IllegalStateException("test transaction interference is already installed");
            }
            nextTransactionInterference = interference;
        }

        synchronized VersionedValue readNow(String key) {
            return Optional.ofNullable(values.get(key)).orElseThrow();
        }

        synchronized Optional<VersionedValue> readOptional(String key) {
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public synchronized CompletionStage<Optional<VersionedValue>> read(String key) {
            if (failNextRead) {
                failNextRead = false;
                return CompletableFuture.failedFuture(new IllegalStateException("injected authoritative read loss"));
            }
            return CompletableFuture.completedFuture(Optional.ofNullable(values.get(key)));
        }

        @Override
        public synchronized CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> exactPredecessor, String key, CanonicalBytes exactCandidate) {
            casCalls++;
            Optional<VersionedValue> current = Optional.ofNullable(values.get(key));
            if (!current.equals(exactPredecessor)) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            if (casResponseUnknownWithoutApply) {
                casResponseUnknownWithoutApply = false;
                return CompletableFuture.completedFuture(MutationOutcome.RESPONSE_UNKNOWN);
            }
            values.put(key, stored(key, exactCandidate));
            if (casResponseUnknownAfterApply) {
                casResponseUnknownAfterApply = false;
                return CompletableFuture.completedFuture(MutationOutcome.RESPONSE_UNKNOWN);
            }
            if (failReadAfterCas) {
                failReadAfterCas = false;
                failNextRead = true;
            }
            return CompletableFuture.completedFuture(MutationOutcome.APPLIED_EXACT);
        }

        @Override
        public synchronized CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
            transactionCalls++;
            if (!transactionSupported) {
                return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
            }
            boolean matches = transaction.conditions().stream()
                    .allMatch(condition ->
                            Optional.ofNullable(values.get(condition.key())).equals(condition.expected()));
            if (!matches) {
                return CompletableFuture.completedFuture(TransactionOutcome.CONDITIONS_UNCHANGED);
            }
            if (nextTransactionInterference != null) {
                Runnable interference = nextTransactionInterference;
                nextTransactionInterference = null;
                interference.run();
                return CompletableFuture.completedFuture(TransactionOutcome.RESPONSE_UNKNOWN);
            }
            if (responseUnknownWithoutApply) {
                responseUnknownWithoutApply = false;
                return CompletableFuture.completedFuture(TransactionOutcome.RESPONSE_UNKNOWN);
            }
            for (ExactPut put : transaction.puts()) {
                values.put(put.key(), stored(put.key(), put.canonicalCandidate()));
            }
            if (failReadAfterTransaction) {
                failReadAfterTransaction = false;
                failNextRead = true;
            }
            if (responseUnknownAfterApply) {
                responseUnknownAfterApply = false;
                return CompletableFuture.completedFuture(TransactionOutcome.RESPONSE_UNKNOWN);
            }
            return CompletableFuture.completedFuture(TransactionOutcome.APPLIED_EXACT);
        }

        @Override
        public boolean supportsAtomicMultiKeyTransactions() {
            return transactionSupported;
        }

        private VersionedValue stored(String key, CanonicalBytes value) {
            MetadataVersion metadataVersion = new MetadataVersion(CanonicalBytes.copyOf(
                    ByteBuffer.allocate(Long.BYTES).putLong(version++).array()));
            return VersionedValue.of(key, value, metadataVersion);
        }
    }
}
