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
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
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
import com.nereusstream.storage.object.retention.M5LogicalTrimCoordinatorV1.Outcome;
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
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredTopicIncarnationTombstoneV1;
import com.nereusstream.storage.object.retention.M5RetirementCoordinatorV1.BatchRetirementRequest;
import com.nereusstream.storage.object.retention.M5RetirementCoordinatorV1.ExternalizationRequest;
import com.nereusstream.storage.object.retention.M5RetirementCoordinatorV1.PulsarRetirementRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
        PulsarRetirementRequest request = new PulsarRetirementRequest(
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
                                cleanup,
                                alternateTombstone))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5RetirementCoordinatorV1.Outcome.ALREADY_RETIRED);

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
                                cleanup,
                                tombstone)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("selector");
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
        InMemoryStore metadata = new InMemoryStore(transactionSupported);
        SourceProtectionIdentity source = new SourceProtectionIdentity(digest("source"), 3, 5, 7, CAPABILITY);
        List<SourceProtectionIdentity> sources = List.of(source);
        Sha256Digest fallback = M4ReadControlCodecV1.calculateFallbackSetSha256(sources);
        SourceRetirementBatch draft = new SourceRetirementBatch(
                BINDING,
                PLACEHOLDER,
                digest("predecessor-core"),
                digest("successor-core"),
                digest("transition"),
                fallback,
                9,
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
        Sha256Digest proofHead = digest("proof-head");
        CanonicalBytes protectionBytes = M4ReadControlCodecV1.encodeProtection(new SourceProtection(
                BINDING, source, ProtectionState.RELEASED, Optional.of(batch.batchIdSha256()), Optional.of(proofHead)));
        VersionedValue protection = metadata.seed("/binding/protection", protectionBytes);
        M4ReleaseBindingV1 release = new M4ReleaseBindingV1(
                source.sourceIdentitySha256(),
                source.protectionGeneration(),
                fact(protection),
                protectionBytes,
                batch.batchIdSha256(),
                proofHead);
        ReferenceFreeProofV1 proof = proof(
                metadata,
                "/externalize-proof",
                ReferenceTargetKindV1.RETIREMENT_BATCH,
                batch.batchIdSha256(),
                exactSelector,
                List.of(release));
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
                List.of(release),
                proof);
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
                IDENTITY,
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

    private static final class InMemoryStore implements ExactMetadataTransactionStoreV1 {
        private final boolean transactionSupported;
        private final Map<String, VersionedValue> values = new LinkedHashMap<>();
        private long version;
        private int transactionCalls;
        private boolean responseUnknownAfterApply;
        private boolean responseUnknownWithoutApply;
        private boolean failReadAfterTransaction;
        private boolean failNextRead;

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
            Optional<VersionedValue> current = Optional.ofNullable(values.get(key));
            if (!current.equals(exactPredecessor)) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            values.put(key, stored(key, exactCandidate));
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
