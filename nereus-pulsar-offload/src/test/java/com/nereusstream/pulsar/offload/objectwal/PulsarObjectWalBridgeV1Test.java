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

package com.nereusstream.pulsar.offload.objectwal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.AppendAck;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.AppendInput;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.BridgeException;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.BridgeRejectionCode;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.Configuration;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ExtentIdentity;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ExtentLocator;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.FailedAppend;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.FailedMember;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.MemberAppendResult;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.MemberFailure;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ObjectWalExtentStore;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PublishResult;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ReadEntry;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ReadFailureScope;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ReadSource;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.RecoverySeed;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.SealedExtentPlan;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.VerifiedAppend;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.VerifiedMember;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.FixedSlice;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.HeadSnapshot;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.LedgerChainAuthority;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.LedgerNode;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.MutationKind;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.MutationOutcome;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.LaneSequenceVector;
import com.nereusstream.storage.object.control.Nwg1RootAdmissionCaps;
import com.nereusstream.storage.object.control.ObjectProviderAccessProfile;
import com.nereusstream.storage.object.control.ObjectProviderRootConfiguration;
import com.nereusstream.storage.object.control.ProviderProofMode;
import com.nereusstream.storage.object.control.ProviderResolvedExtentDescriptor;
import com.nereusstream.storage.object.control.ProviderResolvedExtentRowV1;
import com.nereusstream.storage.object.control.ProviderVersionProof;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalCheckpointPolicy;
import com.nereusstream.storage.object.control.WalCheckpointPublisher;
import com.nereusstream.storage.object.control.WalLaneId;
import com.nereusstream.storage.object.control.WalRunBounds;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunFormatContractV1;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.kms.KmsTransport;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import com.nereusstream.storage.object.nwg1.GroupEncodingPlanV1;
import com.nereusstream.storage.object.nwg1.Nwg1CloseReasonV1;
import com.nereusstream.storage.object.nwg1.Nwg1CommitmentsV1;
import com.nereusstream.storage.object.nwg1.Nwg1DirectoryV1;
import com.nereusstream.storage.object.nwg1.Nwg1EnvelopeV1;
import com.nereusstream.storage.object.nwg1.Nwg1IsolationScopeV1;
import com.nereusstream.storage.object.nwg1.Nwg1RejectionV1;
import com.nereusstream.storage.object.nwg1.Nwg1ValidationStageV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeLimits;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class PulsarObjectWalBridgeV1Test {
    private static final long START = PulsarVirtualLedgerChainControllerV1.RESERVED_START_INCLUSIVE;
    private static final Sha256Digest ROOT_A = digest('a');
    private static final Sha256Digest ROOT_C = digest('c');
    private static final Sha256Digest EXTENT_SHA = digest('b');
    private static final String FRAME_SHA = "d".repeat(64);
    private static final PulsarBindingKey BINDING_A = binding("a");
    private static final PulsarBindingKey BINDING_B = binding("b");
    private static final Configuration CONFIGURATION = new Configuration(1024, 8, 8, 8, 8, 8, 4096);

    @Test
    void publishesSharedExtentWithExactPositionsAndIndependentBindingFrontiers() {
        Fixture fixture = fixture(START, START + 1);
        fixture.activate(BINDING_A, START);
        fixture.activate(BINDING_B, START + 1);

        List<AppendAck> acknowledgements = verifiedAcks(fixture.bridge
                .appendShared(List.of(input(BINDING_A, "a-0"), input(BINDING_B, "b-0")))
                .toCompletableFuture()
                .join());

        assertThat(acknowledgements).hasSize(2);
        assertThat(acknowledgements.get(0).position()).isEqualTo(new PulsarObjectWalBridgeV1.PulsarPosition(START, 0));
        assertThat(acknowledgements.get(1).position())
                .isEqualTo(new PulsarObjectWalBridgeV1.PulsarPosition(START + 1, 0));
        assertThat(acknowledgements)
                .extracting(ack -> ack.locator().identity())
                .containsOnly(acknowledgements.get(0).locator().identity());
        assertThat(acknowledgements)
                .extracting(ack -> ack.bindingFrontiers().bindingDurableThrough())
                .containsExactly(0L, 0L);
        assertThat(acknowledgements.get(0).physicalFrontier().laneSequence()).isZero();
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).ticketsIssued())
                .isOne();
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_B).ticketsIssued())
                .isOne();
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).reservedLocatorCount())
                .isZero();
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_B).reservedLocatorCount())
                .isZero();
        assertThat(fixture.authority.reads).isEqualTo(2);
        assertThat(fixture.authority.mutations).isEqualTo(2);
    }

    @Test
    void installsActiveTailBeforeAckAndReadsEachSharedMemberWithoutCrossBindingPoisoning() {
        Fixture fixture = fixture(START, START + 1);
        fixture.activate(BINDING_A, START);
        fixture.activate(BINDING_B, START + 1);
        List<AppendAck> acknowledgements = verifiedAcks(fixture.bridge
                .appendShared(List.of(input(BINDING_A, "a-0"), input(BINDING_B, "b-0")))
                .toCompletableFuture()
                .join());
        fixture.store.bindingEntryFailure = BINDING_A;

        assertThatThrownBy(() -> fixture.bridge
                        .read(BINDING_A, acknowledgements.get(0).position())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("binding-local frame failure");
        ReadEntry bindingB = fixture.bridge
                .read(BINDING_B, acknowledgements.get(1).position())
                .toCompletableFuture()
                .join();

        assertThat(bindingB.source()).isEqualTo(ReadSource.ACTIVE_TAIL);
        assertThat(bindingB.validatedFailureScope()).isEqualTo(ReadFailureScope.NONE);
        assertThat(new String(bindingB.payload(), StandardCharsets.UTF_8)).isEqualTo("b-0");
    }

    @Test
    void bindingLocalAppendFailureRetainsOnlyItsTypedGapWhileSiblingPublishesAndContinues() {
        Fixture fixture = fixture(START, START + 1);
        fixture.activate(BINDING_A, START);
        fixture.activate(BINDING_B, START + 1);
        fixture.store.bindingPublishFailure = BINDING_A;

        List<MemberAppendResult> results = fixture.bridge
                .appendShared(List.of(input(BINDING_A, "a-0"), input(BINDING_B, "b-0")))
                .toCompletableFuture()
                .join();

        assertThat(results).hasSize(2);
        FailedAppend failed = (FailedAppend) results.get(0);
        AppendAck verified = verifiedAck(results.get(1));
        assertThat(failed.binding()).isEqualTo(BINDING_A);
        assertThat(failed.failure().rejection()).isEqualTo(Nwg1RejectionV1.NATIVE_CHECKSUM_MISMATCH);
        assertThat(failed.failure().stage()).isEqualTo(Nwg1ValidationStageV1.NATIVE_FRAME);
        assertThat(failed.failure().scope()).isEqualTo(Nwg1IsolationScopeV1.APPEND_UNIT);
        assertThat(failed.physicalFrontier().laneSequence()).isZero();
        assertThat(verified.binding()).isEqualTo(BINDING_B);
        assertThat(fixture.bridge.frontiers(BINDING_A).bindingDurableThrough()).isEqualTo(-1);
        assertThat(fixture.bridge.frontiers(BINDING_B).bindingDurableThrough()).isZero();
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).reservedLocatorCount())
                .isOne();
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_B).reservedLocatorCount())
                .isZero();

        fixture.store.bindingPublishFailure = null;
        AppendAck bindingBNext = verifiedAck(fixture.bridge
                .appendShared(List.of(input(BINDING_B, "b-1")))
                .toCompletableFuture()
                .join()
                .get(0));
        assertThat(bindingBNext.position().entryId()).isEqualTo(1);

        AppendAck recoveredA = verifiedAck(fixture.bridge
                .resumeSameEntry(fixture.store.firstPlanId)
                .toCompletableFuture()
                .join()
                .get(0));
        assertThat(recoveredA.position().entryId()).isZero();
        assertThat(fixture.bridge.frontiers(BINDING_A).bindingDurableThrough()).isZero();
    }

    @Test
    void perMemberFailureCannotMasqueradeAsSharedOrPreBindingValidation() {
        assertThatThrownBy(() -> new MemberFailure(
                        Nwg1RejectionV1.DIGEST_MISMATCH,
                        Nwg1ValidationStageV1.OBJECT_BODY_DIGEST,
                        Nwg1IsolationScopeV1.SHARED_OBJECT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BINDING or APPEND_UNIT");
        assertThatThrownBy(() -> new MemberFailure(
                        Nwg1RejectionV1.AUTHORITY_MISMATCH,
                        Nwg1ValidationStageV1.ROOT_AUTHORITY,
                        Nwg1IsolationScopeV1.BINDING))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only after shared Object verification");
    }

    @Test
    void rawNativeResultRoundTripsWithExactSourceJunitCountersAndM6Exclusion() {
        PulsarObjectWalNativeResultV1.Receipt receipt = nativeReceipt();

        byte[] canonical = PulsarObjectWalNativeResultV1.canonicalBytes(receipt);
        PulsarObjectWalNativeResultV1.Receipt parsed = PulsarObjectWalNativeResultV1.parseCanonical(canonical);

        assertThat(parsed).isEqualTo(receipt);
        assertThat(parsed.componentKind()).isEqualTo("P_PULSAR_OBJECT_WAL");
        assertThat(parsed.junit().totals().skipped()).isZero();
        assertThat(parsed.counters().fixedSliceLedgerIds()).isEqualTo(1L << 40);
        assertThat(parsed.counters().completionTicketBits()).isEqualTo(64);
        assertThat(parsed.exclusions()).containsExactly("M6_NATIVE_BROKER_CONTROLLER_ACTIVATION");
    }

    @Test
    void rawNativeResultRejectsCallerStatusTrailingBytesAndReceiptHashTampering() {
        PulsarObjectWalNativeResultV1.Receipt valid = nativeReceipt();
        byte[] canonical = PulsarObjectWalNativeResultV1.canonicalBytes(valid);

        byte[] trailing = java.util.Arrays.copyOf(canonical, canonical.length + 1);
        trailing[trailing.length - 1] = '\n';
        assertThatThrownBy(() -> PulsarObjectWalNativeResultV1.parseCanonical(trailing))
                .isInstanceOf(IllegalArgumentException.class);

        String changedStatus =
                new String(canonical, StandardCharsets.UTF_8).replace("\"status\":\"PASS\"", "\"status\":\"FAIL\"");
        assertThatThrownBy(() ->
                        PulsarObjectWalNativeResultV1.parseCanonical(changedStatus.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class);

        String changedHash =
                new String(canonical, StandardCharsets.UTF_8).replace(valid.receiptSha256(), "f".repeat(64));
        assertThatThrownBy(() ->
                        PulsarObjectWalNativeResultV1.parseCanonical(changedHash.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256");

        PulsarObjectWalNativeResultV1.Receipt callerResealedWrongSource =
                PulsarObjectWalNativeResultV1.sealForTest(new PulsarObjectWalNativeResultV1.Receipt(
                        valid.schema(),
                        valid.componentKind(),
                        valid.status(),
                        new PulsarObjectWalNativeResultV1.TestedSource(
                                "caller-selected-repository",
                                valid.testedSource().commit(),
                                valid.testedSource().treeSha256()),
                        valid.externalSources(),
                        valid.execution(),
                        valid.junit(),
                        valid.requiredTests(),
                        valid.counters(),
                        valid.artifacts(),
                        valid.exclusions(),
                        "0".repeat(64)));
        assertThatThrownBy(() -> PulsarObjectWalNativeResultV1.canonicalBytes(callerResealedWrongSource))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("closed identity/inventory");
    }

    @Test
    void rawNativeResultRejectsAnyFailureErrorOrSkipCounter() {
        PulsarObjectWalNativeResultV1.Receipt valid = nativeReceipt();
        PulsarObjectWalNativeResultV1.JunitEvidence invalidJunit = new PulsarObjectWalNativeResultV1.JunitEvidence(
                valid.junit().xmlRoot(),
                valid.junit().xmlFiles(),
                new PulsarObjectWalNativeResultV1.JunitTotals(2, 46, 0, 0, 1));
        PulsarObjectWalNativeResultV1.Receipt invalid = new PulsarObjectWalNativeResultV1.Receipt(
                valid.schema(),
                valid.componentKind(),
                valid.status(),
                valid.testedSource(),
                valid.externalSources(),
                valid.execution(),
                invalidJunit,
                valid.requiredTests(),
                valid.counters(),
                valid.artifacts(),
                valid.exclusions(),
                "0".repeat(64));

        assertThatThrownBy(() -> PulsarObjectWalNativeResultV1.canonicalBytes(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("zero-failure/skip");
    }

    @Test
    void productionObjectWalAdapterOwnsCommonSessionAndExposesNoPlaintextKeyOrListBudget() {
        assertThat(PulsarObjectWalBridgeV1.ProductionObjectWalExtentStore.class).isAssignableTo(AutoCloseable.class);
        assertThat(java.util.Arrays.stream(
                                PulsarObjectWalBridgeV1.ProductionObjectWalContext.class.getRecordComponents())
                        .map(java.lang.reflect.RecordComponent::getType))
                .containsExactly(Nwg1VerificationContextV1.class, java.util.function.LongSupplier.class)
                .doesNotContain(byte[].class);
        assertThat(java.util.Arrays.stream(PulsarObjectWalBridgeV1.ProductionWalRunClosure.class.getRecordComponents())
                        .map(java.lang.reflect.RecordComponent::getType))
                .doesNotContain(ProviderProofMode.class);
        assertThat(java.util.Arrays.stream(
                                PulsarObjectWalBridgeV1.ProductionObjectWalExtentStore.class.getConstructors())
                        .map(java.lang.reflect.Constructor::getParameterTypes)
                        .map(parameters -> parameters[0]))
                .containsOnly(WalRunObjectSession.class);
    }

    @Test
    void pulsarOwnerFenceCoversRecoveryAndRejectsOldOwnerLatePut() throws Exception {
        WalRunRootRecord root = objectWalRoot();
        WalRunReference reference = objectWalReference(root);
        TestActiveOwnerFenceAuthority authority = new TestActiveOwnerFenceAuthority(8);
        Nwg1VerificationContextV1 verification = verificationContext(root);
        var executor = new PulsarObjectWalBridgeV1.PulsarProtocolOwnerFenceExecutor(authority, verification);
        int[] providerPuts = {0};

        executor.withDurableOwnerFence(
                root.protocolCellIdentity(), reference, reference.rootSha256(), exactVerification -> {
                    assertThat(exactVerification).isSameAs(verification);
                    assertThat(authority.fenceHeld).isTrue();
                    assertThat(authority.attemptProviderPut(7, () -> providerPuts[0]++))
                            .isFalse();
                    assertThat(providerPuts[0]).isZero();
                    assertThat(authority.attemptProviderPut(8, () -> providerPuts[0]++))
                            .isTrue();
                    return null;
                });

        assertThat(authority.fenceHeld).isFalse();
        assertThat(authority.rejectedOldOwnerPuts).isOne();
        assertThat(providerPuts[0]).isOne();
    }

    @Test
    void pulsarOwnerFenceRejectsMonotonicRollbackBeforeRecoveryCallback() throws Exception {
        WalRunRootRecord root = objectWalRoot();
        WalRunReference reference = objectWalReference(root);
        TestActiveOwnerFenceAuthority authority = new TestActiveOwnerFenceAuthority(8);
        var executor =
                new PulsarObjectWalBridgeV1.PulsarProtocolOwnerFenceExecutor(authority, verificationContext(root));
        int[] callbacks = {0};
        executor.withDurableOwnerFence(root.protocolCellIdentity(), reference, reference.rootSha256(), ignored -> {
            callbacks[0]++;
            return null;
        });

        authority.ownerEpoch = 7;
        assertThatThrownBy(() -> executor.withDurableOwnerFence(
                        root.protocolCellIdentity(), reference, reference.rootSha256(), ignored -> {
                            callbacks[0]++;
                            return null;
                        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("regressed");
        assertThat(callbacks[0]).isOne();
        assertThat(authority.fenceHeld).isFalse();
    }

    @Test
    void pulsarOwnerFenceReleasesAuthorityAfterRecoveryCallbackFailure() throws Exception {
        WalRunRootRecord root = objectWalRoot();
        WalRunReference reference = objectWalReference(root);
        TestActiveOwnerFenceAuthority authority = new TestActiveOwnerFenceAuthority(8);
        var executor =
                new PulsarObjectWalBridgeV1.PulsarProtocolOwnerFenceExecutor(authority, verificationContext(root));

        assertThatThrownBy(() -> executor.withDurableOwnerFence(
                        root.protocolCellIdentity(), reference, reference.rootSha256(), ignored -> {
                            throw new IllegalStateException("owner-open callback failed");
                        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("callback failed");
        assertThat(authority.fenceHeld).isFalse();

        executor.withDurableOwnerFence(root.protocolCellIdentity(), reference, reference.rootSha256(), ignored -> null);
        assertThat(authority.callbacks).isEqualTo(2);
        assertThat(authority.fenceHeld).isFalse();
    }

    @Test
    void unknownDrainRetainsCommonRecoveryAndKmsUntilReconcileThenTerminalClose() throws Exception {
        WalRunRootRecord root = objectWalRoot();
        UnknownObjectTransport transport = new UnknownObjectTransport();
        C1ObjectProviderSession provider = new C1ObjectProviderSession(
                transport,
                root.providerScopeId(),
                root.providerConfiguration().exclusiveNamespacePrefix(),
                root.providerConfiguration().maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
        KmsCellSession kms = kms(root);
        TestControlMetadataStore metadata = new TestControlMetadataStore();
        WalRunLifecycleManager ownerLifecycle = new WalRunLifecycleManager(metadata);
        var ownerRoot = ownerLifecycle.createRootAndInitializePointer(
                WalRunControlKeys.rootKey(root.shardId(), root.shardRunEpoch()), root);
        WalRunObjectSession session =
                WalRunObjectSession.openNew(ownerRoot.ownerAuthority().orElseThrow(), provider, kms, () -> 0);
        PulsarObjectWalBridgeV1.PlannedEntry member = new PulsarObjectWalBridgeV1.PlannedEntry(
                BINDING_A,
                7,
                new PulsarObjectWalBridgeV1.PulsarPosition(START, 0),
                id128("production-response-unknown"),
                "payload".getBytes(StandardCharsets.UTF_8));
        Nwg1DirectoryV1.BindingContext exactContext = bindingContext(root, BINDING_A);
        PulsarObjectWalBridgeV1.ProductionObjectWalExtentStore store = productionStore(
                session,
                root,
                (lane, policy, members, maximum) -> preparedPlan(root, members, exactContext, lane, policy),
                (binding, authenticatedContext) -> {},
                metadata);

        SealedExtentPlan plan = store.sealPlan(WalLaneId.OBJECT_BALANCED, 3, List.of(member), 4096);
        PublishResult unknown = store.publish(plan).toCompletableFuture().join();
        assertThat(unknown.outcome()).isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);
        store.drain();
        assertThatThrownBy(store::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exact candidates");
        assertThat(session.state()).isEqualTo(WalRunObjectSession.State.OPEN);
        assertThat(kms.state()).isEqualTo(KmsCellSession.State.OPEN);
        assertThat(kms.cachedRunKeyCount()).isOne();
        assertThat(metadata.contains(WalRunControlKeys.sealKey(root.shardId(), root.shardRunEpoch())))
                .isFalse();

        PublishResult reconciled =
                store.reconcile(plan, unknown.identity()).toCompletableFuture().join();
        assertThat(reconciled.outcome()).isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
        store.close();
        assertThat(session.state()).isEqualTo(WalRunObjectSession.State.CLOSED);
        assertThat(metadata.contains(WalRunControlKeys.sealKey(root.shardId(), root.shardRunEpoch())))
                .isTrue();
        assertThat(kms.cachedRunKeyCount()).isZero();
        assertThat(transport.putCalls).isOne();
        assertThat(transport.listCalls).isOne();
        assertThat(transport.fullGetCalls).isEqualTo(2);
    }

    @Test
    void productionAtomicProjectionIgnoresExistingVersionTokenUnderRootNoneAndPublishesClosure() {
        WalRunRootRecord root = objectWalRoot();
        ExistingExactObjectTransport transport = new ExistingExactObjectTransport();
        C1ObjectProviderSession provider = new C1ObjectProviderSession(
                transport,
                root.providerScopeId(),
                root.providerConfiguration().exclusiveNamespacePrefix(),
                root.providerConfiguration().maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
        KmsCellSession kms = kms(root);
        TestControlMetadataStore metadata = new TestControlMetadataStore();
        WalRunLifecycleManager ownerLifecycle = new WalRunLifecycleManager(metadata);
        var ownerRoot = ownerLifecycle.createRootAndInitializePointer(
                WalRunControlKeys.rootKey(root.shardId(), root.shardRunEpoch()), root);
        WalRunObjectSession session =
                WalRunObjectSession.openNew(ownerRoot.ownerAuthority().orElseThrow(), provider, kms, () -> 0);
        PulsarObjectWalBridgeV1.PlannedEntry member = new PulsarObjectWalBridgeV1.PlannedEntry(
                BINDING_A,
                7,
                new PulsarObjectWalBridgeV1.PulsarPosition(START, 0),
                id128("production-existing-exact"),
                "payload".getBytes(StandardCharsets.UTF_8));
        Nwg1DirectoryV1.BindingContext exactContext = bindingContext(root, BINDING_A);
        PulsarObjectWalBridgeV1.ProductionObjectWalExtentStore store = productionStore(
                session,
                root,
                (lane, policy, members, maximum) -> preparedPlan(root, members, exactContext, lane, policy),
                (binding, authenticated) -> {
                    assertThat(binding).isEqualTo(BINDING_A);
                    assertThat(authenticated.bindingId()).containsExactly(exactContext.bindingId());
                },
                metadata);

        var emptyCheckpoint = store.verifyPhysicalCheckpoint(
                metadata, WalCheckpointHeadV1.empty(WalRunControlCodec.rootSha256(root), root.shardRunEpoch(), 1));
        assertThat(emptyCheckpoint.walRunRootSha256()).isEqualTo(WalRunControlCodec.rootSha256(root));
        assertThat(emptyCheckpoint.coveredThrough()).isEqualTo(LaneSequenceVector.empty());
        assertThat(emptyCheckpoint.aggregateExtentCount()).isZero();
        assertThat(emptyCheckpoint.aggregateCanonicalBodyBytes()).isZero();

        SealedExtentPlan plan = store.sealPlan(WalLaneId.OBJECT_BALANCED, 3, List.of(member), 4096);
        var substitutedMember = new PulsarObjectWalBridgeV1.PlannedEntry(
                member.binding(),
                member.ownerEpoch(),
                member.position(),
                id128("substituted-append-commit-set"),
                member.payload());
        var substitutedPlan = new SealedExtentPlan(
                plan.planId(),
                plan.laneId(),
                plan.packingPolicyVersion(),
                List.of(substitutedMember),
                plan.maximumCanonicalBodyBytes());
        assertBridgeFailure(
                () -> store.publish(substitutedPlan).toCompletableFuture().join(),
                BridgeRejectionCode.RESOLVED_EXTENT_INVALID);
        PublishResult published = store.publish(plan).toCompletableFuture().join();

        assertThat(published.outcome()).isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);
        assertThat(published.resolvedDescriptor().orElseThrow().row().providerProof())
                .isEqualTo(ProviderVersionProof.none());
        // C1 EXISTING_EXACT reconciliation performs one identity GET; publication then performs its sole shared
        // full-body authentication GET and every selected member verifies from that token with zero further I/O.
        assertThat(transport.fullGetCalls).isEqualTo(2);
        assertThat(session.runtimeRecoveryState().resolvedExtentCount()).isOne();
        String checkpointHeadKey = WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch());
        var recoveredCheckpoint = store.verifyPhysicalCheckpoint(
                metadata,
                WalRunControlCodec.decodeCheckpointHead(
                        metadata.get(checkpointHeadKey).orElseThrow()));
        assertThat(recoveredCheckpoint.coveredThrough().get(WalLaneId.OBJECT_BALANCED))
                .isZero();
        assertThat(recoveredCheckpoint.aggregateExtentCount()).isOne();
        Fixture recoveredBridge = fixture(START);
        recoveredBridge.bridge.initializePhysicalFrontiers(recoveredCheckpoint);
        assertThat(recoveredBridge
                        .bridge
                        .physicalFrontier(WalRunControlCodec.rootSha256(root), WalLaneId.OBJECT_BALANCED)
                        .laneSequence())
                .isZero();
        store.drain();
        store.close();
        assertThat(session.state()).isEqualTo(WalRunObjectSession.State.CLOSED);
        assertThat(kms.cachedRunKeyCount()).isZero();
    }

    @Test
    void definitiveAbsenceStopsAdmissionThenResumeUsesTheSameEntry() {
        Fixture fixture = fixture(START);
        fixture.activate(BINDING_A, START);
        fixture.store.nextOutcome = ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED;

        assertBridgeFailure(
                () -> fixture.bridge
                        .appendShared(List.of(input(BINDING_A, "a-0")))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.PROVIDER_DEFINITIVELY_ABSENT);
        String failedPlan = fixture.store.lastPlan.planId();
        assertBridgeFailure(
                () -> fixture.bridge
                        .appendShared(List.of(input(BINDING_A, "a-1")))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.WALRUN_ADMISSION_STOPPED);

        fixture.store.nextOutcome = ProviderObjectOutcome.APPLIED_EXACT;
        AppendAck resumed = verifiedAck(fixture.bridge
                .resumeSameEntry(failedPlan)
                .toCompletableFuture()
                .join()
                .get(0));
        AppendAck next = verifiedAck(fixture.bridge
                .appendShared(List.of(input(BINDING_A, "a-1")))
                .toCompletableFuture()
                .join()
                .get(0));

        assertThat(resumed.position().entryId()).isZero();
        assertThat(next.position().entryId()).isEqualTo(1);
        assertThat(fixture.store.resumePositions).containsExactly(resumed.position());
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).reservedLocatorCount())
                .isZero();
    }

    @Test
    void exceptionalPublishRetainsTheExactPositionAndTicketForSameEntryRecovery() {
        Fixture fixture = fixture(START);
        fixture.activate(BINDING_A, START);
        fixture.store.failPublishStage = true;

        assertBridgeFailure(
                () -> fixture.bridge
                        .appendShared(List.of(input(BINDING_A, "exceptional-publish")))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.PROVIDER_OUTCOME_UNKNOWN);
        String retainedPlan = fixture.store.lastPlan.planId();
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).reservedLocatorCount())
                .isOne();
        assertThat(fixture.bridge.frontiers(BINDING_A).bindingDurableThrough()).isEqualTo(-1);
        assertBridgeFailure(
                () -> discardUnderFence(fixture.bridge, Map.of(BINDING_A, 7L)),
                BridgeRejectionCode.PROVIDER_OUTCOME_UNKNOWN);
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).reservedLocatorCount())
                .isOne();

        fixture.store.failPublishStage = false;
        AppendAck recovered = verifiedAck(fixture.bridge
                .resumeSameEntry(retainedPlan)
                .toCompletableFuture()
                .join()
                .get(0));
        assertThat(recovered.position().entryId()).isZero();
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).reservedLocatorCount())
                .isZero();
    }

    @Test
    void exceptionalReconcileRetainsUnknownPositionAndTicketForSameEntryRecovery() {
        Fixture fixture = fixture(START);
        fixture.activate(BINDING_A, START);
        fixture.store.nextOutcome = ProviderObjectOutcome.OUTCOME_UNKNOWN;
        fixture.store.failReconcileStage = true;

        assertBridgeFailure(
                () -> fixture.bridge
                        .appendShared(List.of(input(BINDING_A, "exceptional-reconcile")))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.PROVIDER_OUTCOME_UNKNOWN);
        String retainedPlan = fixture.store.lastPlan.planId();
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).reservedLocatorCount())
                .isOne();

        fixture.store.failReconcileStage = false;
        fixture.store.nextOutcome = ProviderObjectOutcome.APPLIED_EXACT;
        AppendAck recovered = verifiedAck(fixture.bridge
                .resumeSameEntry(retainedPlan)
                .toCompletableFuture()
                .join()
                .get(0));
        assertThat(recovered.position().entryId()).isZero();
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).reservedLocatorCount())
                .isZero();
    }

    @Test
    void definitivelyAbsentSingleEntryCanSealAndMoveToSuccessorEntryZero() {
        Fixture fixture = fixture(START, START + 1);
        fixture.activate(BINDING_A, START);
        fixture.store.nextOutcome = ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED;
        assertBridgeFailure(
                () -> fixture.bridge
                        .appendShared(List.of(input(BINDING_A, "move-me")))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.PROVIDER_DEFINITIVELY_ABSENT);

        fixture.store.nextOutcome = ProviderObjectOutcome.APPLIED_EXACT;
        AppendAck successor = verifiedAck(fixture.bridge
                .sealAndRolloverFailedEntry(fixture.store.lastPlan.planId())
                .toCompletableFuture()
                .join()
                .get(0));

        assertThat(successor.position()).isEqualTo(new PulsarObjectWalBridgeV1.PulsarPosition(START + 1, 0));
        assertThat(fixture.authority.heads.get(BINDING_A).node().predecessorLedgerId())
                .hasValue(START);
        assertThat(fixture.authority.heads.get(BINDING_A).node().predecessorTerminalEntryId())
                .isEqualTo(-1);
        assertThat(fixture.store.successorPublications).isEqualTo(1);
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).reservedLocatorCount())
                .isZero();
    }

    @Test
    void successorLocalSealFailureRetainsExactEntryZeroForOldPlanRetry() {
        Fixture fixture = fixture(START, START + 1);
        fixture.activate(BINDING_A, START);
        AppendInput append = input(BINDING_A, "retry-successor-zero");
        fixture.store.nextOutcome = ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED;
        assertBridgeFailure(
                () -> fixture.bridge
                        .appendShared(List.of(append))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.PROVIDER_DEFINITIVELY_ABSENT);
        String predecessorPlanId = fixture.store.lastPlan.planId();

        fixture.store.nextOutcome = ProviderObjectOutcome.APPLIED_EXACT;
        fixture.store.failNextSeal = true;
        assertThatThrownBy(() -> fixture.bridge
                        .sealAndRolloverFailedEntry(predecessorPlanId)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("local seal failure");
        assertThat(fixture.authority.heads.get(BINDING_A).node().virtualLedgerId())
                .isEqualTo(START + 1);
        assertThat(fixture.authority.mutations).isEqualTo(2);
        assertBridgeFailure(
                () -> fixture.bridge
                        .appendShared(List.of(input(BINDING_A, "must-remain-stopped")))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.WALRUN_ADMISSION_STOPPED);

        fixture.store.failSuccessorStage = true;
        assertBridgeFailure(
                () -> fixture.bridge
                        .sealAndRolloverFailedEntry(predecessorPlanId)
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.PROVIDER_OUTCOME_UNKNOWN);
        assertThat(fixture.store.successorPublications).isOne();
        fixture.store.failSuccessorStage = false;
        AppendAck recovered = verifiedAck(fixture.bridge
                .sealAndRolloverFailedEntry(predecessorPlanId)
                .toCompletableFuture()
                .join()
                .get(0));
        assertThat(recovered.position()).isEqualTo(new PulsarObjectWalBridgeV1.PulsarPosition(START + 1, 0));
        assertThat(fixture.store.lastPlan.members().get(0).appendCommitSetId()).isEqualTo(append.appendCommitSetId());
        assertThat(fixture.store.lastPlan.members().get(0).payload()).containsExactly(append.payload());
        assertThat(fixture.store.successorPublications).isEqualTo(2);
        assertThat(fixture.authority.mutations).isEqualTo(2);
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).reservedLocatorCount())
                .isZero();
    }

    @Test
    void sharedDefinitiveAbsenceCannotPartiallyRolloverHeads() {
        Fixture fixture = fixture(START, START + 1, START + 2, START + 3);
        fixture.activate(BINDING_A, START);
        fixture.activate(BINDING_B, START + 1);
        fixture.store.nextOutcome = ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED;
        assertBridgeFailure(
                () -> fixture.bridge
                        .appendShared(List.of(input(BINDING_A, "a"), input(BINDING_B, "b")))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.PROVIDER_DEFINITIVELY_ABSENT);

        assertBridgeFailure(
                () -> fixture.bridge
                        .sealAndRolloverFailedEntry(fixture.store.lastPlan.planId())
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.ATOMIC_MULTI_BINDING_ROLLOVER_UNAVAILABLE);
        assertThat(fixture.authority.mutations).isEqualTo(2);
    }

    @Test
    void sharedFailedPlanTakeoverMustFenceAndDiscardEverySiblingAtomically() throws Exception {
        Fixture fixture = fixture(START, START + 1);
        fixture.activate(BINDING_A, START);
        fixture.activate(BINDING_B, START + 1);
        fixture.store.nextOutcome = ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED;
        assertBridgeFailure(
                () -> fixture.bridge
                        .appendShared(List.of(input(BINDING_A, "a"), input(BINDING_B, "b")))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.PROVIDER_DEFINITIVELY_ABSENT);
        String failedPlan = fixture.store.lastPlan.planId();

        assertBridgeFailure(
                () -> discardUnderFence(fixture.bridge, Map.of(BINDING_A, 7L)),
                BridgeRejectionCode.ATOMIC_SHARED_TAKEOVER_REQUIRED);
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).reservedLocatorCount())
                .isOne();
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_B).reservedLocatorCount())
                .isOne();
        assertBridgeFailure(
                () -> fixture.bridge
                        .resumeSameEntry(failedPlan)
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.PROVIDER_DEFINITIVELY_ABSENT);

        discardUnderFence(fixture.bridge, Map.of(BINDING_A, 7L, BINDING_B, 7L));
        assertBridgeFailure(() -> fixture.bridge.frontiers(BINDING_A), BridgeRejectionCode.BINDING_NOT_ACTIVE);
        assertBridgeFailure(() -> fixture.bridge.frontiers(BINDING_B), BridgeRejectionCode.BINDING_NOT_ACTIVE);
    }

    @Test
    void manifestHandoffSwitchesReadSourceOnlyAfterReadableCoverageExists() {
        Fixture fixture = fixture(START);
        fixture.activate(BINDING_A, START);
        AppendAck ack = verifiedAck(fixture.bridge
                .appendShared(List.of(input(BINDING_A, "manifest-value")))
                .toCompletableFuture()
                .join()
                .get(0));
        fixture.store.manifestValues.put(
                key(BINDING_A, ack.position()), "manifest-value".getBytes(StandardCharsets.UTF_8));

        fixture.bridge
                .installManifestHandoff(new PulsarObjectWalBridgeV1.ManifestHandoffRequest(BINDING_A, START, 0, 1))
                .toCompletableFuture()
                .join();
        ReadEntry read = fixture.bridge
                .read(BINDING_A, ack.position())
                .toCompletableFuture()
                .join();

        assertThat(read.source()).isEqualTo(ReadSource.MANIFEST);
        assertThat(fixture.store.lastManifestSource).isNotNull();
        assertThat(fixture.bridge.frontiers(BINDING_A).activeTailLocatorCount()).isZero();
    }

    @Test
    void recoveredManifestCoverageIsAuthorityVerifiedBeforeActivationAndRead() {
        Fixture fixture = fixture(START);
        PulsarObjectWalBridgeV1.ManifestHandoffRequest recoveryManifest =
                new PulsarObjectWalBridgeV1.ManifestHandoffRequest(BINDING_A, START, 0, 1);
        fixture.store.manifestValues.put(
                key(BINDING_A, new PulsarObjectWalBridgeV1.PulsarPosition(START, 0)),
                "recovered-manifest".getBytes(StandardCharsets.UTF_8));

        fixture.bridge
                .activate(BINDING_A, 7, new RecoverySeed(START, 0, 0, 0, 1, Optional.of(recoveryManifest), List.of()))
                .toCompletableFuture()
                .join();
        ReadEntry read = fixture.bridge
                .read(BINDING_A, new PulsarObjectWalBridgeV1.PulsarPosition(START, 0))
                .toCompletableFuture()
                .join();

        assertThat(new String(read.payload(), StandardCharsets.UTF_8)).isEqualTo("recovered-manifest");
        assertThat(fixture.store.lastManifestSource).isNotNull();
    }

    @Test
    void manifestAuthorityMismatchRetainsEveryActiveLocator() {
        Fixture fixture = fixture(START);
        fixture.activate(BINDING_A, START);
        fixture.bridge
                .appendShared(List.of(input(BINDING_A, "manifest-value")))
                .toCompletableFuture()
                .join();
        fixture.store.mismatchManifestVerification = true;

        assertBridgeFailure(
                () -> fixture.bridge
                        .installManifestHandoff(
                                new PulsarObjectWalBridgeV1.ManifestHandoffRequest(BINDING_A, START, 0, 1))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.MANIFEST_HANDOFF_NOT_VERIFIED);
        assertThat(fixture.bridge.frontiers(BINDING_A).manifestThrough()).isEqualTo(-1);
        assertThat(fixture.bridge.frontiers(BINDING_A).activeTailLocatorCount()).isOne();
    }

    @Test
    void exceptionalManifestVerificationCannotPublishCoverageOrReleaseLocator() {
        Fixture fixture = fixture(START);
        fixture.activate(BINDING_A, START);
        fixture.bridge
                .appendShared(List.of(input(BINDING_A, "manifest-value")))
                .toCompletableFuture()
                .join();
        fixture.store.failManifestVerification = true;

        assertBridgeFailure(
                () -> fixture.bridge
                        .installManifestHandoff(
                                new PulsarObjectWalBridgeV1.ManifestHandoffRequest(BINDING_A, START, 0, 1))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.MANIFEST_HANDOFF_NOT_VERIFIED);
        assertThat(fixture.bridge.frontiers(BINDING_A).manifestThrough()).isEqualTo(-1);
        assertThat(fixture.bridge.frontiers(BINDING_A).activeTailLocatorCount()).isOne();
    }

    @Test
    void manifestHandoffWaitsForActiveReadPinsBeforeReleasingCoveredLocator() {
        Fixture fixture = fixture(START);
        fixture.activate(BINDING_A, START);
        AppendAck ack = verifiedAck(fixture.bridge
                .appendShared(List.of(input(BINDING_A, "pinned-value")))
                .toCompletableFuture()
                .join()
                .get(0));
        fixture.store.manifestValues.put(
                key(BINDING_A, ack.position()), "pinned-value".getBytes(StandardCharsets.UTF_8));
        fixture.store.activeReadGate = new CompletableFuture<>();
        CompletableFuture<ReadEntry> pinnedRead =
                fixture.bridge.read(BINDING_A, ack.position()).toCompletableFuture();

        fixture.bridge
                .installManifestHandoff(new PulsarObjectWalBridgeV1.ManifestHandoffRequest(BINDING_A, START, 0, 1))
                .toCompletableFuture()
                .join();
        assertThat(fixture.bridge.frontiers(BINDING_A).manifestThrough()).isZero();
        assertThat(fixture.bridge.frontiers(BINDING_A).activeTailLocatorCount()).isOne();

        fixture.store.activeReadGate.complete(null);
        assertThat(new String(pinnedRead.join().payload(), StandardCharsets.UTF_8))
                .isEqualTo("pinned-value");
        assertThat(fixture.bridge.frontiers(BINDING_A).activeTailLocatorCount()).isZero();
    }

    @Test
    void recoveredActiveTailMustBeBoundedAndExactlyContiguous() {
        Fixture fixture = fixture(START);
        ExtentIdentity identity = identity(ROOT_A, WalLaneId.OBJECT_BALANCED, 0, "object-wal/run/lane-1/0");
        ExtentLocator entryOne = new ExtentLocator(
                BINDING_A, new PulsarObjectWalBridgeV1.PulsarPosition(START, 1), identity, 0, 0, 16, FRAME_SHA);

        assertBridgeFailure(
                () -> fixture.bridge
                        .activate(BINDING_A, 7, new RecoverySeed(START, -1, 1, 1, 0, List.of(entryOne)))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.RECOVERY_MAPPING_INVALID);
    }

    @Test
    void recoveredActiveTailAlsoConsumesTheLiveLocatorCap() {
        Configuration oneLocator = new Configuration(1024, 8, 8, 1, 8, 8, 4096);
        Fixture fixture = fixture(oneLocator, START);
        ExtentIdentity identity = identity(ROOT_A, WalLaneId.OBJECT_BALANCED, 0, "object-wal/run/lane-1/0");
        ExtentLocator entryZero = locator(BINDING_A, START, 0, identity, 0);
        ExtentLocator entryOne = locator(BINDING_A, START, 1, identity, 1);

        assertBridgeFailure(
                () -> fixture.bridge
                        .activate(BINDING_A, 7, new RecoverySeed(START, -1, 1, 1, 0, List.of(entryZero, entryOne)))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.RECOVERY_BOUND_EXCEEDED);
    }

    @Test
    void entryBoundRequiresExplicitRolloverBeforeAnotherPositionIsAllocated() {
        Configuration oneEntry = new Configuration(1024, 1, 8, 8, 8, 8, 4096);
        Fixture fixture = fixture(oneEntry, START, START + 1);
        fixture.activate(BINDING_A, START);
        fixture.bridge
                .appendShared(List.of(input(BINDING_A, "entry-zero")))
                .toCompletableFuture()
                .join();

        assertBridgeFailure(
                () -> fixture.bridge
                        .appendShared(List.of(input(BINDING_A, "would-be-entry-one")))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.LEDGER_ROLLOVER_REQUIRED);

        fixture.bridge.rollover(BINDING_A, 7).toCompletableFuture().join();
        AppendAck successor = verifiedAck(fixture.bridge
                .appendShared(List.of(input(BINDING_A, "successor-zero")))
                .toCompletableFuture()
                .join()
                .get(0));
        assertThat(successor.position()).isEqualTo(new PulsarObjectWalBridgeV1.PulsarPosition(START + 1, 0));
    }

    @Test
    void successorAllocationExhaustionLeavesTheCurrentHeadUnchanged() {
        Fixture fixture = fixture(START);
        fixture.activate(BINDING_A, START);

        assertThatThrownBy(() -> fixture.bridge
                        .rollover(BINDING_A, 7)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(PulsarVirtualLedgerChainControllerV1.SliceExhaustedException.class);
        assertThat(fixture.bridge.frontiers(BINDING_A).virtualLedgerId()).isEqualTo(START);
        assertThat(fixture.authority.mutations).isEqualTo(1);
    }

    @Test
    void rejectsOutOfOrderLaneResolutionBeforePublishingLocatorOrBindingFrontier() {
        Fixture fixture = fixture(START);
        fixture.activate(BINDING_A, START);
        fixture.store.nextSequence = 1;

        assertBridgeFailure(
                () -> fixture.bridge
                        .appendShared(List.of(input(BINDING_A, "out-of-order")))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.PHYSICAL_FRONTIER_MISMATCH);
        assertThat(fixture.bridge.frontiers(BINDING_A).bindingDurableThrough()).isEqualTo(-1);
        assertThat(fixture.bridge.frontiers(BINDING_A).activeTailLocatorCount()).isZero();
    }

    @Test
    void activeLocatorRejectsSealedOffloadObjectKeys() {
        ExtentIdentity offloadIdentity =
                identity(ROOT_A, WalLaneId.OBJECT_LATENCY, 0, "pulsar-offload/v1/ledger-1/attempt-x/data");

        assertThatThrownBy(() -> new ExtentLocator(
                        BINDING_A,
                        new PulsarObjectWalBridgeV1.PulsarPosition(START, 0),
                        offloadIdentity,
                        0,
                        0,
                        1,
                        FRAME_SHA))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sealed NPD1/NPO1 offload objects");
    }

    @Test
    void combinedTrackerAndLocatorReservationFailsBeforeAnySharedPositionOrTicket() {
        Configuration oneLocator = new Configuration(1024, 8, 8, 1, 8, 8, 4096);
        Fixture fixture = fixture(oneLocator, START, START + 1);
        fixture.activate(BINDING_A, START);
        ExtentIdentity recoveredIdentity =
                identity(ROOT_A, WalLaneId.OBJECT_BALANCED, 0, "object-wal/recovered/lane-1/0");
        ExtentLocator recovered = locator(BINDING_B, START + 1, 0, recoveredIdentity, 0);
        fixture.bridge
                .activate(BINDING_B, 7, new RecoverySeed(START + 1, -1, 0, 0, 0, List.of(recovered)))
                .toCompletableFuture()
                .join();

        assertBridgeFailure(
                () -> fixture.bridge
                        .appendShared(List.of(input(BINDING_A, "a-0"), input(BINDING_B, "b-1")))
                        .toCompletableFuture()
                        .join(),
                BridgeRejectionCode.ACTIVE_TAIL_CAPACITY_EXHAUSTED);

        assertThat(fixture.bridge.frontiers(BINDING_A).bindingDurableThrough()).isEqualTo(-1);
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).ticketsIssued())
                .isZero();
        assertThat(fixture.bridge.completionTrackerForTest(BINDING_A).reservedLocatorCount())
                .isZero();
        AppendAck first = verifiedAck(fixture.bridge
                .appendShared(List.of(input(BINDING_A, "a-0")))
                .toCompletableFuture()
                .join()
                .get(0));
        assertThat(first.position().entryId()).isZero();
    }

    @Test
    void everyCancellationUnknownRetryAndCompletionReleaseRequiresExactTicketOwnership() {
        PulsarObjectWalBridgeV1.CompletionTrackerRing ring = new PulsarObjectWalBridgeV1.CompletionTrackerRing(1, 7, 0);
        PulsarObjectWalBridgeV1.PrePositionReservation beforePosition = ring.reserve(7, 0, 8);
        assertThatThrownBy(() -> ring.reserve(7, 0, 8))
                .isInstanceOf(BridgeException.class)
                .extracting(error -> ((BridgeException) error).code())
                .isEqualTo(BridgeRejectionCode.COMPLETION_TRACKER_CAPACITY_EXHAUSTED);
        assertThat(ring.ticketsIssued()).isZero();
        ring.cancelBeforePosition(beforePosition);
        assertThat(ring.reservedLocatorCount()).isZero();
        assertThat(ring.ticketsIssued()).isZero();

        PulsarObjectWalBridgeV1.PulsarPosition position = new PulsarObjectWalBridgeV1.PulsarPosition(START, 0);
        PulsarObjectWalBridgeV1.CompletionTicket uncertain = ring.assignAfterPosition(ring.reserve(7, 0, 8), position);
        ring.markUnknown(uncertain, position);
        ring.retry(uncertain, position);
        assertThat(ring.reservedLocatorCount()).isOne();
        ring.definitiveCancelAndRelease(uncertain, position);
        assertThat(ring.reservedLocatorCount()).isZero();

        PulsarObjectWalBridgeV1.CompletionTicket completed = ring.assignAfterPosition(ring.reserve(7, 0, 8), position);
        ExtentIdentity identity = identity(ROOT_A, WalLaneId.OBJECT_BALANCED, 0, "object-wal/ring/lane-1/0");
        ExtentLocator locator = locator(BINDING_A, START, 0, identity, 0);
        ring.installLocator(completed, position, locator);
        ring.requireCompletionRelease(completed, position, locator);
        ring.completeAndRelease(completed, position, locator);
        assertThat(ring.reservedLocatorCount()).isZero();
        assertThat(ring.ticketsIssued()).isEqualTo(2);
    }

    @Test
    void staleTicketCannotReleaseAReusedRingSlot() {
        PulsarObjectWalBridgeV1.CompletionTrackerRing ring = new PulsarObjectWalBridgeV1.CompletionTrackerRing(1, 7, 0);
        PulsarObjectWalBridgeV1.PulsarPosition position = new PulsarObjectWalBridgeV1.PulsarPosition(START, 0);
        PulsarObjectWalBridgeV1.CompletionTicket stale = ring.assignAfterPosition(ring.reserve(7, 0, 8), position);
        ring.definitiveCancelAndRelease(stale, position);
        PulsarObjectWalBridgeV1.CompletionTicket current = ring.assignAfterPosition(ring.reserve(7, 0, 8), position);

        assertThat(current.slotIndex()).isEqualTo(stale.slotIndex());
        assertThat(current.value()).isNotEqualTo(stale.value());
        assertThatThrownBy(() -> ring.definitiveCancelAndRelease(stale, position))
                .isInstanceOf(BridgeException.class)
                .extracting(error -> ((BridgeException) error).code())
                .isEqualTo(BridgeRejectionCode.COMPLETION_TICKET_MISMATCH);
        assertThat(ring.reservedLocatorCount()).isOne();
        ring.definitiveCancelAndRelease(current, position);
    }

    @Test
    void checkedTicketCounterCrossesSignedBoundaryAndUsesUnsignedMaxOnceThenFailsBeforeAnotherPosition() {
        PulsarObjectWalBridgeV1.CompletionTrackerRing ring =
                new PulsarObjectWalBridgeV1.CompletionTrackerRing(1, 7, Long.MAX_VALUE);
        PulsarObjectWalBridgeV1.PulsarPosition position = new PulsarObjectWalBridgeV1.PulsarPosition(START, 0);
        PulsarObjectWalBridgeV1.CompletionTicket signedMaximum =
                ring.assignAfterPosition(ring.reserve(7, 0, 8), position);
        assertThat(signedMaximum.value()).isEqualTo(Long.MAX_VALUE);
        ring.definitiveCancelAndRelease(signedMaximum, position);
        PulsarObjectWalBridgeV1.CompletionTicket firstUpperHalf =
                ring.assignAfterPosition(ring.reserve(7, 0, 8), position);
        assertThat(firstUpperHalf.value()).isEqualTo(Long.MIN_VALUE);
        ring.definitiveCancelAndRelease(firstUpperHalf, position);

        PulsarObjectWalBridgeV1.CompletionTrackerRing finalPatternRing =
                new PulsarObjectWalBridgeV1.CompletionTrackerRing(1, 7, -1L);
        PulsarObjectWalBridgeV1.CompletionTicket unsignedMaximum =
                finalPatternRing.assignAfterPosition(finalPatternRing.reserve(7, 0, 8), position);
        assertThat(unsignedMaximum.value()).isEqualTo(-1L);
        finalPatternRing.definitiveCancelAndRelease(unsignedMaximum, position);

        assertThatThrownBy(() -> finalPatternRing.reserve(7, 0, 8))
                .isInstanceOf(BridgeException.class)
                .extracting(error -> ((BridgeException) error).code())
                .isEqualTo(BridgeRejectionCode.COMPLETION_TICKET_EXHAUSTED);
        assertThat(ring.ticketsIssued()).isEqualTo(2);
        assertThat(finalPatternRing.ticketsIssued()).isOne();
    }

    @Test
    void durableTakeoverFenceDiscardsOldOwnerReservationsAndTickets() {
        PulsarObjectWalBridgeV1.CompletionTrackerRing ring = new PulsarObjectWalBridgeV1.CompletionTrackerRing(2, 7, 0);
        PulsarObjectWalBridgeV1.PulsarPosition position = new PulsarObjectWalBridgeV1.PulsarPosition(START, 0);
        PulsarObjectWalBridgeV1.CompletionTicket oldOwner = ring.assignAfterPosition(ring.reserve(7, 0, 8), position);
        PulsarObjectWalBridgeV1.PrePositionReservation oldReservation = ring.reserve(7, 0, 8);

        ring.discardAfterFence(7);

        assertThat(ring.reservedLocatorCount()).isZero();
        assertThatThrownBy(() -> ring.markUnknown(oldOwner, position))
                .isInstanceOf(BridgeException.class)
                .extracting(error -> ((BridgeException) error).code())
                .isEqualTo(BridgeRejectionCode.OWNER_LOCAL_STATE_DISCARDED);
        assertThatThrownBy(() -> ring.cancelBeforePosition(oldReservation))
                .isInstanceOf(BridgeException.class)
                .extracting(error -> ((BridgeException) error).code())
                .isEqualTo(BridgeRejectionCode.OWNER_LOCAL_STATE_DISCARDED);
    }

    @Test
    void localSealFailureReleasesTheExactPostPositionTicketBeforeProviderDispatch() {
        Fixture fixture = fixture(START);
        fixture.activate(BINDING_A, START);
        fixture.store.failNextSeal = true;

        assertThatThrownBy(() -> fixture.bridge
                        .appendShared(List.of(input(BINDING_A, "not-dispatched")))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("local seal failure");
        PulsarObjectWalBridgeV1.CompletionTrackerRing ring = fixture.bridge.completionTrackerForTest(BINDING_A);
        assertThat(ring.ticketsIssued()).isOne();
        assertThat(ring.reservedLocatorCount()).isZero();
        assertThat(fixture.store.lastPlan).isNull();

        AppendAck retry = verifiedAck(fixture.bridge
                .appendShared(List.of(input(BINDING_A, "dispatched")))
                .toCompletableFuture()
                .join()
                .get(0));
        assertThat(retry.position().entryId()).isZero();
        assertThat(ring.ticketsIssued()).isEqualTo(2);
    }

    @Test
    void boundedRecoveryValidatesAdjacencyThenReconstructsFreshTickets() {
        Fixture fixture = fixture(START);
        ExtentIdentity identity = identity(ROOT_A, WalLaneId.OBJECT_BALANCED, 0, "object-wal/recovered/lane-1/0");
        ExtentLocator entryZero = locator(BINDING_A, START, 0, identity, 0);
        ExtentLocator entryOne = locator(BINDING_A, START, 1, identity, 1);
        fixture.bridge
                .activate(BINDING_A, 7, new RecoverySeed(START, -1, 1, 1, 0, List.of(entryOne, entryZero)))
                .toCompletableFuture()
                .join();

        PulsarObjectWalBridgeV1.CompletionTrackerRing recovered = fixture.bridge.completionTrackerForTest(BINDING_A);
        assertThat(recovered.ticketsIssued()).isEqualTo(2);
        assertThat(recovered.nextTicketForTest()).isEqualTo(2);
        assertThat(recovered.reservedLocatorCount()).isZero();
        assertThat(fixture.store.recoveryVerifications).isOne();
        assertThat(fixture.store.lastRecoveryRequest.activeTail()).containsExactly(entryOne, entryZero);

        AppendAck next = verifiedAck(fixture.bridge
                .appendShared(List.of(input(BINDING_A, "entry-two")))
                .toCompletableFuture()
                .join()
                .get(0));
        assertThat(next.position().entryId()).isEqualTo(2);
        assertThat(recovered.ticketsIssued()).isEqualTo(3);
        assertThat(recovered.reservedLocatorCount()).isZero();
    }

    @Test
    void failedRecoveryAuthenticationNeverActivatesBindingOrPublishesFrontier() {
        Fixture fixture = fixture(START);
        ExtentIdentity identity = identity(ROOT_A, WalLaneId.OBJECT_BALANCED, 0, "object-wal/recovered/lane-1/0");
        ExtentLocator candidate = locator(BINDING_A, START, 0, identity, 0);
        fixture.store.failRecoveryVerification = true;

        assertThatThrownBy(() -> fixture.bridge
                        .activate(BINDING_A, 7, new RecoverySeed(START, -1, 0, 0, 0, List.of(candidate)))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("active-tail recovery authority unavailable");
        assertBridgeFailure(() -> fixture.bridge.frontiers(BINDING_A), BridgeRejectionCode.BINDING_NOT_ACTIVE);
        assertThat(fixture.store.recoveryVerifications).isOne();
    }

    private static PulsarObjectWalBridgeV1.ProductionObjectWalExtentStore productionStore(
            WalRunObjectSession session, WalRunRootRecord root) {
        return productionStore(
                session,
                root,
                (lane, policy, members, maximum) -> {
                    throw new AssertionError("plan operation is unexpected");
                },
                (binding, authenticatedContext) -> {});
    }

    private static PulsarObjectWalBridgeV1.ProductionObjectWalExtentStore productionStore(
            WalRunObjectSession session,
            WalRunRootRecord root,
            PulsarObjectWalBridgeV1.PulsarNwg1PlanAuthority plans,
            PulsarObjectWalBridgeV1.PulsarBindingContextAuthority bindings) {
        return productionStore(session, root, plans, bindings, new TestControlMetadataStore());
    }

    private static PulsarObjectWalBridgeV1.ProductionObjectWalExtentStore productionStore(
            WalRunObjectSession session,
            WalRunRootRecord root,
            PulsarObjectWalBridgeV1.PulsarNwg1PlanAuthority plans,
            PulsarObjectWalBridgeV1.PulsarBindingContextAuthority bindings,
            TestControlMetadataStore metadata) {
        PulsarObjectWalBridgeV1.ObjectWalManifestOperations manifests =
                new PulsarObjectWalBridgeV1.ObjectWalManifestOperations() {
                    @Override
                    public CompletableFuture<PulsarObjectWalBridgeV1.VerifiedManifestHandoff> verify(
                            PulsarObjectWalBridgeV1.ManifestHandoffRequest request) {
                        return CompletableFuture.failedFuture(new AssertionError("manifest operation is unexpected"));
                    }

                    @Override
                    public CompletableFuture<ReadEntry> read(
                            PulsarBindingKey binding,
                            PulsarObjectWalBridgeV1.PulsarPosition position,
                            long manifestGeneration,
                            PulsarObjectWalBridgeV1.ManifestSource verifiedSource) {
                        return CompletableFuture.failedFuture(new AssertionError("manifest operation is unexpected"));
                    }
                };
        PulsarObjectWalBridgeV1.ObjectWalSuccessorAuthority successor =
                new PulsarObjectWalBridgeV1.ObjectWalSuccessorAuthority() {
                    @Override
                    public CompletableFuture<PublishResult> resumeInSuccessor(
                            SealedExtentPlan plan,
                            Optional<ExtentIdentity> absentCandidate,
                            List<PulsarObjectWalBridgeV1.PlannedEntry> unresolvedMembers) {
                        return CompletableFuture.failedFuture(new AssertionError("successor operation is unexpected"));
                    }

                    @Override
                    public CompletableFuture<PublishResult> publishSuccessor(
                            SealedExtentPlan definitivelyAbsentPlan, SealedExtentPlan successorPlan) {
                        return CompletableFuture.failedFuture(new AssertionError("successor operation is unexpected"));
                    }
                };
        WalRunLifecycleManager lifecycle = new WalRunLifecycleManager(metadata);
        String rootKey = WalRunControlKeys.rootKey(root.shardId(), root.shardRunEpoch());
        var rootReference = lifecycle.createRoot(rootKey, root);
        String checkpointHeadKey = WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch());
        WalCheckpointPublisher checkpoints = new WalCheckpointPublisher(
                metadata,
                checkpointHeadKey,
                WalRunControlKeys.checkpointPagePrefix(root.shardId(), root.shardRunEpoch()),
                root,
                WalCheckpointHeadV1.empty(WalRunControlCodec.rootSha256(root), root.shardRunEpoch(), 1),
                session);
        checkpoints.initializeHead();
        return new PulsarObjectWalBridgeV1.ProductionObjectWalExtentStore(
                session,
                Runnable::run,
                plans,
                (member, locator) -> Optional.empty(),
                bindings,
                manifests,
                successor,
                new PulsarObjectWalBridgeV1.ProductionObjectWalContext(verificationContext(root), () -> 0),
                new PulsarObjectWalBridgeV1.ProductionWalRunClosure(
                        checkpoints,
                        lifecycle,
                        rootReference,
                        checkpointHeadKey,
                        WalRunControlKeys.sealKey(root.shardId(), root.shardRunEpoch())));
    }

    private static Nwg1VerificationContextV1 verificationContext(WalRunRootRecord root) {
        return new Nwg1VerificationContextV1(
                root.protocolCellIdentity(),
                root.providerScopeId().digest().bytes().toByteArray(),
                WalRunControlCodec.rootSha256(root).bytes().toByteArray(),
                Nwg1EnvelopeV1.decode(root.wrappedRunKey().framedBytes().toByteArray()),
                (bindingId, ownerFenceKind, ownerFenceVersion) ->
                        digest('c').bytes().toByteArray(),
                (exactAssignedBytes, partitionId, leaderEpoch, start, end) ->
                        new Nwg1VerificationContextV1.NativeCoverage(start, end),
                START,
                START + PulsarVirtualLedgerChainControllerV1.SLICE_SIZE);
    }

    private static PulsarObjectWalBridgeV1.PreparedNwg1Plan preparedPlan(
            WalRunRootRecord root,
            List<PulsarObjectWalBridgeV1.PlannedEntry> members,
            Nwg1DirectoryV1.BindingContext context,
            WalLaneId lane,
            int packingPolicyVersion) {
        assertThat(members).hasSize(1);
        PulsarObjectWalBridgeV1.PlannedEntry member = members.get(0);
        byte[] payload = member.payload();
        Nwg1VerificationContextV1 verification = verificationContext(root);
        GroupEncodingPlanV1 plan = new GroupEncodingPlanV1(
                2,
                root.shardId(),
                root.shardRunEpoch(),
                lane.code(),
                packingPolicyVersion,
                payload.length,
                0,
                0,
                Nwg1CloseReasonV1.EXPLICIT_FLUSH.code(),
                Nwg1CommitmentsV1.protocolCell(verification.exactNpc1()),
                root.providerScopeId().digest().bytes().toByteArray(),
                WalRunControlCodec.rootSha256(root).bytes().toByteArray(),
                Nwg1CommitmentsV1.wrappedEnvelope(verification.envelope()),
                List.of(context),
                List.of(new Nwg1DirectoryV1.PulsarAppendUnit(
                        0,
                        0,
                        1,
                        member.position().virtualLedgerId(),
                        member.position().entryId(),
                        member.appendCommitSetId().toByteArray(),
                        id128("storage-attempt-" + member.position()).toByteArray(),
                        Nwg1CommitmentsV1.sha256(payload))),
                List.of(new GroupEncodingPlanV1.PlannedFrame(
                        0,
                        payload,
                        payload,
                        member.position().virtualLedgerId(),
                        member.position().entryId(),
                        0,
                        0)));
        return new PulsarObjectWalBridgeV1.PreparedNwg1Plan(plan, List.of(member.binding()));
    }

    private static Nwg1DirectoryV1.BindingContext bindingContext(WalRunRootRecord root, PulsarBindingKey binding) {
        var incarnation = new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString("persistent://tenant/ns/" + binding.topicIncarnation()),
                PulsarTopicName.fromString(binding.topicBindingId()),
                new PulsarBindingGeneration(1));
        byte[] nti1 = TopicIncarnationIdentityCodecV1.encode(incarnation).toByteArray();
        var bindingId = DeterministicTopicIdsV1.deriveBindingId(root.protocolCellIdentity(), incarnation);
        byte[] ownerWitness = digest('c').bytes().toByteArray();
        return new Nwg1DirectoryV1.BindingContext(
                bindingId.digest().bytes().toByteArray(),
                DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 0)
                        .digest()
                        .bytes()
                        .toByteArray(),
                Nwg1CommitmentsV1.ownerFence(2, 1, ownerWitness),
                nti1,
                2,
                1,
                2,
                1,
                1,
                1);
    }

    private static WalRunRootRecord objectWalRoot() {
        Sha256Digest providerScope = digest('a');
        return new WalRunRootRecord(
                7,
                1,
                new Id128(11, 12),
                0,
                new PulsarProtocolCellIdentity(
                        new DeploymentId(new Id128(1, 2)),
                        new ReservationDomainId(new Id128(3, 4)),
                        new PulsarCellId(new Id128(5, 6))),
                new CellProviderScopeId(providerScope),
                WalRunFormatContractV1.frozen(),
                new Nwg1RootAdmissionCaps(1024 * 1024, 4096, 3824, 16, 100, 100, 4080, 4096, 1024 * 1024, 1024 * 1024),
                new WalRunBounds(100, 1024 * 1024, 60_000, 4),
                new WalCheckpointPolicy(0, 16, 1024 * 1024, 5_000, 16, 8192),
                new ObjectProviderRootConfiguration(
                        ObjectProviderAccessProfile.C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST,
                        "test-adapter-v1",
                        "canonical-key-v1",
                        "cell-a/wal/run-1",
                        ProviderProofMode.NONE,
                        0,
                        1024 * 1024,
                        1024 * 1024,
                        4096,
                        1,
                        100,
                        digest('b')),
                new RecoveryEnvelopeLimits(
                        5,
                        4,
                        10,
                        101,
                        100_000,
                        0,
                        10_100,
                        10,
                        pulsarRecoveryCanonicalByteLimit(),
                        10_000,
                        10_000,
                        10_000,
                        1024 * 1024,
                        4,
                        10,
                        60_000_000_000L),
                new WrappedRunKeyEnvelope(
                        "fake-kms", "aes-kw-v1", "kms/cell-a", "version-1", CanonicalBytes.copyOf(new byte[32])),
                Optional.empty());
    }

    private static WalRunReference objectWalReference(WalRunRootRecord root) {
        return new WalRunReference(
                WalRunControlKeys.rootKey(root.shardId(), root.shardRunEpoch()),
                WalRunControlCodec.rootSha256(root),
                root.shardId(),
                root.shardRunEpoch());
    }

    private static void discardUnderFence(PulsarObjectWalBridgeV1 bridge, Map<PulsarBindingKey, Long> fencedOwners) {
        WalRunRootRecord root = objectWalRoot();
        WalRunReference reference = objectWalReference(root);
        TestActiveOwnerFenceAuthority authority = new TestActiveOwnerFenceAuthority(8);
        var executor =
                new PulsarObjectWalBridgeV1.PulsarProtocolOwnerFenceExecutor(authority, verificationContext(root));
        try {
            executor.withDurableOwnerFence(root.protocolCellIdentity(), reference, reference.rootSha256(), ignored -> {
                executor.discardOwnerLocalStatesAfterFence(bridge, fencedOwners);
                return null;
            });
        } catch (IOException failure) {
            throw new java.io.UncheckedIOException("test durable Pulsar owner fence failed", failure);
        }
    }

    private static long pulsarRecoveryCanonicalByteLimit() {
        long controlMetadataBytes = 1024L * 1024;
        long currentPointerAndLiveRoots = Math.multiplyExact(6, controlMetadataBytes);
        long checkpointPagesPerPredecessor = 7;
        long predecessorClosure = Math.multiplyExact(
                4,
                Math.addExact(
                        Math.multiplyExact(2, controlMetadataBytes),
                        Math.multiplyExact(checkpointPagesPerPredecessor, 8192)));
        long currentPhysicalCheckpoint =
                Math.addExact(controlMetadataBytes, Math.multiplyExact(checkpointPagesPerPredecessor, 8192));
        long admittedRangeBodies = controlMetadataBytes;
        long pendingLazyLaneCandidates = Math.multiplyExact(3, controlMetadataBytes);
        long required = Math.addExact(
                Math.addExact(Math.addExact(currentPointerAndLiveRoots, predecessorClosure), currentPhysicalCheckpoint),
                Math.addExact(admittedRangeBodies, pendingLazyLaneCandidates));
        return Math.max(required, 32L * 1024 * 1024);
    }

    private static KmsCellSession kms(WalRunRootRecord root) {
        return new KmsCellSession(
                new TestKmsTransport(), root.providerScopeId(), "kms/cell-a", 2, new SecureRandom(new byte[] {1, 2, 3
                }));
    }

    private static PulsarObjectWalNativeResultV1.Receipt nativeReceipt() {
        var junitFiles = List.of(
                new PulsarObjectWalNativeResultV1.JunitFile(
                        "nereus-pulsar-offload/build/test-results/test/TEST-"
                                + "com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1Test.xml",
                        "1".repeat(64),
                        39,
                        0,
                        0,
                        0),
                new PulsarObjectWalNativeResultV1.JunitFile(
                        "nereus-pulsar-offload/build/test-results/test/TEST-"
                                + "com.nereusstream.pulsar.offload.objectwal."
                                + "PulsarVirtualLedgerChainControllerV1Test.xml",
                        "2".repeat(64),
                        7,
                        0,
                        0,
                        0));
        var unsigned = new PulsarObjectWalNativeResultV1.Receipt(
                PulsarObjectWalNativeResultV1.SCHEMA,
                PulsarObjectWalNativeResultV1.COMPONENT_KIND,
                PulsarObjectWalNativeResultV1.STATUS,
                new PulsarObjectWalNativeResultV1.TestedSource("nereus", "3".repeat(40), "4".repeat(64)),
                List.of(new PulsarObjectWalNativeResultV1.ExternalSource("apache/pulsar", "5".repeat(40))),
                new PulsarObjectWalNativeResultV1.Execution(
                        PulsarObjectWalNativeResultV1.DEFAULT_COMMAND,
                        Instant.parse("2026-08-23T10:00:00Z").toString(),
                        Instant.parse("2026-08-23T10:01:00Z").toString(),
                        "OpenJDK 21",
                        "macOS arm64"),
                new PulsarObjectWalNativeResultV1.JunitEvidence(
                        "nereus-pulsar-offload/build/test-results/test",
                        junitFiles,
                        new PulsarObjectWalNativeResultV1.JunitTotals(2, 46, 0, 0, 0)),
                PulsarObjectWalNativeResultV1.requiredTestsForTest(),
                PulsarObjectWalNativeResultV1.countersForTest(),
                List.of(
                        nativeArtifact("build.gradle.kts", '6'),
                        nativeArtifact(
                                "src/main/java/com/nereusstream/pulsar/offload/objectwal/"
                                        + "PulsarObjectWalBridgeV1.java",
                                '7'),
                        nativeArtifact(
                                "src/main/java/com/nereusstream/pulsar/offload/objectwal/"
                                        + "PulsarObjectWalNativeResultV1.java",
                                '8'),
                        nativeArtifact(
                                "src/main/java/com/nereusstream/pulsar/offload/objectwal/"
                                        + "PulsarVirtualLedgerChainControllerV1.java",
                                '9'),
                        nativeArtifact(
                                "src/test/java/com/nereusstream/pulsar/offload/objectwal/"
                                        + "PulsarObjectWalBridgeV1Test.java",
                                'a'),
                        nativeArtifact(
                                "src/test/java/com/nereusstream/pulsar/offload/objectwal/"
                                        + "PulsarVirtualLedgerChainControllerV1Test.java",
                                'b')),
                List.of(PulsarObjectWalNativeResultV1.M6_EXCLUSION),
                "0".repeat(64));
        return PulsarObjectWalNativeResultV1.sealForTest(unsigned);
    }

    private static PulsarObjectWalNativeResultV1.Artifact nativeArtifact(String moduleRelativePath, char digestDigit) {
        return new PulsarObjectWalNativeResultV1.Artifact(
                "nereus-pulsar-offload/" + moduleRelativePath,
                String.valueOf(digestDigit).repeat(64),
                1024);
    }

    private static Fixture fixture(long... allocatorValues) {
        return fixture(CONFIGURATION, allocatorValues);
    }

    private static Fixture fixture(Configuration configuration, long... allocatorValues) {
        Queue<Long> values = new ArrayDeque<>();
        for (long value : allocatorValues) {
            values.add(value);
        }
        FakeAuthority authority = new FakeAuthority();
        PulsarVirtualLedgerChainControllerV1 controller = new PulsarVirtualLedgerChainControllerV1(
                new FixedSlice(START, START + PulsarVirtualLedgerChainControllerV1.SLICE_SIZE - 1),
                (binding, slice, ownerEpoch) -> {
                    Long value = values.poll();
                    if (value == null) {
                        return CompletableFuture.failedFuture(
                                new PulsarVirtualLedgerChainControllerV1.SliceExhaustedException("exhausted"));
                    }
                    return CompletableFuture.completedFuture(value);
                },
                authority);
        FakeExtentStore store = new FakeExtentStore();
        return new Fixture(new PulsarObjectWalBridgeV1(configuration, controller, store), authority, store);
    }

    private static AppendInput input(PulsarBindingKey binding, String payload) {
        return new AppendInput(
                binding,
                7,
                WalLaneId.OBJECT_BALANCED,
                3,
                id128("idempotency-" + binding.topicBindingId() + '-' + payload),
                payload.getBytes(StandardCharsets.UTF_8));
    }

    private static PulsarBindingKey binding(String suffix) {
        return new PulsarBindingKey(
                "cell-1", "binding-" + suffix, "incarnation-" + suffix, "storage-epoch-1", "position-domain-" + suffix);
    }

    private static ExtentLocator locator(
            PulsarBindingKey binding, long ledgerId, long entryId, ExtentIdentity identity, int frameOrdinal) {
        return new ExtentLocator(
                binding,
                new PulsarObjectWalBridgeV1.PulsarPosition(ledgerId, entryId),
                identity,
                frameOrdinal,
                (long) frameOrdinal * 16,
                16,
                FRAME_SHA);
    }

    private static ExtentIdentity identity(Sha256Digest root, WalLaneId laneId, long sequence, String objectKey) {
        return new ExtentIdentity(root, laneId, sequence, new ObjectIdentity(objectKey, 4096, EXTENT_SHA));
    }

    private static Sha256Digest digest(char value) {
        return Sha256Digest.copyOf(
                java.util.HexFormat.of().parseHex(String.valueOf(value).repeat(64)));
    }

    private static CanonicalBytes id128(String value) {
        return CanonicalBytes.copyOf(java.util.Arrays.copyOf(
                Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)))
                        .bytes()
                        .toByteArray(),
                16));
    }

    private static String key(PulsarBindingKey binding, PulsarObjectWalBridgeV1.PulsarPosition position) {
        return binding.topicBindingId() + ':' + position.virtualLedgerId() + ':' + position.entryId();
    }

    private static List<AppendAck> verifiedAcks(List<MemberAppendResult> results) {
        return results.stream().map(PulsarObjectWalBridgeV1Test::verifiedAck).toList();
    }

    private static AppendAck verifiedAck(MemberAppendResult result) {
        assertThat(result).isInstanceOf(VerifiedAppend.class);
        return ((VerifiedAppend) result).acknowledgement();
    }

    private static void assertBridgeFailure(Runnable action, BridgeRejectionCode expected) {
        Throwable error = org.assertj.core.api.Assertions.catchThrowable(action::run);
        assertThat(error).isNotNull();
        while (error.getCause() != null) {
            error = error.getCause();
        }
        assertThat(error).isInstanceOf(BridgeException.class);
        assertThat(((BridgeException) error).code()).isEqualTo(expected);
    }

    private static final class TestActiveOwnerFenceAuthority
            implements PulsarObjectWalBridgeV1.PulsarActiveOwnerFenceAuthority {
        private long ownerEpoch;
        private boolean fenceHeld;
        private int callbacks;
        private int rejectedOldOwnerPuts;

        private TestActiveOwnerFenceAuthority(long ownerEpoch) {
            this.ownerEpoch = ownerEpoch;
        }

        @Override
        public WalRunObjectSession withAllBindingsActiveMonotonicFence(
                com.nereusstream.domain.protocol.PulsarProtocolCellIdentity exactProtocolCell,
                WalRunReference exactRootReference,
                PulsarObjectWalBridgeV1.PulsarFencedRecoveryAction callback)
                throws IOException {
            if (fenceHeld) {
                throw new IllegalStateException("test owner fence is already held");
            }
            fenceHeld = true;
            callbacks++;
            try {
                return callback.recover(new PulsarObjectWalBridgeV1.ObservedPulsarOwnerFence(
                        exactProtocolCell, exactRootReference, ownerEpoch, digest('f')));
            } finally {
                fenceHeld = false;
            }
        }

        private boolean attemptProviderPut(long candidateOwnerEpoch, Runnable providerPut) {
            java.util.Objects.requireNonNull(providerPut, "providerPut");
            boolean admitted = fenceHeld && candidateOwnerEpoch == ownerEpoch;
            if (!admitted && candidateOwnerEpoch < ownerEpoch) {
                rejectedOldOwnerPuts++;
            }
            if (admitted) {
                providerPut.run();
            }
            return admitted;
        }
    }

    private static final class TestKmsTransport implements KmsTransport {
        @Override
        public WrappedRunKeyEnvelope wrap(String keyIdentity, byte[] plaintextRunKey) {
            return new WrappedRunKeyEnvelope(
                    "fake-kms", "aes-kw-v1", keyIdentity, "version-test", CanonicalBytes.copyOf(plaintextRunKey));
        }

        @Override
        public byte[] unwrap(WrappedRunKeyEnvelope envelope) {
            return envelope.wrappedKey().toByteArray();
        }
    }

    private static final class TestControlMetadataStore implements CanonicalControlMetadataStore {
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
            if (!Optional.ofNullable(values.get(key)).equals(exactExpected)) {
                return ControlMutationOutcome.DEFINITIVE_CONFLICT;
            }
            values.put(key, exactCandidate);
            return ControlMutationOutcome.APPLIED;
        }

        boolean contains(String key) {
            return values.containsKey(key);
        }
    }

    private static final class UnknownObjectTransport implements ObjectProviderTransport {
        private byte[] body;
        private int putCalls;
        private int listCalls;
        private int fullGetCalls;

        @Override
        public ObjectProviderCapabilities capabilities() {
            return new ObjectProviderCapabilities("fake", true, true, true, true, true, 1024 * 1024, 4096, 100);
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) throws IOException {
            putCalls++;
            ByteArrayOutputStream retained = new ByteArrayOutputStream();
            body.transferTo(retained);
            this.body = retained.toByteArray();
            return ConditionalCreateResult.RESPONSE_UNKNOWN;
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) {
            fullGetCalls++;
            return new StreamingObject(body.length, 0, body.length, Optional.empty(), new ByteArrayInputStream(body));
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys) {
            listCalls++;
            return new ListPage(List.of(), Optional.empty());
        }

        @Override
        public FailureKind classifyFailure(IOException failure) {
            return FailureKind.FATAL;
        }
    }

    private static final class ExistingExactObjectTransport implements ObjectProviderTransport {
        private byte[] body;
        private int fullGetCalls;

        @Override
        public ObjectProviderCapabilities capabilities() {
            return new ObjectProviderCapabilities("fake", true, true, true, true, true, 1024 * 1024, 4096, 100);
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream candidate) throws IOException {
            ByteArrayOutputStream retained = new ByteArrayOutputStream();
            candidate.transferTo(retained);
            body = retained.toByteArray();
            return ConditionalCreateResult.ALREADY_EXISTS;
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) {
            fullGetCalls++;
            return new StreamingObject(
                    body.length,
                    0,
                    body.length,
                    Optional.of(CanonicalBytes.copyOf("provider-version".getBytes(StandardCharsets.US_ASCII))),
                    new ByteArrayInputStream(body));
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken) {
            int start = Math.toIntExact(inclusiveStart);
            int end = Math.toIntExact(exclusiveEnd);
            return new StreamingObject(
                    body.length,
                    inclusiveStart,
                    exclusiveEnd,
                    Optional.of(CanonicalBytes.copyOf("provider-version".getBytes(StandardCharsets.US_ASCII))),
                    new ByteArrayInputStream(java.util.Arrays.copyOfRange(body, start, end)));
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys) {
            return new ListPage(List.of(), Optional.empty());
        }
    }

    private record Fixture(PulsarObjectWalBridgeV1 bridge, FakeAuthority authority, FakeExtentStore store) {
        void activate(PulsarBindingKey binding, long ledgerId) {
            bridge.activate(binding, 7, RecoverySeed.empty(ledgerId))
                    .toCompletableFuture()
                    .join();
        }
    }

    private static final class FakeAuthority implements LedgerChainAuthority {
        private final Map<PulsarBindingKey, HeadSnapshot> heads = new HashMap<>();
        private int reads;
        private int mutations;
        private long nextVersion;

        @Override
        public CompletableFuture<Optional<HeadSnapshot>> readHead(PulsarBindingKey binding) {
            reads++;
            return CompletableFuture.completedFuture(Optional.ofNullable(heads.get(binding)));
        }

        @Override
        public CompletableFuture<MutationOutcome> compareAndSetHead(
                PulsarBindingKey binding, Optional<HeadSnapshot> exactPredecessor, LedgerNode candidate) {
            mutations++;
            HeadSnapshot prior = heads.get(binding);
            if (!java.util.Objects.equals(prior, exactPredecessor.orElse(null))) {
                return CompletableFuture.completedFuture(
                        new MutationOutcome(MutationKind.DEFINITIVE_CONFLICT, Optional.ofNullable(prior)));
            }
            HeadSnapshot installed = new HeadSnapshot(version(nextVersion++), candidate);
            heads.put(binding, installed);
            return CompletableFuture.completedFuture(
                    new MutationOutcome(MutationKind.APPLIED_EXACT, Optional.of(installed)));
        }
    }

    private static MetadataVersion version(long value) {
        return new MetadataVersion(CanonicalBytes.copyOf(
                java.nio.ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
    }

    private static final class FakeExtentStore implements ObjectWalExtentStore {
        private ProviderObjectOutcome nextOutcome = ProviderObjectOutcome.APPLIED_EXACT;
        private SealedExtentPlan lastPlan;
        private PulsarBindingKey bindingEntryFailure;
        private PulsarBindingKey bindingPublishFailure;
        private String firstPlanId;
        private long nextSequence;
        private int sealCount;
        private int successorPublications;
        private boolean failNextSeal;
        private boolean failPublishStage;
        private boolean failSuccessorStage;
        private boolean failReconcileStage;
        private boolean mismatchManifestVerification;
        private boolean failManifestVerification;
        private boolean failRecoveryVerification;
        private boolean mismatchRecoveryVerification;
        private int recoveryVerifications;
        private PulsarObjectWalBridgeV1.ActiveTailRecoveryRequest lastRecoveryRequest;
        private CompletableFuture<Void> activeReadGate;
        private PulsarObjectWalBridgeV1.ManifestSource lastManifestSource;
        private final List<PulsarObjectWalBridgeV1.PulsarPosition> resumePositions = new ArrayList<>();
        private final Map<String, byte[]> activeValues = new HashMap<>();
        private final Map<String, byte[]> manifestValues = new HashMap<>();

        @Override
        public SealedExtentPlan sealPlan(
                WalLaneId laneId,
                int packingPolicyVersion,
                List<PulsarObjectWalBridgeV1.PlannedEntry> members,
                long maximumCanonicalBodyBytes) {
            if (failNextSeal) {
                failNextSeal = false;
                throw new IllegalStateException("local seal failure");
            }
            byte[] digestBytes = new byte[Sha256Digest.LENGTH];
            digestBytes[digestBytes.length - 1] = (byte) ++sealCount;
            Sha256Digest planSha = Sha256Digest.copyOf(digestBytes);
            return new SealedExtentPlan(
                    planSha.toHex(), laneId, packingPolicyVersion, List.copyOf(members), maximumCanonicalBodyBytes);
        }

        @Override
        public CompletableFuture<PublishResult> publish(SealedExtentPlan plan) {
            lastPlan = plan;
            if (firstPlanId == null) {
                firstPlanId = plan.planId();
            }
            if (failPublishStage) {
                return CompletableFuture.failedFuture(new IllegalStateException("publish stage failed"));
            }
            return CompletableFuture.completedFuture(result(plan, ROOT_A, nextOutcome));
        }

        @Override
        public CompletableFuture<PublishResult> reconcile(
                SealedExtentPlan plan, Optional<ExtentIdentity> candidateIdentity) {
            if (failReconcileStage) {
                return CompletableFuture.failedFuture(new IllegalStateException("reconcile stage failed"));
            }
            return CompletableFuture.completedFuture(result(plan, ROOT_A, nextOutcome));
        }

        @Override
        public CompletableFuture<PublishResult> resumeSame(
                SealedExtentPlan plan,
                Optional<ExtentIdentity> candidateIdentity,
                List<PulsarObjectWalBridgeV1.PlannedEntry> unresolvedMembers) {
            resumePositions.addAll(
                    unresolvedMembers.stream().map(member -> member.position()).toList());
            return CompletableFuture.completedFuture(candidateIdentity
                    .map(identity -> result(
                            plan, unresolvedMembers, identity, nextOutcome, identity.laneSequence() == nextSequence))
                    .orElseGet(() -> result(plan, unresolvedMembers, ROOT_A, nextOutcome)));
        }

        @Override
        public CompletableFuture<PublishResult> publishSuccessor(
                SealedExtentPlan definitivelyAbsentPlan, SealedExtentPlan successorPlan) {
            successorPublications++;
            lastPlan = successorPlan;
            if (failSuccessorStage) {
                return CompletableFuture.failedFuture(new IllegalStateException("successor publication failed"));
            }
            nextSequence = 0;
            return CompletableFuture.completedFuture(result(successorPlan, ROOT_C, nextOutcome));
        }

        @Override
        public CompletableFuture<PulsarObjectWalBridgeV1.VerifiedManifestHandoff> verifyManifestHandoff(
                PulsarObjectWalBridgeV1.ManifestHandoffRequest request) {
            if (failManifestVerification) {
                return CompletableFuture.failedFuture(new IllegalStateException("manifest authority unavailable"));
            }
            PulsarObjectWalBridgeV1.ManifestHandoffRequest observed = mismatchManifestVerification
                    ? new PulsarObjectWalBridgeV1.ManifestHandoffRequest(
                            request.binding(),
                            request.virtualLedgerId(),
                            request.throughEntryId(),
                            request.manifestGeneration() + 1)
                    : request;
            PulsarObjectWalBridgeV1.ManifestSource source = new PulsarObjectWalBridgeV1.ManifestSource(
                    new ObjectIdentity("pulsar-manifest/verified", 4096, EXTENT_SHA), 3, "e".repeat(64));
            return CompletableFuture.completedFuture(
                    new PulsarObjectWalBridgeV1.VerifiedManifestHandoff(observed, source));
        }

        @Override
        public CompletableFuture<ReadEntry> readActive(ExtentLocator locator) {
            if (locator.binding().equals(bindingEntryFailure)) {
                return CompletableFuture.failedFuture(new IllegalStateException("binding-local frame failure"));
            }
            CompletableFuture<Void> gate =
                    activeReadGate == null ? CompletableFuture.completedFuture(null) : activeReadGate;
            return gate.thenApply(ignored -> {
                byte[] value = activeValues.get(key(locator.binding(), locator.position()));
                return new ReadEntry(
                        locator.binding(), locator.position(), value, ReadSource.ACTIVE_TAIL, ReadFailureScope.NONE);
            });
        }

        @Override
        public CompletableFuture<ReadEntry> readManifest(
                PulsarBindingKey binding,
                PulsarObjectWalBridgeV1.PulsarPosition position,
                long manifestGeneration,
                PulsarObjectWalBridgeV1.ManifestSource verifiedSource) {
            lastManifestSource = verifiedSource;
            return CompletableFuture.completedFuture(new ReadEntry(
                    binding,
                    position,
                    manifestValues.get(key(binding, position)),
                    ReadSource.MANIFEST,
                    ReadFailureScope.NONE));
        }

        @Override
        public CompletableFuture<PulsarObjectWalBridgeV1.VerifiedActiveTailRecovery> verifyRecoveryTail(
                PulsarObjectWalBridgeV1.ActiveTailRecoveryRequest request) {
            recoveryVerifications++;
            lastRecoveryRequest = request;
            if (failRecoveryVerification) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("active-tail recovery authority unavailable"));
            }
            PulsarObjectWalBridgeV1.ActiveTailRecoveryRequest observed = mismatchRecoveryVerification
                    ? new PulsarObjectWalBridgeV1.ActiveTailRecoveryRequest(
                            request.binding(),
                            request.virtualLedgerId(),
                            request.manifestThrough(),
                            request.durableThrough() - 1,
                            request.activeTail())
                    : request;
            return CompletableFuture.completedFuture(new PulsarObjectWalBridgeV1.VerifiedActiveTailRecovery(observed));
        }

        private PublishResult result(SealedExtentPlan plan, Sha256Digest root, ProviderObjectOutcome outcome) {
            return result(plan, plan.members(), root, outcome);
        }

        private PublishResult result(
                SealedExtentPlan plan,
                List<PulsarObjectWalBridgeV1.PlannedEntry> resultMembers,
                Sha256Digest root,
                ProviderObjectOutcome outcome) {
            ExtentIdentity identity = identity(
                    root,
                    plan.laneId(),
                    nextSequence,
                    "object-wal/" + root.toHex().substring(0, 8) + "/lane-"
                            + plan.laneId().leafToken() + '/' + nextSequence);
            return result(plan, resultMembers, identity, outcome, true);
        }

        private PublishResult result(
                SealedExtentPlan plan,
                List<PulsarObjectWalBridgeV1.PlannedEntry> resultMembers,
                ExtentIdentity identity,
                ProviderObjectOutcome outcome,
                boolean advanceSequence) {
            if (outcome != ProviderObjectOutcome.APPLIED_EXACT && outcome != ProviderObjectOutcome.EXISTING_EXACT) {
                return new PublishResult(outcome, Optional.of(identity), Optional.empty(), List.of());
            }
            List<PulsarObjectWalBridgeV1.MemberPublishResult> memberResults = new ArrayList<>();
            for (PulsarObjectWalBridgeV1.PlannedEntry member : resultMembers) {
                int frame = plan.members().indexOf(member);
                byte[] payload = member.payload();
                activeValues.put(key(member.binding(), member.position()), payload);
                if (member.binding().equals(bindingPublishFailure)) {
                    memberResults.add(new FailedMember(
                            member.binding(),
                            member.position(),
                            new MemberFailure(
                                    Nwg1RejectionV1.NATIVE_CHECKSUM_MISMATCH,
                                    Nwg1ValidationStageV1.NATIVE_FRAME,
                                    Nwg1IsolationScopeV1.APPEND_UNIT)));
                } else {
                    memberResults.add(new VerifiedMember(new ExtentLocator(
                            member.binding(),
                            member.position(),
                            identity,
                            frame,
                            (long) frame * 128,
                            payload.length,
                            FRAME_SHA)));
                }
            }
            ProviderResolvedExtentDescriptor descriptor = new ProviderResolvedExtentDescriptor(
                    identity.walRunRootSha256(),
                    new ProviderResolvedExtentRowV1(
                            plan.laneId(),
                            identity.laneSequence(),
                            256,
                            identity.objectIdentity().bodyLength(),
                            identity.objectIdentity().bodySha256(),
                            ProviderVersionProof.none()),
                    1);
            if (advanceSequence) {
                nextSequence++;
            }
            return new PublishResult(outcome, Optional.of(identity), Optional.of(descriptor), memberResults);
        }
    }
}
