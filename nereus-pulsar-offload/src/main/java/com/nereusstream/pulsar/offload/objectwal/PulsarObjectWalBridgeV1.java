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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.LedgerNode;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.OpenedLedger;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.LaneSequenceReservation;
import com.nereusstream.storage.object.control.LaneSequenceVector;
import com.nereusstream.storage.object.control.ProviderResolvedExtentDescriptor;
import com.nereusstream.storage.object.control.ProviderResolvedExtentRowV1;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalCheckpointPublisher;
import com.nereusstream.storage.object.control.WalLaneId;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRuntime;
import com.nereusstream.storage.object.control.WalRunSealRecord;
import com.nereusstream.storage.object.extent.ObjectWalFormatCaps;
import com.nereusstream.storage.object.nwg1.GroupEncodingPlanV1;
import com.nereusstream.storage.object.nwg1.Nwg1DirectoryV1;
import com.nereusstream.storage.object.nwg1.Nwg1IsolationScopeV1;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1RejectionV1;
import com.nereusstream.storage.object.nwg1.Nwg1SealedObjectV1;
import com.nereusstream.storage.object.nwg1.Nwg1ValidationException;
import com.nereusstream.storage.object.nwg1.Nwg1ValidationStageV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.provider.ProviderObjectResult;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import com.nereusstream.storage.object.recovery.OwnerOpenRecoveryCoordinator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * Owner-local live Pulsar Object-WAL writer/reader bridge.
 *
 * <p>This is deliberately separate from the M2 sealed-ledger NPD1/NPO1 offloader. It maps one admitted ManagedLedger
 * entry to one exact {@code (virtualLedgerId, entryId)} position and an authenticated live Object-WAL extent locator.
 * A normal append performs no metadata read or mutation: the chain controller is called only at activation or ledger
 * rollover. The returned ACK stage completes only after hidden locator installation and readable/durable frontier
 * publication.
 */
public final class PulsarObjectWalBridgeV1 {
    private final Configuration configuration;
    private final PulsarVirtualLedgerChainControllerV1 chainController;
    private final ObjectWalExtentStore extentStore;
    private final Object monitor = new Object();
    private final Map<PulsarBindingKey, BindingState> bindings = new HashMap<>();
    private final Map<PulsarBindingKey, BooleanSupplier> m4RetirementGuards = new HashMap<>();
    private final Map<String, FailedPlan> failedPlans = new HashMap<>();
    private final Map<String, PendingSuccessorRollover> pendingSuccessorRollovers = new HashMap<>();
    private final Map<String, SealedSuccessorRollover> sealedSuccessorRollovers = new HashMap<>();
    private final Set<String> allocatingSuccessorRollovers = new HashSet<>();
    private final Map<RunLane, PhysicalLaneState> physicalLanes = new HashMap<>();
    private boolean walRunAdmissionStopped;

    public PulsarObjectWalBridgeV1(
            Configuration configuration,
            PulsarVirtualLedgerChainControllerV1 chainController,
            ObjectWalExtentStore extentStore) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.chainController = Objects.requireNonNull(chainController, "chainController");
        this.extentStore = Objects.requireNonNull(extentStore, "extentStore");
    }

    /** Opens one durable chain head and reconstructs its bounded active tail before admitting appends. */
    public CompletionStage<LedgerFrontiers> activate(
            PulsarBindingKey binding, long ownerEpoch, RecoverySeed recoverySeed) {
        try {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(recoverySeed, "recoverySeed");
            return chainController.open(binding, ownerEpoch).thenCompose(opened -> {
                BindingState.requireRecoveryShape(configuration, opened, recoverySeed);
                ActiveTailRecoveryRequest activeTailRequest =
                        ActiveTailRecoveryRequest.from(opened.node().binding(), recoverySeed);
                return verifyRecoveryManifest(opened, recoverySeed).thenCompose(manifestSource -> verifyRecoveryTail(
                                activeTailRequest)
                        .thenApply(verifiedTail -> {
                            verifiedTail.requireExact(activeTailRequest);
                            BindingState recovered =
                                    BindingState.recover(configuration, opened, recoverySeed, manifestSource);
                            synchronized (monitor) {
                                BindingState previous = bindings.putIfAbsent(binding, recovered);
                                if (previous != null) {
                                    throw rejected(
                                            BridgeRejectionCode.BINDING_ALREADY_ACTIVE, "binding is already active");
                                }
                                return recovered.currentFrontiers();
                            }
                        }));
            });
        } catch (Throwable error) {
            return failed(error);
        }
    }

    private CompletionStage<Optional<ManifestSource>> verifyRecoveryManifest(
            OpenedLedger opened, RecoverySeed recoverySeed) {
        if (recoverySeed.manifestHandoffRequest().isEmpty()) {
            return completed(Optional.empty());
        }
        ManifestHandoffRequest request = recoverySeed.manifestHandoffRequest().orElseThrow();
        if (!request.binding().equals(opened.node().binding())
                || request.virtualLedgerId() != recoverySeed.virtualLedgerId()
                || request.throughEntryId() != recoverySeed.manifestThrough()
                || request.manifestGeneration() != recoverySeed.manifestGeneration()) {
            return failed(rejected(
                    BridgeRejectionCode.MANIFEST_HANDOFF_NOT_VERIFIED,
                    "recovery manifest request differs from its binding/frontier seed"));
        }
        return verifyManifestAuthority(request).thenApply(verified -> Optional.of(verified.source()));
    }

    private CompletionStage<VerifiedActiveTailRecovery> verifyRecoveryTail(ActiveTailRecoveryRequest request) {
        try {
            CompletionStage<VerifiedActiveTailRecovery> verification = Objects.requireNonNull(
                    extentStore.verifyRecoveryTail(request), "active-tail recovery verification stage");
            return verification.thenApply(result -> {
                VerifiedActiveTailRecovery exact =
                        Objects.requireNonNull(result, "verified active-tail recovery result");
                exact.requireExact(request);
                return exact;
            });
        } catch (Throwable error) {
            return failed(error);
        }
    }

    /**
     * Seals an immutable shared-extent plan and publishes it through the live Object-WAL store.
     *
     * <p>Every member must have the same lane and packing policy. This first implementation permits at most one entry
     * from a binding in a shared extent, so an absent extent can never create an intra-ledger {@code n -> n+1} skip.
     */
    public CompletionStage<List<MemberAppendResult>> appendShared(List<AppendInput> inputs) {
        final SealedExtentPlan plan;
        try {
            plan = reserveAndSeal(inputs);
        } catch (Throwable error) {
            return failed(error);
        }
        final CompletionStage<PublishResult> publish;
        try {
            publish = extentStore.publish(plan);
        } catch (Throwable error) {
            return retainExceptionalPublish(plan, error).thenCompose(result -> applyPublishResult(plan, result, false));
        }
        return reconcileOrRetainExceptional(plan, publish)
                .thenCompose(result -> applyPublishResult(plan, result, false));
    }

    /** Reconciles and republishes the exact failed plan without changing any Pulsar position. */
    public CompletionStage<List<MemberAppendResult>> resumeSameEntry(String planId) {
        final FailedPlan failedPlan;
        try {
            synchronized (monitor) {
                failedPlan = requireFailedPlan(planId);
                if (failedPlan.reason() == ProviderObjectOutcome.DEFINITIVE_CONFLICT) {
                    throw rejected(
                            BridgeRejectionCode.FENCED, "a definitive conflict cannot be retried as the same owner");
                }
                failedPlan.unresolvedMembers().forEach(member -> {
                    BindingState state = requireBinding(member.binding());
                    state.requireGap(failedPlan.plan().planId(), member.position());
                });
            }
        } catch (Throwable error) {
            return failed(error);
        }
        final CompletionStage<PublishResult> resume;
        try {
            resume = extentStore.resumeSame(
                    failedPlan.plan(), failedPlan.candidateIdentity(), failedPlan.unresolvedMembers());
        } catch (Throwable error) {
            return retainExceptionalPublish(failedPlan.plan(), error)
                    .thenCompose(result -> applyPublishResult(failedPlan.plan(), result, true));
        }
        return reconcileOrRetainExceptional(failedPlan.plan(), resume)
                .thenCompose(result -> applyPublishResult(failedPlan.plan(), result, true));
    }

    /**
     * For one definitively absent single-binding plan, seals before the missing entry and writes the same payload at
     * entry zero of an explicit successor ledger. Shared-plan rollover requires a future atomic multi-head authority
     * and therefore fails closed; shared plans remain recoverable through {@link #resumeSameEntry(String)}.
     */
    public CompletionStage<List<MemberAppendResult>> sealAndRolloverFailedEntry(String planId) {
        final PendingSuccessorRollover pendingSuccessor;
        final SealedSuccessorRollover sealedSuccessor;
        synchronized (monitor) {
            pendingSuccessor = pendingSuccessorRollovers.get(planId);
            sealedSuccessor = sealedSuccessorRollovers.get(planId);
        }
        if (sealedSuccessor != null) {
            try {
                return publishSealedSuccessor(planId, sealedSuccessor);
            } catch (Throwable error) {
                return failed(error);
            }
        }
        if (pendingSuccessor != null) {
            try {
                return sealAndPublishPendingSuccessor(planId, pendingSuccessor);
            } catch (Throwable error) {
                return failed(error);
            }
        }

        final FailedPlan failedPlan;
        final PlannedEntry oldMember;
        final BindingState state;
        try {
            synchronized (monitor) {
                failedPlan = requireFailedPlan(planId);
                if (failedPlan.reason() != ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED) {
                    throw rejected(
                            BridgeRejectionCode.ABSENCE_NOT_PROVEN,
                            "seal/rollover requires a definitive provider-absence result");
                }
                if (failedPlan.unresolvedMembers().size() != 1
                        || failedPlan.plan().members().size() != 1) {
                    throw rejected(
                            BridgeRejectionCode.ATOMIC_MULTI_BINDING_ROLLOVER_UNAVAILABLE,
                            "shared-plan rollover requires an atomic multi-binding chain authority");
                }
                oldMember = failedPlan.unresolvedMembers().get(0);
                state = requireBinding(oldMember.binding());
                state.requireGap(failedPlan.plan().planId(), oldMember.position());
                if (!allocatingSuccessorRollovers.add(planId)) {
                    throw rejected(
                            BridgeRejectionCode.BINDING_APPEND_IN_FLIGHT,
                            "one exact successor rollover allocation is already in flight");
                }
            }
        } catch (Throwable error) {
            return failed(error);
        }

        long terminalEntryId = oldMember.position().entryId() - 1;
        return chainController
                .sealAndOpenSuccessor(state.current, terminalEntryId, oldMember.ownerEpoch())
                .whenComplete((ignored, error) -> {
                    synchronized (monitor) {
                        allocatingSuccessorRollovers.remove(planId);
                    }
                })
                .thenCompose(successor -> {
                    final PendingSuccessorRollover exactPending;
                    synchronized (monitor) {
                        state.releaseDefinitiveCancellation(failedPlan.plan().planId(), oldMember.position());
                        state.installSuccessor(successor);
                        PlannedEntry successorMember = oldMember.withPosition(
                                new PulsarPosition(successor.node().virtualLedgerId(), 0));
                        exactPending = new PendingSuccessorRollover(failedPlan, successor, successorMember);
                        if (pendingSuccessorRollovers.putIfAbsent(planId, exactPending) != null) {
                            throw rejected(
                                    BridgeRejectionCode.BINDING_APPEND_IN_FLIGHT,
                                    "one exact successor rollover is already pending local seal");
                        }
                    }
                    return sealAndPublishPendingSuccessor(planId, exactPending);
                });
    }

    private CompletionStage<List<MemberAppendResult>> sealAndPublishPendingSuccessor(
            String predecessorPlanId, PendingSuccessorRollover pending) {
        final SealedExtentPlan successorPlan;
        final SealedSuccessorRollover sealedSuccessor;
        synchronized (monitor) {
            if (pendingSuccessorRollovers.get(predecessorPlanId) != pending) {
                return failed(rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "pending successor rollover changed before its exact local seal retry"));
            }
            PlannedEntry successorMember = pending.successorMember();
            BindingState state = requireBinding(successorMember.binding());
            state.requireOwner(successorMember.ownerEpoch());
            state.requireQuiescent();
            if (!state.current.equals(pending.successor())) {
                return failed(rejected(
                        BridgeRejectionCode.RECOVERY_MAPPING_INVALID,
                        "pending successor rollover differs from the exact durable chain Head"));
            }
            PrePositionReservation reservation = state.reserveBeforePosition(configuration);
            CompletionTicket ticket = state.allocateTicketAfterPosition(reservation, successorMember.position());
            try {
                FailedPlan predecessorFailure = pending.predecessorFailure();
                successorPlan = extentStore.sealPlan(
                        predecessorFailure.plan().laneId(),
                        predecessorFailure.plan().packingPolicyVersion(),
                        List.of(successorMember),
                        configuration.maximumCanonicalExtentBodyBytes());
                successorPlan.requireExactRequest(
                        predecessorFailure.plan().laneId(),
                        predecessorFailure.plan().packingPolicyVersion(),
                        List.of(successorMember),
                        configuration.maximumCanonicalExtentBodyBytes());
                if (successorPlan.planId().equals(predecessorPlanId)) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "successor entry-zero plan aliases its definitively absent predecessor plan");
                }
                state.installInFlight(successorPlan.planId(), successorMember.position(), ticket);
            } catch (Throwable error) {
                state.cancelAfterPositionBeforeProvider(ticket, successorMember.position());
                return failed(error);
            }
            state.installGap(successorPlan.planId(), successorMember.position());
            pendingSuccessorRollovers.remove(predecessorPlanId, pending);
            failedPlans.remove(pending.predecessorFailure().plan().planId());
            failedPlans.put(
                    successorPlan.planId(),
                    new FailedPlan(
                            successorPlan,
                            ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED,
                            Optional.empty(),
                            successorPlan.members(),
                            true));
            sealedSuccessor = new SealedSuccessorRollover(pending, successorPlan);
            if (sealedSuccessorRollovers.putIfAbsent(predecessorPlanId, sealedSuccessor) != null) {
                throw rejected(
                        BridgeRejectionCode.BINDING_APPEND_IN_FLIGHT,
                        "one exact sealed successor rollover is already pending publication");
            }
        }
        return publishSealedSuccessor(predecessorPlanId, sealedSuccessor);
    }

    private CompletionStage<List<MemberAppendResult>> publishSealedSuccessor(
            String predecessorPlanId, SealedSuccessorRollover sealed) {
        Objects.requireNonNull(sealed, "sealed");
        SealedExtentPlan successorPlan = sealed.successorPlan();
        synchronized (monitor) {
            if (sealedSuccessorRollovers.get(predecessorPlanId) != sealed) {
                return failed(rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "sealed successor rollover changed before exact publication retry"));
            }
            FailedPlan successorFailure = requireFailedPlan(successorPlan.planId());
            if (successorFailure.reason() == ProviderObjectOutcome.DEFINITIVE_CONFLICT) {
                return failed(rejected(
                        BridgeRejectionCode.FENCED,
                        "a definitive successor publication conflict cannot be retried as the same owner"));
            }
        }
        final CompletionStage<PublishResult> publishSuccessor;
        try {
            publishSuccessor = Objects.requireNonNull(
                    extentStore.publishSuccessor(
                            sealed.pending().predecessorFailure().plan(), successorPlan),
                    "successor publication stage");
        } catch (Throwable error) {
            return retainExceptionalSuccessorPublish(successorPlan, error);
        }
        CompletableFuture<List<MemberAppendResult>> completed = new CompletableFuture<>();
        publishSuccessor.whenComplete((published, publicationError) -> {
            CompletionStage<List<MemberAppendResult>> applied = publicationError == null
                    ? applyPublishResult(successorPlan, published, true)
                    : retainExceptionalSuccessorPublish(successorPlan, publicationError);
            applied.whenComplete(copyTo(completed));
        });
        return completed;
    }

    /** Explicit low-frequency rollover used before an entry-count bound, never during an admitted append. */
    public CompletionStage<LedgerFrontiers> rollover(PulsarBindingKey binding, long ownerEpoch) {
        final BindingState state;
        final long terminal;
        try {
            synchronized (monitor) {
                state = requireBinding(binding);
                state.requireOwner(ownerEpoch);
                state.requireQuiescent();
                terminal = state.currentLedger().durableThrough;
            }
            return chainController
                    .sealAndOpenSuccessor(state.current, terminal, ownerEpoch)
                    .thenApply(successor -> {
                        synchronized (monitor) {
                            state.installSuccessor(successor);
                            return state.currentFrontiers();
                        }
                    });
        } catch (Throwable error) {
            return failed(error);
        }
    }

    /** Installs only a fully streamed, Root-budgeted physical checkpoint before new writes. */
    public void initializePhysicalFrontiers(VerifiedPhysicalCheckpoint checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint");
        synchronized (monitor) {
            for (WalLaneId laneId : WalLaneId.values()) {
                long resolvedThrough = checkpoint.coveredThrough().get(laneId);
                RunLane key = new RunLane(checkpoint.walRunRootSha256(), laneId);
                PhysicalLaneState previous = physicalLanes.get(key);
                if (previous != null && previous.resolvedThrough != resolvedThrough) {
                    throw rejected(
                            BridgeRejectionCode.PHYSICAL_FRONTIER_MISMATCH,
                            "physical frontier was already initialized to another checkpoint sequence");
                }
            }
            for (WalLaneId laneId : WalLaneId.values()) {
                long resolvedThrough = checkpoint.coveredThrough().get(laneId);
                if (resolvedThrough >= 0) {
                    physicalLanes.putIfAbsent(
                            new RunLane(checkpoint.walRunRootSha256(), laneId), new PhysicalLaneState(resolvedThrough));
                }
            }
        }
    }

    /** Verifies a low-frequency manifest handoff before publishing coverage or releasing active locators. */
    public CompletionStage<LedgerFrontiers> installManifestHandoff(ManifestHandoffRequest request) {
        Objects.requireNonNull(request, "request");
        final CompletionStage<VerifiedManifestHandoff> verification;
        try {
            synchronized (monitor) {
                requireBinding(request.binding())
                        .requireLedger(request.virtualLedgerId())
                        .requireManifestHandoff(request);
            }
            verification = verifyManifestAuthority(request);
        } catch (Throwable error) {
            return failed(error);
        }
        return Objects.requireNonNull(verification, "manifest handoff verification")
                .thenApply(verified -> {
                    synchronized (monitor) {
                        BindingState state = requireBinding(request.binding());
                        LedgerState ledger = state.requireLedger(request.virtualLedgerId());
                        ledger.requireManifestHandoff(request);
                        ledger.manifestGeneration = request.manifestGeneration();
                        ledger.manifestThrough = request.throughEntryId();
                        ledger.manifestSource = verified.source();
                        releaseCoveredUnpinnedLocators(request.binding(), ledger);
                        state.advanceReadView();
                        return ledger.frontiers(request.binding());
                    }
                });
    }

    private CompletionStage<VerifiedManifestHandoff> verifyManifestAuthority(ManifestHandoffRequest request) {
        CompletableFuture<VerifiedManifestHandoff> verified = new CompletableFuture<>();
        final CompletionStage<VerifiedManifestHandoff> verification;
        try {
            verification =
                    Objects.requireNonNull(extentStore.verifyManifestHandoff(request), "manifest handoff verification");
        } catch (Throwable error) {
            verified.completeExceptionally(manifestVerificationFailure(error));
            return verified;
        }
        verification.whenComplete((result, error) -> {
            if (error != null) {
                verified.completeExceptionally(manifestVerificationFailure(error));
                return;
            }
            try {
                Objects.requireNonNull(result, "verified manifest handoff").requireExact(request);
                verified.complete(result);
            } catch (Throwable invalid) {
                verified.completeExceptionally(manifestVerificationFailure(invalid));
            }
        });
        return verified;
    }

    private static BridgeException manifestVerificationFailure(Throwable error) {
        if (error instanceof BridgeException bridge
                && bridge.code() == BridgeRejectionCode.MANIFEST_HANDOFF_NOT_VERIFIED) {
            return bridge;
        }
        BridgeException rejected = rejected(
                BridgeRejectionCode.MANIFEST_HANDOFF_NOT_VERIFIED,
                "manifest authority did not return the exact verified source");
        rejected.addSuppressed(error);
        return rejected;
    }

    /** Reads one exact Pulsar position from the captured manifest or active-tail source. */
    public CompletionStage<ReadEntry> read(PulsarBindingKey binding, PulsarPosition position) {
        final CompletionStage<ReadEntry> read;
        final LedgerState pinnedLedger;
        final long pinnedEntryId;
        try {
            synchronized (monitor) {
                BindingState state = requireBinding(binding);
                LedgerState ledger = state.requireLedger(position.virtualLedgerId());
                if (position.entryId() < 0 || position.entryId() > ledger.readableThrough) {
                    throw rejected(
                            BridgeRejectionCode.POSITION_NOT_READABLE, "position is beyond the readable frontier");
                }
                if (position.entryId() <= ledger.manifestThrough) {
                    ManifestSource source = Optional.ofNullable(ledger.manifestSource)
                            .orElseThrow(() -> rejected(
                                    BridgeRejectionCode.MANIFEST_HANDOFF_NOT_VERIFIED,
                                    "manifest-covered position has no authority-verified source"));
                    read = Objects.requireNonNull(
                            extentStore.readManifest(binding, position, ledger.manifestGeneration, source),
                            "manifest read stage");
                    pinnedLedger = null;
                    pinnedEntryId = -1;
                } else {
                    ExtentLocator locator = Optional.ofNullable(ledger.activeTail.get(position.entryId()))
                            .orElseThrow(() -> rejected(
                                    BridgeRejectionCode.ACTIVE_TAIL_LOCATOR_MISSING,
                                    "readable active-tail position has no locator"));
                    ledger.pinActiveRead(position.entryId());
                    try {
                        read = Objects.requireNonNull(extentStore.readActive(locator), "active read stage");
                    } catch (Throwable error) {
                        ledger.unpinActiveRead(position.entryId());
                        throw error;
                    }
                    pinnedLedger = ledger;
                    pinnedEntryId = position.entryId();
                }
            }
            CompletionStage<ReadEntry> validated = read.thenApply(result -> validateRead(binding, position, result));
            if (pinnedLedger == null) {
                return validated;
            }
            return validated.whenComplete((ignored, error) -> {
                synchronized (monitor) {
                    pinnedLedger.unpinActiveRead(pinnedEntryId);
                    releaseCoveredUnpinnedLocators(binding, pinnedLedger);
                }
            });
        } catch (Throwable error) {
            return failed(error);
        }
    }

    /**
     * Reads only the exact source named by an accepted M4 view; a later manifest handoff cannot
     * silently replan the operation. Active-tail use is admitted only if the exact locator can be
     * pinned under the bridge monitor before retirement.
     */
    public CompletionStage<ReadEntry> readCaptured(
            PulsarObjectWalReadViewV1 captured, PulsarBindingKey binding, PulsarPosition position) {
        final CompletionStage<ReadEntry> read;
        final LedgerState pinnedLedger;
        final long pinnedEntryId;
        try {
            Objects.requireNonNull(captured, "captured");
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(position, "position");
            if (!captured.binding().equals(binding)) {
                throw rejected(BridgeRejectionCode.FENCED, "captured M4 view belongs to another Pulsar binding");
            }
            PulsarObjectWalReadViewV1.LedgerView capturedLedger = captured.requireLedger(position.virtualLedgerId());
            PulsarObjectWalReadViewV1.SourceInterval interval = capturedLedger.requireInterval(position.entryId());
            synchronized (monitor) {
                if (interval.source() == ReadSource.MANIFEST) {
                    ManifestSource source = interval.manifest().orElseThrow();
                    read = Objects.requireNonNull(
                            extentStore.readManifest(binding, position, capturedLedger.manifestGeneration(), source),
                            "captured manifest read stage");
                    pinnedLedger = null;
                    pinnedEntryId = -1;
                } else {
                    ExtentLocator locator = interval.activeLocator().orElseThrow();
                    LedgerState currentLedger = requireBinding(binding).requireLedger(position.virtualLedgerId());
                    if (!locator.equals(currentLedger.activeTail.get(position.entryId()))) {
                        throw rejected(
                                BridgeRejectionCode.ACTIVE_TAIL_LOCATOR_MISSING,
                                "captured M4 active locator retired before exact pin admission");
                    }
                    currentLedger.pinActiveRead(position.entryId());
                    try {
                        read = Objects.requireNonNull(extentStore.readActive(locator), "captured active read stage");
                    } catch (Throwable error) {
                        currentLedger.unpinActiveRead(position.entryId());
                        throw error;
                    }
                    pinnedLedger = currentLedger;
                    pinnedEntryId = position.entryId();
                }
            }
            CompletionStage<ReadEntry> validated = read.thenApply(result -> validateRead(binding, position, result));
            if (pinnedLedger == null) {
                return validated;
            }
            return validated.whenComplete((ignored, error) -> {
                synchronized (monitor) {
                    pinnedLedger.unpinActiveRead(pinnedEntryId);
                    releaseCoveredUnpinnedLocators(binding, pinnedLedger);
                }
            });
        } catch (Throwable error) {
            return failed(error);
        }
    }

    public LedgerFrontiers frontiers(PulsarBindingKey binding) {
        synchronized (monitor) {
            return requireBinding(binding).currentFrontiers();
        }
    }

    /** Captures one immutable current-source view for low-frequency M4 authority publication. */
    public PulsarObjectWalReadViewV1 captureReadView(PulsarBindingKey binding) {
        synchronized (monitor) {
            return requireBinding(binding).captureReadView(binding);
        }
    }

    /** Registers the binding-wide M4 hazard scan that closes the capture-to-inner-pin race. */
    public void registerM4RetirementGuard(PulsarBindingKey binding, BooleanSupplier guard) {
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(guard, "guard");
        synchronized (monitor) {
            requireBinding(binding);
            BooleanSupplier previous = m4RetirementGuards.putIfAbsent(binding, guard);
            if (previous != null && previous != guard) {
                throw new IllegalStateException("Pulsar binding already has another M4 retirement guard");
            }
        }
    }

    /** Rechecks retained manifest-covered locators after the exact outer M4 lease drains. */
    public void reconcileM4Retirement(PulsarBindingKey binding) {
        synchronized (monitor) {
            BindingState state = requireBinding(binding);
            if (m4AllowsRetirement(binding)) {
                state.ledgers.values().forEach(LedgerState::releaseCoveredUnpinnedLocators);
            }
        }
    }

    private void releaseCoveredUnpinnedLocators(PulsarBindingKey binding, LedgerState ledger) {
        if (m4AllowsRetirement(binding)) {
            ledger.releaseCoveredUnpinnedLocators();
        }
    }

    private boolean m4AllowsRetirement(PulsarBindingKey binding) {
        BooleanSupplier guard = m4RetirementGuards.get(binding);
        if (guard == null) {
            return true;
        }
        try {
            return guard.getAsBoolean();
        } catch (Throwable failure) {
            return false;
        }
    }

    /**
     * Discards all owner-local reservations and tickets only after the caller has durably fenced this exact owner.
     * The successor owner must activate from durable chain/extent evidence and receives fresh tickets.
     */
    private void discardOwnerLocalStateAfterDurableFence(PulsarBindingKey binding, long fencedOwnerEpoch) {
        discardOwnerLocalStatesAfterDurableFence(Map.of(binding, fencedOwnerEpoch));
    }

    /** Atomically discards a set of fenced owners; every member of an unresolved shared plan must be included. */
    private void discardOwnerLocalStatesAfterDurableFence(Map<PulsarBindingKey, Long> fencedOwners) {
        Objects.requireNonNull(fencedOwners, "fencedOwners");
        if (fencedOwners.isEmpty()) {
            throw new IllegalArgumentException("at least one fenced owner is required");
        }
        synchronized (monitor) {
            Map<PulsarBindingKey, BindingState> discardable = new HashMap<>();
            fencedOwners.forEach((binding, fencedOwnerEpoch) -> {
                Objects.requireNonNull(binding, "fenced binding");
                Objects.requireNonNull(fencedOwnerEpoch, "fenced owner epoch");
                BindingState state = requireBinding(binding);
                state.requireOwner(fencedOwnerEpoch);
                discardable.put(binding, state);
            });
            Set<String> affectedOwnerLocalPlans = new HashSet<>();
            discardable.values().forEach(state -> state.livePlanId().ifPresent(affectedOwnerLocalPlans::add));
            for (String planId : affectedOwnerLocalPlans) {
                boolean everyOwnerLocalSiblingIncluded = bindings.entrySet().stream()
                        .filter(entry -> entry.getValue().matchesPlanId(planId))
                        .allMatch(entry -> discardable.containsKey(entry.getKey()));
                if (!everyOwnerLocalSiblingIncluded) {
                    throw rejected(
                            BridgeRejectionCode.ATOMIC_SHARED_TAKEOVER_REQUIRED,
                            "every owner-local shared-plan sibling must be fenced and discarded atomically");
                }
            }
            for (FailedPlan failedPlan : failedPlans.values()) {
                boolean affected = failedPlan.unresolvedMembers().stream()
                        .anyMatch(member -> discardable.containsKey(member.binding()));
                if (!affected) {
                    continue;
                }
                if (allocatingSuccessorRollovers.contains(failedPlan.plan().planId())) {
                    throw rejected(
                            BridgeRejectionCode.BINDING_APPEND_IN_FLIGHT,
                            "an owner fence cannot discard an in-flight successor Head allocation");
                }
                if (failedPlan.reason() == ProviderObjectOutcome.OUTCOME_UNKNOWN) {
                    throw rejected(
                            BridgeRejectionCode.PROVIDER_OUTCOME_UNKNOWN,
                            "an owner fence cannot discard an exact unknown Provider candidate or position");
                }
                PendingSuccessorRollover pendingSuccessor =
                        pendingSuccessorRollovers.get(failedPlan.plan().planId());
                boolean wholePlanDiscardable = failedPlan.unresolvedMembers().stream()
                        .allMatch(member -> {
                            BindingState sibling = discardable.get(member.binding());
                            Long fencedOwner = fencedOwners.get(member.binding());
                            boolean exactPendingSuccessor = pendingSuccessor != null
                                    && pendingSuccessor
                                            .successorMember()
                                            .binding()
                                            .equals(member.binding())
                                    && sibling != null
                                    && sibling.current.equals(pendingSuccessor.successor());
                            return sibling != null
                                    && fencedOwner != null
                                    && fencedOwner == member.ownerEpoch()
                                    && (sibling.matchesPlanned(failedPlan.plan().planId(), member.position())
                                            || exactPendingSuccessor);
                        });
                if (!wholePlanDiscardable) {
                    throw rejected(
                            BridgeRejectionCode.ATOMIC_SHARED_TAKEOVER_REQUIRED,
                            "every unresolved shared-plan sibling must be durably fenced and discarded atomically");
                }
            }
            discardable.forEach((binding, state) -> {
                state.discardAfterFence(fencedOwners.get(binding));
                bindings.remove(binding, state);
            });
            Set<String> removedPlanIds = new HashSet<>();
            failedPlans.entrySet().removeIf(entry -> {
                boolean remove = entry.getValue().unresolvedMembers().stream()
                        .allMatch(member -> discardable.containsKey(member.binding()));
                if (remove) {
                    removedPlanIds.add(entry.getKey());
                }
                return remove;
            });
            pendingSuccessorRollovers.entrySet().removeIf(entry -> {
                boolean remove = discardable.containsKey(
                        entry.getValue().successorMember().binding());
                if (remove) {
                    removedPlanIds.add(entry.getKey());
                }
                return remove;
            });
            sealedSuccessorRollovers
                    .entrySet()
                    .removeIf(entry -> removedPlanIds.contains(entry.getKey())
                            || removedPlanIds.contains(
                                    entry.getValue().successorPlan().planId())
                            || discardable.containsKey(
                                    entry.getValue().pending().successorMember().binding()));
            walRunAdmissionStopped = failedPlans.values().stream().anyMatch(FailedPlan::stopsWalRun);
        }
    }

    CompletionTrackerRing completionTrackerForTest(PulsarBindingKey binding) {
        synchronized (monitor) {
            return requireBinding(binding).completionRing;
        }
    }

    public LaneExtentResolvedThrough physicalFrontier(Sha256Digest walRunRootSha256, WalLaneId laneId) {
        synchronized (monitor) {
            PhysicalLaneState state = physicalLanes.get(new RunLane(walRunRootSha256, laneId));
            if (state == null) {
                throw rejected(BridgeRejectionCode.PHYSICAL_FRONTIER_MISMATCH, "physical lane is not initialized");
            }
            return new LaneExtentResolvedThrough(walRunRootSha256, laneId, state.resolvedThrough);
        }
    }

    private SealedExtentPlan reserveAndSeal(List<AppendInput> inputs) {
        Objects.requireNonNull(inputs, "inputs");
        if (inputs.isEmpty() || inputs.size() > configuration.maxSharedExtentMembers()) {
            throw rejected(BridgeRejectionCode.SHARED_PLAN_SIZE_INVALID, "shared plan member count is out of bounds");
        }
        WalLaneId laneId = inputs.get(0).laneId();
        int packingPolicyVersion = inputs.get(0).packingPolicyVersion();
        requireLane(laneId);
        if (packingPolicyVersion <= 0) {
            throw rejected(BridgeRejectionCode.PACKING_POLICY_MISMATCH, "packing policy version must be positive");
        }

        synchronized (monitor) {
            if (walRunAdmissionStopped) {
                throw rejected(
                        BridgeRejectionCode.WALRUN_ADMISSION_STOPPED, "WalRun admission is stopped for recovery");
            }
            Set<PulsarBindingKey> uniqueBindings = new HashSet<>();
            List<ValidatedAppend> validated = new ArrayList<>(inputs.size());
            long totalPayloadBytes = 0;
            for (AppendInput input : inputs) {
                Objects.requireNonNull(input, "input");
                if (input.laneId() != laneId || input.packingPolicyVersion() != packingPolicyVersion) {
                    throw rejected(
                            BridgeRejectionCode.PACKING_POLICY_MISMATCH,
                            "one shared extent may contain only one lane and resolved packing policy");
                }
                if (!uniqueBindings.add(input.binding())) {
                    throw rejected(
                            BridgeRejectionCode.DUPLICATE_BINDING_IN_SHARED_PLAN,
                            "first implementation admits at most one entry per binding in a shared extent");
                }
                if (input.payload().length == 0 || input.payload().length > configuration.maxEntryPayloadBytes()) {
                    throw rejected(BridgeRejectionCode.PAYLOAD_SIZE_INVALID, "entry payload is empty or too large");
                }
                totalPayloadBytes = Math.addExact(totalPayloadBytes, input.payload().length);
                if (totalPayloadBytes > configuration.maximumCanonicalExtentBodyBytes()) {
                    throw rejected(
                            BridgeRejectionCode.PAYLOAD_SIZE_INVALID,
                            "shared entry payloads exceed the generic extent body reservation");
                }
                BindingState state = requireBinding(input.binding());
                state.requireOwner(input.ownerEpoch());
                state.requireQuiescent();
                LedgerState currentLedger = state.currentLedger();
                long entryId = Math.addExact(currentLedger.durableThrough, 1);
                if (entryId >= configuration.maxEntriesPerLedger()) {
                    throw rejected(
                            BridgeRejectionCode.LEDGER_ROLLOVER_REQUIRED,
                            "entry-count bound requires seal/rollover before position allocation");
                }
                validated.add(new ValidatedAppend(input, state, entryId));
            }

            List<ReservedAppend> reserved = new ArrayList<>(validated.size());
            try {
                for (ValidatedAppend append : validated) {
                    reserved.add(new ReservedAppend(append, append.state().reserveBeforePosition(configuration)));
                }
            } catch (Throwable error) {
                reserved.forEach(append -> append.state().cancelBeforePosition(append.reservation()));
                throw error;
            }

            List<AllocatedAppend> allocated = new ArrayList<>(reserved.size());
            try {
                for (ReservedAppend append : reserved) {
                    PulsarPosition position = new PulsarPosition(
                            append.state().current.node().virtualLedgerId(),
                            append.validated().entryId());
                    CompletionTicket ticket =
                            append.state().allocateTicketAfterPosition(append.reservation(), position);
                    AppendInput input = append.validated().input();
                    allocated.add(new AllocatedAppend(
                            append.state(),
                            append.reservation(),
                            ticket,
                            new PlannedEntry(
                                    input.binding(),
                                    input.ownerEpoch(),
                                    position,
                                    input.appendCommitSetId(),
                                    input.payload())));
                }
                List<PlannedEntry> members =
                        allocated.stream().map(AllocatedAppend::member).toList();
                SealedExtentPlan plan = extentStore.sealPlan(
                        laneId, packingPolicyVersion, members, configuration.maximumCanonicalExtentBodyBytes());
                plan.requireExactRequest(
                        laneId, packingPolicyVersion, members, configuration.maximumCanonicalExtentBodyBytes());
                for (AllocatedAppend append : allocated) {
                    append.state()
                            .installInFlight(plan.planId(), append.member().position(), append.ticket());
                }
                return plan;
            } catch (Throwable error) {
                Set<PrePositionReservation> allocatedReservations = new HashSet<>();
                for (AllocatedAppend append : allocated) {
                    append.state()
                            .cancelAfterPositionBeforeProvider(
                                    append.ticket(), append.member().position());
                    allocatedReservations.add(append.reservation());
                }
                for (ReservedAppend append : reserved) {
                    if (!allocatedReservations.contains(append.reservation())) {
                        append.state().cancelBeforePosition(append.reservation());
                    }
                }
                throw error;
            }
        }
    }

    private CompletionStage<PublishResult> reconcileOrRetainExceptional(
            SealedExtentPlan plan, CompletionStage<PublishResult> publish) {
        if (publish == null) {
            return retainExceptionalPublish(plan, new NullPointerException("publish stage is null"));
        }
        CompletableFuture<PublishResult> resolved = new CompletableFuture<>();
        publish.whenComplete((result, publishError) -> {
            if (publishError != null) {
                retainExceptionalPublish(plan, publishError).whenComplete(copyTo(resolved));
                return;
            }
            Optional<ExtentIdentity> candidateIdentity = result == null ? Optional.empty() : result.identity();
            try {
                Objects.requireNonNull(result, "publish result");
                if (result.outcome() != ProviderObjectOutcome.OUTCOME_UNKNOWN) {
                    resolved.complete(result);
                    return;
                }
                CompletionStage<PublishResult> reconcile = extentStore.reconcile(plan, result.identity());
                Objects.requireNonNull(reconcile, "reconcile").whenComplete((reconciled, reconcileError) -> {
                    if (reconcileError != null) {
                        retainExceptionalPublish(plan, reconcileError, candidateIdentity)
                                .whenComplete(copyTo(resolved));
                    } else {
                        resolved.complete(Objects.requireNonNull(reconciled, "reconciled publish result"));
                    }
                });
            } catch (Throwable error) {
                retainExceptionalPublish(plan, error, candidateIdentity).whenComplete(copyTo(resolved));
            }
        });
        return resolved;
    }

    private CompletionStage<PublishResult> retainExceptionalPublish(SealedExtentPlan plan, Throwable error) {
        return retainExceptionalPublish(plan, error, Optional.empty());
    }

    private CompletionStage<List<MemberAppendResult>> retainExceptionalSuccessorPublish(
            SealedExtentPlan plan, Throwable error) {
        return retainExceptionalPublish(plan, error).thenApply(ignored -> {
            throw new IllegalStateException("exceptional successor publication retention unexpectedly completed");
        });
    }

    private CompletionStage<PublishResult> retainExceptionalPublish(
            SealedExtentPlan plan, Throwable error, Optional<ExtentIdentity> candidateIdentity) {
        Objects.requireNonNull(candidateIdentity, "candidateIdentity");
        Throwable retentionError = null;
        Optional<ExtentIdentity> exactCandidate = candidateIdentity;
        synchronized (monitor) {
            try {
                Optional<ExtentIdentity> carriedCandidate = retainedCandidateIdentity(error);
                if (exactCandidate.isPresent()
                        && carriedCandidate.isPresent()
                        && !exactCandidate.orElseThrow().equals(carriedCandidate.orElseThrow())) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "exceptional completion carried another Provider candidate identity");
                }
                if (exactCandidate.isEmpty()) {
                    exactCandidate = carriedCandidate;
                }
                if (hasReservedMember(plan)) {
                    retainGap(plan, ProviderObjectOutcome.OUTCOME_UNKNOWN, exactCandidate);
                }
            } catch (Throwable failedRetention) {
                walRunAdmissionStopped = true;
                retentionError = failedRetention;
            }
        }
        BridgeException retained = rejected(
                BridgeRejectionCode.PROVIDER_OUTCOME_UNKNOWN,
                "Object-WAL publish/reconcile completed exceptionally; exact position and ticket are retained");
        retained.addSuppressed(error);
        if (retentionError != null) {
            retained.addSuppressed(retentionError);
        }
        return failed(retained);
    }

    private static Optional<ExtentIdentity> retainedCandidateIdentity(Throwable error) {
        Throwable current = error;
        Set<Throwable> observed = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        while (current != null && observed.add(current)) {
            if (current instanceof RetainedCandidateFailure retained) {
                return Optional.of(retained.identity());
            }
            current = current.getCause();
        }
        return Optional.empty();
    }

    private static <T> java.util.function.BiConsumer<T, Throwable> copyTo(CompletableFuture<T> target) {
        return (value, error) -> {
            if (error != null) {
                target.completeExceptionally(error);
            } else {
                target.complete(value);
            }
        };
    }

    private CompletionStage<List<MemberAppendResult>> applyPublishResult(
            SealedExtentPlan plan, PublishResult result, boolean recoveryAttempt) {
        try {
            synchronized (monitor) {
                return switch (result.outcome()) {
                    case APPLIED_EXACT, EXISTING_EXACT ->
                        completed(installResolvedExtent(plan, result, recoveryAttempt));
                    case DEFINITIVELY_NOT_APPLIED, OUTCOME_UNKNOWN, DEFINITIVE_CONFLICT -> {
                        retainGap(plan, result);
                        yield failed(rejected(
                                rejectionFor(result.outcome()), "Object-WAL publish did not resolve to exact success"));
                    }
                };
            }
        } catch (Throwable error) {
            synchronized (monitor) {
                if (hasReservedMember(plan)) {
                    ProviderObjectOutcome retainedOutcome = result == null
                            ? ProviderObjectOutcome.OUTCOME_UNKNOWN
                            : ProviderObjectOutcome.DEFINITIVE_CONFLICT;
                    Optional<ExtentIdentity> retainedIdentity = result == null ? Optional.empty() : result.identity();
                    retainGap(plan, retainedOutcome, retainedIdentity);
                }
            }
            return failed(error);
        }
    }

    private List<MemberAppendResult> installResolvedExtent(
            SealedExtentPlan plan, PublishResult result, boolean recoveryAttempt) {
        ExtentIdentity identity = result.identity()
                .orElseThrow(() -> rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID, "successful result has no extent identity"));
        if (identity.laneId() != plan.laneId()) {
            throw rejected(BridgeRejectionCode.RESOLVED_EXTENT_INVALID, "resolved extent lane differs from its plan");
        }
        ProviderResolvedExtentDescriptor descriptor = result.resolvedDescriptor()
                .orElseThrow(() -> rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "successful result has no generic provider-resolved descriptor"));
        if (!descriptor.rootSha256().equals(identity.walRunRootSha256())
                || descriptor.row().laneId() != identity.laneId()
                || descriptor.row().laneSequence() != identity.laneSequence()
                || descriptor.row().bodyLength() != identity.objectIdentity().bodyLength()
                || !descriptor
                        .row()
                        .objectSha256()
                        .equals(identity.objectIdentity().bodySha256())) {
            throw rejected(
                    BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                    "generic provider-resolved descriptor differs from the live Pulsar extent identity");
        }
        List<PlannedEntry> unresolvedMembers = unresolvedMembers(plan, recoveryAttempt);
        if (result.memberResults().size() != unresolvedMembers.size()) {
            throw rejected(
                    BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                    "successful result lacks one exact typed result per unresolved member");
        }

        Set<MemberKey> expectedMembers = unresolvedMembers.stream()
                .map(member -> new MemberKey(member.binding(), member.position()))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        Map<MemberKey, MemberPublishResult> memberResults = new HashMap<>();
        for (MemberPublishResult memberResult : result.memberResults()) {
            MemberKey key = new MemberKey(memberResult.binding(), memberResult.position());
            if (!expectedMembers.contains(key)) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "typed member result does not map an unresolved binding and Pulsar position");
            }
            if (memberResult instanceof VerifiedMember verified) {
                verified.locator().requireIdentity(identity);
            }
            MemberPublishResult previous = memberResults.put(key, memberResult);
            if (previous != null) {
                throw rejected(BridgeRejectionCode.RESOLVED_EXTENT_INVALID, "duplicate typed member result");
            }
        }

        FailedPlan recoveryPlan = recoveryAttempt ? requireFailedPlan(plan.planId()) : null;
        boolean memberOnlyRetry = recoveryPlan != null && !recoveryPlan.stopsWalRun();
        final PreparedPhysicalResolution preparedPhysical;
        if (memberOnlyRetry) {
            ExtentIdentity exactCandidate = recoveryPlan
                    .candidateIdentity()
                    .orElseThrow(() -> rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "binding-local retry lacks its already resolved extent identity"));
            if (!exactCandidate.equals(identity)) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "binding-local retry must revalidate the same provider-resolved extent");
            }
            preparedPhysical = null;
        } else {
            preparedPhysical = preparePhysicalResolution(identity);
        }
        List<MemberAppendResult> appendResults = new ArrayList<>(unresolvedMembers.size());
        for (PlannedEntry member : unresolvedMembers) {
            BindingState state = requireBinding(member.binding());
            state.requirePlanned(plan.planId(), member.position());
            MemberPublishResult memberResult = Optional.ofNullable(
                            memberResults.get(new MemberKey(member.binding(), member.position())))
                    .orElseThrow(() -> rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "typed result does not map the exact binding and Pulsar position"));
            if (memberResult instanceof VerifiedMember) {
                state.requireCanInstallLocator(member.position());
            }
        }

        for (PlannedEntry member : unresolvedMembers) {
            BindingState state = requireBinding(member.binding());
            MemberPublishResult memberResult = memberResults.get(new MemberKey(member.binding(), member.position()));
            if (memberResult instanceof VerifiedMember verified) {
                state.installHiddenLocator(member.position(), verified.locator());
            }
        }
        LaneExtentResolvedThrough physical = memberOnlyRetry
                ? requireAlreadyResolvedPhysical(identity)
                : resolvePhysical(identity, preparedPhysical);
        List<PlannedEntry> stillFailed = new ArrayList<>();
        for (PlannedEntry member : unresolvedMembers) {
            BindingState state = requireBinding(member.binding());
            MemberPublishResult memberResult = memberResults.get(new MemberKey(member.binding(), member.position()));
            if (memberResult instanceof VerifiedMember verified) {
                state.publishFrontiers(plan.planId(), member.position());
                appendResults.add(new VerifiedAppend(new AppendAck(
                        member.binding(), member.position(), verified.locator(), state.currentFrontiers(), physical)));
            } else if (memberResult instanceof FailedMember failed) {
                state.installGap(plan.planId(), member.position());
                stillFailed.add(member);
                appendResults.add(new FailedAppend(member.binding(), member.position(), failed.failure(), physical));
            } else {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID, "unknown typed member result implementation");
            }
        }
        if (stillFailed.isEmpty()) {
            removeFailedPlanAndSuccessorRetry(plan.planId());
        } else {
            failedPlans.put(
                    plan.planId(), new FailedPlan(plan, result.outcome(), result.identity(), stillFailed, false));
        }
        if (recoveryAttempt) {
            walRunAdmissionStopped = failedPlans.values().stream().anyMatch(FailedPlan::stopsWalRun);
        }
        return List.copyOf(appendResults);
    }

    private void removeFailedPlanAndSuccessorRetry(String planId) {
        failedPlans.remove(planId);
        sealedSuccessorRollovers
                .entrySet()
                .removeIf(entry -> entry.getValue().successorPlan().planId().equals(planId));
    }

    private List<PlannedEntry> unresolvedMembers(SealedExtentPlan plan, boolean recoveryAttempt) {
        if (!recoveryAttempt) {
            return plan.members();
        }
        FailedPlan failedPlan = requireFailedPlan(plan.planId());
        return failedPlan.unresolvedMembers();
    }

    private PreparedPhysicalResolution preparePhysicalResolution(ExtentIdentity identity) {
        RunLane key = new RunLane(identity.walRunRootSha256(), identity.laneId());
        PhysicalLaneState state = physicalLanes.get(key);
        if (state == null) {
            if (identity.laneSequence() != 0) {
                throw rejected(
                        BridgeRejectionCode.PHYSICAL_FRONTIER_MISMATCH,
                        "recovered non-zero lane sequence requires explicit frontier initialization");
            }
            state = new PhysicalLaneState(-1);
        }
        if (identity.laneSequence() != state.resolvedThrough + 1) {
            throw rejected(
                    BridgeRejectionCode.PHYSICAL_FRONTIER_MISMATCH,
                    "extent store exposed a lane result before its exact predecessor resolved");
        }
        return new PreparedPhysicalResolution(key, state);
    }

    private LaneExtentResolvedThrough resolvePhysical(
            ExtentIdentity identity, PreparedPhysicalResolution preparedResolution) {
        PhysicalLaneState state =
                physicalLanes.computeIfAbsent(preparedResolution.key(), ignored -> preparedResolution.state());
        state.resolvedThrough = identity.laneSequence();
        return new LaneExtentResolvedThrough(identity.walRunRootSha256(), identity.laneId(), state.resolvedThrough);
    }

    private LaneExtentResolvedThrough requireAlreadyResolvedPhysical(ExtentIdentity identity) {
        PhysicalLaneState state = physicalLanes.get(new RunLane(identity.walRunRootSha256(), identity.laneId()));
        if (state == null || state.resolvedThrough < identity.laneSequence()) {
            throw rejected(
                    BridgeRejectionCode.PHYSICAL_FRONTIER_MISMATCH,
                    "binding-local retry refers to an extent that is not already physically resolved");
        }
        return new LaneExtentResolvedThrough(identity.walRunRootSha256(), identity.laneId(), state.resolvedThrough);
    }

    private void retainGap(SealedExtentPlan plan, PublishResult result) {
        retainGap(plan, result.outcome(), result.identity());
    }

    private void retainGap(
            SealedExtentPlan plan, ProviderObjectOutcome outcome, Optional<ExtentIdentity> candidateIdentity) {
        walRunAdmissionStopped = true;
        for (PlannedEntry member : plan.members()) {
            BindingState state = requireBinding(member.binding());
            state.requirePlanned(plan.planId(), member.position());
            state.installGap(plan.planId(), member.position());
        }
        failedPlans.put(plan.planId(), new FailedPlan(plan, outcome, candidateIdentity, plan.members(), true));
    }

    private boolean hasReservedMember(SealedExtentPlan plan) {
        for (PlannedEntry member : plan.members()) {
            BindingState state = bindings.get(member.binding());
            if (state != null && state.matchesPlanned(plan.planId(), member.position())) {
                return true;
            }
        }
        return false;
    }

    private ReadEntry validateRead(PulsarBindingKey binding, PulsarPosition position, ReadEntry result) {
        Objects.requireNonNull(result, "read result");
        if (!result.binding().equals(binding) || !result.position().equals(position)) {
            throw rejected(BridgeRejectionCode.READ_MAPPING_MISMATCH, "read result changed binding or Pulsar position");
        }
        return result;
    }

    private FailedPlan requireFailedPlan(String planId) {
        Objects.requireNonNull(planId, "planId");
        return Optional.ofNullable(failedPlans.get(planId))
                .orElseThrow(() -> rejected(BridgeRejectionCode.FAILED_PLAN_NOT_FOUND, "failed plan is not retained"));
    }

    private BindingState requireBinding(PulsarBindingKey binding) {
        Objects.requireNonNull(binding, "binding");
        return Optional.ofNullable(bindings.get(binding))
                .orElseThrow(() -> rejected(BridgeRejectionCode.BINDING_NOT_ACTIVE, "binding is not active"));
    }

    private static BridgeRejectionCode rejectionFor(ProviderObjectOutcome outcome) {
        return switch (outcome) {
            case DEFINITIVELY_NOT_APPLIED -> BridgeRejectionCode.PROVIDER_DEFINITIVELY_ABSENT;
            case OUTCOME_UNKNOWN -> BridgeRejectionCode.PROVIDER_OUTCOME_UNKNOWN;
            case DEFINITIVE_CONFLICT -> BridgeRejectionCode.FENCED;
            case APPLIED_EXACT, EXISTING_EXACT -> throw new IllegalArgumentException("success is not a rejection");
        };
    }

    private static void requireLane(WalLaneId laneId) {
        Objects.requireNonNull(laneId, "laneId");
    }

    private static void requireSha256(String value, String name) {
        Objects.requireNonNull(value, name);
        if (!value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(name + " must be one lowercase SHA-256 value");
        }
    }

    private static CanonicalBytes exactNonZeroId128(CanonicalBytes value, String name) {
        byte[] exact = Objects.requireNonNull(value, name).toByteArray();
        if (exact.length != 16) {
            throw new IllegalArgumentException(name + " must be exactly 16 bytes");
        }
        boolean nonZero = false;
        for (byte item : exact) {
            nonZero |= item != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return CanonicalBytes.copyOf(exact);
    }

    private static <T> CompletionStage<T> completed(T value) {
        return CompletableFuture.completedFuture(value);
    }

    private static <T> CompletionStage<T> failed(Throwable error) {
        return CompletableFuture.failedFuture(error);
    }

    private static BridgeException rejected(BridgeRejectionCode code, String message) {
        return new BridgeException(code, message);
    }

    public record Configuration(
            int maxEntryPayloadBytes,
            long maxEntriesPerLedger,
            int maxCompletionSlotsPerBinding,
            int maxActiveTailLocatorsPerBinding,
            int maxRecoveryLocators,
            int maxSharedExtentMembers,
            long maximumCanonicalExtentBodyBytes) {
        public Configuration {
            if (maxEntryPayloadBytes <= 0
                    || maxEntriesPerLedger <= 0
                    || maxCompletionSlotsPerBinding <= 0
                    || maxActiveTailLocatorsPerBinding <= 0
                    || maxRecoveryLocators <= 0
                    || maxSharedExtentMembers <= 0
                    || maximumCanonicalExtentBodyBytes <= 0
                    || maximumCanonicalExtentBodyBytes > ObjectWalFormatCaps.MAX_BODY_BYTES) {
                throw new IllegalArgumentException("all Pulsar Object-WAL bridge bounds must be positive");
            }
        }
    }

    /** Bounded reconstruction input: manifest coverage plus only its uncovered active-tail locators. */
    public record RecoverySeed(
            long virtualLedgerId,
            long manifestThrough,
            long durableThrough,
            long readableThrough,
            long manifestGeneration,
            Optional<ManifestHandoffRequest> manifestHandoffRequest,
            List<ExtentLocator> activeTail) {
        public RecoverySeed {
            if (virtualLedgerId <= 0
                    || manifestThrough < -1
                    || durableThrough < manifestThrough
                    || readableThrough != durableThrough
                    || manifestGeneration < 0) {
                throw new IllegalArgumentException("recovery frontier values are invalid");
            }
            manifestHandoffRequest = Objects.requireNonNull(manifestHandoffRequest, "manifestHandoffRequest");
            if ((manifestThrough >= 0) != manifestHandoffRequest.isPresent()) {
                throw new IllegalArgumentException(
                        "recovered manifest coverage requires one request for authority verification");
            }
            activeTail = List.copyOf(Objects.requireNonNull(activeTail, "activeTail"));
        }

        public RecoverySeed(
                long virtualLedgerId,
                long manifestThrough,
                long durableThrough,
                long readableThrough,
                long manifestGeneration,
                List<ExtentLocator> activeTail) {
            this(
                    virtualLedgerId,
                    manifestThrough,
                    durableThrough,
                    readableThrough,
                    manifestGeneration,
                    Optional.empty(),
                    activeTail);
        }

        public static RecoverySeed empty(long virtualLedgerId) {
            return new RecoverySeed(virtualLedgerId, -1, -1, -1, 0, List.of());
        }
    }

    /** Candidate active-tail suffix; no frontier may use it until the production extent store verifies every row. */
    public record ActiveTailRecoveryRequest(
            PulsarBindingKey binding,
            long virtualLedgerId,
            long manifestThrough,
            long durableThrough,
            List<ExtentLocator> activeTail) {
        public ActiveTailRecoveryRequest {
            Objects.requireNonNull(binding, "binding");
            activeTail = List.copyOf(Objects.requireNonNull(activeTail, "activeTail"));
            if (virtualLedgerId <= 0 || manifestThrough < -1 || durableThrough < manifestThrough) {
                throw new IllegalArgumentException("active-tail recovery request frontiers are invalid");
            }
        }

        static ActiveTailRecoveryRequest from(PulsarBindingKey binding, RecoverySeed seed) {
            return new ActiveTailRecoveryRequest(
                    binding, seed.virtualLedgerId(), seed.manifestThrough(), seed.durableThrough(), seed.activeTail());
        }
    }

    /** Typed response from the injected recovery authority; exact request equality is mandatory. */
    public record VerifiedActiveTailRecovery(ActiveTailRecoveryRequest request) {
        public VerifiedActiveTailRecovery {
            Objects.requireNonNull(request, "request");
        }

        void requireExact(ActiveTailRecoveryRequest expected) {
            if (!request.equals(expected)) {
                throw rejected(
                        BridgeRejectionCode.RECOVERY_MAPPING_INVALID,
                        "active-tail recovery authority returned another suffix");
            }
        }
    }

    /** Unforgeable result of the common Root-bound streaming physical-checkpoint verifier. */
    public static final class VerifiedPhysicalCheckpoint {
        private final Sha256Digest walRunRootSha256;
        private final LaneSequenceVector coveredThrough;
        private final long aggregateExtentCount;
        private final long aggregateCanonicalBodyBytes;

        private VerifiedPhysicalCheckpoint(
                Sha256Digest walRunRootSha256,
                LaneSequenceVector coveredThrough,
                long aggregateExtentCount,
                long aggregateCanonicalBodyBytes) {
            this.walRunRootSha256 = Objects.requireNonNull(walRunRootSha256, "walRunRootSha256");
            this.coveredThrough = Objects.requireNonNull(coveredThrough, "coveredThrough");
            this.aggregateExtentCount = aggregateExtentCount;
            this.aggregateCanonicalBodyBytes = aggregateCanonicalBodyBytes;
        }

        public Sha256Digest walRunRootSha256() {
            return walRunRootSha256;
        }

        public LaneSequenceVector coveredThrough() {
            return coveredThrough;
        }

        public long aggregateExtentCount() {
            return aggregateExtentCount;
        }

        public long aggregateCanonicalBodyBytes() {
            return aggregateCanonicalBodyBytes;
        }
    }

    /** Numeric coverage request; it remains invisible until the authority returns an exact verified source. */
    public record ManifestHandoffRequest(
            PulsarBindingKey binding, long virtualLedgerId, long throughEntryId, long manifestGeneration) {
        public ManifestHandoffRequest {
            Objects.requireNonNull(binding, "binding");
            if (virtualLedgerId <= 0 || throughEntryId < 0 || manifestGeneration <= 0) {
                throw new IllegalArgumentException("manifest handoff request values are invalid");
            }
        }
    }

    /** Authority-verified immutable manifest source; no boolean caller assertion can construct a handoff. */
    public record ManifestSource(ObjectIdentity objectIdentity, long authorityVersion, String authorityProofSha256) {
        public ManifestSource {
            Objects.requireNonNull(objectIdentity, "objectIdentity");
            requireSha256(authorityProofSha256, "authorityProofSha256");
            if (authorityVersion < 0) {
                throw new IllegalArgumentException("manifest authority version must be non-negative");
            }
        }
    }

    /** Exact response produced by the manifest authority adapter, never by the numeric caller. */
    public record VerifiedManifestHandoff(ManifestHandoffRequest request, ManifestSource source) {
        public VerifiedManifestHandoff {
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(source, "source");
        }

        void requireExact(ManifestHandoffRequest expected) {
            if (!request.equals(expected)) {
                throw rejected(
                        BridgeRejectionCode.MANIFEST_HANDOFF_NOT_VERIFIED,
                        "manifest authority response differs from the exact requested coverage");
            }
        }
    }

    public record AppendInput(
            PulsarBindingKey binding,
            long ownerEpoch,
            WalLaneId laneId,
            int packingPolicyVersion,
            CanonicalBytes appendCommitSetId,
            byte[] payload) {
        public AppendInput {
            Objects.requireNonNull(binding, "binding");
            appendCommitSetId = exactNonZeroId128(appendCommitSetId, "appendCommitSetId");
            payload = Objects.requireNonNull(payload, "payload").clone();
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof AppendInput candidate
                    && binding.equals(candidate.binding)
                    && ownerEpoch == candidate.ownerEpoch
                    && laneId == candidate.laneId
                    && packingPolicyVersion == candidate.packingPolicyVersion
                    && appendCommitSetId.equals(candidate.appendCommitSetId)
                    && Arrays.equals(payload, candidate.payload);
        }

        @Override
        public int hashCode() {
            return 31 * Objects.hash(binding, ownerEpoch, laneId, packingPolicyVersion, appendCommitSetId)
                    + Arrays.hashCode(payload);
        }
    }

    public record PulsarPosition(long virtualLedgerId, long entryId) {
        public PulsarPosition {
            if (virtualLedgerId <= 0 || entryId < 0) {
                throw new IllegalArgumentException(
                        "Pulsar position requires positive ledger ID and non-negative entry");
            }
        }
    }

    public record PlannedEntry(
            PulsarBindingKey binding,
            long ownerEpoch,
            PulsarPosition position,
            CanonicalBytes appendCommitSetId,
            byte[] payload) {
        public PlannedEntry {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(position, "position");
            appendCommitSetId = exactNonZeroId128(appendCommitSetId, "appendCommitSetId");
            payload = Objects.requireNonNull(payload, "payload").clone();
            if (ownerEpoch <= 0 || payload.length == 0) {
                throw new IllegalArgumentException("planned entry owner/payload is invalid");
            }
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof PlannedEntry candidate
                    && binding.equals(candidate.binding)
                    && ownerEpoch == candidate.ownerEpoch
                    && position.equals(candidate.position)
                    && appendCommitSetId.equals(candidate.appendCommitSetId)
                    && Arrays.equals(payload, candidate.payload);
        }

        @Override
        public int hashCode() {
            return 31 * Objects.hash(binding, ownerEpoch, position, appendCommitSetId) + Arrays.hashCode(payload);
        }

        PlannedEntry withPosition(PulsarPosition successor) {
            return new PlannedEntry(binding, ownerEpoch, successor, appendCommitSetId, payload);
        }
    }

    /** Immutable plan identity is sealed before the storage implementation allocates lane sequence and encrypts. */
    public record SealedExtentPlan(
            String planId,
            WalLaneId laneId,
            int packingPolicyVersion,
            List<PlannedEntry> members,
            long maximumCanonicalBodyBytes) {
        public SealedExtentPlan {
            requireSha256(planId, "planId");
            requireLane(laneId);
            if (packingPolicyVersion <= 0) {
                throw new IllegalArgumentException("packingPolicyVersion must be positive");
            }
            members = List.copyOf(Objects.requireNonNull(members, "members"));
            if (members.isEmpty() || maximumCanonicalBodyBytes <= 0) {
                throw new IllegalArgumentException("sealed extent plan requires members and a positive body cap");
            }
        }

        void requireExactRequest(
                WalLaneId expectedLane,
                int expectedPackingPolicyVersion,
                List<PlannedEntry> expectedMembers,
                long maximumCanonicalBodyBytes) {
            if (laneId != expectedLane
                    || packingPolicyVersion != expectedPackingPolicyVersion
                    || !members.equals(expectedMembers)
                    || this.maximumCanonicalBodyBytes != maximumCanonicalBodyBytes) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "sealed Object-WAL plan differs from the reserved Pulsar members");
            }
        }
    }

    public record ExtentIdentity(
            Sha256Digest walRunRootSha256, WalLaneId laneId, long laneSequence, ObjectIdentity objectIdentity) {
        public ExtentIdentity {
            Objects.requireNonNull(walRunRootSha256, "walRunRootSha256");
            requireLane(laneId);
            Objects.requireNonNull(objectIdentity, "objectIdentity");
            if (laneSequence < 0) {
                throw new IllegalArgumentException("laneSequence must be non-negative");
            }
        }
    }

    /** Exact live mapping; no NPD1/NPO1 offload key or sealed-ledger attempt identity is accepted here. */
    public record ExtentLocator(
            PulsarBindingKey binding,
            PulsarPosition position,
            ExtentIdentity identity,
            int frameOrdinal,
            long extentOffset,
            long extentLength,
            String frameSha256) {
        public ExtentLocator {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(position, "position");
            Objects.requireNonNull(identity, "identity");
            requireSha256(frameSha256, "frameSha256");
            if (frameOrdinal < 0 || extentOffset < 0 || extentLength <= 0) {
                throw new IllegalArgumentException("extent locator ordinal/range is invalid");
            }
            long extentEnd = Math.addExact(extentOffset, extentLength);
            if (extentEnd > identity.objectIdentity().bodyLength()) {
                throw new IllegalArgumentException("extent locator lies outside the generic Object identity");
            }
            if (identity.objectIdentity().key().contains("pulsar-offload/v1/")
                    || identity.objectIdentity().key().endsWith("/data")
                    || identity.objectIdentity().key().endsWith("/root")) {
                throw new IllegalArgumentException(
                        "sealed NPD1/NPO1 offload objects cannot be active Object-WAL locators");
            }
        }

        void requireIdentity(ExtentIdentity expected) {
            if (!identity.equals(expected)) {
                throw rejected(BridgeRejectionCode.RESOLVED_EXTENT_INVALID, "locator extent identity differs");
            }
        }
    }

    public record PublishResult(
            ProviderObjectOutcome outcome,
            Optional<ExtentIdentity> identity,
            Optional<ProviderResolvedExtentDescriptor> resolvedDescriptor,
            List<MemberPublishResult> memberResults) {
        public PublishResult {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(resolvedDescriptor, "resolvedDescriptor");
            memberResults = List.copyOf(Objects.requireNonNull(memberResults, "memberResults"));
            boolean exactSuccess =
                    outcome == ProviderObjectOutcome.APPLIED_EXACT || outcome == ProviderObjectOutcome.EXISTING_EXACT;
            if (exactSuccess != resolvedDescriptor.isPresent()) {
                throw new IllegalArgumentException(
                        "only exact Provider success may carry a provider-resolved descriptor");
            }
            if (!exactSuccess && !memberResults.isEmpty()) {
                throw new IllegalArgumentException("unresolved Provider outcome cannot publish typed member results");
            }
        }

        public static PublishResult fromProvider(
                ProviderObjectResult providerResult,
                Optional<ExtentIdentity> identity,
                Optional<ProviderResolvedExtentDescriptor> resolvedDescriptor,
                List<MemberPublishResult> memberResults) {
            Objects.requireNonNull(providerResult, "providerResult");
            return new PublishResult(providerResult.outcome(), identity, resolvedDescriptor, memberResults);
        }
    }

    /**
     * One result per unresolved shared-plan member, emitted only after Object-global verification has succeeded.
     * Shared/WalRun failures are unrepresentable here and must fail the whole {@link PublishResult} instead.
     */
    public sealed interface MemberPublishResult permits VerifiedMember, FailedMember {
        PulsarBindingKey binding();

        PulsarPosition position();
    }

    public record VerifiedMember(ExtentLocator locator) implements MemberPublishResult {
        public VerifiedMember {
            Objects.requireNonNull(locator, "locator");
        }

        @Override
        public PulsarBindingKey binding() {
            return locator.binding();
        }

        @Override
        public PulsarPosition position() {
            return locator.position();
        }
    }

    /** Stable NWG1 code/stage/scope for one binding-local or append-unit-local validation failure. */
    public record MemberFailure(Nwg1RejectionV1 rejection, Nwg1ValidationStageV1 stage, Nwg1IsolationScopeV1 scope) {
        public MemberFailure {
            Objects.requireNonNull(rejection, "rejection");
            Objects.requireNonNull(stage, "stage");
            Objects.requireNonNull(scope, "scope");
            if (scope != Nwg1IsolationScopeV1.BINDING && scope != Nwg1IsolationScopeV1.APPEND_UNIT) {
                throw new IllegalArgumentException("a per-member failure must have BINDING or APPEND_UNIT isolation");
            }
            if (stage.ordinal() < Nwg1ValidationStageV1.BINDING_SEMANTICS.ordinal()) {
                throw new IllegalArgumentException(
                        "a per-member failure may be emitted only after shared Object verification");
            }
        }
    }

    public record FailedMember(PulsarBindingKey binding, PulsarPosition position, MemberFailure failure)
            implements MemberPublishResult {
        public FailedMember {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(position, "position");
            Objects.requireNonNull(failure, "failure");
        }
    }

    public record AppendAck(
            PulsarBindingKey binding,
            PulsarPosition position,
            ExtentLocator locator,
            LedgerFrontiers bindingFrontiers,
            LaneExtentResolvedThrough physicalFrontier) {
        public AppendAck {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(position, "position");
            Objects.requireNonNull(locator, "locator");
            Objects.requireNonNull(bindingFrontiers, "bindingFrontiers");
            Objects.requireNonNull(physicalFrontier, "physicalFrontier");
        }
    }

    /** Append completion is binding-local after shared Object verification. */
    public sealed interface MemberAppendResult permits VerifiedAppend, FailedAppend {
        PulsarBindingKey binding();

        PulsarPosition position();
    }

    public record VerifiedAppend(AppendAck acknowledgement) implements MemberAppendResult {
        public VerifiedAppend {
            Objects.requireNonNull(acknowledgement, "acknowledgement");
        }

        @Override
        public PulsarBindingKey binding() {
            return acknowledgement.binding();
        }

        @Override
        public PulsarPosition position() {
            return acknowledgement.position();
        }
    }

    public record FailedAppend(
            PulsarBindingKey binding,
            PulsarPosition position,
            MemberFailure failure,
            LaneExtentResolvedThrough physicalFrontier)
            implements MemberAppendResult {
        public FailedAppend {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(position, "position");
            Objects.requireNonNull(failure, "failure");
            Objects.requireNonNull(physicalFrontier, "physicalFrontier");
        }
    }

    public record LedgerFrontiers(
            PulsarBindingKey binding,
            long virtualLedgerId,
            long manifestThrough,
            long readableThrough,
            long bindingDurableThrough,
            int activeTailLocatorCount) {
        public LedgerFrontiers {
            Objects.requireNonNull(binding, "binding");
        }
    }

    public record LaneExtentResolvedThrough(Sha256Digest walRunRootSha256, WalLaneId laneId, long laneSequence) {
        public LaneExtentResolvedThrough {
            Objects.requireNonNull(walRunRootSha256, "walRunRootSha256");
            requireLane(laneId);
            if (laneSequence < -1) {
                throw new IllegalArgumentException("laneSequence must be -1 or non-negative");
            }
        }
    }

    public record ReadEntry(
            PulsarBindingKey binding,
            PulsarPosition position,
            byte[] payload,
            ReadSource source,
            ReadFailureScope validatedFailureScope) {
        public ReadEntry {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(position, "position");
            payload = Objects.requireNonNull(payload, "payload").clone();
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(validatedFailureScope, "validatedFailureScope");
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }
    }

    public enum ReadSource {
        ACTIVE_TAIL,
        MANIFEST
    }

    public enum ReadFailureScope {
        NONE,
        BINDING_ENTRY,
        SHARED_EXTENT
    }

    /**
     * Narrow adapter seam for the parallel generic Object-WAL implementation.
     *
     * <p>Successful stages for one Root/lane must be exposed in contiguous lane-sequence order. Dispatch may be
     * concurrent, but a later success remains internal until every predecessor outcome is provider-resolved. The bridge
     * rejects an out-of-order result before locator/frontier publication.
     */
    public interface ObjectWalExtentStore {
        /** Local NWG1 plan seal; it must not allocate lane sequence or perform Provider/control-plane I/O. */
        SealedExtentPlan sealPlan(
                WalLaneId laneId, int packingPolicyVersion, List<PlannedEntry> members, long maximumCanonicalBodyBytes);

        CompletionStage<PublishResult> publish(SealedExtentPlan plan);

        CompletionStage<PublishResult> reconcile(SealedExtentPlan plan, Optional<ExtentIdentity> candidateIdentity);

        CompletionStage<PublishResult> resumeSame(
                SealedExtentPlan plan,
                Optional<ExtentIdentity> candidateIdentity,
                List<PlannedEntry> unresolvedMembers);

        CompletionStage<PublishResult> publishSuccessor(
                SealedExtentPlan definitivelyAbsentPlan, SealedExtentPlan successorPlan);

        /** Low-frequency authority/source verification; never called by the normal active-tail append path. */
        CompletionStage<VerifiedManifestHandoff> verifyManifestHandoff(ManifestHandoffRequest request);

        CompletionStage<ReadEntry> readActive(ExtentLocator locator);

        CompletionStage<ReadEntry> readManifest(
                PulsarBindingKey binding,
                PulsarPosition position,
                long manifestGeneration,
                ManifestSource verifiedSource);

        /** Authenticates every candidate active-tail locator before recovery publishes any Binding frontier. */
        CompletionStage<VerifiedActiveTailRecovery> verifyRecoveryTail(ActiveTailRecoveryRequest request);
    }

    /** Local Pulsar-to-NWG1 projection. Common storage validates and seals it; this performs no sequence or I/O. */
    public interface PulsarNwg1PlanAuthority {
        PreparedNwg1Plan seal(
                WalLaneId laneId, int packingPolicyVersion, List<PlannedEntry> members, long maximumCanonicalBodyBytes);
    }

    /**
     * Trusted Pulsar projection of the NWG1 plan. {@code bindingContexts} maps every encoded context ordinal back to
     * the exact live Pulsar binding; common storage derives plan hash/body facts itself and accepts neither from here.
     */
    public record PreparedNwg1Plan(GroupEncodingPlanV1 encodingPlan, List<PulsarBindingKey> bindingContexts) {
        public PreparedNwg1Plan {
            Objects.requireNonNull(encodingPlan, "encodingPlan");
            bindingContexts = List.copyOf(Objects.requireNonNull(bindingContexts, "bindingContexts"));
            if (bindingContexts.isEmpty()) {
                throw new IllegalArgumentException("Pulsar NWG1 projection requires binding contexts");
            }
        }
    }

    /**
     * Binding-local M2 fenced publication/admission check executed after shared NWG1 verification. Empty means exact
     * verification; a failure must be explicitly typed as BINDING/APPEND_UNIT. Unexpected exceptions fail the shared
     * operation closed and are never guessed into a local failure.
     */
    @FunctionalInterface
    public interface PulsarMemberPublicationAuthority {
        Optional<MemberFailure> verify(PlannedEntry member, ExtentLocator exactLocator);
    }

    /** Root/M2-bound reverse mapping from an authenticated NWG1 context row to the exact live Pulsar binding. */
    @FunctionalInterface
    public interface PulsarBindingContextAuthority {
        void requireExact(PulsarBindingKey binding, Nwg1DirectoryV1.BindingContext authenticatedContext);
    }

    /** Low-frequency manifest authority/reader remains distinct from active Object-WAL Objects. */
    public interface ObjectWalManifestOperations {
        CompletionStage<VerifiedManifestHandoff> verify(ManifestHandoffRequest request);

        CompletionStage<ReadEntry> read(
                PulsarBindingKey binding,
                PulsarPosition position,
                long manifestGeneration,
                ManifestSource verifiedSource);
    }

    /** A definitively absent run or explicit successor must be moved by an external Root/successor authority. */
    public interface ObjectWalSuccessorAuthority {
        CompletionStage<PublishResult> resumeInSuccessor(
                SealedExtentPlan plan, Optional<ExtentIdentity> absentCandidate, List<PlannedEntry> unresolvedMembers);

        CompletionStage<PublishResult> publishSuccessor(
                SealedExtentPlan definitivelyAbsentPlan, SealedExtentPlan successorPlan);
    }

    /** Protocol verification and clock inputs; Provider/KMS/Root/recovery authority remains in WalRunObjectSession. */
    public record ProductionObjectWalContext(Nwg1VerificationContextV1 verificationContext, LongSupplier clock) {
        public ProductionObjectWalContext {
            Objects.requireNonNull(verificationContext, "verificationContext");
            Objects.requireNonNull(clock, "clock");
        }
    }

    /** Exact ACTIVE M1/Oxia ownership observation made while the authority still holds its all-Binding fence. */
    public record ObservedPulsarOwnerFence(
            PulsarProtocolCellIdentity protocolCell,
            WalRunReference rootReference,
            long ownerEpoch,
            Sha256Digest ownerFenceCommitment) {
        public ObservedPulsarOwnerFence {
            Objects.requireNonNull(protocolCell, "protocolCell");
            Objects.requireNonNull(rootReference, "rootReference");
            Objects.requireNonNull(ownerFenceCommitment, "ownerFenceCommitment");
            if (ownerEpoch <= 0 || ownerFenceCommitment.isZero()) {
                throw new IllegalArgumentException("ACTIVE Pulsar owner fence identity is invalid");
            }
        }
    }

    @FunctionalInterface
    public interface PulsarFencedRecoveryAction {
        WalRunObjectSession recover(ObservedPulsarOwnerFence exactActiveFence) throws IOException;
    }

    /**
     * Production M1/Oxia adapter. The implementation must establish one current ACTIVE monotonic Cell ownership cut,
     * block the prior owner from dispatch, execute the callback synchronously, and release the cut in {@code finally}.
     */
    @FunctionalInterface
    public interface PulsarActiveOwnerFenceAuthority {
        WalRunObjectSession withAllBindingsActiveMonotonicFence(
                PulsarProtocolCellIdentity exactProtocolCell,
                WalRunReference exactRootReference,
                PulsarFencedRecoveryAction callback)
                throws IOException;
    }

    /** Root-exact production adapter for the sole common owner-open recovery boundary. */
    public static final class PulsarProtocolOwnerFenceExecutor
            implements OwnerOpenRecoveryCoordinator.ProtocolOwnerFenceExecutor {
        private final PulsarActiveOwnerFenceAuthority authority;
        private final Nwg1VerificationContextV1 exactVerificationContext;
        private final Object monitor = new Object();
        private final Map<PulsarProtocolCellIdentity, FenceHighWater> highWater = new HashMap<>();
        private final Set<PulsarProtocolCellIdentity> executing = new HashSet<>();
        private final ThreadLocal<ObservedPulsarOwnerFence> currentFence = new ThreadLocal<>();

        public PulsarProtocolOwnerFenceExecutor(
                PulsarActiveOwnerFenceAuthority authority, Nwg1VerificationContextV1 exactVerificationContext) {
            this.authority = Objects.requireNonNull(authority, "authority");
            this.exactVerificationContext =
                    Objects.requireNonNull(exactVerificationContext, "exactVerificationContext");
        }

        @Override
        public WalRunObjectSession withDurableOwnerFence(
                ProtocolCellIdentity exactProtocolCell,
                WalRunReference exactRootReference,
                Sha256Digest exactRootSha256,
                OwnerOpenRecoveryCoordinator.FencedRecoveryCallback callback)
                throws IOException {
            Objects.requireNonNull(exactProtocolCell, "exactProtocolCell");
            Objects.requireNonNull(exactRootReference, "exactRootReference");
            Objects.requireNonNull(exactRootSha256, "exactRootSha256");
            Objects.requireNonNull(callback, "callback");
            if (!(exactProtocolCell instanceof PulsarProtocolCellIdentity pulsarCell)
                    || !exactRootReference.rootSha256().equals(exactRootSha256)
                    || !exactVerificationContext.protocolCell().equals(exactProtocolCell)
                    || !Arrays.equals(
                            exactVerificationContext.walRunRootSha256(),
                            exactRootSha256.bytes().toByteArray())) {
                throw new IllegalArgumentException("Pulsar owner fence differs from the exact Root Protocol Cell");
            }
            synchronized (monitor) {
                if (!executing.add(pulsarCell)) {
                    throw new IllegalStateException("Pulsar owner-open recovery is already executing for this Cell");
                }
            }
            try {
                var observedOnce = new java.util.concurrent.atomic.AtomicBoolean();
                var recoveredOnce = new java.util.concurrent.atomic.AtomicReference<WalRunObjectSession>();
                WalRunObjectSession result =
                        authority.withAllBindingsActiveMonotonicFence(pulsarCell, exactRootReference, observed -> {
                            if (!observedOnce.compareAndSet(false, true)) {
                                throw new IllegalStateException("Pulsar owner fence invoked recovery more than once");
                            }
                            WalRunObjectSession recovered =
                                    executeObservedFence(pulsarCell, exactRootReference, observed, callback);
                            recoveredOnce.set(recovered);
                            return recovered;
                        });
                if (!observedOnce.get()) {
                    throw new IllegalStateException("Pulsar owner fence returned without executing recovery");
                }
                if (result != recoveredOnce.get()) {
                    throw new IllegalStateException(
                            "Pulsar owner fence substituted the exact recovery callback result");
                }
                return result;
            } finally {
                currentFence.remove();
                synchronized (monitor) {
                    executing.remove(pulsarCell);
                }
            }
        }

        private WalRunObjectSession executeObservedFence(
                PulsarProtocolCellIdentity exactCell,
                WalRunReference exactRoot,
                ObservedPulsarOwnerFence observed,
                OwnerOpenRecoveryCoordinator.FencedRecoveryCallback callback)
                throws IOException {
            Objects.requireNonNull(observed, "observed ACTIVE Pulsar owner fence");
            if (!observed.protocolCell().equals(exactCell)
                    || !observed.rootReference().equals(exactRoot)) {
                throw new IllegalStateException("ACTIVE Pulsar owner fence substituted the Root or Protocol Cell");
            }
            synchronized (monitor) {
                FenceHighWater previous = highWater.get(exactCell);
                if (previous != null
                        && (observed.ownerEpoch() < previous.ownerEpoch()
                                || (observed.ownerEpoch() == previous.ownerEpoch()
                                        && !observed.ownerFenceCommitment().equals(previous.ownerFenceCommitment())))) {
                    throw new IllegalStateException("ACTIVE Pulsar owner fence regressed or changed at the same epoch");
                }
                highWater.put(exactCell, new FenceHighWater(observed.ownerEpoch(), observed.ownerFenceCommitment()));
            }
            if (currentFence.get() != null) {
                throw new IllegalStateException("Pulsar owner fence callback is reentrant");
            }
            currentFence.set(observed);
            try {
                return callback.recover(exactVerificationContext);
            } finally {
                currentFence.remove();
            }
        }

        /** The old owner-local bridge can be discarded only synchronously inside the durable fenced callback. */
        public void discardOwnerLocalStateAfterFence(
                PulsarObjectWalBridgeV1 bridge, PulsarBindingKey binding, long fencedOwnerEpoch) {
            requireStrictlyOlderOwner(fencedOwnerEpoch);
            Objects.requireNonNull(bridge, "bridge").discardOwnerLocalStateAfterDurableFence(binding, fencedOwnerEpoch);
        }

        /** Shared plans require every fenced sibling in the same atomic owner-fence callback. */
        public void discardOwnerLocalStatesAfterFence(
                PulsarObjectWalBridgeV1 bridge, Map<PulsarBindingKey, Long> fencedOwners) {
            Objects.requireNonNull(fencedOwners, "fencedOwners");
            fencedOwners.values().forEach(this::requireStrictlyOlderOwner);
            Objects.requireNonNull(bridge, "bridge").discardOwnerLocalStatesAfterDurableFence(fencedOwners);
        }

        private void requireStrictlyOlderOwner(long fencedOwnerEpoch) {
            if (fencedOwnerEpoch <= 0
                    || fencedOwnerEpoch >= requireCurrentFence().ownerEpoch()) {
                throw new IllegalArgumentException(
                        "owner-local discard requires an exact owner epoch below the live ACTIVE fence");
            }
        }

        private ObservedPulsarOwnerFence requireCurrentFence() {
            return Optional.ofNullable(currentFence.get())
                    .orElseThrow(() -> new IllegalStateException(
                            "owner-local discard requires the live ACTIVE Pulsar owner-fence callback"));
        }

        private record FenceHighWater(long ownerEpoch, Sha256Digest ownerFenceCommitment) {}
    }

    /** Exact physical checkpoint and immutable Seal publication authority for the same WalRun Root. */
    public record ProductionWalRunClosure(
            WalCheckpointPublisher checkpointPublisher,
            WalRunLifecycleManager lifecycleManager,
            WalRunReference rootReference,
            String checkpointHeadKey,
            String sealKey) {
        public ProductionWalRunClosure {
            Objects.requireNonNull(checkpointPublisher, "checkpointPublisher");
            Objects.requireNonNull(lifecycleManager, "lifecycleManager");
            Objects.requireNonNull(rootReference, "rootReference");
            checkpointHeadKey =
                    requireText(checkpointHeadKey, "checkpointHeadKey", WalRunReference.MAX_METADATA_KEY_BYTES);
            sealKey = requireText(sealKey, "sealKey", WalRunReference.MAX_METADATA_KEY_BYTES);
            WalRunControlKeys.requireCheckpointHeadKey(
                    checkpointHeadKey, rootReference.shardId(), rootReference.shardRunEpoch());
            WalRunControlKeys.requireSealKey(sealKey, rootReference.shardId(), rootReference.shardRunEpoch());
            WalCheckpointHeadV1 head = checkpointPublisher.head();
            if (!head.rootSha256().equals(rootReference.rootSha256())
                    || head.shardRunEpoch() != rootReference.shardRunEpoch()) {
                throw new IllegalArgumentException("checkpoint publisher differs from the exact closure Root");
            }
        }
    }

    /**
     * Production ObjectWalExtentStore over the generic M3 control, NWG1, C1 Provider, and selected-frame verifier
     * components. The Provider work runs only on the injected production I/O executor. This adapter persists no
     * protocol/control wire and never treats NPD1/NPO1 sealed offload Objects as the active WAL.
     */
    public static final class ProductionObjectWalExtentStore implements ObjectWalExtentStore, AutoCloseable {
        private final WalRunObjectSession objectSession;
        private final Executor providerExecutor;
        private final PulsarNwg1PlanAuthority planAuthority;
        private final PulsarMemberPublicationAuthority memberAuthority;
        private final PulsarBindingContextAuthority bindingContextAuthority;
        private final ObjectWalManifestOperations manifestOperations;
        private final ObjectWalSuccessorAuthority successorAuthority;
        private final ProductionObjectWalContext context;
        private final ProductionWalRunClosure closure;
        private final Object monitor = new Object();
        private final Map<String, LocalPlan> locallySealed = new HashMap<>();
        private final Map<String, ProviderCandidate> candidates = new HashMap<>();
        private LocalLifecycle lifecycle = LocalLifecycle.OPEN;
        private int acceptedProviderTasks;
        private boolean checkpointPublishScheduled;
        private Throwable checkpointFailure;

        public ProductionObjectWalExtentStore(
                WalRunObjectSession objectSession,
                Executor providerExecutor,
                PulsarNwg1PlanAuthority planAuthority,
                PulsarMemberPublicationAuthority memberAuthority,
                PulsarBindingContextAuthority bindingContextAuthority,
                ObjectWalManifestOperations manifestOperations,
                ObjectWalSuccessorAuthority successorAuthority,
                ProductionObjectWalContext context,
                ProductionWalRunClosure closure) {
            this.objectSession = Objects.requireNonNull(objectSession, "objectSession");
            this.providerExecutor = Objects.requireNonNull(providerExecutor, "providerExecutor");
            this.planAuthority = Objects.requireNonNull(planAuthority, "planAuthority");
            this.memberAuthority = Objects.requireNonNull(memberAuthority, "memberAuthority");
            this.bindingContextAuthority = Objects.requireNonNull(bindingContextAuthority, "bindingContextAuthority");
            this.manifestOperations = Objects.requireNonNull(manifestOperations, "manifestOperations");
            this.successorAuthority = Objects.requireNonNull(successorAuthority, "successorAuthority");
            this.context = Objects.requireNonNull(context, "context");
            this.closure = Objects.requireNonNull(closure, "closure");
            var sessionRoot = objectSession.rootRecord();
            if (!Arrays.equals(
                            objectSession.rootSha256().bytes().toByteArray(),
                            context.verificationContext().walRunRootSha256())
                    || !objectSession
                            .rootSha256()
                            .equals(closure.rootReference().rootSha256())
                    || sessionRoot.shardId() != closure.rootReference().shardId()
                    || sessionRoot.shardRunEpoch() != closure.rootReference().shardRunEpoch()) {
                throw new IllegalArgumentException("Pulsar verification context differs from WalRunObjectSession Root");
            }
        }

        private void requireExactPulsarProjection(
                GroupEncodingPlanV1 encoding,
                List<PulsarBindingKey> bindingContexts,
                WalLaneId laneId,
                int packingPolicyVersion,
                List<PlannedEntry> members,
                long maximumCanonicalBodyBytes) {
            Objects.requireNonNull(encoding, "encoding");
            List<PulsarBindingKey> exactContexts =
                    List.copyOf(Objects.requireNonNull(bindingContexts, "bindingContexts"));
            List<PlannedEntry> exactMembers = List.copyOf(Objects.requireNonNull(members, "members"));
            if (maximumCanonicalBodyBytes <= 0
                    || encoding.protocolKind() != 2
                    || encoding.laneId() != laneId.code()
                    || encoding.packingPolicyVersion() != packingPolicyVersion
                    || !Arrays.equals(
                            encoding.rootSha256(),
                            objectSession.rootSha256().bytes().toByteArray())
                    || encoding.bindings().size() != exactContexts.size()
                    || exactContexts.size() != exactMembers.size()
                    || encoding.appendUnits().size() != exactMembers.size()
                    || encoding.frames().size() != exactMembers.size()
                    || new HashSet<>(exactContexts).size() != exactContexts.size()) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "local Pulsar NWG1 projection differs from the exact Root, lane, members, or contexts");
            }
            Set<Integer> usedContexts = new HashSet<>();
            for (int contextOrdinal = 0; contextOrdinal < exactContexts.size(); contextOrdinal++) {
                try {
                    bindingContextAuthority.requireExact(
                            exactContexts.get(contextOrdinal),
                            encoding.bindings().get(contextOrdinal));
                } catch (Throwable authorityFailure) {
                    BridgeException failure = rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "NWG1 context differs from the exact Pulsar binding authority");
                    failure.addSuppressed(authorityFailure);
                    throw failure;
                }
            }
            Map<MemberKey, PlannedEntry> membersByKey = new HashMap<>();
            for (PlannedEntry member : exactMembers) {
                membersByKey.put(new MemberKey(member.binding(), member.position()), member);
            }
            Set<MemberKey> usedMembers = new HashSet<>();
            for (int ordinal = 0; ordinal < exactMembers.size(); ordinal++) {
                Nwg1DirectoryV1.AppendUnit unit = encoding.appendUnits().get(ordinal);
                GroupEncodingPlanV1.PlannedFrame frame = encoding.frames().get(ordinal);
                if (!(unit instanceof Nwg1DirectoryV1.PulsarAppendUnit pulsar)) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "Pulsar NWG1 projection contains a non-Pulsar append unit");
                }
                int contextOrdinal = Math.toIntExact(pulsar.contextOrdinal());
                if (contextOrdinal < 0 || contextOrdinal >= exactContexts.size()) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "Pulsar NWG1 unit references an unknown binding context");
                }
                PulsarBindingKey binding = exactContexts.get(contextOrdinal);
                MemberKey key = new MemberKey(binding, new PulsarPosition(pulsar.virtualLedgerId(), pulsar.entryId()));
                PlannedEntry member = membersByKey.get(key);
                if (member == null || !usedMembers.add(key)) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "Pulsar NWG1 unit does not map one exact reserved member");
                }
                byte[] payload = member.payload();
                byte[] payloadSha = Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(payload))
                        .bytes()
                        .toByteArray();
                if (!usedContexts.add(contextOrdinal)
                        || pulsar.firstFrameOrdinal() != ordinal
                        || pulsar.frameCount() != 1
                        || pulsar.virtualLedgerId() != member.position().virtualLedgerId()
                        || pulsar.entryId() != member.position().entryId()
                        || !Arrays.equals(
                                pulsar.appendCommitSetId(),
                                member.appendCommitSetId().toByteArray())
                        || isZeroId128(pulsar.storageAttemptId())
                        || !Arrays.equals(pulsar.assignedPayloadSha256(), payloadSha)
                        || frame.appendUnitOrdinal() != ordinal
                        || frame.coverage0() != member.position().virtualLedgerId()
                        || frame.coverage1() != member.position().entryId()
                        || !Arrays.equals(frame.decodedPayload(), payload)) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "NWG1 unit/frame substituted a Pulsar binding, position, commit identity, or payload");
                }
            }
            if (usedContexts.size() != exactContexts.size() || usedMembers.size() != exactMembers.size()) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "NWG1 projection contains an unused Pulsar binding context");
            }
        }

        private static boolean isZeroId128(byte[] value) {
            if (value.length != 16) {
                return true;
            }
            for (byte item : value) {
                if (item != 0) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public SealedExtentPlan sealPlan(
                WalLaneId laneId,
                int packingPolicyVersion,
                List<PlannedEntry> members,
                long maximumCanonicalBodyBytes) {
            synchronized (monitor) {
                requireLocalOpen();
                PreparedNwg1Plan prepared = Objects.requireNonNull(
                        planAuthority.seal(laneId, packingPolicyVersion, members, maximumCanonicalBodyBytes),
                        "prepared NWG1 plan");
                GroupEncodingPlanV1 encoding = prepared.encodingPlan();
                requireExactPulsarProjection(
                        encoding,
                        prepared.bindingContexts(),
                        laneId,
                        packingPolicyVersion,
                        members,
                        maximumCanonicalBodyBytes);
                WalRunObjectSession.ValidatedNwg1Plan validated =
                        objectSession.validateNwg1Plan(encoding, context.verificationContext());
                if (validated.canonicalBodyBytes() > maximumCanonicalBodyBytes) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "Root-validated NWG1 body exceeds the exact Pulsar reservation cap");
                }
                SealedExtentPlan result = new SealedExtentPlan(
                        validated.canonicalPlanSha256().toHex(),
                        laneId,
                        packingPolicyVersion,
                        members,
                        maximumCanonicalBodyBytes);
                if (locallySealed.putIfAbsent(result.planId(), new LocalPlan(prepared, validated, result)) != null
                        || candidates.containsKey(result.planId())) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "canonical Pulsar plan identity was already sealed in this run");
                }
                return result;
            }
        }

        @Override
        public CompletionStage<PublishResult> publish(SealedExtentPlan plan) {
            try {
                ProviderCandidate candidate = admitCandidate(plan);
                return candidateInitialIo(
                        candidate,
                        () -> applyProviderResult(
                                candidate, objectSession.conditionalCreateNwg1(candidate.admitted()), plan.members()));
            } catch (Throwable error) {
                return failed(error);
            }
        }

        @Override
        public CompletionStage<PublishResult> reconcile(
                SealedExtentPlan plan, Optional<ExtentIdentity> candidateIdentity) {
            try {
                ProviderCandidate candidate = requireCandidate(plan, candidateIdentity);
                return candidateIo(
                        candidate,
                        () -> applyProviderResult(
                                candidate,
                                objectSession.reconcileUnknownExtent(
                                        candidate.identity().objectIdentity()),
                                plan.members()));
            } catch (Throwable error) {
                return failed(error);
            }
        }

        @Override
        public CompletionStage<PublishResult> resumeSame(
                SealedExtentPlan plan,
                Optional<ExtentIdentity> candidateIdentity,
                List<PlannedEntry> unresolvedMembers) {
            try {
                ProviderCandidate candidate;
                synchronized (monitor) {
                    candidate = candidates.get(plan.planId());
                }
                if (candidate == null) {
                    if (candidateIdentity.isPresent()) {
                        throw rejected(
                                BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                                "same-entry recovery supplied an identity before the exact candidate existed");
                    }
                    candidate = admitCandidate(plan);
                    ProviderCandidate exact = candidate;
                    return candidateInitialIo(
                            exact,
                            () -> applyProviderResult(
                                    exact,
                                    objectSession.conditionalCreateNwg1(exact.admitted()),
                                    requireWholeUnpublishedPlan(plan, unresolvedMembers)));
                }
                candidate.requireIdentity(candidateIdentity);
                if (candidate.disposition() == CandidateDisposition.RESOLVED) {
                    ProviderCandidate exact = candidate;
                    return candidateIo(exact, () -> reverifyResolvedMembers(exact, unresolvedMembers));
                }
                if (candidate.disposition() == CandidateDisposition.ABSENT) {
                    return cleanupAfterSuccessor(
                            candidate,
                            Optional.empty(),
                            unresolvedMembers,
                            successorAuthority.resumeInSuccessor(plan, candidateIdentity, unresolvedMembers));
                }
                Optional<ProviderObjectResult> knownExact = candidate.exactProviderResult();
                if (knownExact.isPresent()) {
                    ProviderCandidate exact = candidate;
                    return candidateIo(
                            exact,
                            () -> applyProviderResult(
                                    exact,
                                    knownExact.orElseThrow(),
                                    requireWholeUnpublishedPlan(plan, unresolvedMembers)));
                }
                if (candidate.needsInitialProviderAttempt()) {
                    ProviderCandidate exact = candidate;
                    return candidateInitialIo(
                            exact,
                            () -> applyProviderResult(
                                    exact,
                                    objectSession.conditionalCreateNwg1(exact.admitted()),
                                    requireWholeUnpublishedPlan(plan, unresolvedMembers)));
                }
                return reconcile(plan, candidateIdentity);
            } catch (Throwable error) {
                return failed(error);
            }
        }

        @Override
        public CompletionStage<PublishResult> publishSuccessor(
                SealedExtentPlan definitivelyAbsentPlan, SealedExtentPlan successorPlan) {
            try {
                ExtentIdentity absentIdentity;
                synchronized (monitor) {
                    absentIdentity = Optional.ofNullable(candidates.get(definitivelyAbsentPlan.planId()))
                            .orElseThrow(() -> rejected(
                                    BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                                    "successor publication lacks the exact absent candidate"))
                            .identity();
                }
                ProviderCandidate absent = requireCandidate(definitivelyAbsentPlan, Optional.of(absentIdentity));
                if (absent.disposition() != CandidateDisposition.ABSENT) {
                    throw rejected(
                            BridgeRejectionCode.ABSENCE_NOT_PROVEN,
                            "successor publication requires the exact definitively absent candidate");
                }
                synchronized (monitor) {
                    if (!locallySealed.containsKey(successorPlan.planId())) {
                        throw rejected(
                                BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                                "successor publication requires the exact locally sealed successor plan");
                    }
                }
                return cleanupAfterSuccessor(
                        absent,
                        Optional.of(successorPlan.planId()),
                        successorPlan.members(),
                        successorAuthority.publishSuccessor(definitivelyAbsentPlan, successorPlan));
            } catch (Throwable error) {
                return failed(error);
            }
        }

        private CompletionStage<PublishResult> cleanupAfterSuccessor(
                ProviderCandidate absent,
                Optional<String> localSuccessorPlanId,
                List<PlannedEntry> expectedMembers,
                CompletionStage<PublishResult> successorStage) {
            Objects.requireNonNull(absent, "absent");
            Objects.requireNonNull(localSuccessorPlanId, "localSuccessorPlanId");
            List<PlannedEntry> exactExpected =
                    List.copyOf(Objects.requireNonNull(expectedMembers, "expected successor members"));
            CompletionStage<PublishResult> exact =
                    Objects.requireNonNull(successorStage, "successor publication stage");
            return exact.thenApply(result -> {
                PublishResult published = Objects.requireNonNull(result, "successor publication result");
                if (published.outcome() != ProviderObjectOutcome.APPLIED_EXACT
                        && published.outcome() != ProviderObjectOutcome.EXISTING_EXACT) {
                    return published;
                }
                if (published.memberResults().stream().anyMatch(FailedMember.class::isInstance)) {
                    return published;
                }
                requireExactSuccessfulSuccessorResult(published, exactExpected);
                synchronized (monitor) {
                    candidates.remove(absent.planId(), absent);
                    localSuccessorPlanId.ifPresent(locallySealed::remove);
                }
                return published;
            });
        }

        private static void requireExactSuccessfulSuccessorResult(
                PublishResult published, List<PlannedEntry> expectedMembers) {
            ExtentIdentity identity = published
                    .identity()
                    .orElseThrow(() -> rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "successful successor result lacks its exact extent identity"));
            ProviderResolvedExtentDescriptor descriptor = published
                    .resolvedDescriptor()
                    .orElseThrow(() -> rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "successful successor result lacks its physical descriptor"));
            if (!descriptor.rootSha256().equals(identity.walRunRootSha256())
                    || descriptor.row().laneId() != identity.laneId()
                    || descriptor.row().laneSequence() != identity.laneSequence()
                    || descriptor.row().bodyLength()
                            != identity.objectIdentity().bodyLength()
                    || !descriptor
                            .row()
                            .objectSha256()
                            .equals(identity.objectIdentity().bodySha256())
                    || published.memberResults().size() != expectedMembers.size()) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "successful successor result changed its exact physical extent or member count");
            }
            Set<MemberKey> expected = expectedMembers.stream()
                    .map(member -> new MemberKey(member.binding(), member.position()))
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            Set<MemberKey> observed = new HashSet<>();
            for (MemberPublishResult result : published.memberResults()) {
                if (!(result instanceof VerifiedMember verified)) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "successful successor cleanup requires exact verified members");
                }
                MemberKey member = new MemberKey(verified.binding(), verified.position());
                verified.locator().requireIdentity(identity);
                if (!expected.contains(member) || !observed.add(member)) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "successful successor result changed or duplicated a member");
                }
            }
        }

        @Override
        public CompletionStage<VerifiedManifestHandoff> verifyManifestHandoff(ManifestHandoffRequest request) {
            return manifestOperations.verify(request);
        }

        @Override
        public CompletionStage<ReadEntry> readActive(ExtentLocator locator) {
            try {
                return providerIo(() -> {
                    RoutineVerification verified = verifyRoutine(locator, true);
                    return new ReadEntry(
                            locator.binding(),
                            locator.position(),
                            verified.requirePayload(),
                            ReadSource.ACTIVE_TAIL,
                            ReadFailureScope.NONE);
                });
            } catch (Throwable error) {
                return failed(error);
            }
        }

        @Override
        public CompletionStage<ReadEntry> readManifest(
                PulsarBindingKey binding,
                PulsarPosition position,
                long manifestGeneration,
                ManifestSource verifiedSource) {
            return manifestOperations.read(binding, position, manifestGeneration, verifiedSource);
        }

        @Override
        public CompletionStage<VerifiedActiveTailRecovery> verifyRecoveryTail(ActiveTailRecoveryRequest request) {
            Objects.requireNonNull(request, "request");
            Map<ExtentIdentity, List<ExtentLocator>> byExtent = new LinkedHashMap<>();
            for (ExtentLocator locator : request.activeTail()) {
                if (!locator.binding().equals(request.binding())
                        || !locator.identity().walRunRootSha256().equals(objectSession.rootSha256())) {
                    return failed(rejected(
                            BridgeRejectionCode.RECOVERY_MAPPING_INVALID,
                            "active-tail recovery locator substituted the binding or WalRun Root"));
                }
                byExtent.computeIfAbsent(locator.identity(), ignored -> new ArrayList<>())
                        .add(locator);
            }
            CompletionStage<Void> verified = completed(null);
            for (Map.Entry<ExtentIdentity, List<ExtentLocator>> extent : byExtent.entrySet()) {
                verified = verified.thenCompose(ignored -> providerIo(() -> {
                    Nwg1ObjectReaderV1.AuthenticatedPrefix prefix = objectSession.recoverAndVerifyNwg1Directory(
                            extent.getKey().objectIdentity(), context.verificationContext());
                    for (ExtentLocator locator : extent.getValue()) {
                        int[] frameCount = {0};
                        Nwg1ObjectReaderV1.VerifiedAppendUnit appendUnit = objectSession.recoverAndVerifyNwg1AppendUnit(
                                extent.getKey().objectIdentity(),
                                prefix,
                                context.verificationContext(),
                                locator.frameOrdinal(),
                                (frame, payload) -> requireVerifiedFrameMapping(
                                        locator, frame, frameCount, BridgeRejectionCode.RECOVERY_MAPPING_INVALID));
                        requireDecodedMapping(
                                locator, prefix.directory(), BridgeRejectionCode.RECOVERY_MAPPING_INVALID);
                        requireVerifiedAppendUnitMapping(
                                locator,
                                prefix.directory(),
                                appendUnit,
                                frameCount,
                                BridgeRejectionCode.RECOVERY_MAPPING_INVALID);
                        if (!locator.identity().equals(extent.getKey())) {
                            throw rejected(
                                    BridgeRejectionCode.RECOVERY_MAPPING_INVALID,
                                    "active-tail recovery changed the exact extent");
                        }
                    }
                    return null;
                }));
            }
            return verified.thenApply(ignored -> {
                for (ExtentLocator locator : request.activeTail()) {
                    if (!locator.binding().equals(request.binding())) {
                        throw rejected(
                                BridgeRejectionCode.RECOVERY_MAPPING_INVALID,
                                "active-tail recovery verification changed the exact Pulsar binding");
                    }
                }
                return new VerifiedActiveTailRecovery(request);
            });
        }

        private ProviderCandidate admitCandidate(SealedExtentPlan plan) {
            Objects.requireNonNull(plan, "plan");
            synchronized (monitor) {
                requireNotClosed();
                ProviderCandidate existing = candidates.get(plan.planId());
                if (existing != null) {
                    if (!existing.matchesPlan(plan)) {
                        throw rejected(
                                BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                                "plan identity aliases another exact Pulsar projection");
                    }
                    return existing;
                }
                LocalPlan local = Optional.ofNullable(locallySealed.get(plan.planId()))
                        .orElseThrow(() -> rejected(
                                BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                                "publish requires the exact locally sealed NWG1 plan"));
                plan.requireExactRequest(
                        plan.laneId(),
                        plan.packingPolicyVersion(),
                        local.plan().members(),
                        local.plan().maximumCanonicalBodyBytes());
                WalRunObjectSession.AdmittedNwg1Candidate admitted;
                try {
                    admitted = objectSession.admitAndSealNwg1(
                            local.validated(), context.clock().getAsLong());
                } catch (WalRunObjectSession.Nwg1AdmissionFailure failure) {
                    if (!failure.canonicalPlanSha256().toHex().equals(plan.planId())
                            || failure.sequenceEffect().stream()
                                    .anyMatch(effect -> effect.laneId() != plan.laneId()
                                            || !effect.canonicalPlanSha256()
                                                    .toHex()
                                                    .equals(plan.planId()))) {
                        throw rejected(
                                BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                                "NWG1 admission failure carried another exact plan or lane");
                    }
                    throw failure;
                }
                ProviderCandidate candidate = createCandidate(plan, local.projection(), admitted);
                candidates.put(plan.planId(), candidate);
                locallySealed.remove(plan.planId(), local);
                return candidate;
            }
        }

        /** Streams and stages the complete physical checkpoint; no frontier is exposed on partial verification. */
        public VerifiedPhysicalCheckpoint verifyPhysicalCheckpoint(
                CanonicalControlMetadataStore metadata, WalCheckpointHeadV1 head) {
            Objects.requireNonNull(metadata, "metadata");
            Objects.requireNonNull(head, "head");
            List<ProviderResolvedExtentRowV1> stagedRows = new ArrayList<>();
            var verified = objectSession.verifyCheckpointChainStreaming(metadata, head, stagedRows::add);
            if (stagedRows.size() != verified.aggregateExtentCount()) {
                throw rejected(
                        BridgeRejectionCode.RECOVERY_MAPPING_INVALID,
                        "streamed physical checkpoint row count differs from its verified aggregate");
            }
            return new VerifiedPhysicalCheckpoint(
                    objectSession.rootSha256(),
                    verified.coveredThrough(),
                    verified.aggregateExtentCount(),
                    verified.aggregateCanonicalBodyBytes());
        }

        private ProviderCandidate createCandidate(
                SealedExtentPlan plan,
                PreparedNwg1Plan projection,
                WalRunObjectSession.AdmittedNwg1Candidate admitted) {
            LaneSequenceReservation reservation = admitted.reservation();
            Nwg1SealedObjectV1 sealed = admitted.sealed();
            ObjectIdentity objectIdentity = admitted.identity();
            ExtentIdentity identity = new ExtentIdentity(
                    objectSession.rootSha256(), reservation.laneId(), reservation.laneSequence(), objectIdentity);
            return new ProviderCandidate(
                    plan,
                    requireMemberOrdinals(plan, projection.bindingContexts(), sealed.directory()),
                    admitted,
                    reservation,
                    identity,
                    sealed.header().directoryPrefixEnd(),
                    sealed.directory(),
                    new ExactRepeatableBody(objectIdentity, sealed.body()));
        }

        private static Map<MemberKey, Integer> requireMemberOrdinals(
                SealedExtentPlan plan, List<PulsarBindingKey> bindingContexts, Nwg1DirectoryV1 directory) {
            Map<MemberKey, Integer> expected = new HashMap<>();
            plan.members().forEach(member -> expected.put(new MemberKey(member.binding(), member.position()), -1));
            for (int ordinal = 0; ordinal < directory.appendUnits().size(); ordinal++) {
                if (!(directory.appendUnits().get(ordinal) instanceof Nwg1DirectoryV1.PulsarAppendUnit pulsar)) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "sealed Pulsar Object contains a non-Pulsar append unit");
                }
                int contextOrdinal = Math.toIntExact(pulsar.contextOrdinal());
                if (contextOrdinal < 0 || contextOrdinal >= bindingContexts.size()) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "sealed Pulsar append unit references another binding context");
                }
                MemberKey key = new MemberKey(
                        bindingContexts.get(contextOrdinal),
                        new PulsarPosition(pulsar.virtualLedgerId(), pulsar.entryId()));
                Integer prior = expected.replace(key, ordinal);
                if (prior == null || prior >= 0) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "sealed Pulsar append unit changed or duplicated one reserved member");
                }
            }
            if (expected.values().stream().anyMatch(value -> value < 0)) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "sealed Pulsar Object omitted one reserved member");
            }
            return Map.copyOf(expected);
        }

        private PublishResult applyProviderResult(
                ProviderCandidate candidate, ProviderObjectResult providerResult, List<PlannedEntry> requestedMembers)
                throws IOException {
            ProviderObjectOutcome outcome = providerResult.outcome();
            if (outcome == ProviderObjectOutcome.APPLIED_EXACT || outcome == ProviderObjectOutcome.EXISTING_EXACT) {
                if (candidate.disposition() == CandidateDisposition.PENDING) {
                    candidate.rememberExactProviderResult(providerResult);
                    WalRunObjectSession.AuthenticatedNwg1PublicationExtent publication =
                            objectSession.readAndAuthenticateNwg1ForPublication(
                                    candidate.admitted(), context.verificationContext());
                    List<MemberPublishResult> memberResults =
                            validatePublicationMembers(candidate, requestedMembers, publication);
                    candidate.prepareCheckpointDescriptor(
                            descriptor(candidate, publication), closure.checkpointPublisher());
                    objectSession.providerResolved(candidate.admitted());
                    candidate.resolveAndReleaseBody();
                    scheduleCheckpointPublication();
                    synchronized (monitor) {
                        locallySealed.remove(candidate.planId());
                    }
                    if (memberResults.stream().noneMatch(FailedMember.class::isInstance)) {
                        synchronized (monitor) {
                            candidates.remove(candidate.planId(), candidate);
                        }
                    }
                    return new PublishResult(
                            outcome,
                            Optional.of(candidate.identity()),
                            Optional.of(candidate.resolvedDescriptor()),
                            memberResults);
                }
                ProviderResolvedExtentDescriptor descriptor = candidate.resolvedDescriptor();
                List<MemberPublishResult> memberResults = reverifyResolvedMemberResults(candidate, requestedMembers);
                if (memberResults.stream().noneMatch(FailedMember.class::isInstance)) {
                    synchronized (monitor) {
                        candidates.remove(candidate.planId(), candidate);
                    }
                }
                return new PublishResult(
                        outcome, Optional.of(candidate.identity()), Optional.of(descriptor), memberResults);
            }
            if (outcome == ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED
                    && candidate.disposition() == CandidateDisposition.PENDING) {
                objectSession.providerAbsent(candidate.admitted());
                candidate.markAbsent();
                synchronized (monitor) {
                    locallySealed.remove(candidate.planId());
                }
            } else if (outcome == ProviderObjectOutcome.DEFINITIVE_CONFLICT
                    && candidate.disposition() == CandidateDisposition.PENDING) {
                objectSession.providerConflict(candidate.admitted());
                candidate.markConflict();
                synchronized (monitor) {
                    locallySealed.remove(candidate.planId());
                    candidates.remove(candidate.planId(), candidate);
                }
            }
            return new PublishResult(outcome, Optional.of(candidate.identity()), Optional.empty(), List.of());
        }

        private static List<PlannedEntry> requireWholeUnpublishedPlan(
                SealedExtentPlan plan, List<PlannedEntry> unresolvedMembers) {
            List<PlannedEntry> exact = List.copyOf(Objects.requireNonNull(unresolvedMembers, "unresolvedMembers"));
            if (!exact.equals(plan.members())) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "pre-candidate retry must retain every member of the exact unpublished shared plan");
            }
            return exact;
        }

        private PublishResult reverifyResolvedMembers(ProviderCandidate candidate, List<PlannedEntry> unresolvedMembers)
                throws IOException {
            List<MemberPublishResult> results = new ArrayList<>(unresolvedMembers.size());
            for (PlannedEntry member : unresolvedMembers) {
                results.add(reverifyResolvedMemberResult(candidate, member));
            }
            if (results.stream().noneMatch(FailedMember.class::isInstance)) {
                synchronized (monitor) {
                    candidates.remove(candidate.planId(), candidate);
                }
            }
            return new PublishResult(
                    ProviderObjectOutcome.EXISTING_EXACT,
                    Optional.of(candidate.identity()),
                    Optional.of(candidate.resolvedDescriptor()),
                    results);
        }

        private List<MemberPublishResult> validatePublicationMembers(
                ProviderCandidate candidate,
                List<PlannedEntry> requestedMembers,
                WalRunObjectSession.AuthenticatedNwg1PublicationExtent publication)
                throws IOException {
            List<MemberPublishResult> results = new ArrayList<>(requestedMembers.size());
            for (PlannedEntry member : requestedMembers) {
                int memberOrdinal = candidate.memberOrdinal(member);
                if (memberOrdinal < 0) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "member does not belong to the immutable shared plan");
                }
                try {
                    Nwg1DirectoryV1 directory =
                            publication.authenticatedPrefix().directory();
                    ExtentLocator locator = locator(candidate, member, memberOrdinal, directory);
                    int[] frameCount = {0};
                    Nwg1ObjectReaderV1.VerifiedAppendUnit appendUnit =
                            objectSession.verifySelectedNwg1AppendUnitForPublication(
                                    publication,
                                    context.verificationContext(),
                                    memberOrdinal,
                                    (frame, payload) -> requireVerifiedFrameMapping(
                                            locator, frame, frameCount, BridgeRejectionCode.READ_MAPPING_MISMATCH));
                    requireDecodedMapping(locator, directory, BridgeRejectionCode.READ_MAPPING_MISMATCH);
                    requireVerifiedAppendUnitMapping(
                            locator, directory, appendUnit, frameCount, BridgeRejectionCode.READ_MAPPING_MISMATCH);
                    results.add(memberResult(member, locator));
                } catch (Nwg1ValidationException failure) {
                    if ((failure.scope() != Nwg1IsolationScopeV1.BINDING
                                    && failure.scope() != Nwg1IsolationScopeV1.APPEND_UNIT)
                            || failure.stage().ordinal() < Nwg1ValidationStageV1.BINDING_SEMANTICS.ordinal()) {
                        throw failure;
                    }
                    results.add(new FailedMember(
                            member.binding(),
                            member.position(),
                            new MemberFailure(failure.rejection(), failure.stage(), failure.scope())));
                } catch (BridgeException mappingFailure) {
                    if (mappingFailure.code() != BridgeRejectionCode.READ_MAPPING_MISMATCH) {
                        throw mappingFailure;
                    }
                    results.add(new FailedMember(
                            member.binding(),
                            member.position(),
                            new MemberFailure(
                                    Nwg1RejectionV1.AUTHORITY_MISMATCH,
                                    Nwg1ValidationStageV1.BINDING_SEMANTICS,
                                    Nwg1IsolationScopeV1.BINDING)));
                }
            }
            return List.copyOf(results);
        }

        private List<MemberPublishResult> reverifyResolvedMemberResults(
                ProviderCandidate candidate, List<PlannedEntry> requestedMembers) throws IOException {
            List<MemberPublishResult> results = new ArrayList<>(requestedMembers.size());
            for (PlannedEntry member : requestedMembers) {
                results.add(reverifyResolvedMemberResult(candidate, member));
            }
            return List.copyOf(results);
        }

        private MemberPublishResult reverifyResolvedMemberResult(ProviderCandidate candidate, PlannedEntry member)
                throws IOException {
            int memberOrdinal = candidate.memberOrdinal(member);
            if (memberOrdinal < 0) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "recovery member does not belong to the immutable shared plan");
            }
            try {
                ExtentLocator locator = locator(candidate, member, memberOrdinal, candidate.directory());
                verifyRoutine(locator, false);
                return memberResult(member, locator);
            } catch (Nwg1ValidationException failure) {
                if ((failure.scope() != Nwg1IsolationScopeV1.BINDING
                                && failure.scope() != Nwg1IsolationScopeV1.APPEND_UNIT)
                        || failure.stage().ordinal() < Nwg1ValidationStageV1.BINDING_SEMANTICS.ordinal()) {
                    throw failure;
                }
                return new FailedMember(
                        member.binding(),
                        member.position(),
                        new MemberFailure(failure.rejection(), failure.stage(), failure.scope()));
            } catch (BridgeException mappingFailure) {
                if (mappingFailure.code() != BridgeRejectionCode.READ_MAPPING_MISMATCH) {
                    throw mappingFailure;
                }
                return new FailedMember(
                        member.binding(),
                        member.position(),
                        new MemberFailure(
                                Nwg1RejectionV1.AUTHORITY_MISMATCH,
                                Nwg1ValidationStageV1.BINDING_SEMANTICS,
                                Nwg1IsolationScopeV1.BINDING));
            }
        }

        private MemberPublishResult memberResult(PlannedEntry member, ExtentLocator locator) {
            Optional<MemberFailure> failure = Objects.requireNonNull(
                    memberAuthority.verify(member, locator), "typed Pulsar member publication result");
            return failure.<MemberPublishResult>map(
                            value -> new FailedMember(member.binding(), member.position(), value))
                    .orElseGet(() -> new VerifiedMember(locator));
        }

        private ExtentLocator locator(
                ProviderCandidate candidate, PlannedEntry member, int memberOrdinal, Nwg1DirectoryV1 directory) {
            if (memberOrdinal >= directory.appendUnits().size()) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID, "NWG1 directory lacks the Pulsar append unit");
            }
            Nwg1DirectoryV1.AppendUnit appendUnit = directory.appendUnits().get(memberOrdinal);
            if (!(appendUnit instanceof Nwg1DirectoryV1.PulsarAppendUnit pulsar)
                    || pulsar.virtualLedgerId() != member.position().virtualLedgerId()
                    || pulsar.entryId() != member.position().entryId()
                    || pulsar.frameCount() != 1
                    || pulsar.firstFrameOrdinal() >= directory.frames().size()) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "NWG1 append unit differs from the exact Pulsar position");
            }
            int frameOrdinal = Math.toIntExact(pulsar.firstFrameOrdinal());
            Nwg1DirectoryV1.Frame frame = directory.frames().get(frameOrdinal);
            if (frame.appendUnitOrdinal() != memberOrdinal) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "NWG1 frame does not belong to the exact Pulsar append unit");
            }
            return new ExtentLocator(
                    member.binding(),
                    member.position(),
                    candidate.identity(),
                    frameOrdinal,
                    frame.storedBodyOffset(),
                    frame.storedBlockBytes(),
                    frameDigest(candidate, frame));
        }

        private String frameDigest(ProviderCandidate candidate, Nwg1DirectoryV1.Frame frame) {
            byte[] retainedBody = candidate.retainedBody();
            if (retainedBody == null) {
                return candidate.frameDigest(frame);
            }
            int start = Math.toIntExact(frame.storedBodyOffset());
            int end = Math.toIntExact(Math.addExact(frame.storedBodyOffset(), frame.storedBlockBytes()));
            String digest = Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(
                            Arrays.copyOfRange(retainedBody, start, end)))
                    .toHex();
            candidate.rememberFrameDigest(frame, digest);
            return digest;
        }

        private RoutineVerification verifyRoutine(ExtentLocator locator, boolean retainPayload) throws IOException {
            byte[][] retainedPayload = {null};
            int[] frameCount = {0};
            WalRunObjectSession.VerifiedRoutineNwg1AppendUnit verified = objectSession.readRoutineNwg1AppendUnit(
                    locator.identity().objectIdentity(),
                    context.verificationContext(),
                    locator.frameOrdinal(),
                    (frame, payload) -> {
                        requireVerifiedFrameMapping(
                                locator, frame, frameCount, BridgeRejectionCode.READ_MAPPING_MISMATCH);
                        if (retainPayload) {
                            ByteBuffer borrowed = payload.asReadOnlyBuffer();
                            byte[] exactPayload = new byte[borrowed.remaining()];
                            borrowed.get(exactPayload);
                            retainedPayload[0] = exactPayload;
                        }
                    });
            Nwg1DirectoryV1 directory = verified.authenticatedPrefix().directory();
            requireDecodedMapping(locator, directory, BridgeRejectionCode.READ_MAPPING_MISMATCH);
            requireVerifiedAppendUnitMapping(
                    locator, directory, verified.appendUnit(), frameCount, BridgeRejectionCode.READ_MAPPING_MISMATCH);
            if (retainPayload && retainedPayload[0] == null) {
                throw rejected(
                        BridgeRejectionCode.READ_MAPPING_MISMATCH,
                        "routine NWG1 read did not yield the exact selected Pulsar frame");
            }
            return new RoutineVerification(retainedPayload[0]);
        }

        private void requireDecodedMapping(
                ExtentLocator locator, Nwg1DirectoryV1 directory, BridgeRejectionCode failureCode) {
            if (locator.frameOrdinal() >= directory.frames().size()) {
                throw rejected(failureCode, "read directory lacks the selected frame ordinal");
            }
            Nwg1DirectoryV1.Frame frame = directory.frames().get(locator.frameOrdinal());
            int unitOrdinal = Math.toIntExact(frame.appendUnitOrdinal());
            if (unitOrdinal >= directory.appendUnits().size()
                    || !(directory.appendUnits().get(unitOrdinal) instanceof Nwg1DirectoryV1.PulsarAppendUnit pulsar)
                    || pulsar.virtualLedgerId() != locator.position().virtualLedgerId()
                    || pulsar.entryId() != locator.position().entryId()
                    || frame.storedBodyOffset() != locator.extentOffset()
                    || frame.storedBlockBytes() != locator.extentLength()) {
                throw rejected(failureCode, "selected NWG1 frame differs from the active Pulsar locator");
            }
            int contextOrdinal = Math.toIntExact(pulsar.contextOrdinal());
            if (contextOrdinal < 0 || contextOrdinal >= directory.bindings().size()) {
                throw rejected(failureCode, "selected NWG1 append unit lacks its authenticated binding context");
            }
            try {
                bindingContextAuthority.requireExact(
                        locator.binding(), directory.bindings().get(contextOrdinal));
            } catch (Throwable authorityFailure) {
                BridgeException failure = rejected(
                        failureCode, "authenticated NWG1 binding context differs from the exact Pulsar binding");
                failure.addSuppressed(authorityFailure);
                throw failure;
            }
        }

        private void requireVerifiedFrameMapping(
                ExtentLocator locator,
                Nwg1ObjectReaderV1.VerifiedFrame frame,
                int[] observedFrameCount,
                BridgeRejectionCode failureCode) {
            if (observedFrameCount[0] != 0
                    || frame.absoluteFrameOrdinal() != locator.frameOrdinal()
                    || frame.appendUnitFrameOrdinal() != 0
                    || frame.coverage0() != locator.position().virtualLedgerId()
                    || frame.coverage1() != locator.position().entryId()) {
                throw rejected(failureCode, "streamed NWG1 frame differs from the exact Pulsar locator");
            }
            observedFrameCount[0] = 1;
        }

        private void requireVerifiedAppendUnitMapping(
                ExtentLocator locator,
                Nwg1DirectoryV1 directory,
                Nwg1ObjectReaderV1.VerifiedAppendUnit verified,
                int[] observedFrameCount,
                BridgeRejectionCode failureCode) {
            Nwg1DirectoryV1.Frame frame = directory.frames().get(locator.frameOrdinal());
            int appendUnitOrdinal = Math.toIntExact(frame.appendUnitOrdinal());
            if (!(directory.appendUnits().get(appendUnitOrdinal) instanceof Nwg1DirectoryV1.PulsarAppendUnit pulsar)
                    || observedFrameCount[0] != 1
                    || verified.protocolKind() != 2
                    || verified.appendUnitOrdinal() != appendUnitOrdinal
                    || verified.contextOrdinal() != pulsar.contextOrdinal()
                    || verified.firstFrameOrdinal() != pulsar.firstFrameOrdinal()
                    || verified.frameCount() != 1
                    || verified.decodedPayloadBytes() != frame.decodedPayloadBytes()
                    || verified.coverage0() != locator.position().virtualLedgerId()
                    || verified.coverage1() != locator.position().entryId()
                    || !Arrays.equals(verified.appendCommitSetId(), pulsar.appendCommitSetId())
                    || !Arrays.equals(verified.storageAttemptId(), pulsar.storageAttemptId())
                    || !Arrays.equals(verified.assignedPayloadSha256(), pulsar.assignedPayloadSha256())) {
                throw rejected(failureCode, "verified NWG1 append unit differs from the authenticated Pulsar row");
            }
        }

        private record RoutineVerification(byte[] payload) {
            private byte[] requirePayload() {
                if (payload == null) {
                    throw rejected(
                            BridgeRejectionCode.READ_MAPPING_MISMATCH,
                            "routine NWG1 verification did not retain its selected Pulsar payload");
                }
                return payload.clone();
            }
        }

        private ProviderResolvedExtentDescriptor descriptor(
                ProviderCandidate candidate, WalRunObjectSession.AuthenticatedNwg1PublicationExtent publication) {
            return new ProviderResolvedExtentDescriptor(
                    objectSession.rootSha256(),
                    new ProviderResolvedExtentRowV1(
                            candidate.reservation().laneId(),
                            candidate.reservation().laneSequence(),
                            Math.toIntExact(candidate.directoryPrefixEnd()),
                            candidate.identity().objectIdentity().bodyLength(),
                            candidate.identity().objectIdentity().bodySha256(),
                            publication.providerProof()),
                    context.clock().getAsLong());
        }

        /** Drains Provider operations and erases the WalRun key through the owning common session. */
        @Override
        public void close() {
            synchronized (monitor) {
                if (lifecycle == LocalLifecycle.CLOSED) {
                    return;
                }
                if (lifecycle != LocalLifecycle.DRAINING) {
                    throw new IllegalStateException("Pulsar Object-WAL adapter must drain before terminal close");
                }
                if (acceptedProviderTasks != 0 || !locallySealed.isEmpty() || !candidates.isEmpty()) {
                    throw new IllegalStateException(
                            "Pulsar Object-WAL adapter retains scheduled work, sealed plans, or exact candidates");
                }
                LaneSequenceVector terminalSequence = objectSession.sealRuntime();
                while (closure.checkpointPublisher().publishNext().isPresent()) {
                    // Publish the complete Root-bounded queue before the immutable physical Seal.
                }
                checkpointFailure = null;
                // Common drain is deliberately delayed until every local Provider/reconciliation operation and the
                // complete physical-checkpoint queue have reached a stable terminal state.  A failed terminal
                // preflight remains retryable: drain preserves both Provider reconciliation and the KMS key.
                objectSession.drain();
                closure.checkpointPublisher().requireFinalCoverage(terminalSequence);
                WalRunRuntime.RecoveredState sealed = objectSession.runtimeRecoveryState();
                var finalHead = closure.checkpointPublisher().head();
                WalRunSealRecord seal = new WalRunSealRecord(
                        closure.rootReference(),
                        terminalSequence,
                        closure.checkpointHeadKey(),
                        Sha256Digest.hash(WalRunControlCodec.encodeCheckpointHead(finalHead)),
                        sealed.resolvedExtentCount(),
                        sealed.resolvedCanonicalBodyBytes());
                objectSession.requireTerminalClosable();
                closure.lifecycleManager().publishSeal(closure.sealKey(), seal, objectSession);
                // A refused common close leaves every local map and lifecycle bit intact for a later exact retry.
                objectSession.close();
                for (ProviderCandidate candidate : candidates.values()) {
                    candidate.releaseRetainedBody();
                }
                candidates.clear();
                locallySealed.clear();
                lifecycle = LocalLifecycle.CLOSED;
            }
        }

        private ProviderCandidate requireCandidate(SealedExtentPlan plan, Optional<ExtentIdentity> candidateIdentity) {
            ProviderCandidate candidate;
            synchronized (monitor) {
                candidate = candidates.get(plan.planId());
            }
            if (candidate == null || !candidate.matchesPlan(plan)) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "reconciliation lacks the exact immutable Provider candidate");
            }
            candidate.requireIdentity(candidateIdentity);
            return candidate;
        }

        private <T> CompletionStage<T> providerIo(IoOperation<T> operation) {
            Objects.requireNonNull(operation, "operation");
            CompletableFuture<T> result = new CompletableFuture<>();
            synchronized (monitor) {
                requireNotClosed();
                acceptedProviderTasks = Math.incrementExact(acceptedProviderTasks);
            }
            try {
                providerExecutor.execute(() -> {
                    try {
                        result.complete(operation.execute());
                    } catch (Throwable error) {
                        result.completeExceptionally(error);
                    } finally {
                        providerTaskFinished();
                    }
                });
            } catch (Throwable schedulingFailure) {
                result.completeExceptionally(schedulingFailure);
                providerTaskFinished();
            }
            return result;
        }

        private void scheduleCheckpointPublication() {
            synchronized (monitor) {
                requireNotClosed();
                if (checkpointPublishScheduled) {
                    return;
                }
                checkpointPublishScheduled = true;
            }
            CompletionStage<Optional<com.nereusstream.storage.object.control.WalRunCheckpointPageV1>> publication =
                    providerIo(() -> closure.checkpointPublisher().publishNext());
            publication.whenComplete((page, error) -> {
                boolean continuePublishing = false;
                synchronized (monitor) {
                    checkpointPublishScheduled = false;
                    if (error != null) {
                        checkpointFailure = error;
                    } else {
                        checkpointFailure = null;
                        continuePublishing =
                                closure.checkpointPublisher().queueDepth() > 0 && lifecycle != LocalLifecycle.CLOSED;
                    }
                }
                if (continuePublishing) {
                    scheduleCheckpointPublication();
                }
            });
        }

        private void providerTaskFinished() {
            synchronized (monitor) {
                if (acceptedProviderTasks <= 0) {
                    throw new IllegalStateException("Pulsar Provider task accounting underflow");
                }
                acceptedProviderTasks--;
            }
        }

        private <T> CompletionStage<T> candidateIo(ProviderCandidate candidate, IoOperation<T> operation) {
            Objects.requireNonNull(candidate, "candidate");
            Objects.requireNonNull(operation, "operation");
            try {
                return providerIo(() -> {
                    try {
                        return operation.execute();
                    } catch (RetainedCandidateFailure exact) {
                        if (!exact.identity().equals(candidate.identity())) {
                            throw new RetainedCandidateFailure(candidate.identity(), exact);
                        }
                        throw exact;
                    } catch (Throwable error) {
                        throw new RetainedCandidateFailure(candidate.identity(), error);
                    }
                });
            } catch (Throwable schedulingFailure) {
                return failed(new RetainedCandidateFailure(candidate.identity(), schedulingFailure));
            }
        }

        private <T> CompletionStage<T> candidateInitialIo(ProviderCandidate candidate, IoOperation<T> operation) {
            return candidateIo(candidate, () -> {
                candidate.markProviderAttemptStarted();
                return operation.execute();
            });
        }

        /** Stops new admission while retaining exact UNKNOWN candidates for bounded same-candidate reconciliation. */
        public void drain() {
            synchronized (monitor) {
                if (lifecycle == LocalLifecycle.CLOSED || lifecycle == LocalLifecycle.DRAINING) {
                    return;
                }
                lifecycle = LocalLifecycle.DRAINING;
                // Stop sequence admission now so sealRuntime is legal, but defer Provider/KMS session drain until
                // all scheduled work, exact reconciliation, and checkpoint publication debt have completed.
                objectSession.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
            }
        }

        private void requireLocalOpen() {
            if (lifecycle != LocalLifecycle.OPEN) {
                throw new IllegalStateException("Pulsar Object-WAL adapter does not accept new local plans");
            }
            requireCheckpointHealthy();
        }

        private void requireNotClosed() {
            if (lifecycle == LocalLifecycle.CLOSED) {
                throw new IllegalStateException("Pulsar Object-WAL adapter is closed");
            }
        }

        private void requireCheckpointHealthy() {
            if (checkpointFailure != null) {
                throw new IllegalStateException(
                        "WalRun checkpoint publication requires exact retry", checkpointFailure);
            }
        }

        @FunctionalInterface
        private interface IoOperation<T> {
            T execute() throws Exception;
        }

        private enum CandidateDisposition {
            PENDING,
            RESOLVED,
            ABSENT,
            CONFLICT
        }

        private enum LocalLifecycle {
            OPEN,
            DRAINING,
            CLOSED
        }

        private record LocalPlan(
                PreparedNwg1Plan projection, WalRunObjectSession.ValidatedNwg1Plan validated, SealedExtentPlan plan) {
            private LocalPlan {
                Objects.requireNonNull(projection, "projection");
                Objects.requireNonNull(validated, "validated");
                Objects.requireNonNull(plan, "plan");
            }
        }

        private static final class ProviderCandidate {
            private final SealedExtentPlan exactPlan;
            private final List<MemberKey> members;
            private final Map<MemberKey, Integer> memberOrdinals;
            private final WalRunObjectSession.AdmittedNwg1Candidate admitted;
            private final LaneSequenceReservation reservation;
            private final ExtentIdentity identity;
            private final long directoryPrefixEnd;
            private final Nwg1DirectoryV1 directory;
            private ExactRepeatableBody repeatableBody;
            private CandidateDisposition disposition = CandidateDisposition.PENDING;
            private boolean providerAttemptStarted;
            private ProviderResolvedExtentDescriptor resolvedDescriptor;
            private ProviderObjectResult exactProviderResult;
            private boolean checkpointEnqueued;
            private final Map<FrameRange, String> frameDigests = new HashMap<>();

            private ProviderCandidate(
                    SealedExtentPlan plan,
                    Map<MemberKey, Integer> memberOrdinals,
                    WalRunObjectSession.AdmittedNwg1Candidate admitted,
                    LaneSequenceReservation reservation,
                    ExtentIdentity identity,
                    long directoryPrefixEnd,
                    Nwg1DirectoryV1 directory,
                    ExactRepeatableBody repeatableBody) {
                this.exactPlan = Objects.requireNonNull(plan, "plan");
                this.members = plan.members().stream()
                        .map(member -> new MemberKey(member.binding(), member.position()))
                        .toList();
                this.memberOrdinals = Map.copyOf(Objects.requireNonNull(memberOrdinals, "memberOrdinals"));
                if (!this.memberOrdinals.keySet().equals(new HashSet<>(this.members))) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "sealed member ordinal map differs from the exact Pulsar plan");
                }
                this.admitted = Objects.requireNonNull(admitted, "admitted");
                this.reservation = reservation;
                this.identity = identity;
                this.directoryPrefixEnd = directoryPrefixEnd;
                this.directory = directory;
                this.repeatableBody = repeatableBody;
            }

            String planId() {
                return exactPlan.planId();
            }

            int memberOrdinal(PlannedEntry member) {
                return memberOrdinals.getOrDefault(new MemberKey(member.binding(), member.position()), -1);
            }

            boolean matchesPlan(SealedExtentPlan candidate) {
                return exactPlan.equals(candidate);
            }

            LaneSequenceReservation reservation() {
                return reservation;
            }

            WalRunObjectSession.AdmittedNwg1Candidate admitted() {
                return admitted;
            }

            ExtentIdentity identity() {
                return identity;
            }

            long directoryPrefixEnd() {
                return directoryPrefixEnd;
            }

            Nwg1DirectoryV1 directory() {
                return directory;
            }

            synchronized ExactRepeatableBody repeatableBody() {
                if (repeatableBody == null) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "Provider candidate body was already released");
                }
                return repeatableBody;
            }

            synchronized byte[] retainedBody() {
                return repeatableBody == null ? null : repeatableBody.body();
            }

            synchronized CandidateDisposition disposition() {
                return disposition;
            }

            synchronized void markProviderAttemptStarted() {
                providerAttemptStarted = true;
            }

            synchronized boolean needsInitialProviderAttempt() {
                return disposition == CandidateDisposition.PENDING && !providerAttemptStarted;
            }

            synchronized void rememberExactProviderResult(ProviderObjectResult result) {
                Objects.requireNonNull(result, "result");
                if (result.outcome() != ProviderObjectOutcome.APPLIED_EXACT
                        && result.outcome() != ProviderObjectOutcome.EXISTING_EXACT) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "retained Provider result is not an exact application");
                }
                if (exactProviderResult != null && !exactProviderResult.equals(result)) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "exact Provider retry changed its outcome or version proof");
                }
                exactProviderResult = result;
            }

            synchronized Optional<ProviderObjectResult> exactProviderResult() {
                return Optional.ofNullable(exactProviderResult);
            }

            synchronized void prepareCheckpointDescriptor(
                    ProviderResolvedExtentDescriptor descriptor, WalCheckpointPublisher publisher) {
                Objects.requireNonNull(descriptor, "descriptor");
                Objects.requireNonNull(publisher, "publisher");
                if (resolvedDescriptor != null) {
                    if (!resolvedDescriptor.rootSha256().equals(descriptor.rootSha256())
                            || !resolvedDescriptor.row().equals(descriptor.row())) {
                        throw rejected(
                                BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                                "Provider exact retry changed its checkpoint descriptor");
                    }
                    descriptor = resolvedDescriptor;
                }
                resolvedDescriptor = descriptor;
                if (checkpointEnqueued) {
                    return;
                }
                publisher.enqueue(descriptor);
                checkpointEnqueued = true;
            }

            synchronized void resolveAndReleaseBody() {
                if (disposition != CandidateDisposition.PENDING || resolvedDescriptor == null || !checkpointEnqueued) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "Provider candidate cannot resolve before its exact checkpoint descriptor is retained");
                }
                disposition = CandidateDisposition.RESOLVED;
                for (Nwg1DirectoryV1.Frame frame : directory.frames()) {
                    frameDigest(frame);
                }
                repeatableBody.erase();
                repeatableBody = null;
            }

            synchronized ProviderResolvedExtentDescriptor resolvedDescriptor() {
                if (disposition != CandidateDisposition.RESOLVED || resolvedDescriptor == null) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "Provider candidate lacks its exact resolved descriptor");
                }
                return resolvedDescriptor;
            }

            synchronized void markAbsent() {
                disposition = CandidateDisposition.ABSENT;
                if (repeatableBody != null) {
                    repeatableBody.erase();
                    repeatableBody = null;
                }
            }

            synchronized void markConflict() {
                disposition = CandidateDisposition.CONFLICT;
                if (repeatableBody != null) {
                    repeatableBody.erase();
                    repeatableBody = null;
                }
            }

            synchronized void releaseRetainedBody() {
                if (repeatableBody != null) {
                    repeatableBody.erase();
                    repeatableBody = null;
                }
            }

            synchronized void requireIdentity(Optional<ExtentIdentity> candidateIdentity) {
                if (candidateIdentity.isEmpty() || !identity.equals(candidateIdentity.orElseThrow())) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "candidate identity differs from the exact sealed Provider request");
                }
            }

            synchronized String frameDigest(Nwg1DirectoryV1.Frame frame) {
                FrameRange range = new FrameRange(frame.storedBodyOffset(), frame.storedBlockBytes());
                String existing = frameDigests.get(range);
                if (existing != null) {
                    return existing;
                }
                if (repeatableBody == null) {
                    throw rejected(
                            BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                            "frame digest was not retained before resolved payload release");
                }
                byte[] body = repeatableBody.body();
                int start = Math.toIntExact(range.offset());
                int end = Math.toIntExact(Math.addExact(range.offset(), range.length()));
                String computed = Sha256Digest.hash(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(
                                Arrays.copyOfRange(body, start, end)))
                        .toHex();
                frameDigests.put(range, computed);
                return computed;
            }

            synchronized void rememberFrameDigest(Nwg1DirectoryV1.Frame frame, String digest) {
                frameDigests.putIfAbsent(new FrameRange(frame.storedBodyOffset(), frame.storedBlockBytes()), digest);
            }
        }

        private record FrameRange(long offset, long length) {}

        private static final class ExactRepeatableBody implements RepeatableObjectBody {
            private final ObjectIdentity identity;
            private byte[] body;

            private ExactRepeatableBody(ObjectIdentity identity, byte[] body) {
                this.identity = Objects.requireNonNull(identity, "identity");
                this.body = Objects.requireNonNull(body, "body").clone();
            }

            @Override
            public ObjectIdentity identity() {
                return identity;
            }

            @Override
            public synchronized InputStream openStream() throws IOException {
                if (body == null) {
                    throw new IOException("exact repeatable body was released");
                }
                return new ByteArrayInputStream(body.clone());
            }

            synchronized byte[] body() {
                if (body == null) {
                    throw rejected(BridgeRejectionCode.RESOLVED_EXTENT_INVALID, "exact repeatable body was released");
                }
                return body.clone();
            }

            synchronized void erase() {
                if (body != null) {
                    Arrays.fill(body, (byte) 0);
                    body = null;
                }
            }
        }
    }

    public enum BridgeRejectionCode {
        BINDING_ALREADY_ACTIVE,
        BINDING_NOT_ACTIVE,
        STALE_OWNER,
        SHARED_PLAN_SIZE_INVALID,
        DUPLICATE_BINDING_IN_SHARED_PLAN,
        PACKING_POLICY_MISMATCH,
        INVALID_LANE,
        PAYLOAD_SIZE_INVALID,
        ACTIVE_TAIL_CAPACITY_EXHAUSTED,
        COMPLETION_TRACKER_CAPACITY_EXHAUSTED,
        COMPLETION_TICKET_EXHAUSTED,
        COMPLETION_TICKET_MISMATCH,
        COMPLETION_SLOT_STATE_INVALID,
        OWNER_LOCAL_STATE_DISCARDED,
        BINDING_APPEND_IN_FLIGHT,
        BINDING_GAP_UNRESOLVED,
        LEDGER_ROLLOVER_REQUIRED,
        WALRUN_ADMISSION_STOPPED,
        RESOLVED_EXTENT_INVALID,
        PHYSICAL_FRONTIER_MISMATCH,
        PROVIDER_DEFINITIVELY_ABSENT,
        PROVIDER_OUTCOME_UNKNOWN,
        ABSENCE_NOT_PROVEN,
        ATOMIC_MULTI_BINDING_ROLLOVER_UNAVAILABLE,
        ATOMIC_SHARED_TAKEOVER_REQUIRED,
        FAILED_PLAN_NOT_FOUND,
        FENCED,
        MANIFEST_HANDOFF_INVALID,
        MANIFEST_HANDOFF_NOT_VERIFIED,
        POSITION_NOT_READABLE,
        ACTIVE_TAIL_LOCATOR_MISSING,
        READ_MAPPING_MISMATCH,
        RECOVERY_BOUND_EXCEEDED,
        RECOVERY_MAPPING_INVALID
    }

    public static final class BridgeException extends IllegalStateException {
        private final BridgeRejectionCode code;

        BridgeException(BridgeRejectionCode code, String message) {
            super(message);
            this.code = Objects.requireNonNull(code, "code");
        }

        public BridgeRejectionCode code() {
            return code;
        }
    }

    private static final class BindingState {
        private OpenedLedger current;
        private final NavigableMap<Long, LedgerState> ledgers = new TreeMap<>();
        private final CompletionTrackerRing completionRing;
        private long readViewVersion = 1;
        private String inFlightPlanId;
        private PulsarPosition inFlightPosition;
        private CompletionTicket inFlightTicket;
        private String gapPlanId;
        private PulsarPosition gapPosition;
        private CompletionTicket gapTicket;

        private BindingState(OpenedLedger current, LedgerState ledger, CompletionTrackerRing completionRing) {
            this.current = current;
            this.completionRing = completionRing;
            ledgers.put(current.node().virtualLedgerId(), ledger);
        }

        static BindingState recover(
                Configuration configuration,
                OpenedLedger opened,
                RecoverySeed seed,
                Optional<ManifestSource> manifestSource) {
            Objects.requireNonNull(manifestSource, "manifestSource");
            if ((seed.manifestThrough() >= 0) != manifestSource.isPresent()) {
                throw rejected(
                        BridgeRejectionCode.MANIFEST_HANDOFF_NOT_VERIFIED,
                        "recovery manifest coverage lacks its authority-verified source");
            }
            List<ExtentLocator> ordered = requireRecoveryShape(configuration, opened, seed);
            NavigableMap<Long, ExtentLocator> locators = new TreeMap<>();
            ordered.forEach(locator -> locators.put(locator.position().entryId(), locator));
            LedgerState ledger = new LedgerState(
                    opened.node().virtualLedgerId(),
                    seed.manifestThrough(),
                    seed.durableThrough(),
                    seed.readableThrough(),
                    seed.manifestGeneration(),
                    manifestSource.orElse(null),
                    locators);
            CompletionTrackerRing completionRing = new CompletionTrackerRing(
                    configuration.maxCompletionSlotsPerBinding(), opened.node().ownerEpoch(), 0);
            for (ExtentLocator locator : ordered) {
                PrePositionReservation reservation = completionRing.reserve(
                        opened.node().ownerEpoch(), 0, configuration.maxActiveTailLocatorsPerBinding());
                CompletionTicket ticket = completionRing.assignAfterPosition(reservation, locator.position());
                completionRing.installLocator(ticket, locator.position(), locator);
                completionRing.requireCompletionRelease(ticket, locator.position(), locator);
                completionRing.completeAndRelease(ticket, locator.position(), locator);
            }
            return new BindingState(opened, ledger, completionRing);
        }

        static List<ExtentLocator> requireRecoveryShape(
                Configuration configuration, OpenedLedger opened, RecoverySeed seed) {
            if (opened.node().virtualLedgerId() != seed.virtualLedgerId()) {
                throw rejected(
                        BridgeRejectionCode.RECOVERY_MAPPING_INVALID,
                        "recovery seed ledger differs from the durable chain head");
            }
            if (seed.activeTail().size() > configuration.maxRecoveryLocators()
                    || seed.activeTail().size() > configuration.maxActiveTailLocatorsPerBinding()) {
                throw rejected(
                        BridgeRejectionCode.RECOVERY_BOUND_EXCEEDED,
                        "active-tail recovery locator count exceeds its hard bound");
            }
            NavigableMap<Long, ExtentLocator> locators = new TreeMap<>();
            long expected = Math.addExact(seed.manifestThrough(), 1);
            List<ExtentLocator> ordered = seed.activeTail().stream()
                    .sorted(Comparator.comparingLong(
                            locator -> locator.position().entryId()))
                    .toList();
            for (ExtentLocator locator : ordered) {
                if (!locator.binding().equals(opened.node().binding())
                        || locator.position().virtualLedgerId() != opened.node().virtualLedgerId()
                        || locator.position().entryId() != expected
                        || locators.put(expected, locator) != null) {
                    throw rejected(
                            BridgeRejectionCode.RECOVERY_MAPPING_INVALID,
                            "active-tail recovery locators are not one exact contiguous Pulsar range");
                }
                expected = Math.addExact(expected, 1);
            }
            if (Math.subtractExact(expected, 1) != seed.durableThrough()) {
                throw rejected(
                        BridgeRejectionCode.RECOVERY_MAPPING_INVALID,
                        "recovery locators do not close the manifest-to-durable gap");
            }
            return ordered;
        }

        void requireOwner(long ownerEpoch) {
            if (current.node().ownerEpoch() != ownerEpoch) {
                throw rejected(BridgeRejectionCode.STALE_OWNER, "append owner epoch is stale");
            }
        }

        void requireQuiescent() {
            if (inFlightPlanId != null) {
                throw rejected(BridgeRejectionCode.BINDING_APPEND_IN_FLIGHT, "binding already has an admitted append");
            }
            if (gapPlanId != null) {
                throw rejected(BridgeRejectionCode.BINDING_GAP_UNRESOLVED, "binding has an unresolved entry gap");
            }
        }

        PrePositionReservation reserveBeforePosition(Configuration configuration) {
            requireQuiescent();
            return completionRing.reserve(
                    current.node().ownerEpoch(),
                    currentLedger().activeTail.size(),
                    configuration.maxActiveTailLocatorsPerBinding());
        }

        void cancelBeforePosition(PrePositionReservation reservation) {
            completionRing.cancelBeforePosition(reservation);
        }

        CompletionTicket allocateTicketAfterPosition(PrePositionReservation reservation, PulsarPosition position) {
            return completionRing.assignAfterPosition(reservation, position);
        }

        void cancelAfterPositionBeforeProvider(CompletionTicket ticket, PulsarPosition position) {
            completionRing.definitiveCancelAndRelease(ticket, position);
            if (Objects.equals(inFlightTicket, ticket) && Objects.equals(inFlightPosition, position)) {
                inFlightPlanId = null;
                inFlightPosition = null;
                inFlightTicket = null;
            }
        }

        void installInFlight(String planId, PulsarPosition position, CompletionTicket ticket) {
            requireQuiescent();
            completionRing.requireLive(ticket, position);
            inFlightPlanId = planId;
            inFlightPosition = position;
            inFlightTicket = ticket;
        }

        void installGap(String planId, PulsarPosition position) {
            CompletionTicket ticket = requireTicket(planId, position);
            completionRing.markUnknown(ticket, position);
            inFlightPlanId = null;
            inFlightPosition = null;
            inFlightTicket = null;
            gapPlanId = planId;
            gapPosition = position;
            gapTicket = ticket;
        }

        void requireGap(String planId, PulsarPosition position) {
            if (!Objects.equals(gapPlanId, planId) || !Objects.equals(gapPosition, position)) {
                throw rejected(BridgeRejectionCode.BINDING_GAP_UNRESOLVED, "binding gap differs from recovery plan");
            }
            completionRing.retry(gapTicket, position);
        }

        void requirePlanned(String planId, PulsarPosition position) {
            if (!matchesPlanned(planId, position)) {
                throw rejected(BridgeRejectionCode.RESOLVED_EXTENT_INVALID, "result does not match reserved position");
            }
            completionRing.requireLive(requireTicket(planId, position), position);
        }

        boolean matchesPlanned(String planId, PulsarPosition position) {
            boolean inFlightMatches =
                    Objects.equals(inFlightPlanId, planId) && Objects.equals(inFlightPosition, position);
            boolean gapMatches = Objects.equals(gapPlanId, planId) && Objects.equals(gapPosition, position);
            return inFlightMatches || gapMatches;
        }

        boolean matchesPlanId(String planId) {
            return Objects.equals(inFlightPlanId, planId) || Objects.equals(gapPlanId, planId);
        }

        Optional<String> livePlanId() {
            return Optional.ofNullable(inFlightPlanId != null ? inFlightPlanId : gapPlanId);
        }

        void requireCanInstallLocator(PulsarPosition position) {
            LedgerState ledger = requireLedger(position.virtualLedgerId());
            if (position.entryId() != ledger.durableThrough + 1 || ledger.activeTail.containsKey(position.entryId())) {
                throw rejected(
                        BridgeRejectionCode.RESOLVED_EXTENT_INVALID,
                        "locator installation is not the exact next contiguous Pulsar entry");
            }
        }

        void installHiddenLocator(PulsarPosition position, ExtentLocator locator) {
            LedgerState ledger = requireLedger(position.virtualLedgerId());
            CompletionTicket ticket = requireTicketForPosition(position);
            completionRing.installLocator(ticket, position, locator);
            ledger.activeTail.put(position.entryId(), locator);
        }

        void publishFrontiers(String planId, PulsarPosition position) {
            requirePlanned(planId, position);
            LedgerState ledger = requireLedger(position.virtualLedgerId());
            ExtentLocator locator = ledger.activeTail.get(position.entryId());
            if (locator == null) {
                throw rejected(
                        BridgeRejectionCode.ACTIVE_TAIL_LOCATOR_MISSING,
                        "frontier publication requires an already-installed active-tail locator");
            }
            CompletionTicket ticket = requireTicket(planId, position);
            completionRing.requireCompletionRelease(ticket, position, locator);
            ledger.readableThrough = position.entryId();
            ledger.durableThrough = position.entryId();
            advanceReadView();
            completionRing.completeAndRelease(ticket, position, locator);
            inFlightPlanId = null;
            inFlightPosition = null;
            inFlightTicket = null;
            gapPlanId = null;
            gapPosition = null;
            gapTicket = null;
        }

        void releaseDefinitiveCancellation(String planId, PulsarPosition position) {
            requireGap(planId, position);
            completionRing.definitiveCancelAndRelease(gapTicket, position);
            gapPlanId = null;
            gapPosition = null;
            gapTicket = null;
        }

        void installSuccessor(OpenedLedger successor) {
            requireGapOrQuiescentForSuccessor();
            LedgerNode prior = current.node();
            LedgerNode next = successor.node();
            if (!next.binding().equals(prior.binding())
                    || next.predecessorLedgerId().isEmpty()
                    || next.predecessorLedgerId().getAsLong() != prior.virtualLedgerId()) {
                throw rejected(
                        BridgeRejectionCode.RECOVERY_MAPPING_INVALID,
                        "successor does not explicitly link to the current ledger");
            }
            current = successor;
            ledgers.put(
                    next.virtualLedgerId(),
                    new LedgerState(next.virtualLedgerId(), -1, -1, -1, 0, null, new TreeMap<>()));
            advanceReadView();
            inFlightPlanId = null;
            inFlightPosition = null;
            inFlightTicket = null;
            gapPlanId = null;
            gapPosition = null;
            gapTicket = null;
        }

        private void requireGapOrQuiescentForSuccessor() {
            if (inFlightPlanId != null || gapPlanId != null) {
                throw rejected(BridgeRejectionCode.BINDING_APPEND_IN_FLIGHT, "cannot rollover an in-flight append");
            }
        }

        void advanceReadView() {
            readViewVersion = Math.addExact(readViewVersion, 1);
        }

        PulsarObjectWalReadViewV1 captureReadView(PulsarBindingKey binding) {
            List<PulsarObjectWalReadViewV1.LedgerView> views = new ArrayList<>(ledgers.size());
            for (LedgerState ledger : ledgers.values()) {
                List<PulsarObjectWalReadViewV1.SourceInterval> intervals = new ArrayList<>();
                if (ledger.manifestThrough >= 0) {
                    intervals.add(new PulsarObjectWalReadViewV1.SourceInterval(
                            ledger.virtualLedgerId,
                            0,
                            Math.addExact(ledger.manifestThrough, 1),
                            ReadSource.MANIFEST,
                            Optional.of(Objects.requireNonNull(ledger.manifestSource, "manifest source")),
                            Optional.empty()));
                }
                for (ExtentLocator locator : ledger.activeTail.values()) {
                    if (locator.position().entryId() <= ledger.readableThrough
                            && locator.position().entryId() > ledger.manifestThrough) {
                        intervals.add(new PulsarObjectWalReadViewV1.SourceInterval(
                                ledger.virtualLedgerId,
                                locator.position().entryId(),
                                Math.addExact(locator.position().entryId(), 1),
                                ReadSource.ACTIVE_TAIL,
                                Optional.empty(),
                                Optional.of(locator)));
                    }
                }
                views.add(new PulsarObjectWalReadViewV1.LedgerView(
                        ledger.virtualLedgerId,
                        ledger.manifestThrough,
                        ledger.readableThrough,
                        ledger.durableThrough,
                        ledger.manifestGeneration,
                        intervals));
            }
            return new PulsarObjectWalReadViewV1(binding, current.node().ownerEpoch(), readViewVersion, views);
        }

        void discardAfterFence(long fencedOwnerEpoch) {
            completionRing.discardAfterFence(fencedOwnerEpoch);
            inFlightPlanId = null;
            inFlightPosition = null;
            inFlightTicket = null;
            gapPlanId = null;
            gapPosition = null;
            gapTicket = null;
        }

        private CompletionTicket requireTicket(String planId, PulsarPosition position) {
            if (Objects.equals(inFlightPlanId, planId) && Objects.equals(inFlightPosition, position)) {
                return Objects.requireNonNull(inFlightTicket, "inFlightTicket");
            }
            if (Objects.equals(gapPlanId, planId) && Objects.equals(gapPosition, position)) {
                return Objects.requireNonNull(gapTicket, "gapTicket");
            }
            throw rejected(BridgeRejectionCode.COMPLETION_TICKET_MISMATCH, "plan has no exact completion ticket");
        }

        private CompletionTicket requireTicketForPosition(PulsarPosition position) {
            if (Objects.equals(inFlightPosition, position)) {
                return Objects.requireNonNull(inFlightTicket, "inFlightTicket");
            }
            if (Objects.equals(gapPosition, position)) {
                return Objects.requireNonNull(gapTicket, "gapTicket");
            }
            throw rejected(BridgeRejectionCode.COMPLETION_TICKET_MISMATCH, "position has no exact completion ticket");
        }

        LedgerState currentLedger() {
            return requireLedger(current.node().virtualLedgerId());
        }

        LedgerState requireLedger(long ledgerId) {
            return Optional.ofNullable(ledgers.get(ledgerId))
                    .orElseThrow(() -> rejected(
                            BridgeRejectionCode.POSITION_NOT_READABLE,
                            "ledger is not in the explicit owner-local chain view"));
        }

        LedgerFrontiers currentFrontiers() {
            return currentLedger().frontiers(current.node().binding());
        }
    }

    /** Owner-local bounded ring. Completion tickets are deliberately absent from every persisted/public record. */
    static final class CompletionTrackerRing {
        private final CompletionSlot[] slots;
        private final long ownerEpoch;
        private long nextTicket;
        private boolean ticketExhausted;
        private boolean discarded;
        private int nextSlot;
        private int reservedLocatorCount;
        private int pendingTicketReservations;
        private long ticketsIssued;

        CompletionTrackerRing(int capacity, long ownerEpoch, long initialTicket) {
            if (capacity <= 0 || ownerEpoch <= 0) {
                throw new IllegalArgumentException("completion ring capacity/owner is invalid");
            }
            this.slots = new CompletionSlot[capacity];
            for (int index = 0; index < capacity; index++) {
                slots[index] = new CompletionSlot();
            }
            this.ownerEpoch = ownerEpoch;
            this.nextTicket = initialTicket;
        }

        PrePositionReservation reserve(long expectedOwnerEpoch, int activeTailCount, int locatorCapacity) {
            requireOwner(expectedOwnerEpoch);
            if (activeTailCount < 0 || locatorCapacity <= 0) {
                throw new IllegalArgumentException("locator counts are invalid");
            }
            // CompletionTicket is one unsigned 64-bit bit-pattern. Java long stores those bits; signed order is never
            // ticket authority. Account for every already-reserved future value before admitting another position.
            long greatestStartWithRoom = -1L - pendingTicketReservations;
            if (ticketExhausted || Long.compareUnsigned(nextTicket, greatestStartWithRoom) > 0) {
                throw rejected(
                        BridgeRejectionCode.COMPLETION_TICKET_EXHAUSTED,
                        "checked 64-bit completion ticket space is exhausted before position allocation");
            }
            if ((long) activeTailCount + reservedLocatorCount >= locatorCapacity) {
                throw rejected(
                        BridgeRejectionCode.ACTIVE_TAIL_CAPACITY_EXHAUSTED,
                        "combined tracker/locator capacity is exhausted before position allocation");
            }
            for (int offset = 0; offset < slots.length; offset++) {
                int slotIndex = (nextSlot + offset) % slots.length;
                CompletionSlot slot = slots[slotIndex];
                if (slot.state == CompletionSlotState.FREE) {
                    PrePositionReservation reservation = new PrePositionReservation(ownerEpoch, slotIndex);
                    slot.state = CompletionSlotState.RESERVED;
                    slot.ownerEpoch = ownerEpoch;
                    slot.reservation = reservation;
                    reservedLocatorCount++;
                    pendingTicketReservations++;
                    nextSlot = (slotIndex + 1) % slots.length;
                    return reservation;
                }
            }
            throw rejected(
                    BridgeRejectionCode.COMPLETION_TRACKER_CAPACITY_EXHAUSTED,
                    "completion tracker capacity is exhausted before position allocation");
        }

        void cancelBeforePosition(PrePositionReservation reservation) {
            CompletionSlot slot = requireReservation(reservation);
            pendingTicketReservations--;
            release(slot);
        }

        CompletionTicket assignAfterPosition(PrePositionReservation reservation, PulsarPosition position) {
            Objects.requireNonNull(position, "position");
            CompletionSlot slot = requireReservation(reservation);
            if (ticketExhausted) {
                throw rejected(
                        BridgeRejectionCode.COMPLETION_TICKET_EXHAUSTED,
                        "checked 64-bit completion ticket space exhausted after reservation");
            }
            long value = nextTicket;
            if (value == -1L) {
                ticketExhausted = true;
            } else {
                nextTicket = value + 1;
            }
            pendingTicketReservations--;
            ticketsIssued++;
            CompletionTicket ticket = new CompletionTicket(ownerEpoch, value, reservation.slotIndex());
            slot.state = CompletionSlotState.ALLOCATED;
            slot.ticketValue = value;
            slot.position = position;
            slot.reservation = null;
            return ticket;
        }

        void requireLive(CompletionTicket ticket, PulsarPosition position) {
            CompletionSlot slot = requireExact(ticket, position);
            if (slot.state != CompletionSlotState.ALLOCATED
                    && slot.state != CompletionSlotState.UNKNOWN
                    && slot.state != CompletionSlotState.LOCATOR_INSTALLED) {
                throw invalidSlotState("completion ticket is not live");
            }
        }

        void markUnknown(CompletionTicket ticket, PulsarPosition position) {
            CompletionSlot slot = requireExact(ticket, position);
            if (slot.state != CompletionSlotState.ALLOCATED && slot.state != CompletionSlotState.UNKNOWN) {
                throw invalidSlotState("UNKNOWN transition requires allocated/UNKNOWN ticket");
            }
            slot.state = CompletionSlotState.UNKNOWN;
        }

        void retry(CompletionTicket ticket, PulsarPosition position) {
            CompletionSlot slot = requireExact(ticket, position);
            if (slot.state != CompletionSlotState.UNKNOWN && slot.state != CompletionSlotState.ALLOCATED) {
                throw invalidSlotState("retry requires an allocated/UNKNOWN ticket");
            }
        }

        void installLocator(CompletionTicket ticket, PulsarPosition position, ExtentLocator locator) {
            CompletionSlot slot = requireExact(ticket, position);
            Objects.requireNonNull(locator, "locator");
            if (slot.state != CompletionSlotState.ALLOCATED && slot.state != CompletionSlotState.UNKNOWN) {
                throw invalidSlotState("locator installation requires an allocated/UNKNOWN ticket");
            }
            slot.locator = locator;
            slot.state = CompletionSlotState.LOCATOR_INSTALLED;
        }

        void requireCompletionRelease(CompletionTicket ticket, PulsarPosition position, ExtentLocator locator) {
            CompletionSlot slot = requireExact(ticket, position);
            if (slot.state != CompletionSlotState.LOCATOR_INSTALLED || !Objects.equals(slot.locator, locator)) {
                throw invalidSlotState("completion release requires the exact installed locator");
            }
        }

        void completeAndRelease(CompletionTicket ticket, PulsarPosition position, ExtentLocator locator) {
            CompletionSlot slot = requireExact(ticket, position);
            if (slot.state != CompletionSlotState.LOCATOR_INSTALLED || !Objects.equals(slot.locator, locator)) {
                throw invalidSlotState("completion release requires the exact installed locator");
            }
            release(slot);
        }

        void definitiveCancelAndRelease(CompletionTicket ticket, PulsarPosition position) {
            CompletionSlot slot = requireExact(ticket, position);
            if (slot.state != CompletionSlotState.ALLOCATED && slot.state != CompletionSlotState.UNKNOWN) {
                throw invalidSlotState("definitive cancellation requires an allocated/UNKNOWN ticket");
            }
            release(slot);
        }

        void discardAfterFence(long fencedOwnerEpoch) {
            requireOwner(fencedOwnerEpoch);
            for (CompletionSlot slot : slots) {
                clear(slot);
            }
            reservedLocatorCount = 0;
            pendingTicketReservations = 0;
            discarded = true;
        }

        int reservedLocatorCount() {
            return reservedLocatorCount;
        }

        long ticketsIssued() {
            return ticketsIssued;
        }

        long nextTicketForTest() {
            return nextTicket;
        }

        private CompletionSlot requireReservation(PrePositionReservation reservation) {
            Objects.requireNonNull(reservation, "reservation");
            requireOwner(reservation.ownerEpoch());
            if (reservation.slotIndex() < 0 || reservation.slotIndex() >= slots.length) {
                throw rejected(BridgeRejectionCode.COMPLETION_TICKET_MISMATCH, "reservation slot is invalid");
            }
            CompletionSlot slot = slots[reservation.slotIndex()];
            if (slot.state != CompletionSlotState.RESERVED
                    || slot.ownerEpoch != reservation.ownerEpoch()
                    || slot.reservation != reservation) {
                throw rejected(
                        BridgeRejectionCode.COMPLETION_TICKET_MISMATCH,
                        "pre-position reservation no longer owns its exact slot");
            }
            return slot;
        }

        private CompletionSlot requireExact(CompletionTicket ticket, PulsarPosition position) {
            Objects.requireNonNull(ticket, "ticket");
            Objects.requireNonNull(position, "position");
            requireOwner(ticket.ownerEpoch());
            if (ticket.slotIndex() < 0 || ticket.slotIndex() >= slots.length) {
                throw rejected(BridgeRejectionCode.COMPLETION_TICKET_MISMATCH, "completion slot is invalid");
            }
            CompletionSlot slot = slots[ticket.slotIndex()];
            if (slot.state == CompletionSlotState.FREE
                    || slot.state == CompletionSlotState.RESERVED
                    || slot.ownerEpoch != ticket.ownerEpoch()
                    || slot.ticketValue != ticket.value()
                    || !Objects.equals(slot.position, position)) {
                throw rejected(
                        BridgeRejectionCode.COMPLETION_TICKET_MISMATCH,
                        "owner epoch, full ticket, slot, and position must all match");
            }
            return slot;
        }

        private void requireOwner(long expectedOwnerEpoch) {
            if (discarded) {
                throw rejected(
                        BridgeRejectionCode.OWNER_LOCAL_STATE_DISCARDED,
                        "owner-local completion state was discarded after a durable fence");
            }
            if (expectedOwnerEpoch != ownerEpoch) {
                throw rejected(BridgeRejectionCode.STALE_OWNER, "completion ticket owner epoch is stale");
            }
        }

        private void release(CompletionSlot slot) {
            clear(slot);
            reservedLocatorCount--;
            if (reservedLocatorCount < 0) {
                throw new IllegalStateException("completion locator reservation underflow");
            }
        }

        private static void clear(CompletionSlot slot) {
            slot.state = CompletionSlotState.FREE;
            slot.ownerEpoch = 0;
            slot.ticketValue = 0;
            slot.position = null;
            slot.locator = null;
            slot.reservation = null;
        }

        private static BridgeException invalidSlotState(String message) {
            return rejected(BridgeRejectionCode.COMPLETION_SLOT_STATE_INVALID, message);
        }
    }

    static final class PrePositionReservation {
        private final long ownerEpoch;
        private final int slotIndex;

        private PrePositionReservation(long ownerEpoch, int slotIndex) {
            this.ownerEpoch = ownerEpoch;
            this.slotIndex = slotIndex;
        }

        long ownerEpoch() {
            return ownerEpoch;
        }

        int slotIndex() {
            return slotIndex;
        }
    }

    static record CompletionTicket(long ownerEpoch, long value, int slotIndex) {
        @Override
        public String toString() {
            return "CompletionTicket[ownerEpoch=" + ownerEpoch + ", unsignedValue=" + Long.toUnsignedString(value)
                    + ", slotIndex=" + slotIndex + "]";
        }
    }

    private static final class CompletionSlot {
        private CompletionSlotState state = CompletionSlotState.FREE;
        private long ownerEpoch;
        private long ticketValue;
        private PulsarPosition position;
        private ExtentLocator locator;
        private PrePositionReservation reservation;
    }

    private enum CompletionSlotState {
        FREE,
        RESERVED,
        ALLOCATED,
        UNKNOWN,
        LOCATOR_INSTALLED
    }

    private static final class LedgerState {
        private final long virtualLedgerId;
        private long manifestThrough;
        private long durableThrough;
        private long readableThrough;
        private long manifestGeneration;
        private ManifestSource manifestSource;
        private final NavigableMap<Long, ExtentLocator> activeTail;
        private final Map<Long, Integer> activeReadPins = new HashMap<>();

        private LedgerState(
                long virtualLedgerId,
                long manifestThrough,
                long durableThrough,
                long readableThrough,
                long manifestGeneration,
                ManifestSource manifestSource,
                NavigableMap<Long, ExtentLocator> activeTail) {
            this.virtualLedgerId = virtualLedgerId;
            this.manifestThrough = manifestThrough;
            this.durableThrough = durableThrough;
            this.readableThrough = readableThrough;
            this.manifestGeneration = manifestGeneration;
            this.manifestSource = manifestSource;
            this.activeTail = activeTail;
        }

        void requireManifestHandoff(ManifestHandoffRequest request) {
            if (request.virtualLedgerId() != virtualLedgerId
                    || request.manifestGeneration() <= manifestGeneration
                    || request.throughEntryId() < manifestThrough
                    || request.throughEntryId() > readableThrough) {
                throw rejected(
                        BridgeRejectionCode.MANIFEST_HANDOFF_INVALID,
                        "manifest generation/range must advance within the readable frontier");
            }
        }

        void pinActiveRead(long entryId) {
            activeReadPins.merge(entryId, 1, Math::addExact);
        }

        void unpinActiveRead(long entryId) {
            Integer pins = activeReadPins.get(entryId);
            if (pins == null || pins <= 0) {
                throw new IllegalStateException("active read pin underflow");
            }
            if (pins == 1) {
                activeReadPins.remove(entryId);
            } else {
                activeReadPins.put(entryId, pins - 1);
            }
        }

        void releaseCoveredUnpinnedLocators() {
            activeTail
                    .headMap(manifestThrough, true)
                    .entrySet()
                    .removeIf(entry -> !activeReadPins.containsKey(entry.getKey()));
        }

        LedgerFrontiers frontiers(PulsarBindingKey binding) {
            return new LedgerFrontiers(
                    binding, virtualLedgerId, manifestThrough, readableThrough, durableThrough, activeTail.size());
        }
    }

    private static final class PhysicalLaneState {
        private long resolvedThrough;

        private PhysicalLaneState(long resolvedThrough) {
            this.resolvedThrough = resolvedThrough;
        }
    }

    private record PendingSuccessorRollover(
            FailedPlan predecessorFailure, OpenedLedger successor, PlannedEntry successorMember) {
        private PendingSuccessorRollover {
            Objects.requireNonNull(predecessorFailure, "predecessorFailure");
            Objects.requireNonNull(successor, "successor");
            Objects.requireNonNull(successorMember, "successorMember");
            if (predecessorFailure.reason() != ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED
                    || predecessorFailure.unresolvedMembers().size() != 1
                    || !successor.node().binding().equals(successorMember.binding())
                    || successor.node().virtualLedgerId()
                            != successorMember.position().virtualLedgerId()
                    || successorMember.position().entryId() != 0) {
                throw new IllegalArgumentException("pending successor rollover differs from exact no-gap entry zero");
            }
        }
    }

    private record SealedSuccessorRollover(PendingSuccessorRollover pending, SealedExtentPlan successorPlan) {
        private SealedSuccessorRollover {
            Objects.requireNonNull(pending, "pending");
            Objects.requireNonNull(successorPlan, "successorPlan");
            if (!successorPlan.members().equals(List.of(pending.successorMember()))) {
                throw new IllegalArgumentException("sealed successor rollover changed the exact entry-zero member");
            }
        }
    }

    private record FailedPlan(
            SealedExtentPlan plan,
            ProviderObjectOutcome reason,
            Optional<ExtentIdentity> candidateIdentity,
            List<PlannedEntry> unresolvedMembers,
            boolean stopsWalRun) {
        private FailedPlan {
            Objects.requireNonNull(plan, "plan");
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(candidateIdentity, "candidateIdentity");
            unresolvedMembers = List.copyOf(Objects.requireNonNull(unresolvedMembers, "unresolvedMembers"));
            if (unresolvedMembers.isEmpty() || !plan.members().containsAll(unresolvedMembers)) {
                throw new IllegalArgumentException("failed plan must retain a non-empty subset of exact members");
            }
        }
    }

    private record RunLane(Sha256Digest walRunRootSha256, WalLaneId laneId) {}

    private record PreparedPhysicalResolution(RunLane key, PhysicalLaneState state) {}

    private record MemberKey(PulsarBindingKey binding, PulsarPosition position) {}

    /** Only the production store can bind an exceptional completion to its exact already-created candidate. */
    private static final class RetainedCandidateFailure extends RuntimeException {
        private final ExtentIdentity identity;

        private RetainedCandidateFailure(ExtentIdentity identity, Throwable cause) {
            super("exact Provider candidate retained after exceptional completion", Objects.requireNonNull(cause));
            this.identity = Objects.requireNonNull(identity, "identity");
        }

        private ExtentIdentity identity() {
            return identity;
        }
    }

    private record ValidatedAppend(AppendInput input, BindingState state, long entryId) {}

    private record ReservedAppend(ValidatedAppend validated, PrePositionReservation reservation) {
        BindingState state() {
            return validated.state();
        }
    }

    private record AllocatedAppend(
            BindingState state, PrePositionReservation reservation, CompletionTicket ticket, PlannedEntry member) {}

    private static String requireText(String value, String name, int maximumLength) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.length() > maximumLength || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must be non-blank bounded text without NUL");
        }
        return value;
    }
}
