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

package com.nereusstream.kafka.bookkeeper.object.publication;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.storage.object.control.ProviderResolvedExtentDescriptor;
import com.nereusstream.storage.object.control.ProviderResolvedExtentRowV1;
import com.nereusstream.storage.object.control.ProviderVersionProof;
import com.nereusstream.storage.object.control.WalCheckpointPublisher;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunSealRecord;
import com.nereusstream.storage.object.control.WalRunTerminalClosureProofV1;
import com.nereusstream.storage.object.nwg1.GroupEncodingPlanV1;
import com.nereusstream.storage.object.nwg1.Nwg1DirectoryV1;
import com.nereusstream.storage.object.nwg1.Nwg1HeaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1IsolationScopeV1;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1RejectionV1;
import com.nereusstream.storage.object.nwg1.Nwg1ValidationException;
import com.nereusstream.storage.object.nwg1.Nwg1ValidationStageV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.provider.ProviderObjectResult;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Production Object-WAL path from an immutable NWG1 plan through Provider resolution and strict readback. */
public final class KafkaNwg1ObjectPipelineV1 {
    public record SharedMember(
            KafkaObjectCompletionTrackerV1.AssignedTicket ticket,
            KafkaSpeculativeCommitV1 commit,
            KafkaObjectNativeStateV1 nativeState,
            KafkaObjectCompletionTrackerV1 completionTracker) {
        public SharedMember {
            Objects.requireNonNull(ticket, "ticket");
            Objects.requireNonNull(commit, "commit");
            Objects.requireNonNull(nativeState, "nativeState");
            Objects.requireNonNull(completionTracker, "completionTracker");
        }
    }

    public SharedMember member(
            KafkaObjectCompletionTrackerV1.AssignedTicket ticket,
            KafkaSpeculativeCommitV1 commit,
            KafkaObjectNativeStateV1 nativeState) {
        return new SharedMember(ticket, commit, nativeState, tracker);
    }

    public record VerifiedMember(
            KafkaObjectCompletionTrackerV1.AssignedTicket ticket, KafkaVerifiedNwg1CommitV1 verifiedCommit) {
        public VerifiedMember {
            Objects.requireNonNull(ticket, "ticket");
            Objects.requireNonNull(verifiedCommit, "verifiedCommit");
        }
    }

    public record IsolatedMemberFailure(
            KafkaObjectCompletionTrackerV1.AssignedTicket ticket,
            Nwg1RejectionV1 rejection,
            Nwg1ValidationStageV1 stage,
            Nwg1IsolationScopeV1 scope) {
        public IsolatedMemberFailure {
            Objects.requireNonNull(ticket, "ticket");
            Objects.requireNonNull(rejection, "rejection");
            Objects.requireNonNull(stage, "stage");
            Objects.requireNonNull(scope, "scope");
            if (scope != Nwg1IsolationScopeV1.BINDING && scope != Nwg1IsolationScopeV1.APPEND_UNIT) {
                throw new IllegalArgumentException("only binding-local NWG1 failures may be isolated");
            }
        }
    }

    public record AuthorizationMemberFailure(KafkaObjectCompletionTrackerV1.AssignedTicket ticket, String reason) {}

    public record SharedWriteResult(
            ObjectIdentity identity,
            List<VerifiedMember> verifiedMembers,
            List<IsolatedMemberFailure> isolatedFailures,
            List<AuthorizationMemberFailure> authorizationFailures) {
        public SharedWriteResult {
            Objects.requireNonNull(identity, "identity");
            verifiedMembers = List.copyOf(verifiedMembers);
            isolatedFailures = List.copyOf(isolatedFailures);
            authorizationFailures = List.copyOf(authorizationFailures);
            if (verifiedMembers.isEmpty() && isolatedFailures.isEmpty() && authorizationFailures.isEmpty()) {
                throw new IllegalArgumentException("shared result contains no members");
            }
        }
    }

    private final Sha256Digest rootSha;
    private final WalRunRootRecord root;
    private final WalRunObjectSession objectSession;
    private final Nwg1VerificationContextV1 verificationContext;
    private final KafkaObjectPhysicalFrontiersV1 physicalFrontiers;
    private final KafkaObjectCompletionTrackerV1 tracker;
    private final WalCheckpointPublisher checkpointPublisher;
    private final KafkaObjectAuthorizationV1 authorization;
    private final java.util.Set<KafkaObjectCompletionTrackerV1> memberTrackers = new java.util.HashSet<>();
    private final Map<KafkaObjectCompletionTrackerV1.AssignedTicket, PendingWrite> pending = new LinkedHashMap<>();
    private final Map<Sha256Digest, PendingSharedWrite> pendingShared = new LinkedHashMap<>();

    @SuppressWarnings("ParameterNumber")
    public KafkaNwg1ObjectPipelineV1(
            WalRunRootRecord root,
            WalRunObjectSession objectSession,
            Nwg1VerificationContextV1 verificationContext,
            KafkaObjectPhysicalFrontiersV1 physicalFrontiers,
            KafkaObjectCompletionTrackerV1 tracker,
            WalCheckpointPublisher checkpointPublisher,
            KafkaObjectAuthorizationV1 authorization) {
        this.root = Objects.requireNonNull(root, "root");
        this.objectSession = Objects.requireNonNull(objectSession, "objectSession");
        this.rootSha = objectSession.rootSha256();
        this.verificationContext = Objects.requireNonNull(verificationContext, "verificationContext");
        this.physicalFrontiers = Objects.requireNonNull(physicalFrontiers, "physicalFrontiers");
        this.tracker = Objects.requireNonNull(tracker, "tracker");
        this.checkpointPublisher = Objects.requireNonNull(checkpointPublisher, "checkpointPublisher");
        this.authorization = Objects.requireNonNull(authorization, "authorization");
        memberTrackers.add(tracker);
        tracker.bindCheckpointPublisher(checkpointPublisher);
        if (rootSha.isZero()
                || !com.nereusstream.storage.object.control.WalRunControlCodec.rootSha256(root)
                        .equals(rootSha)
                || !Arrays.equals(rootSha.bytes().toByteArray(), verificationContext.walRunRootSha256())
                || !rootSha.equals(physicalFrontiers.walRunRootSha())
                || !root.providerScopeId()
                        .digest()
                        .equals(Sha256Digest.copyOf(verificationContext.cellProviderScopeId()))
                || !checkpointPublisher.head().rootSha256().equals(rootSha)
                || checkpointPublisher.head().shardRunEpoch() != root.shardRunEpoch()
                || !checkpointPublisher.head().coveredThrough().equals(physicalFrontiers.snapshotVector())) {
            throw new IllegalArgumentException("Kafka NWG1 pipeline authorities differ from the exact WalRun Root");
        }
    }

    public synchronized void bindTracker(KafkaObjectCompletionTrackerV1 memberTracker) {
        memberTracker.bindCheckpointPublisher(checkpointPublisher);
        memberTrackers.add(memberTracker);
    }

    public WalCheckpointPublisher.Reservation reservePhysicalBeforePosition(long maximumBodyBytes) {
        return checkpointPublisher.reserveBeforePosition(maximumBodyBytes);
    }

    /** Performs every effect in order and installs only a fully authenticated complete commit-set locator. */
    public KafkaVerifiedNwg1CommitV1 writeResolveAndInstall(
            GroupEncodingPlanV1 plan,
            KafkaObjectCompletionTrackerV1.AssignedTicket ticket,
            KafkaSpeculativeCommitV1 commit,
            KafkaObjectNativeStateV1 nativeState,
            long nowMillis) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(ticket, "ticket");
        Objects.requireNonNull(commit, "commit");
        Objects.requireNonNull(nativeState, "nativeState");
        requirePlanAuthority(plan);
        requireSinglePlanMember(plan, ticket, commit);
        tracker.requireAuthorization(authorization, commit.expectedFence());
        PendingWrite write;
        synchronized (this) {
            write = pending.get(ticket);
            if (write == null) {
                WalRunObjectSession.ValidatedNwg1Plan validated =
                        objectSession.validateNwg1Plan(plan, verificationContext);
                var physicalReservation = checkpointPublisher.bindPlan(
                        List.of(tracker.checkpointAttachment(ticket, checkpointPublisher)),
                        plan.canonicalPlanSha256(),
                        validated.canonicalBodyBytes());
                tracker.requireCandidateBodyCharge(validated.canonicalBodyBytes());
                KafkaObjectCompletionTrackerV1.SequenceClaim claim = tracker.claimSequence(ticket);
                write = new PendingWrite(plan.canonicalPlanSha256(), commit, nativeState, validated, claim);
                write.physicalReservation = physicalReservation;
                pending.put(ticket, write);
            } else {
                write.requireRetry(plan, commit, nativeState);
            }
            if (write.inFlight) {
                throw new IllegalStateException("exact NWG1 pipeline candidate already has in-flight work");
            }
            write.inFlight = true;
        }
        try {
            if (write.candidate == null) {
                try {
                    write.candidate = objectSession.admitAndSealNwg1(write.validatedPlan, nowMillis);
                    checkpointPublisher.sequenceStarted(write.physicalReservation);
                    if (write.sequenceClaim != null) {
                        tracker.completeSequenceAfterEffect(write.sequenceClaim);
                        write.sequenceClaim = null;
                    }
                } catch (WalRunObjectSession.Nwg1AdmissionFailure failure) {
                    if (failure.sequenceEffect().isPresent() && write.sequenceClaim != null) {
                        checkpointPublisher.sequenceStarted(write.physicalReservation);
                        tracker.completeSequenceAfterEffect(write.sequenceClaim);
                        write.sequenceClaim = null;
                    } else if (failure.sequenceEffect().isEmpty() && write.sequenceClaim != null) {
                        tracker.abortSequenceBeforeEffect(write.sequenceClaim);
                        removePending(ticket);
                    } else if (failure.sequenceEffect().isEmpty()) {
                        throw new IllegalStateException(
                                "retried NWG1 admission lost its retained sequence effect", failure);
                    }
                    throw failure;
                }
            }
            WalRunObjectSession.AdmittedNwg1Candidate candidate = write.candidate;
            if (!write.dispatched) {
                tracker.providerDispatched(ticket);
                write.dispatched = true;
            }
            ObjectIdentity identity = candidate.identity();
            try {
                if (!write.providerExact) {
                    resolveSingleProviderCandidate(write, candidate, ticket);
                    write.providerExact = true;
                }
                if (write.publicationExtent == null) {
                    write.publicationExtent =
                            objectSession.readAndAuthenticateNwg1ForPublication(candidate, verificationContext);
                }
                var publicationExtent = write.publicationExtent;
                if (write.verified == null) {
                    long selectedFrameOrdinal = plan.appendUnits().get(0).firstFrameOrdinal();
                    var verifiedUnit = objectSession.verifySelectedNwg1AppendUnitForPublication(
                            publicationExtent,
                            verificationContext,
                            selectedFrameOrdinal,
                            (frame, payload) -> verifyAssignedPayload(commit, frame, payload));
                    write.verified = verifyCommit(
                            publicationExtent.authenticatedPrefix(), verifiedUnit, commit, ticket, identity);
                }
                var verified = write.verified;
                if (!write.physicalResolved) {
                    physicalFrontiers.requireNext(verified.locator().extent());
                    ProviderResolvedExtentDescriptor descriptor = descriptor(
                            publicationExtent.authenticatedPrefix().header(),
                            identity,
                            publicationExtent.providerProof(),
                            nowMillis);
                    objectSession.providerResolved(candidate);
                    physicalFrontiers.resolve(verified.locator().extent());
                    checkpointPublisher.enqueue(write.physicalReservation, descriptor);
                    write.physicalResolved = true;
                }
                try {
                    verified.authorize(authorization, identity, commit);
                } catch (KafkaObjectAuthorizationV1.Fenced fenced) {
                    tracker.bindingRejectedAfterPhysicalResolution(
                            ticket, physicalFrontiers, verified.locator().extent());
                    removePending(ticket);
                    throw fenced;
                }
                tracker.providerResolved(ticket, physicalFrontiers, verified, nativeState);
                removePending(ticket);
                return verified;
            } catch (IOException failure) {
                throw new IllegalStateException("Kafka NWG1 Provider pipeline failed", failure);
            }
        } finally {
            synchronized (this) {
                write.inFlight = false;
                notifyAll();
            }
        }
    }

    private ProviderObjectResult resolveSingleProviderCandidate(
            PendingWrite write,
            WalRunObjectSession.AdmittedNwg1Candidate candidate,
            KafkaObjectCompletionTrackerV1.AssignedTicket ticket)
            throws IOException {
        ObjectIdentity identity = candidate.identity();
        ProviderObjectResult result;
        if (write.requiresReconciliation) {
            result = objectSession.reconcileUnknownNwg1Candidate(candidate);
        } else {
            try {
                result = objectSession.conditionalCreateNwg1(candidate);
            } catch (IOException failure) {
                write.requiresReconciliation = true;
                try {
                    result = objectSession.reconcileUnknownNwg1Candidate(candidate);
                } catch (IOException | RuntimeException reconciliationFailure) {
                    reconciliationFailure.addSuppressed(failure);
                    throw reconciliationFailure;
                }
            }
        }
        if (result.outcome() == ProviderObjectOutcome.OUTCOME_UNKNOWN) {
            write.requiresReconciliation = true;
            result = objectSession.reconcileUnknownNwg1Candidate(candidate);
        }
        if (result.outcome() == ProviderObjectOutcome.DEFINITIVE_CONFLICT) {
            objectSession.providerConflict(candidate);
            checkpointPublisher.definitiveNoObject(write.physicalReservation);
            removePending(ticket);
            throw new IllegalStateException("Provider returned a definitive NWG1 identity conflict");
        }
        if (result.outcome() == ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED) {
            objectSession.providerAbsent(candidate);
            checkpointPublisher.definitiveNoObject(write.physicalReservation);
            removePending(ticket);
            throw new IllegalStateException("Provider proved the allocated NWG1 identity absent");
        }
        if (result.outcome() != ProviderObjectOutcome.APPLIED_EXACT
                && result.outcome() != ProviderObjectOutcome.EXISTING_EXACT) {
            throw new IllegalStateException("Provider NWG1 outcome remained unresolved");
        }
        write.requiresReconciliation = false;
        return result;
    }

    /**
     * Resolves one physical shared Object exactly once, then range-verifies every selected Kafka append unit. Typed
     * binding/unit failures remain attached to their owning tickets while verified siblings can reach protocol ACK.
     */
    public SharedWriteResult writeResolveAndInstallShared(
            GroupEncodingPlanV1 plan, List<SharedMember> members, long nowMillis) {
        Objects.requireNonNull(plan, "plan");
        members = List.copyOf(members);
        if (members.isEmpty()) {
            throw new IllegalArgumentException("Kafka NWG1 member path requires at least one member");
        }
        requirePlanAuthority(plan);
        requirePlanMembers(plan, members);
        for (SharedMember member : members) {
            member.completionTracker()
                    .requireAuthorization(authorization, member.commit().expectedFence());
        }
        Sha256Digest planSha = plan.canonicalPlanSha256();
        PendingSharedWrite write;
        synchronized (this) {
            write = pendingShared.get(planSha);
            if (write == null) {
                WalRunObjectSession.ValidatedNwg1Plan validated =
                        objectSession.validateNwg1Plan(plan, verificationContext);
                var physicalReservation = checkpointPublisher.bindPlan(
                        members.stream()
                                .map(member -> member.completionTracker()
                                        .checkpointAttachment(member.ticket(), checkpointPublisher))
                                .toList(),
                        planSha,
                        validated.canonicalBodyBytes());
                for (SharedMember member : members) {
                    member.completionTracker().requireCandidateBodyCharge(validated.canonicalBodyBytes());
                }
                List<KafkaObjectCompletionTrackerV1.SequenceClaim> claims = claimShared(members);
                write = new PendingSharedWrite(planSha, members, validated, claims);
                write.physicalReservation = physicalReservation;
                pendingShared.put(planSha, write);
            } else {
                write.requireRetry(plan, members);
            }
            if (write.inFlight) {
                throw new IllegalStateException("exact NWG1 pipeline candidate already has in-flight work");
            }
            write.inFlight = true;
        }
        try {
            if (write.candidate == null) {
                try {
                    write.candidate = objectSession.admitAndSealNwg1(write.validatedPlan, nowMillis);
                    checkpointPublisher.sequenceStarted(write.physicalReservation);
                    if (!write.sequenceClaims.isEmpty()) {
                        finishSharedClaims(write, true);
                        write.sequenceClaims = List.of();
                    }
                } catch (WalRunObjectSession.Nwg1AdmissionFailure failure) {
                    if (failure.sequenceEffect().isPresent() && !write.sequenceClaims.isEmpty()) {
                        checkpointPublisher.sequenceStarted(write.physicalReservation);
                        finishSharedClaims(write, true);
                        write.sequenceClaims = List.of();
                    } else if (failure.sequenceEffect().isEmpty() && !write.sequenceClaims.isEmpty()) {
                        finishSharedClaims(write, false);
                        removeShared(planSha);
                    } else if (failure.sequenceEffect().isEmpty()) {
                        throw new IllegalStateException(
                                "retried shared NWG1 admission lost its retained sequence effect", failure);
                    }
                    throw failure;
                }
            }
            if (!write.dispatched) {
                for (SharedMember member : members) {
                    member.completionTracker().providerDispatched(member.ticket());
                }
                write.dispatched = true;
            }
            try {
                authenticateSharedPublication(write);
                for (int index = 0; index < members.size(); index++) {
                    if (write.verifiedMembers.containsKey(index)
                            || write.isolatedFailures.containsKey(index)
                            || write.authorizationFailures.containsKey(index)) {
                        continue;
                    }
                    SharedMember member = members.get(index);
                    long selectedFrame = plan.appendUnits().get(index).firstFrameOrdinal();
                    try {
                        Nwg1ObjectReaderV1.VerifiedAppendUnit selected =
                                objectSession.verifySelectedNwg1AppendUnitForPublication(
                                        write.publicationExtent,
                                        verificationContext,
                                        selectedFrame,
                                        (frame, payload) -> verifyAssignedPayload(member.commit(), frame, payload));
                        KafkaVerifiedNwg1CommitV1 verified = verifyCommit(
                                write.publicationExtent.authenticatedPrefix(),
                                selected,
                                member.commit(),
                                member.ticket(),
                                write.identity);
                        write.verifiedMembers.put(index, new VerifiedMember(member.ticket(), verified));
                    } catch (Nwg1ValidationException failure) {
                        if (failure.scope() != Nwg1IsolationScopeV1.BINDING
                                && failure.scope() != Nwg1IsolationScopeV1.APPEND_UNIT) {
                            throw failure;
                        }
                        write.isolatedFailures.put(
                                index,
                                new IsolatedMemberFailure(
                                        member.ticket(), failure.rejection(), failure.stage(), failure.scope()));
                    }
                }
                for (int index = 0; index < members.size(); index++) {
                    VerifiedMember verified = write.verifiedMembers.get(index);
                    if (verified != null) {
                        try {
                            verified.verifiedCommit()
                                    .authorize(
                                            authorization,
                                            write.identity,
                                            members.get(index).commit());
                        } catch (KafkaObjectAuthorizationV1.Fenced fenced) {
                            write.verifiedMembers.remove(index);
                            write.authorizationFailures.put(
                                    index,
                                    new AuthorizationMemberFailure(
                                            members.get(index).ticket(), fenced.getMessage()));
                        }
                    }
                }
                if (write.verifiedMembers.size() + write.isolatedFailures.size() + write.authorizationFailures.size()
                        != members.size()) {
                    throw new IllegalStateException("shared Kafka NWG1 result did not classify every member");
                }
                finalizeSharedPhysical(write, nowMillis);
                SharedWriteResult result = write.result();
                removeShared(planSha);
                return result;
            } catch (IOException failure) {
                throw new IllegalStateException("Kafka shared NWG1 Provider pipeline failed", failure);
            }
        } finally {
            synchronized (this) {
                write.inFlight = false;
                notifyAll();
            }
        }
    }

    private List<KafkaObjectCompletionTrackerV1.SequenceClaim> claimShared(List<SharedMember> members) {
        var claims = new ArrayList<KafkaObjectCompletionTrackerV1.SequenceClaim>();
        try {
            for (SharedMember member : members) {
                memberTrackers.add(member.completionTracker());
                claims.add(member.completionTracker().claimSequence(member.ticket()));
            }
        } catch (RuntimeException failure) {
            for (int index = 0; index < claims.size(); index++) {
                members.get(index).completionTracker().abortSequenceBeforeEffect(claims.get(index));
            }
            throw failure;
        }
        return List.copyOf(claims);
    }

    private static void finishSharedClaims(PendingSharedWrite write, boolean effected) {
        for (int index = 0; index < write.sequenceClaims.size(); index++) {
            var tracker = write.members.get(index).completionTracker();
            if (effected) {
                tracker.completeSequenceAfterEffect(write.sequenceClaims.get(index));
            } else {
                tracker.abortSequenceBeforeEffect(write.sequenceClaims.get(index));
            }
        }
    }

    private void authenticateSharedPublication(PendingSharedWrite write) throws IOException {
        WalRunObjectSession.AdmittedNwg1Candidate candidate = write.candidate;
        if (!write.providerExact) {
            resolveProviderCandidate(write, candidate);
            write.providerExact = true;
        }
        if (write.publicationExtent == null) {
            write.publicationExtent =
                    objectSession.readAndAuthenticateNwg1ForPublication(candidate, verificationContext);
            write.identity = write.publicationExtent.identity();
            write.extent = extent(write.publicationExtent.authenticatedPrefix().header(), write.identity);
        }
    }

    private void finalizeSharedPhysical(PendingSharedWrite write, long nowMillis) {
        if (write.physicalResolved) {
            return;
        }
        for (int index = 0; index < write.members.size(); index++) {
            SharedMember member = write.members.get(index);
            VerifiedMember verified = write.verifiedMembers.get(index);
            if (verified != null) {
                member.completionTracker()
                        .requireProviderResolvedCandidate(
                                member.ticket(), verified.verifiedCommit(), member.nativeState());
            } else {
                member.completionTracker().requireBindingRejectionCandidate(member.ticket());
            }
        }
        physicalFrontiers.requireNext(write.extent);
        ProviderResolvedExtentDescriptor descriptor = descriptor(
                write.publicationExtent.authenticatedPrefix().header(),
                write.identity,
                write.publicationExtent.providerProof(),
                nowMillis);
        WalRunObjectSession.AdmittedNwg1Candidate candidate = write.candidate;
        objectSession.providerResolved(candidate);
        physicalFrontiers.resolve(write.extent);
        checkpointPublisher.enqueue(write.physicalReservation, descriptor);
        for (int index = 0; index < write.members.size(); index++) {
            SharedMember member = write.members.get(index);
            VerifiedMember verified = write.verifiedMembers.get(index);
            if (verified != null) {
                member.completionTracker()
                        .providerResolved(
                                member.ticket(), physicalFrontiers, verified.verifiedCommit(), member.nativeState());
            } else {
                member.completionTracker()
                        .bindingRejectedAfterPhysicalResolution(member.ticket(), physicalFrontiers, write.extent);
            }
        }
        write.physicalResolved = true;
    }

    private ProviderObjectResult resolveProviderCandidate(
            PendingSharedWrite write, WalRunObjectSession.AdmittedNwg1Candidate candidate) throws IOException {
        ObjectIdentity identity = candidate.identity();
        ProviderObjectResult result;
        if (write.requiresReconciliation) {
            result = objectSession.reconcileUnknownNwg1Candidate(candidate);
        } else {
            try {
                result = objectSession.conditionalCreateNwg1(candidate);
            } catch (IOException failure) {
                write.requiresReconciliation = true;
                try {
                    result = objectSession.reconcileUnknownNwg1Candidate(candidate);
                } catch (IOException | RuntimeException reconciliationFailure) {
                    reconciliationFailure.addSuppressed(failure);
                    throw reconciliationFailure;
                }
            }
        }
        if (result.outcome() == ProviderObjectOutcome.OUTCOME_UNKNOWN) {
            write.requiresReconciliation = true;
            result = objectSession.reconcileUnknownNwg1Candidate(candidate);
        }
        if (result.outcome() == ProviderObjectOutcome.DEFINITIVE_CONFLICT) {
            objectSession.providerConflict(candidate);
            checkpointPublisher.definitiveNoObject(write.physicalReservation);
            removeShared(write.planSha);
            throw new IllegalStateException("Provider returned a definitive shared NWG1 identity conflict");
        }
        if (result.outcome() == ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED) {
            objectSession.providerAbsent(candidate);
            checkpointPublisher.definitiveNoObject(write.physicalReservation);
            removeShared(write.planSha);
            throw new IllegalStateException("Provider proved the shared NWG1 identity absent");
        }
        if (result.outcome() != ProviderObjectOutcome.APPLIED_EXACT
                && result.outcome() != ProviderObjectOutcome.EXISTING_EXACT) {
            throw new IllegalStateException("Provider shared NWG1 outcome remained unresolved");
        }
        write.requiresReconciliation = false;
        return result;
    }

    /** Flushes the same Root-bound physical publisher used by normal resolution before constructing the Seal. */
    public void flushCheckpointForSeal() {
        checkpointPublisher.flush();
        checkpointPublisher.requireFinalCoverage(physicalFrontiers.snapshotVector());
    }

    /** Final local barrier before the caller builds and publishes the physical Seal through the same session. */
    public void prepareTerminalClosure() {
        synchronized (this) {
            if (!pending.isEmpty()
                    || !pendingShared.isEmpty()
                    || memberTrackers.stream().anyMatch(t -> t.pendingUnits() != 0)) {
                throw new IllegalStateException(
                        "unresolved Kafka Object candidates or M2 completion tickets forbid terminal closure");
            }
        }
        if (objectSession.runtimeState() != com.nereusstream.storage.object.control.WalRunRuntime.State.SEALED) {
            throw new IllegalStateException("Kafka Object runtime must be stopped and sealed before closure");
        }
        flushCheckpointForSeal();
        checkpointPublisher.closeAfterDrain();
        objectSession.drain();
        objectSession.requireTerminalClosable();
    }

    /** Publishes the exact physical Seal derived only from this pipeline's Root-bound publisher and Object session. */
    public WalRunTerminalClosureProofV1 publishPhysicalSeal(WalRunLifecycleManager lifecycle) {
        Objects.requireNonNull(lifecycle, "lifecycle");
        prepareTerminalClosure();
        var head = checkpointPublisher.head();
        var runtime = objectSession.runtimeRecoveryState();
        if (!head.coveredThrough().equals(physicalFrontiers.snapshotVector())) {
            throw new IllegalStateException("Kafka physical Head differs from the exact sealed lane vector");
        }
        String headKey = WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch());
        WalRunReference reference = new WalRunReference(
                WalRunControlKeys.rootKey(root.shardId(), root.shardRunEpoch()),
                rootSha,
                root.shardId(),
                root.shardRunEpoch());
        WalRunSealRecord seal = new WalRunSealRecord(
                reference,
                head.coveredThrough(),
                headKey,
                Sha256Digest.hash(WalRunControlCodec.encodeCheckpointHead(head)),
                runtime.resolvedExtentCount(),
                runtime.resolvedCanonicalBodyBytes());
        return lifecycle.publishSeal(
                WalRunControlKeys.sealKey(root.shardId(), root.shardRunEpoch()), seal, objectSession);
    }

    private ProviderResolvedExtentDescriptor descriptor(
            Nwg1HeaderV1 header, ObjectIdentity identity, ProviderVersionProof providerProof, long nowMillis) {
        Objects.requireNonNull(providerProof, "providerProof");
        return new ProviderResolvedExtentDescriptor(
                rootSha,
                new ProviderResolvedExtentRowV1(
                        com.nereusstream.storage.object.control.WalLaneId.fromCode(header.laneId()),
                        header.laneSequence(),
                        Math.toIntExact(header.directoryPrefixEnd()),
                        header.canonicalBodyLength(),
                        identity.bodySha256(),
                        providerProof),
                nowMillis);
    }

    private synchronized void removePending(KafkaObjectCompletionTrackerV1.AssignedTicket ticket) {
        pending.remove(ticket);
    }

    private synchronized void removeShared(Sha256Digest planSha) {
        pendingShared.remove(planSha);
    }

    private void requirePlanAuthority(GroupEncodingPlanV1 plan) {
        if (plan.protocolKind() != 1
                || plan.shardId() != root.shardId()
                || plan.shardRunEpoch() != root.shardRunEpoch()
                || plan.packingPolicyVersion() != root.formatContract().packingPolicyCatalogVersion()
                || !Arrays.equals(plan.rootSha256(), rootSha.bytes().toByteArray())
                || !Arrays.equals(plan.providerScopeId(), verificationContext.cellProviderScopeId())) {
            throw new IllegalArgumentException("NWG1 plan substituted protocol/Root/ProviderScope authority");
        }
    }

    private void requireSinglePlanMember(
            GroupEncodingPlanV1 plan,
            KafkaObjectCompletionTrackerV1.AssignedTicket ticket,
            KafkaSpeculativeCommitV1 commit) {
        if (plan.appendUnits().size() != 1) {
            throw new IllegalArgumentException("single-commit Kafka NWG1 path cannot admit a shared extent");
        }
        requirePlanMember(plan, 0, ticket, commit);
    }

    private void requirePlanMembers(GroupEncodingPlanV1 plan, List<SharedMember> members) {
        if (plan.appendUnits().size() != members.size()) {
            throw new IllegalArgumentException("shared Kafka NWG1 plan/member inventory differs");
        }
        Map<KafkaObjectCompletionTrackerV1, HashSet<KafkaObjectCompletionTrackerV1.AssignedTicket>> tickets =
                new java.util.IdentityHashMap<>();
        for (int index = 0; index < members.size(); index++) {
            SharedMember member = members.get(index);
            if (!tickets.computeIfAbsent(member.completionTracker(), ignored -> new HashSet<>())
                    .add(member.ticket())) {
                throw new IllegalArgumentException("shared Kafka NWG1 members repeat a completion ticket");
            }
            requirePlanMember(plan, index, member.ticket(), member.commit());
        }
    }

    private void requirePlanMember(
            GroupEncodingPlanV1 plan,
            int unitOrdinal,
            KafkaObjectCompletionTrackerV1.AssignedTicket ticket,
            KafkaSpeculativeCommitV1 commit) {
        Nwg1DirectoryV1.KafkaAppendUnit unit =
                (Nwg1DirectoryV1.KafkaAppendUnit) plan.appendUnits().get(unitOrdinal);
        KafkaObjectBindingKeyV1 binding = binding(commit);
        if (unit.contextOrdinal() >= plan.bindings().size()) {
            throw new IllegalArgumentException("Kafka NWG1 unit refers outside the plan Binding inventory");
        }
        Nwg1DirectoryV1.BindingContext context = plan.bindings().get(Math.toIntExact(unit.contextOrdinal()));
        byte[] expectedNti = TopicIncarnationIdentityCodecV1.encode(
                        commit.expectedFence().topicIncarnation())
                .toByteArray();
        if (!Arrays.equals(
                        context.bindingId(),
                        binding.bindingId().digest().bytes().toByteArray())
                || !Arrays.equals(
                        context.storageEpochId(),
                        binding.storageEpochId().digest().bytes().toByteArray())
                || !Arrays.equals(context.nti1Bytes(), expectedNti)
                || unit.partitionId() != binding.partitionId()
                || unit.kafkaLeaderEpoch() != commit.expectedFence().kafkaLeaderEpoch()
                || unit.startOffset() != commit.startOffset()
                || unit.endOffsetExclusive() != commit.endOffsetExclusive()
                || !Arrays.equals(unit.appendCommitSetId(), commitSetId(commit))
                || !Arrays.equals(unit.storageAttemptId(), storageAttemptId(ticket))) {
            throw new IllegalArgumentException("Kafka NWG1 plan differs from its exact commit/ticket authority");
        }
        long next = commit.startOffset();
        for (long ordinal = unit.firstFrameOrdinal();
                ordinal < unit.firstFrameOrdinal() + unit.frameCount();
                ordinal++) {
            GroupEncodingPlanV1.PlannedFrame frame = plan.frames().get(Math.toIntExact(ordinal));
            if (frame.appendUnitOrdinal() != unitOrdinal || frame.coverage0() != next) {
                throw new IllegalArgumentException("Kafka NWG1 planned frame coverage is not complete and contiguous");
            }
            next = frame.coverage1();
        }
        if (next != commit.endOffsetExclusive()) {
            throw new IllegalArgumentException("Kafka NWG1 planned frames differ from complete commit-set coverage");
        }
    }

    private static void verifyAssignedPayload(
            KafkaSpeculativeCommitV1 commit, Nwg1ObjectReaderV1.VerifiedFrame frame, java.nio.ByteBuffer payload) {
        int ordinal = Math.toIntExact(frame.appendUnitFrameOrdinal());
        if (ordinal >= commit.batches().size()) {
            throw new IllegalArgumentException("Object payload has excess native batches");
        }
        byte[] raw = new byte[payload.remaining()];
        payload.get(raw);
        try {
            var assigned = com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1.validate(
                    com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1.parse(
                            CanonicalBytes.copyOf(raw)));
            var expected = commit.batches().get(ordinal);
            var delta = new com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1()
                    .protocolDelta(assigned);
            if (assigned.baseOffset() != expected.startOffset()
                    || assigned.endOffsetExclusive() != expected.endOffsetExclusive()
                    || assigned.partitionLeaderEpoch() != commit.expectedFence().kafkaLeaderEpoch()
                    || !delta.equals(expected.delta())) {
                throw new IllegalArgumentException(
                        "Object native payload differs from its complete speculative commit");
            }
        } finally {
            Arrays.fill(raw, (byte) 0);
        }
    }

    private KafkaVerifiedNwg1CommitV1 verifyCommit(
            Nwg1ObjectReaderV1.AuthenticatedPrefix prefix,
            Nwg1ObjectReaderV1.VerifiedAppendUnit verifiedUnit,
            KafkaSpeculativeCommitV1 commit,
            KafkaObjectCompletionTrackerV1.AssignedTicket ticket,
            ObjectIdentity identity) {
        KafkaObjectBindingKeyV1 binding = binding(commit);
        byte[] expectedNti = TopicIncarnationIdentityCodecV1.encode(
                        commit.expectedFence().topicIncarnation())
                .toByteArray();
        Nwg1DirectoryV1 directory = prefix.directory();
        int contextOrdinal = -1;
        for (int index = 0; index < directory.bindings().size(); index++) {
            Nwg1DirectoryV1.BindingContext candidate = directory.bindings().get(index);
            if (Arrays.equals(
                            candidate.bindingId(),
                            binding.bindingId().digest().bytes().toByteArray())
                    && Arrays.equals(
                            candidate.storageEpochId(),
                            binding.storageEpochId().digest().bytes().toByteArray())
                    && Arrays.equals(candidate.nti1Bytes(), expectedNti)) {
                if (contextOrdinal >= 0) {
                    throw new IllegalStateException("NWG1 directory repeats the selected Kafka binding context");
                }
                contextOrdinal = index;
            }
        }
        if (contextOrdinal < 0) {
            throw new IllegalStateException("NWG1 directory omits the exact Kafka binding context");
        }
        Nwg1DirectoryV1.KafkaAppendUnit selected = null;
        for (Nwg1DirectoryV1.AppendUnit candidate : directory.appendUnits()) {
            Nwg1DirectoryV1.KafkaAppendUnit kafka = (Nwg1DirectoryV1.KafkaAppendUnit) candidate;
            if (kafka.contextOrdinal() == contextOrdinal
                    && kafka.startOffset() == commit.startOffset()
                    && kafka.endOffsetExclusive() == commit.endOffsetExclusive()) {
                if (selected != null) {
                    throw new IllegalStateException("NWG1 directory repeats the selected Kafka commit set");
                }
                selected = kafka;
            }
        }
        if (selected == null
                || selected.partitionId() != binding.partitionId()
                || selected.kafkaLeaderEpoch() != commit.expectedFence().kafkaLeaderEpoch()
                || !Arrays.equals(selected.appendCommitSetId(), commitSetId(commit))
                || !Arrays.equals(selected.storageAttemptId(), storageAttemptId(ticket))
                || verifiedUnit.protocolKind() != directory.protocolKind()
                || verifiedUnit.appendUnitOrdinal() != directory.appendUnits().indexOf(selected)
                || verifiedUnit.contextOrdinal() != selected.contextOrdinal()
                || verifiedUnit.firstFrameOrdinal() != selected.firstFrameOrdinal()
                || verifiedUnit.frameCount() != selected.frameCount()
                || verifiedUnit.coverage0() != selected.startOffset()
                || verifiedUnit.coverage1() != selected.endOffsetExclusive()
                || !Arrays.equals(verifiedUnit.appendCommitSetId(), selected.appendCommitSetId())
                || !Arrays.equals(verifiedUnit.storageAttemptId(), selected.storageAttemptId())
                || !Arrays.equals(verifiedUnit.assignedPayloadSha256(), selected.assignedPayloadSha256())) {
            throw new IllegalStateException("NWG1 directory unit differs from the assigned Kafka commit/ticket");
        }
        Sha256Digest payloadSha = Sha256Digest.copyOf(verifiedUnit.assignedPayloadSha256());
        KafkaObjectExtentIdentityV1 extent = extent(prefix.header(), identity);
        KafkaObjectExtentLocatorV1 locator = new KafkaObjectExtentLocatorV1(
                binding,
                commit.startOffset(),
                commit.endOffsetExclusive(),
                extent,
                Math.toIntExact(selected.firstFrameOrdinal()),
                Math.toIntExact(selected.frameCount()));
        return new KafkaVerifiedNwg1CommitV1(locator, payloadSha, Math.toIntExact(selected.frameCount()));
    }

    private KafkaObjectExtentIdentityV1 extent(Nwg1HeaderV1 header, ObjectIdentity identity) {
        return new KafkaObjectExtentIdentityV1(
                rootSha,
                header.laneId(),
                header.laneSequence(),
                header.directoryPrefixEnd(),
                header.canonicalBodyLength(),
                identity.bodySha256());
    }

    private static KafkaObjectBindingKeyV1 binding(KafkaSpeculativeCommitV1 commit) {
        return new KafkaObjectBindingKeyV1(
                commit.expectedFence().bindingId(),
                commit.expectedFence().topicIncarnation().topicId(),
                commit.expectedFence().partitionId(),
                commit.expectedFence().storageEpochId());
    }

    public static byte[] commitSetId(KafkaSpeculativeCommitV1 commit) {
        Objects.requireNonNull(commit, "commit");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeUTF("M3-KAFKA-APPEND-COMMIT-SET-ID-V1");
                out.writeLong(commit.startOffset());
                out.writeLong(commit.endOffsetExclusive());
                var fence = commit.expectedFence();
                writeBytes(out, fence.bindingId().digest().bytes().toByteArray());
                writeBytes(
                        out,
                        TopicIncarnationIdentityCodecV1.encode(fence.topicIncarnation())
                                .toByteArray());
                out.writeInt(fence.partitionId());
                out.writeLong(fence.bindingGeneration());
                writeBytes(out, fence.storageEpochId().digest().bytes().toByteArray());
                out.writeLong(fence.ownerEpoch());
                out.writeInt(fence.kafkaLeaderEpoch());
                out.writeInt(commit.batches().size());
                for (var batch : commit.batches()) {
                    out.writeLong(batch.startOffset());
                    out.writeLong(batch.endOffsetExclusive());
                    var delta = batch.delta();
                    out.writeLong(delta.logicalOffsetCount());
                    out.writeBoolean(delta.duplicateIdentity().isPresent());
                    if (delta.duplicateIdentity().isPresent()) {
                        var duplicate = delta.duplicateIdentity().orElseThrow();
                        out.writeLong(duplicate.producerId());
                        out.writeShort(duplicate.producerEpoch());
                        out.writeInt(duplicate.baseSequence());
                        out.writeInt(duplicate.lastSequence());
                    }
                    out.writeInt(delta.transactionKind().ordinal());
                    out.writeLong(delta.transactionalProducerId());
                    out.writeInt(delta.coordinatorEpoch());
                }
            }
            byte[] digest = Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()))
                    .bytes()
                    .toByteArray();
            return Arrays.copyOf(digest, 16);
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory Kafka commit-set ID encoding failed", failure);
        }
    }

    public static byte[] storageAttemptId(KafkaObjectCompletionTrackerV1.AssignedTicket ticket) {
        java.nio.ByteBuffer bytes = java.nio.ByteBuffer.allocate(16);
        bytes.putLong(ticket.ownerEpoch());
        bytes.putLong(ticket.ticket());
        return bytes.array();
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static final class PendingWrite {
        private WalCheckpointPublisher.Reservation physicalReservation;
        private boolean inFlight;
        private final Sha256Digest planSha;
        private final KafkaSpeculativeCommitV1 commit;
        private final KafkaObjectNativeStateV1 nativeState;
        private final WalRunObjectSession.ValidatedNwg1Plan validatedPlan;
        private KafkaObjectCompletionTrackerV1.SequenceClaim sequenceClaim;
        private WalRunObjectSession.AdmittedNwg1Candidate candidate;
        private boolean dispatched;
        private boolean requiresReconciliation;
        private boolean providerExact;
        private boolean physicalResolved;
        private WalRunObjectSession.AuthenticatedNwg1PublicationExtent publicationExtent;
        private KafkaVerifiedNwg1CommitV1 verified;

        private PendingWrite(
                Sha256Digest planSha,
                KafkaSpeculativeCommitV1 commit,
                KafkaObjectNativeStateV1 nativeState,
                WalRunObjectSession.ValidatedNwg1Plan validatedPlan,
                KafkaObjectCompletionTrackerV1.SequenceClaim sequenceClaim) {
            this.planSha = planSha;
            this.commit = commit;
            this.nativeState = nativeState;
            this.validatedPlan = validatedPlan;
            this.sequenceClaim = sequenceClaim;
        }

        private void requireRetry(
                GroupEncodingPlanV1 plan,
                KafkaSpeculativeCommitV1 suppliedCommit,
                KafkaObjectNativeStateV1 suppliedNativeState) {
            if (!planSha.equals(plan.canonicalPlanSha256())
                    || !commit.equals(suppliedCommit)
                    || !nativeState.equals(suppliedNativeState)) {
                throw new IllegalArgumentException("Kafka NWG1 retry substituted plan/commit/native authority");
            }
        }
    }

    private static final class PendingSharedWrite {
        private WalCheckpointPublisher.Reservation physicalReservation;
        private boolean inFlight;
        private final Sha256Digest planSha;
        private final List<SharedMember> members;
        private final WalRunObjectSession.ValidatedNwg1Plan validatedPlan;
        private List<KafkaObjectCompletionTrackerV1.SequenceClaim> sequenceClaims;
        private final Map<Integer, VerifiedMember> verifiedMembers = new LinkedHashMap<>();
        private final Map<Integer, IsolatedMemberFailure> isolatedFailures = new LinkedHashMap<>();
        private final Map<Integer, AuthorizationMemberFailure> authorizationFailures = new LinkedHashMap<>();
        private WalRunObjectSession.AdmittedNwg1Candidate candidate;
        private ObjectIdentity identity;
        private KafkaObjectExtentIdentityV1 extent;
        private WalRunObjectSession.AuthenticatedNwg1PublicationExtent publicationExtent;
        private boolean dispatched;
        private boolean requiresReconciliation;
        private boolean providerExact;
        private boolean physicalResolved;

        private PendingSharedWrite(
                Sha256Digest planSha,
                List<SharedMember> members,
                WalRunObjectSession.ValidatedNwg1Plan validatedPlan,
                List<KafkaObjectCompletionTrackerV1.SequenceClaim> sequenceClaims) {
            this.planSha = planSha;
            this.members = List.copyOf(members);
            this.validatedPlan = validatedPlan;
            this.sequenceClaims = List.copyOf(sequenceClaims);
        }

        private void requireRetry(GroupEncodingPlanV1 plan, List<SharedMember> suppliedMembers) {
            if (!planSha.equals(plan.canonicalPlanSha256()) || !members.equals(suppliedMembers)) {
                throw new IllegalArgumentException("shared Kafka NWG1 retry substituted plan/member authority");
            }
        }

        private SharedWriteResult result() {
            ArrayList<VerifiedMember> verified = new ArrayList<>();
            ArrayList<IsolatedMemberFailure> failures = new ArrayList<>();
            ArrayList<AuthorizationMemberFailure> authorization = new ArrayList<>();
            for (int index = 0; index < members.size(); index++) {
                Optional.ofNullable(verifiedMembers.get(index)).ifPresent(verified::add);
                Optional.ofNullable(isolatedFailures.get(index)).ifPresent(failures::add);
                Optional.ofNullable(authorizationFailures.get(index)).ifPresent(authorization::add);
            }
            return new SharedWriteResult(identity, verified, failures, authorization);
        }
    }
}
