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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.kms.RunKeyCacheIdentity;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import com.nereusstream.storage.object.nwg1.GroupEncodingPlanV1;
import com.nereusstream.storage.object.nwg1.Nwg1CommitmentsV1;
import com.nereusstream.storage.object.nwg1.Nwg1EnvelopeV1;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1RootAuthorityV1;
import com.nereusstream.storage.object.nwg1.Nwg1SealedObjectV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationPathV1;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ProviderObjectResult;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import com.nereusstream.storage.object.recovery.BoundedObjectTailRecovery;
import com.nereusstream.storage.object.recovery.CumulativeRecoveryBudget;
import com.nereusstream.storage.object.recovery.RecoveredWalRunRuntimeCut;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Root-bound production owner for admission, Provider/KMS Cell sessions, and one cumulative recovery envelope. It is
 * the only common API that hands plaintext run-key material to NWG1, and that handoff never crosses this class.
 */
public final class WalRunObjectSession implements AutoCloseable {
    public enum State {
        OPEN,
        DRAINING,
        CLOSED
    }

    private final WalRunRootRecord root;
    private final Sha256Digest rootSha256;
    private final WalRunRuntime runtime;
    private final C1ObjectProviderSession.WalRunLease provider;
    private final KmsCellSession.WalRunLease kms;
    private final RunKeyCacheIdentity runKeyIdentity;
    private final BoundedObjectTailRecovery recovery;
    private final RecoveredWalRunRuntimeCut recoveredCut;
    private final Nwg1RootAuthorityV1 nwg1RootAuthority;
    private final boolean recoveredCurrentRoot;
    private boolean recoveredRowsReleased;
    private State state = State.OPEN;

    /** Package-local isolated-test constructor; production fresh opens require lifecycle owner authority. */
    WalRunObjectSession(
            WalRunRootRecord root, C1ObjectProviderSession provider, KmsCellSession kms, LongSupplier nanoTime) {
        this(
                root,
                new WalRunRuntime(root),
                provider,
                kms,
                BoundedObjectTailRecovery.prepareNewRoot(root, nanoTime),
                null,
                null);
    }

    /** Package-local isolated-test constructor; production fresh opens require lifecycle owner authority. */
    WalRunObjectSession(
            WalRunRootRecord root,
            WalRunRuntime runtime,
            C1ObjectProviderSession provider,
            KmsCellSession kms,
            LongSupplier nanoTime) {
        this(root, runtime, provider, kms, BoundedObjectTailRecovery.prepareNewRoot(root, nanoTime), null, null);
    }

    /** Fresh production open consumes an exact lifecycle-published Root authority once, after session readiness. */
    public static WalRunObjectSession openNew(
            WalRunLifecycleManager.NewWalRunOwnerAuthority ownerAuthority,
            C1ObjectProviderSession provider,
            KmsCellSession kms,
            LongSupplier nanoTime) {
        Objects.requireNonNull(ownerAuthority, "ownerAuthority");
        WalRunRootRecord root = ownerAuthority.requireConsumableRoot();
        return new WalRunObjectSession(
                root,
                new WalRunRuntime(root),
                provider,
                kms,
                BoundedObjectTailRecovery.prepareNewRoot(root, nanoTime),
                null,
                ownerAuthority);
    }

    private WalRunObjectSession(
            WalRunRootRecord root,
            WalRunRuntime runtime,
            C1ObjectProviderSession rawProvider,
            KmsCellSession kms,
            BoundedObjectTailRecovery.PreparedNewRootRecovery preparedNewRoot,
            RecoveredWalRunRuntimeCut recoveredCut,
            WalRunLifecycleManager.NewWalRunOwnerAuthority newOwnerAuthority) {
        this.root = Objects.requireNonNull(root, "root");
        this.root.requireM3ProductionProviderProofMode();
        this.rootSha256 = WalRunControlCodec.rootSha256(root);
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        C1ObjectProviderSession suppliedProvider =
                recoveredCut == null ? Objects.requireNonNull(rawProvider, "provider") : rawProvider;
        KmsCellSession suppliedKms = recoveredCut == null ? Objects.requireNonNull(kms, "kms") : kms;
        if (!runtime.rootRecord().equals(root)
                || (recoveredCut == null
                        && (!suppliedProvider.providerScopeId().equals(root.providerScopeId())
                                || !suppliedProvider
                                        .exclusiveNamespacePrefix()
                                        .equals(root.providerConfiguration().exclusiveNamespacePrefix())
                                || suppliedProvider.admittedMaximumObjectBytes()
                                        != root.providerConfiguration().maxObjectBodyBytes()
                                || suppliedProvider.admittedMaximumPrefixBytes()
                                        != root.nwg1AdmissionCaps().maxDirectoryPrefixBytes()
                                || !suppliedKms.providerScopeId().equals(root.providerScopeId())))) {
            throw new IllegalArgumentException(
                    "WalRun authority differs: runtime/Provider/KMS Cell authorities differ from the Root");
        }
        this.runKeyIdentity = new RunKeyCacheIdentity(root.shardId(), root.shardRunEpoch());
        byte[] npc1 =
                ProtocolCellIdentityCodecV1.encode(root.protocolCellIdentity()).toByteArray();
        Nwg1EnvelopeV1 envelope =
                Nwg1EnvelopeV1.decode(root.wrappedRunKey().framedBytes().toByteArray());
        this.nwg1RootAuthority = new Nwg1RootAuthorityV1(
                npc1,
                Nwg1CommitmentsV1.protocolCell(npc1),
                root.providerScopeId().digest().bytes().toByteArray(),
                rootSha256.bytes().toByteArray(),
                root.wrappedRunKey().framedBytes().toByteArray(),
                Nwg1CommitmentsV1.wrappedEnvelope(envelope));
        if ((preparedNewRoot == null) == (recoveredCut == null)) {
            throw new IllegalArgumentException(
                    "WalRun session must be new-root or recovered-current-root exactly once");
        }
        if (recoveredCut != null) {
            recoveredCut.requireConsumableFor(root);
        }
        ProviderOwnerAuthority providerAuthority = new ProviderOwnerAuthority(
                this,
                root.providerScopeId(),
                root.providerConfiguration().exclusiveNamespacePrefix(),
                root.providerConfiguration().maxObjectBodyBytes(),
                root.nwg1AdmissionCaps().maxDirectoryPrefixBytes());
        KmsOwnerAuthority kmsAuthority =
                new KmsOwnerAuthority(this, runKeyIdentity, root.wrappedRunKey(), rootSha256, root.providerScopeId());
        C1ObjectProviderSession.WalRunLease transferredProvider;
        KmsCellSession.WalRunLease transferredKms;
        BoundedObjectTailRecovery transferredRecovery;
        if (recoveredCut != null) {
            synchronized (recoveredCut) {
                recoveredCut.requireConsumableFor(root);
                var promoted = recoveredCut.promoteRecoveryLeases(root, providerAuthority, kmsAuthority);
                transferredProvider = promoted.provider();
                transferredKms = promoted.kms();
                transferredRecovery =
                        BoundedObjectTailRecovery.fromRecoveredRuntimeCut(transferredProvider, root, recoveredCut);
            }
        } else if (newOwnerAuthority != null) {
            synchronized (newOwnerAuthority) {
                newOwnerAuthority.requireConsumableRoot();
                synchronized (suppliedProvider) {
                    synchronized (suppliedKms) {
                        suppliedProvider.requireTransferReady(providerAuthority);
                        suppliedKms.requireTransferReady(kmsAuthority);
                        newOwnerAuthority.consumeFor(root);
                        transferredProvider = suppliedProvider.transferToWalRun(providerAuthority);
                        transferredKms = suppliedKms.transferToWalRun(kmsAuthority);
                        transferredRecovery = BoundedObjectTailRecovery.fromPreparedNewRoot(
                                transferredProvider, root, Objects.requireNonNull(preparedNewRoot, "preparedNewRoot"));
                    }
                }
            }
        } else {
            // This constructor path is package-visible solely for isolated common tests. Production callers cannot
            // name it and must use openNew(NewWalRunOwnerAuthority, ...).
            synchronized (suppliedProvider) {
                synchronized (suppliedKms) {
                    suppliedProvider.requireTransferReady(providerAuthority);
                    suppliedKms.requireTransferReady(kmsAuthority);
                    transferredProvider = suppliedProvider.transferToWalRun(providerAuthority);
                    transferredKms = suppliedKms.transferToWalRun(kmsAuthority);
                    transferredRecovery = BoundedObjectTailRecovery.fromPreparedNewRoot(
                            transferredProvider, root, Objects.requireNonNull(preparedNewRoot, "preparedNewRoot"));
                }
            }
        }
        this.provider = transferredProvider;
        this.kms = transferredKms;
        this.recovery = transferredRecovery;
        this.recoveredCut = recoveredCut;
        this.recoveredCurrentRoot = recoveredCut != null;
    }

    public static WalRunObjectSession restore(WalRunRootRecord root, RecoveredWalRunRuntimeCut recoveredCut) {
        Objects.requireNonNull(recoveredCut, "recoveredCut");
        WalRunRuntime restoredRuntime = recoveredCut.restoreRuntimeFor(root);
        return new WalRunObjectSession(root, restoredRuntime, null, null, null, recoveredCut, null);
    }

    /** Performs the complete no-effect Root/format/cap/plan validation before a caller locks local rollback state. */
    public synchronized ValidatedNwg1Plan validateNwg1Plan(
            GroupEncodingPlanV1 plan, Nwg1VerificationContextV1 verificationContext) {
        requireOpen();
        Objects.requireNonNull(plan, "plan");
        requireVerificationContext(verificationContext);
        GroupEncodingPlanV1.AdmissionFacts facts = plan.requireAdmission(root.nwg1AdmissionCaps());
        if (plan.protocolKind() != root.protocolCellIdentity().protocolKind().code()
                || plan.shardId() != root.shardId()
                || plan.shardRunEpoch() != root.shardRunEpoch()
                || !Arrays.equals(plan.protocolCellCommitment(), nwg1RootAuthority.protocolCellCommitment())
                || !Arrays.equals(plan.providerScopeId(), nwg1RootAuthority.cellProviderScopeId())
                || !Arrays.equals(plan.rootSha256(), rootSha256.bytes().toByteArray())
                || !Arrays.equals(plan.envelopeCommitment(), nwg1RootAuthority.wrappedEnvelopeCommitment())) {
            throw new IllegalArgumentException("NWG1 plan substituted the exact WalRun Root authority");
        }
        plan.requireSemanticAdmission(verificationContext);
        Sha256Digest planSha256 = plan.canonicalPlanSha256();
        ImmutableExtentPlan immutablePlan = new ImmutableExtentPlan(
                WalLaneId.fromCode(plan.laneId()), plan.packingPolicyVersion(), planSha256, facts.canonicalBodyBytes());
        return new ValidatedNwg1Plan(this, plan, verificationContext, immutablePlan, facts);
    }

    public synchronized ProviderObjectResult conditionalCreateNwg1(AdmittedNwg1Candidate candidate) throws IOException {
        requireOpen();
        requireCandidate(candidate);
        chargeConditionalCreateAttempt(candidate.conditionalCreateAttempts);
        candidate.conditionalCreateAttempts = Math.incrementExact(candidate.conditionalCreateAttempts);
        return provider.conditionalCreate(
                new CanonicalCandidateBody(candidate.identity, CanonicalBytes.copyOf(candidate.sealed.body())));
    }

    /** Creates a Root-bound Kafka NWKCP1 token only after exact content-key/body validation. */
    public synchronized ValidatedKafkaProtocolObject validateKafkaProtocolObject(
            ObjectIdentity identity, CanonicalBytes canonicalBody) {
        requireUsable();
        requireKafkaProtocolIdentity(identity);
        Objects.requireNonNull(canonicalBody, "canonicalBody");
        if (canonicalBody.length() != identity.bodyLength()
                || !Sha256Digest.hash(canonicalBody).equals(identity.bodySha256())) {
            throw new IllegalArgumentException("Kafka protocol Object body differs from its content identity");
        }
        return new ValidatedKafkaProtocolObject(this, identity, canonicalBody);
    }

    public synchronized ProviderObjectResult conditionalCreateKafkaProtocolObject(
            ValidatedKafkaProtocolObject candidate) throws IOException {
        requireOpen();
        Objects.requireNonNull(candidate, "candidate");
        if (candidate.owner != this) {
            throw new IllegalArgumentException("Kafka protocol Object token belongs to another WalRun session");
        }
        chargeConditionalCreateAttempt(candidate.conditionalCreateAttempts);
        candidate.conditionalCreateAttempts = Math.incrementExact(candidate.conditionalCreateAttempts);
        return provider.conditionalCreate(new CanonicalCandidateBody(candidate.identity, candidate.body));
    }

    /** Projects the sole Root-authorized full Object identity from a sealed NWG1 relative leaf and body. */
    public synchronized ObjectIdentity requireNwg1Identity(Nwg1SealedObjectV1 sealed) {
        requireUsable();
        Objects.requireNonNull(sealed, "sealed");
        ObjectWalLeafKeyV1 leaf = ObjectWalLeafKeyV1.parseRelative(sealed.leafUtf8());
        Sha256Digest bodySha = Sha256Digest.copyOf(sealed.bodySha256());
        if (leaf.bodyLength() != sealed.body().length
                || leaf.directoryPrefixEnd() != sealed.header().directoryPrefixEnd()
                || !leaf.objectSha256().equals(bodySha)) {
            throw new IllegalArgumentException("sealed NWG1 leaf differs from its exact Header/body identity");
        }
        return new ObjectIdentity(leaf.fullKey(root.providerConfiguration()), leaf.bodyLength(), bodySha);
    }

    /**
     * The sole production sequence/seal cut. Validation has no sequence effect; any exception from this method
     * reports whether the exact pending reservation now exists and must be retained for same-plan retry.
     */
    public synchronized AdmittedNwg1Candidate admitAndSealNwg1(ValidatedNwg1Plan validatedPlan, long nowMillis) {
        Objects.requireNonNull(validatedPlan, "validatedPlan");
        if (validatedPlan.owner != this) {
            throw new Nwg1AdmissionFailure(
                    Optional.empty(),
                    validatedPlan.immutablePlan.canonicalPlanSha256(),
                    new IllegalArgumentException("validated NWG1 plan belongs to another WalRun session"));
        }
        requireUsable();
        if (validatedPlan.candidate != null) {
            if (validatedPlan.candidate.terminal) {
                throw new Nwg1AdmissionFailure(
                        Optional.of(validatedPlan.candidate.reservation),
                        validatedPlan.immutablePlan.canonicalPlanSha256(),
                        new IllegalStateException("validated NWG1 candidate is already terminal"));
            }
            return validatedPlan.candidate;
        }
        LaneSequenceReservation reservation = null;
        try {
            reservation = runtime.admitOrRetrySealedPlan(validatedPlan.immutablePlan, nowMillis);
            if (validatedPlan.reservation != null && !validatedPlan.reservation.equals(reservation)) {
                throw new IllegalStateException("validated NWG1 retry resolved to a different lane reservation");
            }
            validatedPlan.reservation = reservation;
            Nwg1SealedObjectV1 sealed =
                    kms.sealNwg1(validatedPlan.plan, reservation.laneSequence(), validatedPlan.verificationContext);
            if (sealed.body().length != validatedPlan.facts.canonicalBodyBytes()) {
                throw new IllegalStateException("sealed NWG1 body differs from the pre-sequence admission facts");
            }
            ObjectIdentity identity = requireNwg1Identity(sealed);
            AdmittedNwg1Candidate candidate =
                    new AdmittedNwg1Candidate(this, validatedPlan, reservation, sealed, identity);
            validatedPlan.candidate = candidate;
            return candidate;
        } catch (Nwg1AdmissionFailure failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new Nwg1AdmissionFailure(
                    Optional.ofNullable(reservation), validatedPlan.immutablePlan.canonicalPlanSha256(), failure);
        }
    }

    public synchronized void providerResolved(AdmittedNwg1Candidate candidate) {
        requireCandidate(candidate);
        runtime.providerResolved(candidate.reservation, candidate.identity.bodyLength());
        candidate.terminal = true;
    }

    public synchronized void providerAbsent(AdmittedNwg1Candidate candidate) {
        requireCandidate(candidate);
        runtime.providerAbsent(candidate.reservation);
        candidate.terminal = true;
    }

    public synchronized void providerConflict(AdmittedNwg1Candidate candidate) {
        requireCandidate(candidate);
        runtime.providerConflict(candidate.reservation);
        candidate.terminal = true;
    }

    public synchronized void stopAdmission(WalRunRuntime.StopReason reason) {
        requireUsable();
        runtime.stopAdmission(reason);
    }

    public synchronized LaneSequenceVector sealRuntime() {
        requireUsable();
        return runtime.seal();
    }

    public synchronized WalRunRuntime.State runtimeState() {
        requireUsable();
        return runtime.state();
    }

    public synchronized WalRunRuntime.RecoveredState runtimeRecoveryState() {
        requireUsable();
        return runtime.recoveryState();
    }

    synchronized void requireExactSealedClosure(WalRunRootRecord expectedRoot, WalRunSealRecord seal) {
        requireUsable();
        Objects.requireNonNull(expectedRoot, "expectedRoot");
        Objects.requireNonNull(seal, "seal");
        if (!root.equals(expectedRoot)
                || runtime.state() != WalRunRuntime.State.SEALED
                || !runtime.resolvedVector().equals(seal.terminalSequence())
                || runtime.resolvedExtentCount() != seal.aggregateExtentCount()
                || runtime.resolvedCanonicalBodyBytes() != seal.aggregateCanonicalBodyBytes()) {
            throw new IllegalStateException("physical Seal differs from the exact WalRun Object session closure");
        }
    }

    /**
     * Side-effect-free preflight required before publishing an immutable physical Seal. No accepted or unknown C1
     * operation may survive the cut, and the Root-bound KMS lease must remain available until publication finishes.
     */
    public synchronized void requireTerminalClosable() {
        if (state != State.DRAINING
                || runtime.state() != WalRunRuntime.State.SEALED
                || provider.acceptedOperations() != 0
                || provider.unknownObjectCount() != 0
                || kms.state() != KmsCellSession.State.OPEN) {
            throw new IllegalStateException(
                    "WalRun Object session is not a drained, sealed, Provider-clean, KMS-live terminal cut");
        }
    }

    /** Full GET and strict NWG1 verification share the Root cumulative recovery budget and KMS cache lifecycle. */
    public synchronized Nwg1ObjectReaderV1.DecodedObject readAndVerifyNwg1(
            ObjectIdentity identity,
            Nwg1VerificationPathV1 path,
            Nwg1VerificationContextV1 verificationContext,
            long selectedFrameOrdinal)
            throws IOException {
        requireUsable();
        requireVerificationContext(verificationContext);
        return switch (path) {
            case ROUTINE_RANGE_READ ->
                throw new IllegalArgumentException(
                        "routine selected-unit reads require the streaming frame-consumer API");
            case OPEN_RUN_RECOVERY -> {
                Nwg1ObjectReaderV1.AuthenticatedPrefix prefix =
                        recoverAndVerifyNwg1Directory(identity, verificationContext);
                yield new Nwg1ObjectReaderV1.DecodedObject(prefix.header(), prefix.directory(), List.of());
            }
            case FULL_BODY_RECONCILIATION -> {
                ObjectWalLeafKeyV1 leaf = requireNwg1LeafIdentity(identity);
                CanonicalBytes body = recovery.readVerifiedExtent(identity);
                yield kms.verifyNwg1(
                        path,
                        nwg1RootAuthority,
                        verificationContext,
                        leaf.relativeKey().getBytes(StandardCharsets.US_ASCII),
                        body,
                        selectedFrameOrdinal);
            }
        };
    }

    /**
     * Performs the sole publication full GET/SHA proof and authenticates only shared Header/Directory structure.
     * Member-local Binding, frame, codec, digest, and native semantics are deliberately deferred to the typed token
     * API so one bad Binding cannot quarantine a valid sibling and no second Provider/KMS read is required.
     */
    public synchronized AuthenticatedNwg1PublicationExtent readAndAuthenticateNwg1ForPublication(
            AdmittedNwg1Candidate candidate, Nwg1VerificationContextV1 verificationContext) throws IOException {
        requireCandidate(candidate);
        requireVerificationContext(verificationContext);
        ObjectWalLeafKeyV1 leaf = requireNwg1LeafIdentity(candidate.identity);
        C1ObjectProviderSession.VerifiedObjectRead verifiedRead =
                provider.readVerifiedObjectWithVersion(candidate.identity);
        byte[] exactBody = verifiedRead.canonicalBody().toByteArray();
        byte[] exactPrefix = Arrays.copyOfRange(exactBody, 0, Math.toIntExact(leaf.directoryPrefixEnd()));
        Nwg1ObjectReaderV1.AuthenticatedPrefix prefix =
                kms.readAuthenticatedPrefix(exactPrefix, candidate.identity.bodyLength(), verificationContext);
        requirePrefixIdentity(prefix, leaf);
        requirePrefixWithinRootCaps(prefix);
        return new AuthenticatedNwg1PublicationExtent(
                this, candidate, prefix, exactBody, requireProviderProof(verifiedRead.immutableVersionToken()));
    }

    /** Verifies one member-selected append unit from the authenticated publication body with zero Provider I/O. */
    public synchronized Nwg1ObjectReaderV1.VerifiedAppendUnit verifySelectedNwg1AppendUnitForPublication(
            AuthenticatedNwg1PublicationExtent extent,
            Nwg1VerificationContextV1 verificationContext,
            long selectedFrameOrdinal,
            Nwg1ObjectReaderV1.VerifiedFrameConsumer consumer)
            throws IOException {
        requireUsable();
        Objects.requireNonNull(extent, "extent");
        if (extent.owner != this) {
            throw new IllegalArgumentException("publication extent token belongs to another WalRun session");
        }
        requireCandidate(extent.candidate);
        requireVerificationContext(verificationContext);
        return kms.readSelectedAppendUnitStreaming(
                extent.prefix,
                (range, ignored) -> Arrays.copyOfRange(
                        extent.exactBody,
                        Math.toIntExact(range.inclusiveStart()),
                        Math.toIntExact(range.exclusiveEnd())),
                selectedFrameOrdinal,
                verificationContext,
                Objects.requireNonNull(consumer, "consumer"));
    }

    /** Two-stage C1 prefix + exact selected append-unit frame ranges; no full GET and no recovery-budget charge. */
    public synchronized Nwg1ObjectReaderV1.VerifiedAppendUnit readRoutineNwg1AppendUnit(
            ObjectIdentity identity,
            Nwg1VerificationContextV1 verificationContext,
            long selectedFrameOrdinal,
            Nwg1ObjectReaderV1.VerifiedFrameConsumer consumer)
            throws IOException {
        requireUsable();
        requireVerificationContext(verificationContext);
        ObjectWalLeafKeyV1 leaf = requireNwg1LeafIdentity(identity);
        CanonicalBytes prefixBytes =
                provider.readDirectoryPrefix(identity, leaf.directoryPrefixEnd(), Optional.empty());
        Nwg1ObjectReaderV1.AuthenticatedPrefix prefix =
                kms.readAuthenticatedPrefix(prefixBytes.toByteArray(), identity.bodyLength(), verificationContext);
        requirePrefixIdentity(prefix, leaf);
        requirePrefixWithinRootCaps(prefix);
        return kms.readSelectedAppendUnitStreaming(
                prefix,
                (range, ignored) -> provider.readExactRange(
                                identity, range.inclusiveStart(), range.exclusiveEnd(), Optional.empty())
                        .toByteArray(),
                selectedFrameOrdinal,
                verificationContext,
                Objects.requireNonNull(consumer, "consumer"));
    }

    /** One cumulative range-GET authenticates the complete Directory needed to rebuild exact recovery locators. */
    public synchronized Nwg1ObjectReaderV1.AuthenticatedPrefix recoverAndVerifyNwg1Directory(
            ObjectIdentity identity, Nwg1VerificationContextV1 verificationContext) throws IOException {
        requireUsable();
        requireVerificationContext(verificationContext);
        ObjectWalLeafKeyV1 leaf = requireNwg1LeafIdentity(identity);
        CanonicalBytes prefixBytes = recovery.reconstructDirectoryPrefixes(Map.of(identity, leaf.directoryPrefixEnd()))
                .get(identity);
        Nwg1ObjectReaderV1.AuthenticatedPrefix prefix =
                kms.readAuthenticatedPrefix(prefixBytes.toByteArray(), identity.bodyLength(), verificationContext);
        requirePrefixIdentity(prefix, leaf);
        requirePrefixWithinRootCaps(prefix);
        recovery.chargeDecoded(
                prefix.directory().bindings().size(),
                prefix.directory().frames().size(),
                prefix.directory().appendUnits().size());
        return prefix;
    }

    /** Uses a previously authenticated recovery prefix and charges only its selected append-unit frame ranges. */
    public synchronized Nwg1ObjectReaderV1.VerifiedAppendUnit recoverAndVerifyNwg1AppendUnit(
            ObjectIdentity identity,
            Nwg1ObjectReaderV1.AuthenticatedPrefix prefix,
            Nwg1VerificationContextV1 verificationContext,
            long selectedFrameOrdinal,
            Nwg1ObjectReaderV1.VerifiedFrameConsumer consumer)
            throws IOException {
        requireUsable();
        requireVerificationContext(verificationContext);
        ObjectWalLeafKeyV1 leaf = requireNwg1LeafIdentity(identity);
        requirePrefixIdentity(prefix, leaf);
        requirePrefixWithinRootCaps(prefix);
        return kms.readSelectedAppendUnitStreaming(
                prefix,
                (range, ignored) -> recovery.readExactFrameRange(identity, range.inclusiveStart(), range.exclusiveEnd())
                        .toByteArray(),
                selectedFrameOrdinal,
                verificationContext,
                Objects.requireNonNull(consumer, "consumer"));
    }

    public synchronized ProviderObjectResult reconcileUnknownExtent(ObjectIdentity identity) throws IOException {
        requireUsable();
        return recovery.reconcileUnknownExtent(identity);
    }

    /** Complete streaming lane discovery is available only to a lineage-restored current-Root owner. */
    public synchronized BoundedObjectTailRecovery.RecoveredLaneFold discoverUncoveredLaneStreaming(
            WalLaneId laneId, BoundedObjectTailRecovery.RecoveredExtentConsumer recoveredExtentConsumer)
            throws IOException {
        requireRecoveredCurrentRoot();
        return recovery.discoverUncoveredLaneStreaming(laneId, recoveredExtentConsumer);
    }

    /** One-use complete physical inventory already authenticated and protocol-staged inside the durable fence. */
    public synchronized RecoveredWalRunRuntimeCut.PhysicalRowsSummary consumeRecoveredPhysicalRows(
            RecoveredWalRunRuntimeCut.PhysicalRowConsumer consumer) throws IOException {
        requireRecoveredCurrentRoot();
        try {
            return Objects.requireNonNull(recoveredCut, "recoveredCut").consumeAuthenticatedPhysicalRows(consumer);
        } finally {
            if (!recoveredRowsReleased) {
                recovery.finishRecoveredPhysicalRows();
                recoveredRowsReleased = true;
            }
        }
    }

    public synchronized ProviderObjectResult reconcileUnknownProtocolObject(ObjectIdentity identity)
            throws IOException {
        requireUsable();
        requireKafkaProtocolIdentity(identity);
        return recovery.reconcileUnknownProtocolObject(identity);
    }

    public synchronized CanonicalBytes readVerifiedProtocolObject(ObjectIdentity identity) throws IOException {
        requireUsable();
        requireKafkaProtocolIdentity(identity);
        return recovery.readVerifiedProtocolCheckpoint(identity);
    }

    /**
     * Returns the predecessor-Root exact-read capability for a terminal-lineage verifier. The verifier's supplied
     * recovery context, not this session, precharges the one current-pointer budget; this capability therefore
     * must
     * not be used for ordinary tail recovery.
     */
    public synchronized TerminalProtocolCheckpointVerifierV1.ProtocolObjectRecoveryReaderV1
            terminalLineageProtocolObjectReader() {
        requireUsable();
        return identity -> {
            synchronized (WalRunObjectSession.this) {
                requireUsable();
                requireKafkaProtocolIdentity(identity);
                return provider.readVerifiedObject(identity);
            }
        };
    }

    /** Publication readback is exact full-body verification but is not charged as recovery work. */
    public synchronized CanonicalBytes readVerifiedProtocolObjectForPublication(ObjectIdentity identity)
            throws IOException {
        requireUsable();
        requireKafkaProtocolIdentity(identity);
        return provider.readVerifiedObject(identity);
    }

    public Sha256Digest rootSha256() {
        return rootSha256;
    }

    public synchronized void chargeRecoveryRoot(boolean predecessor, long canonicalBytes) {
        requireUsable();
        recovery.chargeRoot(predecessor, canonicalBytes);
    }

    public synchronized void chargeRecoveryControlMetadata(long canonicalBytes) {
        requireUsable();
        recovery.chargeControlMetadata(canonicalBytes);
    }

    public synchronized void chargeRecoveryDecoded(long contexts, long frames, long commitSets) {
        requireUsable();
        recovery.chargeDecoded(contexts, frames, commitSets);
    }

    /**
     * Stages exact physical checkpoint rows under this session's live recovery budget. Callers must publish staged
     * protocol state only after this method has returned successfully.
     */
    public synchronized WalCheckpointChainVerifier.StreamingVerification verifyCheckpointChainStreaming(
            CanonicalControlMetadataStore metadata,
            WalCheckpointHeadV1 head,
            java.util.function.Consumer<ProviderResolvedExtentRowV1> verifiedRowConsumer) {
        requireUsable();
        return recovery.verifyCheckpointChainStreaming(metadata, head, verifiedRowConsumer);
    }

    /** Streaming descendant proof without retaining a checkpoint page chain. */
    public synchronized WalCheckpointChainVerifier.StreamingVerification verifyCheckpointChainDescendant(
            CanonicalControlMetadataStore metadata,
            WalCheckpointHeadV1 head,
            long anchorOrdinal,
            String anchorKey,
            Sha256Digest anchorSha256) {
        requireUsable();
        if (anchorOrdinal < 0) {
            return recovery.verifyCheckpointChainStreaming(metadata, head, ignored -> {});
        }
        Objects.requireNonNull(anchorKey, "anchorKey");
        Objects.requireNonNull(anchorSha256, "anchorSha256");
        boolean[] found = {false};
        WalCheckpointChainVerifier.StreamingVerification verified =
                recovery.verifyCheckpointChainStreaming(metadata, head, ignored -> {}, page -> {
                    if (page.ordinal() == anchorOrdinal
                            && page.key().equals(anchorKey)
                            && page.sha256().equals(anchorSha256)) {
                        found[0] = true;
                    }
                });
        if (!found[0]) {
            throw new IllegalStateException("checkpoint Head conflict is not a descendant of the exact anchor page");
        }
        return verified;
    }

    public synchronized CumulativeRecoveryBudget.Snapshot enterRecoveryFallback() {
        requireUsable();
        return recovery.enterFallback();
    }

    public synchronized CumulativeRecoveryBudget.Snapshot recoverySnapshot() {
        requireUsable();
        return recovery.snapshot();
    }

    /**
     * Acquires one exact slice of the current Root's live recovery working set. The returned lease is one-use and
     * releases precisely that slice on close, allowing protocol adapters to stage bounded commit spools without a
     * caller-created recovery budget.
     */
    public synchronized RecoveryWorkingSetLease acquireRecoveryWorkingSet(long exactBytes) {
        requireUsable();
        if (exactBytes <= 0) {
            throw new IllegalArgumentException("recovery working-set bytes must be positive");
        }
        recovery.acquireWorkingSet(exactBytes);
        return new RecoveryWorkingSetLease(this, exactBytes);
    }

    public static final class RecoveryWorkingSetLease implements AutoCloseable {
        private final WalRunObjectSession owner;
        private long exactBytes;
        private boolean closed;

        private RecoveryWorkingSetLease(WalRunObjectSession owner, long exactBytes) {
            this.owner = owner;
            this.exactBytes = exactBytes;
        }

        /** Adds bytes to this one spool lease without consuming another recovery-concurrency slot. */
        public void grow(long additionalExactBytes) {
            synchronized (owner) {
                if (closed || additionalExactBytes <= 0) {
                    throw new IllegalStateException("recovery working-set lease is closed or growth is non-positive");
                }
                owner.requireUsable();
                owner.recovery.growWorkingSet(additionalExactBytes);
                exactBytes = Math.addExact(exactBytes, additionalExactBytes);
            }
        }

        /** Releases transformed/discarded bytes while retaining this spool's single concurrency reservation. */
        public void shrink(long releasedExactBytes) {
            synchronized (owner) {
                if (closed || releasedExactBytes <= 0 || releasedExactBytes > exactBytes) {
                    throw new IllegalStateException("recovery working-set lease shrink is invalid");
                }
                owner.requireUsable();
                owner.recovery.shrinkWorkingSet(releasedExactBytes);
                exactBytes = Math.subtractExact(exactBytes, releasedExactBytes);
            }
        }

        @Override
        public void close() {
            synchronized (owner) {
                if (closed) {
                    throw new IllegalStateException("recovery working-set lease was already closed");
                }
                owner.requireUsable();
                owner.recovery.releaseWorkingSet(exactBytes);
                closed = true;
            }
        }
    }

    private void requireVerificationContext(Nwg1VerificationContextV1 context) {
        Objects.requireNonNull(context, "verificationContext");
        if (!context.protocolCell().equals(root.protocolCellIdentity())
                || !Arrays.equals(
                        context.cellProviderScopeId(),
                        root.providerScopeId().digest().bytes().toByteArray())
                || !Arrays.equals(context.walRunRootSha256(), rootSha256.bytes().toByteArray())
                || !Arrays.equals(
                        context.envelope().framedBytes(),
                        root.wrappedRunKey().framedBytes().toByteArray())) {
            throw new IllegalArgumentException("NWG1 verification context differs from the exact WalRun Root");
        }
    }

    private void requireCandidate(AdmittedNwg1Candidate candidate) {
        requireUsable();
        Objects.requireNonNull(candidate, "candidate");
        if (candidate.owner != this
                || candidate.terminal
                || candidate.validatedPlan.candidate != candidate
                || !runtime.retryReservation(
                                candidate.reservation.laneId(),
                                candidate.reservation.laneSequence(),
                                candidate.reservation.canonicalPlanSha256())
                        .equals(candidate.reservation)) {
            throw new IllegalArgumentException("NWG1 candidate is not the exact pending WalRun reservation");
        }
    }

    private void chargeConditionalCreateAttempt(int priorAttempts) {
        if (priorAttempts < 0 || priorAttempts >= 2) {
            throw new IllegalStateException("C1 frozen failure model permits only PUT1 plus exact-candidate PUT2");
        }
        if (priorAttempts == 1) {
            recovery.chargeConditionalCreateRetry();
        }
    }

    private void requireKafkaProtocolIdentity(ObjectIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        if (root.protocolCellIdentity().protocolKind().code() != 1 || identity.bodyLength() > 64L * 1024 * 1024) {
            throw new IllegalArgumentException("WalRun does not admit this Kafka protocol Object identity");
        }
        String namespace = root.providerConfiguration().exclusiveNamespacePrefix();
        String marker = "/protocol/kafka/nwkcp1-v1/objects/sha256-v1-";
        int markerOffset = identity.key().lastIndexOf(marker);
        if (markerOffset < namespace.length()
                || !identity.key().startsWith(namespace + "/")
                || markerOffset != identity.key().indexOf(marker)) {
            throw new IllegalArgumentException("Kafka protocol Object key differs from the exact Root-bound grammar");
        }
        String expected = identity.key().substring(0, markerOffset)
                + marker
                + identity.bodySha256().toHex()
                + ".nwkcp1";
        if (!identity.key().equals(expected) || expected.getBytes(StandardCharsets.US_ASCII).length > 1024) {
            throw new IllegalArgumentException("Kafka protocol Object key differs from the exact Root-bound grammar");
        }
    }

    private ProviderVersionProof requireProviderProof(Optional<CanonicalBytes> immutableVersionToken) {
        Objects.requireNonNull(immutableVersionToken, "immutableVersionToken");
        ProviderProofMode mode = root.providerConfiguration().proofMode();
        if (mode == ProviderProofMode.NONE) {
            return ProviderVersionProof.none();
        }
        CanonicalBytes token = immutableVersionToken.orElseThrow(
                () -> new IllegalStateException("Root VERSION proof mode requires an immutable token"));
        if (token.isEmpty() || token.length() > root.providerConfiguration().proofTokenHardCap()) {
            throw new IllegalStateException("Provider immutable token exceeds the exact Root proof cap");
        }
        return new ProviderVersionProof(mode, token);
    }

    private ObjectWalLeafKeyV1 requireNwg1LeafIdentity(ObjectIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        ObjectWalLeafKeyV1 leaf = ObjectWalLeafKeyV1.parseFull(root.providerConfiguration(), identity.key());
        if (leaf.bodyLength() != identity.bodyLength() || !leaf.objectSha256().equals(identity.bodySha256())) {
            throw new IllegalArgumentException("NWG1 Object identity differs from its exact Root-bound leaf");
        }
        return leaf;
    }

    private static void requirePrefixIdentity(Nwg1ObjectReaderV1.AuthenticatedPrefix prefix, ObjectWalLeafKeyV1 leaf) {
        if (prefix.header().laneId() != leaf.laneId().code()
                || prefix.header().laneSequence() != leaf.laneSequence()
                || prefix.header().directoryPrefixEnd() != leaf.directoryPrefixEnd()
                || prefix.header().canonicalBodyLength() != leaf.bodyLength()) {
            throw new IllegalArgumentException("authenticated NWG1 prefix differs from the exact Object leaf");
        }
    }

    private void requirePrefixWithinRootCaps(Nwg1ObjectReaderV1.AuthenticatedPrefix prefix) {
        Nwg1RootAdmissionCaps caps = root.nwg1AdmissionCaps();
        if (prefix.header().canonicalBodyLength() > caps.maxCanonicalBodyBytes()
                || prefix.header().directoryPrefixEnd() > caps.maxDirectoryPrefixBytes()
                || prefix.header().directoryPlaintextLength() > caps.maxDirectoryPlaintextBytes()
                || prefix.directory().bindings().size() > caps.maxBindingContexts()
                || prefix.directory().appendUnits().size() > caps.maxAppendUnits()
                || prefix.directory().frames().size() > caps.maxFrames()
                || prefix.header().actualPayloadBytesAtPlanSeal() > caps.maxTotalDecodedPayloadBytes()) {
            throw new IllegalArgumentException("authenticated NWG1 Directory exceeds the exact Root caps");
        }
        long[] decodedPerUnit = new long[prefix.directory().appendUnits().size()];
        long decodedTotal = 0;
        for (com.nereusstream.storage.object.nwg1.Nwg1DirectoryV1.Frame frame :
                prefix.directory().frames()) {
            if (frame.decodedPayloadBytes() > caps.maxDecodedFrameBytes()
                    || frame.storedBlockBytes() > caps.maxStoredFrameBytes()
                    || frame.storedBlockBytes() > root.providerConfiguration().maxSingleRangeReadBytes()) {
                throw new IllegalArgumentException("authenticated NWG1 frame exceeds the exact Root caps");
            }
            int unitOrdinal = Math.toIntExact(frame.appendUnitOrdinal());
            decodedPerUnit[unitOrdinal] = Math.addExact(decodedPerUnit[unitOrdinal], frame.decodedPayloadBytes());
            decodedTotal = Math.addExact(decodedTotal, frame.decodedPayloadBytes());
        }
        for (long decodedBytes : decodedPerUnit) {
            if (decodedBytes > caps.maxDecodedAppendUnitBytes()) {
                throw new IllegalArgumentException("authenticated NWG1 append unit exceeds the exact Root caps");
            }
        }
        if (decodedTotal != prefix.header().actualPayloadBytesAtPlanSeal()
                || decodedTotal > caps.maxTotalDecodedPayloadBytes()) {
            throw new IllegalArgumentException("authenticated NWG1 decoded total differs from the exact Root caps");
        }
    }

    private void requireOpen() {
        if (state != State.OPEN) {
            throw new IllegalStateException("WalRun Object session does not accept new work: " + state);
        }
    }

    private void requireUsable() {
        if (state == State.CLOSED) {
            throw new IllegalStateException("WalRun Object session is closed");
        }
    }

    /** Stops new admission while preserving KMS keys and existing Provider unknown operations for reconciliation. */
    public synchronized void drain() {
        if (state == State.CLOSED || state == State.DRAINING) {
            return;
        }
        if (runtime.state() == WalRunRuntime.State.ADMITTING) {
            runtime.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
        }
        provider.drain();
        state = State.DRAINING;
    }

    public synchronized State state() {
        return state;
    }

    /** Exact immutable Root already verified by this owner; recovery adapters must not reload or recharge it. */
    public synchronized WalRunRootRecord rootRecord() {
        requireUsable();
        return root;
    }

    /** Fail-closed guard for owner-open recovery entry points that must continue a lineage-carried live budget. */
    public synchronized void requireRecoveredCurrentRoot() {
        requireUsable();
        if (!recoveredCurrentRoot) {
            throw new IllegalStateException(
                    "owner-open recovery requires a WalRun session restored from RecoveredLineage");
        }
    }

    /** Exact Root authority for checkpoint proof projection; adapters must not restate this mode. */
    public synchronized ProviderProofMode providerProofMode() {
        requireUsable();
        return root.providerConfiguration().proofMode();
    }

    public static final class ValidatedNwg1Plan {
        private final WalRunObjectSession owner;
        private final GroupEncodingPlanV1 plan;
        private final Nwg1VerificationContextV1 verificationContext;
        private final ImmutableExtentPlan immutablePlan;
        private final GroupEncodingPlanV1.AdmissionFacts facts;
        private LaneSequenceReservation reservation;
        private AdmittedNwg1Candidate candidate;

        private ValidatedNwg1Plan(
                WalRunObjectSession owner,
                GroupEncodingPlanV1 plan,
                Nwg1VerificationContextV1 verificationContext,
                ImmutableExtentPlan immutablePlan,
                GroupEncodingPlanV1.AdmissionFacts facts) {
            this.owner = owner;
            this.plan = plan;
            this.verificationContext = verificationContext;
            this.immutablePlan = immutablePlan;
            this.facts = facts;
        }

        public Sha256Digest canonicalPlanSha256() {
            return immutablePlan.canonicalPlanSha256();
        }

        public long canonicalBodyBytes() {
            return facts.canonicalBodyBytes();
        }

        public Optional<LaneSequenceReservation> sequenceEffect() {
            return Optional.ofNullable(reservation);
        }
    }

    /**
     * Unforgeable one-shot capability used only to bind a KMS lease to this exact WalRun owner. The constructor is
     * private and no session API exposes an instance.
     */
    public static final class KmsOwnerAuthority {
        private final WalRunObjectSession owner;
        private final RunKeyCacheIdentity runKeyIdentity;
        private final WrappedRunKeyEnvelope wrappedRunKey;
        private final Sha256Digest rootSha256;
        private final CellProviderScopeId providerScopeId;

        private KmsOwnerAuthority(
                WalRunObjectSession owner,
                RunKeyCacheIdentity runKeyIdentity,
                WrappedRunKeyEnvelope wrappedRunKey,
                Sha256Digest rootSha256,
                CellProviderScopeId providerScopeId) {
            this.owner = Objects.requireNonNull(owner, "owner");
            this.runKeyIdentity = Objects.requireNonNull(runKeyIdentity, "runKeyIdentity");
            this.wrappedRunKey = Objects.requireNonNull(wrappedRunKey, "wrappedRunKey");
            this.rootSha256 = Objects.requireNonNull(rootSha256, "rootSha256");
            this.providerScopeId = Objects.requireNonNull(providerScopeId, "providerScopeId");
        }

        public RunKeyCacheIdentity runKeyIdentity() {
            requireLiveOwner();
            return runKeyIdentity;
        }

        public WrappedRunKeyEnvelope wrappedRunKey() {
            requireLiveOwner();
            return wrappedRunKey;
        }

        public Sha256Digest rootSha256() {
            requireLiveOwner();
            return rootSha256;
        }

        public CellProviderScopeId providerScopeId() {
            requireLiveOwner();
            return providerScopeId;
        }

        private void requireLiveOwner() {
            if (owner.kms != null) {
                throw new IllegalStateException("KMS owner authority has already been consumed");
            }
        }
    }

    /** Unforgeable one-shot capability binding a raw C1 Provider to this exact WalRun owner. */
    public static final class ProviderOwnerAuthority {
        private final WalRunObjectSession owner;
        private final CellProviderScopeId providerScopeId;
        private final String exclusiveNamespacePrefix;
        private final long admittedMaximumObjectBytes;
        private final int admittedMaximumPrefixBytes;

        private ProviderOwnerAuthority(
                WalRunObjectSession owner,
                CellProviderScopeId providerScopeId,
                String exclusiveNamespacePrefix,
                long admittedMaximumObjectBytes,
                int admittedMaximumPrefixBytes) {
            this.owner = Objects.requireNonNull(owner, "owner");
            this.providerScopeId = Objects.requireNonNull(providerScopeId, "providerScopeId");
            this.exclusiveNamespacePrefix =
                    Objects.requireNonNull(exclusiveNamespacePrefix, "exclusiveNamespacePrefix");
            this.admittedMaximumObjectBytes = admittedMaximumObjectBytes;
            this.admittedMaximumPrefixBytes = admittedMaximumPrefixBytes;
        }

        public CellProviderScopeId providerScopeId() {
            requireLiveOwner();
            return providerScopeId;
        }

        public String exclusiveNamespacePrefix() {
            requireLiveOwner();
            return exclusiveNamespacePrefix;
        }

        public long admittedMaximumObjectBytes() {
            requireLiveOwner();
            return admittedMaximumObjectBytes;
        }

        public int admittedMaximumPrefixBytes() {
            requireLiveOwner();
            return admittedMaximumPrefixBytes;
        }

        public Sha256Digest rootSha256() {
            requireLiveOwner();
            return owner.rootSha256;
        }

        private void requireLiveOwner() {
            if (owner.provider != null) {
                throw new IllegalStateException("Provider owner authority has already been consumed");
            }
        }
    }

    public static final class AdmittedNwg1Candidate {
        private final WalRunObjectSession owner;
        private final ValidatedNwg1Plan validatedPlan;
        private final LaneSequenceReservation reservation;
        private final Nwg1SealedObjectV1 sealed;
        private final ObjectIdentity identity;
        private int conditionalCreateAttempts;
        private boolean terminal;

        private AdmittedNwg1Candidate(
                WalRunObjectSession owner,
                ValidatedNwg1Plan validatedPlan,
                LaneSequenceReservation reservation,
                Nwg1SealedObjectV1 sealed,
                ObjectIdentity identity) {
            this.owner = owner;
            this.validatedPlan = validatedPlan;
            this.reservation = reservation;
            this.sealed = sealed;
            this.identity = identity;
        }

        public LaneSequenceReservation reservation() {
            return reservation;
        }

        public Nwg1SealedObjectV1 sealed() {
            return sealed;
        }

        public ObjectIdentity identity() {
            return identity;
        }
    }

    /** Unforgeable shared publication proof retained only until the exact candidate becomes terminal. */
    public static final class AuthenticatedNwg1PublicationExtent {
        private final WalRunObjectSession owner;
        private final AdmittedNwg1Candidate candidate;
        private final Nwg1ObjectReaderV1.AuthenticatedPrefix prefix;
        private final byte[] exactBody;
        private final ProviderVersionProof providerProof;

        private AuthenticatedNwg1PublicationExtent(
                WalRunObjectSession owner,
                AdmittedNwg1Candidate candidate,
                Nwg1ObjectReaderV1.AuthenticatedPrefix prefix,
                byte[] exactBody,
                ProviderVersionProof providerProof) {
            this.owner = Objects.requireNonNull(owner, "owner");
            this.candidate = Objects.requireNonNull(candidate, "candidate");
            this.prefix = Objects.requireNonNull(prefix, "prefix");
            this.exactBody = Objects.requireNonNull(exactBody, "exactBody");
            this.providerProof = Objects.requireNonNull(providerProof, "providerProof");
        }

        public ObjectIdentity identity() {
            return candidate.identity;
        }

        public Nwg1ObjectReaderV1.AuthenticatedPrefix authenticatedPrefix() {
            return prefix;
        }

        public ProviderVersionProof providerProof() {
            return providerProof;
        }
    }

    public static final class ValidatedKafkaProtocolObject {
        private final WalRunObjectSession owner;
        private final ObjectIdentity identity;
        private final CanonicalBytes body;
        private int conditionalCreateAttempts;

        private ValidatedKafkaProtocolObject(WalRunObjectSession owner, ObjectIdentity identity, CanonicalBytes body) {
            this.owner = owner;
            this.identity = identity;
            this.body = CanonicalBytes.copyOf(body.toByteArray());
        }

        public ObjectIdentity identity() {
            return identity;
        }
    }

    public static final class Nwg1AdmissionFailure extends IllegalStateException {
        private final Optional<LaneSequenceReservation> sequenceEffect;
        private final Sha256Digest canonicalPlanSha256;

        private Nwg1AdmissionFailure(
                Optional<LaneSequenceReservation> sequenceEffect,
                Sha256Digest canonicalPlanSha256,
                RuntimeException cause) {
            super("NWG1 admission/seal failed", cause);
            this.sequenceEffect = Objects.requireNonNull(sequenceEffect, "sequenceEffect");
            this.canonicalPlanSha256 = Objects.requireNonNull(canonicalPlanSha256, "canonicalPlanSha256");
        }

        public Optional<LaneSequenceReservation> sequenceEffect() {
            return sequenceEffect;
        }

        public Sha256Digest canonicalPlanSha256() {
            return canonicalPlanSha256;
        }
    }

    private record CanonicalCandidateBody(ObjectIdentity identity, CanonicalBytes bytes)
            implements RepeatableObjectBody {
        private CanonicalCandidateBody {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(bytes, "bytes");
        }

        @Override
        public InputStream openStream() {
            return new ByteArrayInputStream(bytes.toByteArray());
        }
    }

    @Override
    public synchronized void close() {
        if (state == State.CLOSED) {
            return;
        }
        drain();
        if (provider.acceptedOperations() != 0 || provider.unknownObjectCount() != 0) {
            throw new IllegalStateException(
                    "WalRun Object session retains Provider operations; reconcile before close");
        }
        if (recoveredCut != null && !recoveredRowsReleased) {
            recoveredCut.discardUnconsumedPhysicalRows();
            recovery.finishRecoveredPhysicalRows();
            recoveredRowsReleased = true;
        }
        provider.close();
        kms.close();
        state = State.CLOSED;
    }
}
