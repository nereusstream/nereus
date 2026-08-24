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

import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeLimits;
import java.util.Objects;
import java.util.Optional;

/** Bounded immutable control-metadata value for one Object WalRun. */
public record WalRunRootRecord(
        int shardId,
        long shardRunEpoch,
        Id128 walRunSessionId,
        long openedAtMillis,
        ProtocolCellIdentity protocolCellIdentity,
        CellProviderScopeId providerScopeId,
        WalRunFormatContractV1 formatContract,
        Nwg1RootAdmissionCaps nwg1AdmissionCaps,
        WalRunBounds bounds,
        WalCheckpointPolicy checkpointPolicy,
        ObjectProviderRootConfiguration providerConfiguration,
        RecoveryEnvelopeLimits recoveryEnvelope,
        WrappedRunKeyEnvelope wrappedRunKey,
        Optional<WalRunPredecessor> predecessor) {
    private static final long MAX_KAFKA_PROTOCOL_CHECKPOINT_BYTES = 64L * 1024 * 1024;
    private static final long MAX_PROVIDER_OBJECT_KEY_BYTES = 1024;
    /**
     * M3 materializes the sealed NWG1 Object in byte[] on its writer/readback paths.  The v1 4-GiB value remains a
     * format-hard ceiling, but a Root may not admit a body the current implementation cannot represent before it
     * allocates a sequence.  This matches the 64-MiB real-MinIO D2 contract target; D1 accounting is not transfer
     * evidence and does not widen this implementation cap.
     */
    public static final long IMPLEMENTATION_MAX_CANONICAL_BODY_BYTES = 64L * 1024 * 1024;

    private static final long MAX_CONTROL_METADATA_BYTES = 1024L * 1024;
    private static final long PROOF_NONE_CHECKPOINT_ROW_BYTES =
            WalRunControlCodec.proofNoneCheckpointRowCanonicalLength();

    public WalRunRootRecord {
        if (shardId < 0 || shardRunEpoch < 0 || openedAtMillis < 0) {
            throw new IllegalArgumentException("shard identity and run epoch must be non-negative");
        }
        Objects.requireNonNull(walRunSessionId, "walRunSessionId");
        if (walRunSessionId.isZero()) {
            throw new IllegalArgumentException("WalRun session ID must be non-zero");
        }
        Objects.requireNonNull(protocolCellIdentity, "protocolCellIdentity");
        Objects.requireNonNull(providerScopeId, "providerScopeId");
        Objects.requireNonNull(formatContract, "formatContract");
        Objects.requireNonNull(nwg1AdmissionCaps, "nwg1AdmissionCaps");
        Objects.requireNonNull(bounds, "bounds");
        Objects.requireNonNull(checkpointPolicy, "checkpointPolicy");
        Objects.requireNonNull(providerConfiguration, "providerConfiguration");
        Objects.requireNonNull(recoveryEnvelope, "recoveryEnvelope");
        Objects.requireNonNull(wrappedRunKey, "wrappedRunKey");
        Objects.requireNonNull(predecessor, "predecessor");
        if (nwg1AdmissionCaps.maxCanonicalBodyBytes() > IMPLEMENTATION_MAX_CANONICAL_BODY_BYTES) {
            throw new IllegalArgumentException(
                    "Root body cap exceeds the M3 byte-array implementation cap; 4 GiB remains format-only");
        }
        if (nwg1AdmissionCaps.maxCanonicalBodyBytes() > providerConfiguration.maxObjectBodyBytes()
                || nwg1AdmissionCaps.maxCanonicalBodyBytes() > providerConfiguration.maxSinglePutBytes()) {
            throw new IllegalArgumentException("Root NWG1 body cap exceeds the Provider single-PUT contract");
        }
        if (providerConfiguration.maxSingleRangeReadBytes() < nwg1AdmissionCaps.maxStoredFrameBytes()) {
            throw new IllegalArgumentException("Provider range cap cannot read one maximum admitted stored frame");
        }
        long prefixSegments = Math.floorDiv(
                Math.addExact(
                        nwg1AdmissionCaps.maxDirectoryPrefixBytes(),
                        (long) providerConfiguration.maxSingleRangeReadBytes() - 1),
                providerConfiguration.maxSingleRangeReadBytes());
        if (prefixSegments > providerConfiguration.maxPrefixSegmentsPerExtent()) {
            throw new IllegalArgumentException("Provider prefix-segment contract cannot read the admitted directory");
        }
        if (bounds.maxCanonicalBodyBytes() < nwg1AdmissionCaps.maxCanonicalBodyBytes()) {
            throw new IllegalArgumentException(
                    "aggregate WalRun body bound cannot be smaller than one admitted Object");
        }
        if (bounds.maxExtentCount() > Integer.MAX_VALUE / PROOF_NONE_CHECKPOINT_ROW_BYTES) {
            throw new IllegalArgumentException(
                    "WalRun extent cap exceeds the M3 fixed physical-row spool array domain");
        }
        if (checkpointPolicy.maxUncheckpointedBytes() < nwg1AdmissionCaps.maxCanonicalBodyBytes()) {
            throw new IllegalArgumentException(
                    "checkpoint uncheckpointed-byte bound cannot admit one maximum NWG1 Object");
        }
        if (bounds.maxRecoverablePredecessorRuns() > recoveryEnvelope.maxPredecessorRuns()
                || Math.addExact(bounds.maxRecoverablePredecessorRuns(), 1) > recoveryEnvelope.maxLiveRoots()) {
            throw new IllegalArgumentException("WalRun lineage bounds exceed the persisted recovery envelope");
        }
        if (providerConfiguration.accessProfile() == ObjectProviderAccessProfile.C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST
                && recoveryEnvelope.maxHeadRequests() != 0) {
            throw new IllegalArgumentException("C1 recovery contract requires zero Provider HEAD requests");
        }
        requireWorstCaseRecoveryClosure(
                protocolCellIdentity.protocolKind(),
                bounds,
                nwg1AdmissionCaps,
                checkpointPolicy,
                providerConfiguration,
                recoveryEnvelope);
        predecessor.ifPresent(value -> {
            if (value.root().shardId() != shardId || value.root().shardRunEpoch() >= shardRunEpoch) {
                throw new IllegalArgumentException("predecessor must be an older run of the same shard");
            }
            if (protocolCellIdentity.protocolKind() == ProtocolKindV1.PULSAR
                    && value.terminalProtocolCheckpoint().isPresent()) {
                throw new IllegalArgumentException(
                        "a Pulsar M3 successor Root must omit the unassigned terminal protocol Head binding");
            }
            value.terminalProtocolCheckpoint().ifPresent(binding -> {
                if (binding.protocolKind() != protocolCellIdentity.protocolKind()) {
                    throw new IllegalArgumentException(
                            "predecessor terminal protocol checkpoint kind differs from the Protocol Cell");
                }
            });
            if (protocolCellIdentity.protocolKind() == ProtocolKindV1.KAFKA
                    && value.terminalProtocolCheckpoint().isEmpty()) {
                throw new IllegalArgumentException(
                        "a Kafka successor Root requires the exact terminal Kafka protocol Head binding");
            }
        });
    }

    /** M3 production admission is deliberately narrower than the preserved NWR1 proof-mode wire. */
    public void requireM3ProductionProviderProofMode() {
        if (providerConfiguration.proofMode() != ProviderProofMode.NONE) {
            throw new IllegalStateException(
                    "M3 production Root admission requires ProviderProofMode.NONE; VERSION wire is reserved");
        }
    }

    private static void requireWorstCaseRecoveryClosure(
            ProtocolKindV1 protocolKind,
            WalRunBounds bounds,
            Nwg1RootAdmissionCaps caps,
            WalCheckpointPolicy checkpointPolicy,
            ObjectProviderRootConfiguration provider,
            RecoveryEnvelopeLimits recovery) {
        long extentCount = bounds.maxExtentCount();
        long protocolObjectCount = protocolKind == ProtocolKindV1.KAFKA ? 1 : 0;
        // reserveRemainingList() temporarily charges at least one key for an empty terminal probe and then
        // settles it back to zero. One reusable slot is therefore required in addition to the maximum live
        // inventory; otherwise a first lane containing all M extents can strand the two empty lane probes.
        long reusableLaneProbeCount = 1;
        long requiredListedKeys =
                Math.addExact(Math.addExact(extentCount, reusableLaneProbeCount), protocolObjectCount);
        long requiredRangeGetRequests = Math.multiplyExact(extentCount, Math.addExact((long) caps.maxFrames(), 1L));
        if (recovery.maxRangeGetRequests() < requiredRangeGetRequests
                || recovery.maxListedKeys() < requiredListedKeys) {
            throw new IllegalArgumentException(
                    "recovery request/key envelope cannot cover every admitted extent and protocol Object");
        }
        long pendingCandidateCount = Math.min(3L, extentCount);
        long completeProtocolObjectCount = Math.multiplyExact(
                protocolObjectCount, Math.addExact((long) bounds.maxRecoverablePredecessorRuns(), 1L));
        long requiredFullGets = Math.addExact(pendingCandidateCount, completeProtocolObjectCount);
        if (recovery.maxFullGetRequests() < requiredFullGets) {
            throw new IllegalArgumentException(
                    "recovery full-GET envelope cannot reconcile every lazy-lane candidate and protocol Object");
        }
        long listDataPages = ceilingDivide(extentCount, provider.maxListPageKeys());
        // The lane term is exact for three permanent prefixes. Kafka additionally reserves one exact-key LIST for one
        // unresolved content-addressed NWKCP1 candidate; family-prefix inventory is forbidden.
        long requiredListPages = Math.addExact(Math.addExact(listDataPages, 2), protocolObjectCount);
        if (recovery.maxListPages() < requiredListPages) {
            throw new IllegalArgumentException(
                    "recovery LIST-page envelope cannot cover all lazy lanes and the protocol Object");
        }
        long maximumLeafKeyBytes = ObjectWalLeafKeyV1.maximumFullKeyBytes(provider);
        long requiredListedKeyBytes = Math.addExact(
                Math.multiplyExact(Math.addExact(extentCount, reusableLaneProbeCount), maximumLeafKeyBytes),
                Math.multiplyExact(protocolObjectCount, MAX_PROVIDER_OBJECT_KEY_BYTES));
        if (recovery.maxListedKeyBytes() < requiredListedKeyBytes) {
            throw new IllegalArgumentException("recovery listed-key byte envelope cannot cover every extent key");
        }
        // Pointer/current Root bootstrap plus the complete retained predecessor control closure.  Roots themselves
        // are covered by maxLiveRoots; every predecessor additionally retains one Seal, final physical Head,
        // bounded physical page chain, and (Kafka only) terminal protocol Head.
        long pointerControlBytes = MAX_CONTROL_METADATA_BYTES;
        long rootControlBytes = Math.multiplyExact(recovery.maxLiveRoots(), MAX_CONTROL_METADATA_BYTES);
        long maxCheckpointPagesPerPredecessor = ceilingDivide(extentCount, checkpointPolicy.maxRowsPerPage());
        long checkpointPageBytes =
                Math.multiplyExact(maxCheckpointPagesPerPredecessor, checkpointPolicy.maxCanonicalPageBytes());
        long perPredecessorControlBytes = Math.addExact(
                Math.addExact(MAX_CONTROL_METADATA_BYTES, MAX_CONTROL_METADATA_BYTES),
                Math.addExact(
                        checkpointPageBytes, Math.multiplyExact(protocolObjectCount, MAX_CONTROL_METADATA_BYTES)));
        long retainedPredecessorControlBytes =
                Math.multiplyExact(bounds.maxRecoverablePredecessorRuns(), perPredecessorControlBytes);
        long currentPhysicalControlBytes = Math.addExact(MAX_CONTROL_METADATA_BYTES, checkpointPageBytes);
        long currentProtocolControlBytes = Math.multiplyExact(protocolObjectCount, MAX_CONTROL_METADATA_BYTES);
        // Prefix plus every selected-frame range is bounded by the aggregate canonical body bytes admitted to the
        // run, not by M times the per-Object cap.
        long rangeRecoveryBytes = bounds.maxCanonicalBodyBytes();
        long pendingCandidateBytes = Math.multiplyExact(pendingCandidateCount, caps.maxCanonicalBodyBytes());
        long singleProtocolCheckpointBytes = Math.multiplyExact(
                protocolObjectCount, Math.min(MAX_KAFKA_PROTOCOL_CHECKPOINT_BYTES, provider.maxObjectBodyBytes()));
        long protocolCheckpointBytes = Math.multiplyExact(
                Math.addExact((long) bounds.maxRecoverablePredecessorRuns(), 1L), singleProtocolCheckpointBytes);
        long requiredCanonicalBytes = Math.addExact(
                Math.addExact(
                        Math.addExact(
                                Math.addExact(pointerControlBytes, rootControlBytes),
                                Math.addExact(
                                        retainedPredecessorControlBytes,
                                        Math.addExact(currentPhysicalControlBytes, currentProtocolControlBytes))),
                        rangeRecoveryBytes),
                Math.addExact(pendingCandidateBytes, protocolCheckpointBytes));
        if (recovery.maxCanonicalBodyBytes() < requiredCanonicalBytes) {
            throw new IllegalArgumentException(
                    "recovery canonical-byte envelope cannot cover current-pointer retained lineage/control closure");
        }
        if (recovery.maxDecodedContexts() < Math.multiplyExact(extentCount, caps.maxBindingContexts())
                || recovery.maxDecodedFrames() < Math.multiplyExact(extentCount, caps.maxFrames())
                || recovery.maxDecodedCommitSets() < Math.multiplyExact(extentCount, caps.maxAppendUnits())) {
            throw new IllegalArgumentException("recovery decode envelope cannot cover the maximum admitted inventory");
        }
        // Owner-open holds one exact proof-NONE checkpoint-row spool while it streams exactly one physical page,
        // authenticated prefix, frame, or complete LIST materialization through the same composite lease. Kafka
        // protocol Objects are verified earlier in the lineage pass and are therefore a sequential, not additive,
        // working set. No M-row object graph or second physical inventory is admitted.
        long compactSpoolBytes = Math.multiplyExact(extentCount, PROOF_NONE_CHECKPOINT_ROW_BYTES);
        long physicalTransientBytes = Math.max(
                checkpointPolicy.maxCanonicalPageBytes(),
                Math.max(
                        caps.maxDirectoryPrefixBytes(),
                        Math.max(
                                Math.addExact(caps.maxDecodedFrameBytes(), 256),
                                Math.max(caps.maxStoredFrameBytes(), requiredListedKeyBytes))));
        long physicalWorkingMemory = Math.addExact(compactSpoolBytes, physicalTransientBytes);
        long minimumWorkingMemory = Math.max(physicalWorkingMemory, singleProtocolCheckpointBytes);
        if (recovery.maxWorkingMemoryBytes() < minimumWorkingMemory || recovery.maxConcurrency() < 1) {
            throw new IllegalArgumentException(
                    "recovery memory/concurrency cannot hold compact spool plus one live stream");
        }
    }

    private static long ceilingDivide(long value, long divisor) {
        return Math.floorDiv(Math.addExact(value, divisor - 1), divisor);
    }
}
