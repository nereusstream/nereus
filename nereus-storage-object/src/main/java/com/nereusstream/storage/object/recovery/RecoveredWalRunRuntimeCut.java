/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.object.recovery;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.control.LaneSequenceVector;
import com.nereusstream.storage.object.control.WalCheckpointChainVerifier;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunRuntime;
import java.util.Objects;

/**
 * Opaque owner-open cut produced only by the common physical-checkpoint and three-lane recovery fold. It retains
 * the sole live lineage budget until a Root-bound Provider/KMS session has completed all readiness checks.
 */
public final class RecoveredWalRunRuntimeCut {
    private final WalRunRootRecord root;
    private final Sha256Digest rootSha256;
    private final WalRunLineageRecovery.RecoveredLineage lineage;
    private final String physicalHeadKey;
    private final CanonicalBytes physicalHeadValue;
    private final Sha256Digest physicalHeadSha256;
    private final WalCheckpointHeadV1 physicalHead;
    private final WalCheckpointChainVerifier.StreamingVerification physicalChain;
    private final LaneSequenceVector resolvedVector;
    private final WalRunRuntime.RecoveredState runtimeState;
    private final OwnerOpenRecoveryLeasePair recoveryLeases;
    private final RecoveredPhysicalRowSpool physicalRows;
    private final CumulativeRecoveryBudget.CompositeWorkingSetLease compositeWorkingSet;
    private CumulativeRecoveryBudget liveBudget;
    private boolean leasesPromoted;
    private boolean sessionInstalled;
    private boolean rowsConsumed;
    private boolean resourcesClosed;

    RecoveredWalRunRuntimeCut(
            WalRunRootRecord root,
            WalRunLineageRecovery.RecoveredLineage lineage,
            String physicalHeadKey,
            CanonicalBytes physicalHeadValue,
            WalCheckpointHeadV1 physicalHead,
            WalCheckpointChainVerifier.StreamingVerification physicalChain,
            LaneSequenceVector resolvedVector,
            WalRunRuntime.RecoveredState runtimeState,
            CumulativeRecoveryBudget liveBudget,
            OwnerOpenRecoveryLeasePair recoveryLeases,
            RecoveredPhysicalRowSpool physicalRows,
            CumulativeRecoveryBudget.CompositeWorkingSetLease compositeWorkingSet) {
        this.root = Objects.requireNonNull(root, "root");
        this.rootSha256 = WalRunControlCodec.rootSha256(root);
        this.lineage = Objects.requireNonNull(lineage, "lineage");
        this.physicalHeadKey = Objects.requireNonNull(physicalHeadKey, "physicalHeadKey");
        this.physicalHeadValue = CanonicalBytes.copyOf(
                Objects.requireNonNull(physicalHeadValue, "physicalHeadValue").toByteArray());
        this.physicalHeadSha256 = Sha256Digest.hash(physicalHeadValue);
        this.physicalHead = Objects.requireNonNull(physicalHead, "physicalHead");
        this.physicalChain = Objects.requireNonNull(physicalChain, "physicalChain");
        this.resolvedVector = Objects.requireNonNull(resolvedVector, "resolvedVector");
        this.runtimeState = Objects.requireNonNull(runtimeState, "runtimeState");
        this.liveBudget = Objects.requireNonNull(liveBudget, "liveBudget");
        this.recoveryLeases = Objects.requireNonNull(recoveryLeases, "recoveryLeases");
        this.physicalRows = Objects.requireNonNull(physicalRows, "physicalRows");
        this.compositeWorkingSet = Objects.requireNonNull(compositeWorkingSet, "compositeWorkingSet");
        if (!physicalHead.rootSha256().equals(rootSha256)
                || physicalHead.shardRunEpoch() != root.shardRunEpoch()
                || !physicalChain.coveredThrough().equals(physicalHead.coveredThrough())
                || !resolvedVector.componentWiseAtLeast(physicalHead.coveredThrough())) {
            throw new IllegalArgumentException("recovered physical checkpoint cut differs from the exact Root/vector");
        }
        lineage.requireConsumableFor(root);
    }

    public String physicalHeadKey() {
        return physicalHeadKey;
    }

    public CanonicalBytes physicalHeadValue() {
        return CanonicalBytes.copyOf(physicalHeadValue.toByteArray());
    }

    public Sha256Digest physicalHeadSha256() {
        return physicalHeadSha256;
    }

    public WalCheckpointHeadV1 physicalHead() {
        return physicalHead;
    }

    public WalCheckpointChainVerifier.StreamingVerification physicalChain() {
        return physicalChain;
    }

    public LaneSequenceVector resolvedVector() {
        return resolvedVector;
    }

    public synchronized void requireConsumableFor(WalRunRootRecord expectedRoot) {
        requireExactRoot(expectedRoot);
        if (liveBudget == null) {
            throw new IllegalStateException("recovered WalRun runtime cut was already consumed");
        }
        lineage.requireConsumableFor(expectedRoot);
    }

    public synchronized WalRunRuntime restoreRuntimeFor(WalRunRootRecord expectedRoot) {
        requireConsumableFor(expectedRoot);
        return WalRunRuntime.restore(expectedRoot, runtimeState);
    }

    /** Only the exact not-yet-published WalRun session can promote the pair that performed this recovery. */
    public synchronized OwnerOpenRecoveryLeasePair.Promoted promoteRecoveryLeases(
            WalRunRootRecord expectedRoot,
            WalRunObjectSession.ProviderOwnerAuthority providerAuthority,
            WalRunObjectSession.KmsOwnerAuthority kmsAuthority) {
        requireConsumableFor(expectedRoot);
        if (leasesPromoted) {
            throw new IllegalStateException("recovered Provider/KMS lease pair was already promoted");
        }
        OwnerOpenRecoveryLeasePair.Promoted promoted = recoveryLeases.promote(providerAuthority, kmsAuthority);
        leasesPromoted = true;
        return promoted;
    }

    /** One-use complete authenticated physical inventory; no metadata or Provider I/O occurs here. */
    public synchronized PhysicalRowsSummary consumeAuthenticatedPhysicalRows(PhysicalRowConsumer consumer)
            throws java.io.IOException {
        Objects.requireNonNull(consumer, "consumer");
        if (!leasesPromoted || !sessionInstalled || rowsConsumed || resourcesClosed) {
            throw new IllegalStateException("recovered physical rows are not owned by one live restored session");
        }
        rowsConsumed = true;
        try {
            physicalRows.consumeAuthenticatedRows(consumer::accept);
            return new PhysicalRowsSummary(
                    physicalHeadKey, physicalHeadSha256, physicalRows.rowCount(), resolvedVector);
        } finally {
            closeRowResources();
        }
    }

    synchronized CumulativeRecoveryBudget consumeBudgetFor(WalRunRootRecord expectedRoot) {
        requireConsumableFor(expectedRoot);
        if (!leasesPromoted) {
            throw new IllegalStateException("Provider/KMS recovery leases must promote before budget transfer");
        }
        CumulativeRecoveryBudget lineageBudget = lineage.consumeFor(expectedRoot);
        if (lineageBudget != liveBudget) {
            throw new IllegalStateException("recovered runtime cut no longer owns the exact live lineage budget");
        }
        liveBudget = null;
        sessionInstalled = true;
        return lineageBudget;
    }

    /** Fenced factory failure before promotion closes both recovery leases and all retained row resources. */
    public synchronized void abortBeforeSessionInstall() {
        if (sessionInstalled) {
            throw new IllegalStateException("installed recovered session cannot be aborted as a recovery cut");
        }
        RuntimeException failure = null;
        try {
            recoveryLeases.close();
        } catch (RuntimeException closeFailure) {
            failure = closeFailure;
        }
        try {
            closeRowResources();
        } catch (RuntimeException closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** Session close before protocol consumption burns the row cut and releases its composite working slot. */
    public synchronized void discardUnconsumedPhysicalRows() {
        if (!sessionInstalled) {
            throw new IllegalStateException("recovered row cut is not installed in a session");
        }
        rowsConsumed = true;
        closeRowResources();
    }

    private void closeRowResources() {
        if (resourcesClosed) {
            return;
        }
        RuntimeException failure = null;
        try {
            physicalRows.close();
        } catch (RuntimeException closeFailure) {
            failure = closeFailure;
        }
        try {
            compositeWorkingSet.close();
        } catch (RuntimeException closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        resourcesClosed = true;
        if (failure != null) {
            throw failure;
        }
    }

    public record PhysicalRowsSummary(
            String physicalHeadKey,
            Sha256Digest physicalHeadSha256,
            long aggregateExtentCount,
            LaneSequenceVector resolvedThrough) {
        public PhysicalRowsSummary {
            Objects.requireNonNull(physicalHeadKey, "physicalHeadKey");
            Objects.requireNonNull(physicalHeadSha256, "physicalHeadSha256");
            Objects.requireNonNull(resolvedThrough, "resolvedThrough");
            if (aggregateExtentCount < 0) {
                throw new IllegalArgumentException("aggregateExtentCount must be non-negative");
            }
        }
    }

    @FunctionalInterface
    public interface PhysicalRowConsumer {
        void accept(com.nereusstream.storage.object.control.ProviderResolvedExtentRowV1 row) throws java.io.IOException;
    }

    private void requireExactRoot(WalRunRootRecord expectedRoot) {
        Objects.requireNonNull(expectedRoot, "expectedRoot");
        if (!root.equals(expectedRoot) || !rootSha256.equals(WalRunControlCodec.rootSha256(expectedRoot))) {
            throw new IllegalArgumentException("recovered runtime cut differs from the exact current Root");
        }
    }

    /** Fail-closed typed tail-fold causes; no incomplete cut can be passed to a production restore. */
    public enum TailDisposition {
        ABSENT_GAP,
        DUPLICATE,
        WRONG_IDENTITY
    }

    public static final class TailRecoveryRejectedException extends IllegalStateException {
        private final TailDisposition disposition;

        TailRecoveryRejectedException(TailDisposition disposition, String message) {
            super(message);
            this.disposition = Objects.requireNonNull(disposition, "disposition");
        }

        public TailDisposition disposition() {
            return disposition;
        }
    }
}
