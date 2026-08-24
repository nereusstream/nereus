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

package com.nereusstream.storage.object.recovery;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunRuntime;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import java.io.IOException;
import java.util.Objects;

/** The sole public owner-open assembler; lineage, Cell transfers, tail fold, staging and install share one fence. */
public final class OwnerOpenRecoveryCoordinator {
    private OwnerOpenRecoveryCoordinator() {}

    public static WalRunObjectSession recoverUnderDurableFence(
            ProtocolOwnerFenceExecutor fenceExecutor,
            ProtocolCellIdentity protocolCell,
            WalRunReference rootReference,
            WalRunRootRecord root,
            CanonicalControlMetadataStore metadata,
            C1ObjectProviderSession rawProvider,
            KmsCellSession rawKms,
            WalRunLineageRecovery.RecoveredLineage recoveredLineage,
            ProtocolRecoveryHandler protocolRecoveryHandler)
            throws IOException {
        Objects.requireNonNull(fenceExecutor, "fenceExecutor");
        Objects.requireNonNull(protocolCell, "protocolCell");
        Objects.requireNonNull(rootReference, "rootReference");
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(rawProvider, "rawProvider");
        Objects.requireNonNull(rawKms, "rawKms");
        Objects.requireNonNull(recoveredLineage, "recoveredLineage");
        Objects.requireNonNull(protocolRecoveryHandler, "protocolRecoveryHandler");
        requireExactRoot(protocolCell, rootReference, root);

        WalRunObjectSession recovered = fenceExecutor.withDurableOwnerFence(
                protocolCell,
                rootReference,
                rootReference.rootSha256(),
                verificationContext -> recoverInsideFence(
                        rootReference,
                        root,
                        metadata,
                        rawProvider,
                        rawKms,
                        recoveredLineage,
                        verificationContext,
                        protocolRecoveryHandler));
        requireExactStoppingSession(rootReference, recovered);
        return recovered;
    }

    private static WalRunObjectSession recoverInsideFence(
            WalRunReference rootReference,
            WalRunRootRecord root,
            CanonicalControlMetadataStore metadata,
            C1ObjectProviderSession rawProvider,
            KmsCellSession rawKms,
            WalRunLineageRecovery.RecoveredLineage recoveredLineage,
            Nwg1VerificationContextV1 verificationContext,
            ProtocolRecoveryHandler protocolRecoveryHandler)
            throws IOException {
        OwnerOpenRecoveryLeasePair recoveryLeases = null;
        RecoveredWalRunRuntimeCut cut = null;
        WalRunObjectSession session = null;
        Throwable failure = null;
        try {
            recoveryLeases = OwnerOpenRecoveryLeasePair.acquire(root, recoveredLineage, rawProvider, rawKms);
            cut = BoundedObjectTailRecovery.recoverCurrentRuntimeCut(
                    root,
                    metadata,
                    recoveryLeases,
                    recoveredLineage,
                    verificationContext,
                    protocolRecoveryHandler::stage);
            session = WalRunObjectSession.restore(root, cut);
            requireExactStoppingSession(rootReference, session);
            protocolRecoveryHandler.install(session);
            return session;
        } catch (IOException | RuntimeException | Error caught) {
            failure = caught;
            throw caught;
        } finally {
            if (failure != null) {
                try {
                    protocolRecoveryHandler.abort();
                } catch (RuntimeException abortFailure) {
                    failure.addSuppressed(abortFailure);
                }
                try {
                    if (session != null) {
                        session.close();
                    } else if (cut != null) {
                        cut.abortBeforeSessionInstall();
                    } else if (recoveryLeases != null) {
                        recoveryLeases.close();
                    }
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
    }

    private static void requireExactRoot(
            ProtocolCellIdentity protocolCell, WalRunReference reference, WalRunRootRecord root) {
        if (!root.protocolCellIdentity().equals(protocolCell)
                || root.shardId() != reference.shardId()
                || root.shardRunEpoch() != reference.shardRunEpoch()
                || !WalRunControlCodec.rootSha256(root).equals(reference.rootSha256())) {
            throw new IllegalArgumentException("owner-open Root differs from the exact durable-fence reference");
        }
    }

    private static void requireExactStoppingSession(WalRunReference rootReference, WalRunObjectSession recovered) {
        if (recovered == null
                || recovered.runtimeState() != WalRunRuntime.State.STOPPING
                || !recovered.rootSha256().equals(rootReference.rootSha256())) {
            throw new IllegalStateException(
                    "durable owner-fence recovery did not return the exact stopping Root session");
        }
        recovered.requireRecoveredCurrentRoot();
    }

    @FunctionalInterface
    public interface ProtocolOwnerFenceExecutor {
        WalRunObjectSession withDurableOwnerFence(
                ProtocolCellIdentity exactProtocolCell,
                WalRunReference exactRootReference,
                Sha256Digest exactRootSha256,
                FencedRecoveryCallback callback)
                throws IOException;
    }

    @FunctionalInterface
    public interface FencedRecoveryCallback {
        WalRunObjectSession recover(Nwg1VerificationContextV1 exactVerificationContext) throws IOException;
    }

    /**
     * Protocol-local staging is transactional: install runs only after the complete common cut; abort cleans failure.
     */
    public interface ProtocolRecoveryHandler {
        void stage(
                com.nereusstream.storage.object.control.ProviderResolvedExtentRowV1 physicalRow,
                com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1.AuthenticatedPrefix authenticatedPrefix,
                BoundedObjectTailRecovery.SelectedAppendUnitReader appendUnitReader)
                throws IOException;

        void install(WalRunObjectSession recoveredSession) throws IOException;

        void abort();
    }
}
