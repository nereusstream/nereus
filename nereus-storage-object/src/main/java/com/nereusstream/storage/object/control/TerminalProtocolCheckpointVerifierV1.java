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
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.recovery.CumulativeRecoveryBudget;
import java.io.IOException;
import java.util.Objects;

/** Protocol adapter hook that strictly decodes and verifies one exact terminal protocol Head. */
@FunctionalInterface
public interface TerminalProtocolCheckpointVerifierV1 {
    void verifyTerminal(
            WalRunRootRecord predecessorRoot,
            WalRunSealRecord predecessorSeal,
            TerminalProtocolCheckpointBindingV1 binding,
            CanonicalBytes exactTerminalHeadValue,
            RecoveryContext recoveryContext);

    /**
     * The only recovery authority exposed to a protocol terminal verifier.  The budget owner is the current
     * pointer Root; the protocol-object Root is the retained predecessor whose terminal Head is being verified.
     * Protocol adapters must use {@link #readVerifiedProtocolObject(ObjectIdentity)} rather than a backend read so
     * that selected objects remain Root-bound and consume the same cumulative recovery envelope.
     */
    record RecoveryContext(
            WalRunRootRecord budgetOwnerRoot,
            WalRunRootRecord protocolObjectRoot,
            CumulativeRecoveryBudget budget,
            ProtocolObjectRecoveryReaderFactoryV1 readerFactory) {
        public RecoveryContext {
            Objects.requireNonNull(budgetOwnerRoot, "budgetOwnerRoot");
            Objects.requireNonNull(protocolObjectRoot, "protocolObjectRoot");
            Objects.requireNonNull(budget, "budget");
            Objects.requireNonNull(readerFactory, "readerFactory");
        }

        public CanonicalBytes readVerifiedProtocolObject(ObjectIdentity identity) {
            Objects.requireNonNull(identity, "identity");
            // The selected terminal Object belongs to the retained predecessor, but its I/O is charged to the one
            // current-pointer Root budget before the reader can contact a Provider. A failed reader conservatively
            // leaves this exact full-GET/canonical charge consumed, while the transient full-body working set is
            // always released.
            budget.acquireWorkingSet(identity.bodyLength());
            try {
                budget.chargeFullGet(identity.bodyLength());
                return readerFactory.readerFor(protocolObjectRoot).readVerifiedProtocolObject(identity);
            } catch (IOException failure) {
                throw new IllegalStateException("bounded terminal protocol Object recovery failed", failure);
            } finally {
                budget.releaseWorkingSet(identity.bodyLength());
            }
        }
    }

    @FunctionalInterface
    interface ProtocolObjectRecoveryReaderV1 {
        CanonicalBytes readVerifiedProtocolObject(ObjectIdentity identity) throws IOException;

        static ProtocolObjectRecoveryReaderV1 failClosed() {
            return identity -> {
                throw new IllegalStateException("no Root-bound protocol Object recovery reader is installed");
            };
        }
    }

    /** Selects an exact predecessor-Root Provider grammar/capability without replacing the current shared budget. */
    @FunctionalInterface
    interface ProtocolObjectRecoveryReaderFactoryV1 {
        ProtocolObjectRecoveryReaderV1 readerFor(WalRunRootRecord protocolObjectRoot);

        static ProtocolObjectRecoveryReaderFactoryV1 failClosed() {
            return root -> ProtocolObjectRecoveryReaderV1.failClosed();
        }
    }

    static TerminalProtocolCheckpointVerifierV1 failClosed() {
        return (root, seal, binding, value, context) -> {
            throw new IllegalStateException("no protocol adapter is installed to verify the terminal checkpoint Head");
        };
    }
}
