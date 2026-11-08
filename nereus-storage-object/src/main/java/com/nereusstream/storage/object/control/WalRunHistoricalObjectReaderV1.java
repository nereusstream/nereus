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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.recovery.WalRunLineageRecovery;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/** Read-only C1 ranges from a retained Root whose exact Seal was verified by the current lineage. */
public final class WalRunHistoricalObjectReaderV1 implements AutoCloseable {
    private final WalRunRootRecord root;
    private final WalRunSealRecord seal;
    private final C1ObjectProviderSession provider;
    private final KmsCellSession.HistoricalReadLease kms;
    private boolean closed;

    public WalRunHistoricalObjectReaderV1(
            WalRunLineageRecovery.RecoveredLineage lineage,
            WalRunReference reference,
            C1ObjectProviderSession provider,
            KmsCellSession kms) {
        var selected = lineage.requireSealedPredecessor(reference);
        this.root = selected.root();
        this.seal = selected.seal();
        this.provider = Objects.requireNonNull(provider, "provider");
        root.requireM3ProductionProviderProofMode();
        var config = root.providerConfiguration();
        if (provider.state() != C1ObjectProviderSession.State.OPEN
                || config.accessProfile() != ObjectProviderAccessProfile.C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST
                || !provider.providerScopeId().equals(root.providerScopeId())
                || !provider.exclusiveNamespacePrefix().equals(config.exclusiveNamespacePrefix())
                || provider.admittedMaximumObjectBytes() != config.maxObjectBodyBytes()
                || provider.admittedMaximumPrefixBytes()
                        != root.nwg1AdmissionCaps().maxDirectoryPrefixBytes()) {
            throw new IllegalArgumentException("historical Object reader Provider differs from the sealed Root");
        }
        this.kms = Objects.requireNonNull(kms, "kms").openHistoricalReader(lineage, reference);
    }

    public WalRunRootRecord rootRecord() {
        return root;
    }

    public Sha256Digest rootSha256() {
        return seal.root().rootSha256();
    }

    /** Caller holds durable source protection and consumes no bytes until the complete unit is verified. */
    public synchronized Nwg1ObjectReaderV1.VerifiedAppendUnit readAppendUnit(
            ObjectIdentity identity,
            Nwg1VerificationContextV1 verification,
            long selectedFrameOrdinal,
            Nwg1ObjectReaderV1.VerifiedFrameConsumer consumer)
            throws IOException {
        if (closed) {
            throw new IllegalStateException("historical Object reader is closed");
        }
        var leaf = WalRunObjectSession.requireNwg1LeafIdentity(root, identity);
        if (leaf.laneSequence() > seal.terminalSequence().get(leaf.laneId())) {
            throw new IllegalArgumentException("historical Object locator lies beyond the sealed lane");
        }
        var bytes = provider.readDirectoryPrefix(identity, leaf.directoryPrefixEnd(), Optional.empty());
        return kms.readAppendUnit(
                bytes.toByteArray(),
                identity.bodyLength(),
                verification,
                prefix -> {
                    WalRunObjectSession.requirePrefixIdentity(prefix, leaf);
                    WalRunObjectSession.requirePrefixWithinRootCaps(root, prefix);
                },
                (range, ignored) -> provider.readExactRange(
                                identity, range.inclusiveStart(), range.exclusiveEnd(), Optional.empty())
                        .toByteArray(),
                selectedFrameOrdinal,
                Objects.requireNonNull(consumer, "consumer"));
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            provider.close();
            kms.close();
            closed = true;
        }
    }
}
