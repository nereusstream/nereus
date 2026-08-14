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

package com.nereusstream.storage.api.bookkeeper;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;
import java.util.regex.Pattern;

/** Closed, immutable BookKeeper capability/configuration input admitted before a run opens. */
public record BookKeeperCapabilitySnapshotV1(
        CellProviderScopeId providerScopeId,
        String clientSourceCommit,
        Sha256Digest clientArtifactSha256,
        String serverSourceCommit,
        Sha256Digest serverImageManifestSha256,
        BookKeeperProtocolModeV1 protocolMode,
        int clientFrameLimitBytes,
        int serverFrameLimitBytes,
        int maximumAddPayloadBytes,
        boolean explicitEntryIdsSupported,
        int ensembleSize,
        int writeQuorumSize,
        int ackQuorumSize,
        BookKeeperDigestTypeV1 digestType,
        boolean fencingSupported,
        boolean recoverySupported,
        BookKeeperTimeoutClassV1 timeoutClass,
        String credentialIdentityVersion,
        Sha256Digest configurationDigest) {
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");
    private static final Pattern IDENTITY_VERSION = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}");

    public BookKeeperCapabilitySnapshotV1 {
        Objects.requireNonNull(providerScopeId, "providerScopeId");
        requireCommit(clientSourceCommit, "clientSourceCommit");
        requireNonZero(clientArtifactSha256, "clientArtifactSha256");
        requireCommit(serverSourceCommit, "serverSourceCommit");
        requireNonZero(serverImageManifestSha256, "serverImageManifestSha256");
        Objects.requireNonNull(protocolMode, "protocolMode");
        if (clientFrameLimitBytes <= 0 || serverFrameLimitBytes <= 0 || maximumAddPayloadBytes <= 0) {
            throw new IllegalArgumentException("frame and payload limits must be positive");
        }
        if (maximumAddPayloadBytes >= Math.min(clientFrameLimitBytes, serverFrameLimitBytes)) {
            throw new IllegalArgumentException("maximum add payload must leave protocol/digest framing headroom");
        }
        if (!explicitEntryIdsSupported || !fencingSupported || !recoverySupported) {
            throw new IllegalArgumentException("M2 requires explicit entry IDs, fencing, and recovery");
        }
        if (ackQuorumSize <= 0 || writeQuorumSize < ackQuorumSize || ensembleSize < writeQuorumSize) {
            throw new IllegalArgumentException("quorum sizes must satisfy 0 < ack <= write <= ensemble");
        }
        Objects.requireNonNull(digestType, "digestType");
        Objects.requireNonNull(timeoutClass, "timeoutClass");
        Objects.requireNonNull(credentialIdentityVersion, "credentialIdentityVersion");
        if (!IDENTITY_VERSION.matcher(credentialIdentityVersion).matches()) {
            throw new IllegalArgumentException("credential identity version is not canonical");
        }
        requireNonZero(configurationDigest, "configurationDigest");
    }

    private static void requireCommit(String value, String name) {
        Objects.requireNonNull(value, name);
        if (!COMMIT.matcher(value).matches()) {
            throw new IllegalArgumentException(name + " must be 40 lowercase hex characters");
        }
    }

    private static void requireNonZero(Sha256Digest value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero()) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
    }
}
