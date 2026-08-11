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

package com.nereusstream.metadata.spi.model;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.ReservationDomainId;
import java.util.Objects;

/**
 * Typed Registry key identity/epoch plus exact caller-supplied stored bytes.
 *
 * <p>The opaque bytes deliberately define no writer count, assignment parser, or production capacity validator.
 */
public record PulsarVirtualLedgerNamespaceRegistryValueV1(
        DeploymentId deploymentId,
        ReservationDomainId reservationDomainId,
        Sha256Digest ledgerIdCompatibilityNamespaceId,
        long registryEpoch,
        CanonicalBytes canonicalStoredBytes,
        Sha256Digest canonicalStoredDigest) {
    public PulsarVirtualLedgerNamespaceRegistryValueV1 {
        Objects.requireNonNull(deploymentId, "deploymentId");
        Objects.requireNonNull(reservationDomainId, "reservationDomainId");
        Objects.requireNonNull(ledgerIdCompatibilityNamespaceId, "ledgerIdCompatibilityNamespaceId");
        if (registryEpoch < 0) {
            throw new IllegalArgumentException("Registry epoch must be non-negative");
        }
        ExactStoredValue.requireMatchingDigest(canonicalStoredBytes, canonicalStoredDigest);
    }
}
