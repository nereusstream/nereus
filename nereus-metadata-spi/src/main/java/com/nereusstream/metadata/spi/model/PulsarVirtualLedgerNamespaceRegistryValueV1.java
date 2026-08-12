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
import com.nereusstream.domain.registry.Nvr1RegistryCodecV1;
import com.nereusstream.domain.registry.PulsarVirtualLedgerRegistryV1;
import java.util.Objects;

/**
 * Typed Registry key identity/epoch plus exact NVR1 stored bytes.
 *
 * <p>The SPI does not expose child writer or assignment authorities. Production callers map one complete domain
 * Registry to one exact stored value through {@link #fromDomain(PulsarVirtualLedgerRegistryV1)}.
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
        if (registryEpoch <= 0) {
            throw new IllegalArgumentException("Registry epoch must be positive");
        }
        ExactStoredValue.requireMatchingDigest(canonicalStoredBytes, canonicalStoredDigest);
    }

    public static PulsarVirtualLedgerNamespaceRegistryValueV1 fromDomain(PulsarVirtualLedgerRegistryV1 registry) {
        Objects.requireNonNull(registry, "registry");
        CanonicalBytes bytes = new Nvr1RegistryCodecV1().encode(registry);
        return new PulsarVirtualLedgerNamespaceRegistryValueV1(
                registry.deploymentId(),
                registry.reservationDomainId(),
                registry.ledgerIdCompatibilityNamespaceId(),
                registry.registryEpoch(),
                bytes,
                Sha256Digest.hash(bytes));
    }

    public PulsarVirtualLedgerRegistryV1 domainValue() {
        PulsarVirtualLedgerRegistryV1 decoded = new Nvr1RegistryCodecV1().decode(canonicalStoredBytes);
        if (!deploymentId.equals(decoded.deploymentId())
                || !reservationDomainId.equals(decoded.reservationDomainId())
                || !ledgerIdCompatibilityNamespaceId.equals(decoded.ledgerIdCompatibilityNamespaceId())
                || registryEpoch != decoded.registryEpoch()) {
            throw new IllegalArgumentException("Registry wrapper identity or epoch differs from exact NVR1 bytes");
        }
        return decoded;
    }
}
