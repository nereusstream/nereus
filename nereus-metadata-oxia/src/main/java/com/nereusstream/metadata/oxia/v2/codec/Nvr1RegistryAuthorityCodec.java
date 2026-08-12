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

package com.nereusstream.metadata.oxia.v2.codec;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.registry.Nvr1RegistryCodecV1;
import com.nereusstream.domain.registry.PulsarVirtualLedgerRegistryV1;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import com.nereusstream.metadata.spi.model.VersionedRegistrySnapshot;
import java.util.Objects;

/** Production bridge between exact NVR1 bytes and the narrow Registry SPI value. */
final class Nvr1RegistryAuthorityCodec implements RegistryAuthorityCodec {
    private final Nvr1RegistryCodecV1 codec = new Nvr1RegistryCodecV1();

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public CanonicalBytes encode(PulsarVirtualLedgerNamespaceRegistryValueV1 candidate) {
        Objects.requireNonNull(candidate, "candidate");
        PulsarVirtualLedgerRegistryV1 domain = candidate.domainValue();
        CanonicalBytes encoded = codec.encode(domain);
        if (!encoded.equals(candidate.canonicalStoredBytes())
                || !Sha256Digest.hash(encoded).equals(candidate.canonicalStoredDigest())) {
            throw new IllegalArgumentException("Registry candidate differs from strict NVR1 canonical bytes");
        }
        return encoded;
    }

    @Override
    public VersionedRegistrySnapshot decode(
            String expectedAuthorityKey,
            DeploymentId expectedDeploymentId,
            ReservationDomainId expectedReservationDomainId,
            Sha256Digest expectedNamespaceId,
            CanonicalBytes storedBytes,
            MetadataVersion metadataVersion) {
        if (Objects.requireNonNull(expectedAuthorityKey, "expectedAuthorityKey").isBlank()) {
            throw new IllegalArgumentException("expected Registry authority key must not be blank");
        }
        Objects.requireNonNull(expectedDeploymentId, "expectedDeploymentId");
        Objects.requireNonNull(expectedReservationDomainId, "expectedReservationDomainId");
        Objects.requireNonNull(expectedNamespaceId, "expectedNamespaceId");
        Objects.requireNonNull(metadataVersion, "metadataVersion");
        PulsarVirtualLedgerRegistryV1 decoded = codec.decode(Objects.requireNonNull(storedBytes, "storedBytes"));
        if (!expectedDeploymentId.equals(decoded.deploymentId())
                || !expectedReservationDomainId.equals(decoded.reservationDomainId())
                || !expectedNamespaceId.equals(decoded.ledgerIdCompatibilityNamespaceId())) {
            throw new IllegalArgumentException("NVR1 key identity differs from the requested Registry authority");
        }
        return new VersionedRegistrySnapshot(
                PulsarVirtualLedgerNamespaceRegistryValueV1.fromDomain(decoded), metadataVersion);
    }
}
