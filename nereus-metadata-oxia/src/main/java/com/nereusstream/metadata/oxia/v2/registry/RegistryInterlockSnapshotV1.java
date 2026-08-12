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

package com.nereusstream.metadata.oxia.v2.registry;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.RegistryAdmissionEvidenceV1;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Closed, receipt-attachable writer-interlock snapshot held across one Registry mutation. */
public record RegistryInterlockSnapshotV1(
        RegistryAdmissionEvidenceV1 evidence,
        long providerGeneration,
        boolean initialFreshRootProven,
        boolean instanceIdentityContinuityProven,
        boolean exclusiveAdminInterlockProven,
        boolean unrestrictedLegacyPrincipalRevoked,
        boolean negativeAllocationProven,
        Sha256Digest authorityAttestationDigest) {
    private static final byte[] MAGIC = "RIS1".getBytes(StandardCharsets.US_ASCII);
    private static final int BYTES = 80;

    public RegistryInterlockSnapshotV1 {
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(authorityAttestationDigest, "authorityAttestationDigest");
        if (providerGeneration <= 0 || authorityAttestationDigest.isZero()) {
            throw new IllegalArgumentException("interlock provider generation and attestation must be non-zero");
        }
    }

    public void validateFor(RegistryMutationRequestV1 request) {
        Objects.requireNonNull(request, "request");
        evidence.validateCandidate(request.candidate(), request.predecessor().orElse(null));
        if (!instanceIdentityContinuityProven
                || !exclusiveAdminInterlockProven
                || !unrestrictedLegacyPrincipalRevoked
                || !negativeAllocationProven
                || (request.predecessor().isEmpty() && !initialFreshRootProven)) {
            throw new IllegalStateException("Registry writer interlock proof is incomplete");
        }
    }

    public CanonicalBytes canonicalBytes() {
        int flags = (initialFreshRootProven ? 1 : 0)
                | (instanceIdentityContinuityProven ? 1 << 1 : 0)
                | (exclusiveAdminInterlockProven ? 1 << 2 : 0)
                | (unrestrictedLegacyPrincipalRevoked ? 1 << 3 : 0)
                | (negativeAllocationProven ? 1 << 4 : 0);
        ByteBuffer output = ByteBuffer.allocate(BYTES);
        output.put(MAGIC).putShort((short) 1).put((byte) flags).put((byte) 0);
        output.putLong(providerGeneration);
        output.put(authorityAttestationDigest.bytes().toByteArray());
        output.put(evidence.reference().digest().bytes().toByteArray());
        return CanonicalBytes.copyOf(output.array());
    }
}
