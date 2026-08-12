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

package com.nereusstream.domain.registry;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;

/** Irreversible fence/drain/revoke proof for one writer removed from an NVR1 successor. */
public record RegistryWriterRemovalV1(
        RegistryWriterAdmissionV1 removedWriter,
        Sha256Digest fenceProofDigest,
        Sha256Digest drainProofDigest,
        Sha256Digest principalRevocationProofDigest) {
    public static final int BYTES = RegistryWriterAdmissionV1.BYTES + 3 * Sha256Digest.LENGTH;
    public static final Comparator<RegistryWriterRemovalV1> CANONICAL_ORDER =
            Comparator.comparing(RegistryWriterRemovalV1::removedWriter, RegistryWriterAdmissionV1.CANONICAL_ORDER);

    public RegistryWriterRemovalV1 {
        Objects.requireNonNull(removedWriter, "removedWriter");
        Objects.requireNonNull(fenceProofDigest, "fenceProofDigest");
        Objects.requireNonNull(drainProofDigest, "drainProofDigest");
        Objects.requireNonNull(principalRevocationProofDigest, "principalRevocationProofDigest");
        if (fenceProofDigest.isZero() || drainProofDigest.isZero() || principalRevocationProofDigest.isZero()) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_WRITER_LIFECYCLE_VIOLATION,
                    "removed writer requires non-zero fence, drain, and principal-revocation proofs");
        }
    }

    void encodeTo(ByteBuffer output) {
        removedWriter.encodeTo(output);
        output.put(fenceProofDigest.bytes().toByteArray());
        output.put(drainProofDigest.bytes().toByteArray());
        output.put(principalRevocationProofDigest.bytes().toByteArray());
    }

    static RegistryWriterRemovalV1 decodeFrom(ByteBuffer input) {
        return new RegistryWriterRemovalV1(
                RegistryWriterAdmissionV1.decodeFrom(input),
                RegistryWriterAdmissionV1.readDigest(input),
                RegistryWriterAdmissionV1.readDigest(input),
                RegistryWriterAdmissionV1.readDigest(input));
    }
}
