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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

/** Evidence projection of one admitted writer, excluding the evidence record's own content digest. */
public record RegistryWriterAdmissionV1(
        RegistryWriterKindV1 writerKind,
        int exclusionContractVersion,
        long principalGeneration,
        Sha256Digest principalDigest,
        long interlockGeneration,
        Sha256Digest interlockDigest,
        Sha256Digest sourceQualificationDigest) {
    public static final int BYTES = 116;
    public static final Comparator<RegistryWriterAdmissionV1> CANONICAL_ORDER = Comparator.comparingInt(
                    (RegistryWriterAdmissionV1 value) -> value.writerKind.code())
            .thenComparingLong(RegistryWriterAdmissionV1::principalGeneration)
            .thenComparing((left, right) -> Arrays.compareUnsigned(
                    left.principalDigest.bytes().toByteArray(),
                    right.principalDigest.bytes().toByteArray()));

    public RegistryWriterAdmissionV1 {
        Objects.requireNonNull(writerKind, "writerKind");
        Objects.requireNonNull(principalDigest, "principalDigest");
        Objects.requireNonNull(interlockDigest, "interlockDigest");
        Objects.requireNonNull(sourceQualificationDigest, "sourceQualificationDigest");
        if (exclusionContractVersion != RegistryWriterRowV1.EXCLUSION_CONTRACT_VERSION
                || principalGeneration <= 0
                || interlockGeneration <= 0
                || principalDigest.isZero()
                || interlockDigest.isZero()
                || sourceQualificationDigest.isZero()) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_UNAUTHORIZED_WRITER,
                    "writer evidence has an unknown contract, non-positive generation, or zero digest");
        }
    }

    public RegistryWriterRowV1 writerRow(RegistryEvidenceReferenceV1 evidence) {
        return new RegistryWriterRowV1(
                writerKind,
                exclusionContractVersion,
                principalGeneration,
                principalDigest,
                interlockGeneration,
                interlockDigest,
                evidence);
    }

    boolean matches(RegistryWriterRowV1 writer) {
        return writerKind == writer.writerKind()
                && exclusionContractVersion == writer.exclusionContractVersion()
                && principalGeneration == writer.principalGeneration()
                && principalDigest.equals(writer.principalDigest())
                && interlockGeneration == writer.interlockGeneration()
                && interlockDigest.equals(writer.interlockDigest());
    }

    void encodeTo(ByteBuffer output) {
        RegistryWriterRowV1.putU16(output, writerKind.code());
        RegistryWriterRowV1.putU16(output, exclusionContractVersion);
        output.putLong(principalGeneration).put(principalDigest.bytes().toByteArray());
        output.putLong(interlockGeneration).put(interlockDigest.bytes().toByteArray());
        output.put(sourceQualificationDigest.bytes().toByteArray());
    }

    static RegistryWriterAdmissionV1 decodeFrom(ByteBuffer input) {
        return new RegistryWriterAdmissionV1(
                RegistryWriterKindV1.fromCode(RegistryWriterRowV1.readU16(input)),
                RegistryWriterRowV1.readU16(input),
                input.getLong(),
                readDigest(input),
                input.getLong(),
                readDigest(input),
                readDigest(input));
    }

    static Sha256Digest readDigest(ByteBuffer input) {
        byte[] value = new byte[Sha256Digest.LENGTH];
        input.get(value);
        return Sha256Digest.copyOf(value);
    }
}
