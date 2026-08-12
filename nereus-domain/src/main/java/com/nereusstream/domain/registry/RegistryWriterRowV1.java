/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

/** Exact fixed-width writer cohort committed by NVR1. */
public record RegistryWriterRowV1(
        RegistryWriterKindV1 writerKind,
        int exclusionContractVersion,
        long principalGeneration,
        Sha256Digest principalDigest,
        long interlockGeneration,
        Sha256Digest interlockDigest,
        RegistryEvidenceReferenceV1 evidence) {
    public static final int BYTES = 120;
    public static final int EXCLUSION_CONTRACT_VERSION = 1;
    public static final Comparator<RegistryWriterRowV1> CANONICAL_ORDER = Comparator.comparingInt(
                    (RegistryWriterRowV1 value) -> value.writerKind.code())
            .thenComparingLong(RegistryWriterRowV1::principalGeneration)
            .thenComparing((left, right) -> Arrays.compareUnsigned(
                    left.principalDigest.bytes().toByteArray(),
                    right.principalDigest.bytes().toByteArray()));

    public RegistryWriterRowV1 {
        Objects.requireNonNull(writerKind, "writerKind");
        Objects.requireNonNull(principalDigest, "principalDigest");
        Objects.requireNonNull(interlockDigest, "interlockDigest");
        Objects.requireNonNull(evidence, "evidence");
        if (exclusionContractVersion != EXCLUSION_CONTRACT_VERSION
                || principalGeneration <= 0
                || interlockGeneration <= 0
                || principalDigest.isZero()
                || interlockDigest.isZero()) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_UNAUTHORIZED_WRITER,
                    "Registry writer row has an unknown contract, non-positive generation, or zero digest");
        }
    }

    public CanonicalBytes encode() {
        ByteBuffer output = ByteBuffer.allocate(BYTES);
        putU16(output, writerKind.code());
        putU16(output, exclusionContractVersion);
        output.putLong(principalGeneration).put(principalDigest.bytes().toByteArray());
        output.putLong(interlockGeneration).put(interlockDigest.bytes().toByteArray());
        putU16(output, evidence.kind());
        putU16(output, evidence.version());
        output.put(evidence.digest().bytes().toByteArray());
        return CanonicalBytes.copyOf(output.array());
    }

    static RegistryWriterRowV1 decode(ByteBuffer input) {
        return new RegistryWriterRowV1(
                RegistryWriterKindV1.fromCode(readU16(input)),
                readU16(input),
                input.getLong(),
                readDigest(input),
                input.getLong(),
                readDigest(input),
                new RegistryEvidenceReferenceV1(readU16(input), readU16(input), readDigest(input)));
    }

    private static Sha256Digest readDigest(ByteBuffer input) {
        byte[] value = new byte[Sha256Digest.LENGTH];
        input.get(value);
        return Sha256Digest.copyOf(value);
    }

    static int readU16(ByteBuffer input) {
        return Short.toUnsignedInt(input.getShort());
    }

    static void putU16(ByteBuffer output, int value) {
        if (value < 0 || value > 0xffff) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL, "u16 value is out of range");
        }
        output.putShort((short) value);
    }
}
