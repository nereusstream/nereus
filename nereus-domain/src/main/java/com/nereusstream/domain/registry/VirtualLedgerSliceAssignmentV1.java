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
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** One immutable 192-byte NVA1 slice row. */
public record VirtualLedgerSliceAssignmentV1(
        DeploymentId deploymentId,
        ReservationDomainId reservationDomainId,
        PulsarCellId pulsarCellId,
        Sha256Digest ledgerIdCompatibilityNamespaceId,
        Sha256Digest sliceAssignmentId,
        long startInclusive,
        long endInclusive,
        VirtualLedgerSliceLifecycleV1 lifecycle) {
    public static final int BYTES = 192;
    public static final long RESERVED_START_INCLUSIVE = 1L << 62;
    public static final long RESERVED_END_INCLUSIVE = Long.MAX_VALUE - 1;
    public static final int SLICE_EXPONENT = 40;
    public static final long SLICE_SIZE = 1L << SLICE_EXPONENT;
    private static final int RESERVED_ZERO_BYTES = 54;
    private static final byte[] ROW_MAGIC = "NVA1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ID_MAGIC = "NVI1".getBytes(StandardCharsets.US_ASCII);

    public VirtualLedgerSliceAssignmentV1 {
        Objects.requireNonNull(deploymentId, "deploymentId");
        Objects.requireNonNull(reservationDomainId, "reservationDomainId");
        Objects.requireNonNull(pulsarCellId, "pulsarCellId");
        Objects.requireNonNull(ledgerIdCompatibilityNamespaceId, "ledgerIdCompatibilityNamespaceId");
        Objects.requireNonNull(sliceAssignmentId, "sliceAssignmentId");
        Objects.requireNonNull(lifecycle, "lifecycle");
        requireGeometry(startInclusive, endInclusive);
        Sha256Digest expected = deriveAssignmentId(
                deploymentId,
                reservationDomainId,
                pulsarCellId,
                ledgerIdCompatibilityNamespaceId,
                startInclusive,
                endInclusive);
        if (!sliceAssignmentId.equals(expected)) {
            throw invalid("sliceAssignmentId does not match NVI1 identity and bounds");
        }
    }

    public static VirtualLedgerSliceAssignmentV1 create(
            DeploymentId deploymentId,
            ReservationDomainId reservationDomainId,
            PulsarCellId pulsarCellId,
            Sha256Digest namespaceId,
            long startInclusive,
            VirtualLedgerSliceLifecycleV1 lifecycle) {
        final long endInclusive;
        try {
            endInclusive = Math.addExact(startInclusive, SLICE_SIZE - 1);
        } catch (ArithmeticException error) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID, "slice end overflows", error);
        }
        return new VirtualLedgerSliceAssignmentV1(
                deploymentId,
                reservationDomainId,
                pulsarCellId,
                namespaceId,
                deriveAssignmentId(
                        deploymentId, reservationDomainId, pulsarCellId, namespaceId, startInclusive, endInclusive),
                startInclusive,
                endInclusive,
                lifecycle);
    }

    public VirtualLedgerSliceAssignmentV1 withLifecycle(VirtualLedgerSliceLifecycleV1 successor) {
        return new VirtualLedgerSliceAssignmentV1(
                deploymentId,
                reservationDomainId,
                pulsarCellId,
                ledgerIdCompatibilityNamespaceId,
                sliceAssignmentId,
                startInclusive,
                endInclusive,
                successor);
    }

    public CanonicalBytes encode() {
        ByteBuffer output = ByteBuffer.allocate(BYTES);
        output.put(ROW_MAGIC);
        RegistryWriterRowV1.putU16(output, 1);
        RegistryWriterRowV1.putU16(output, ProtocolKindV1.PULSAR.code());
        putId(output, deploymentId.value());
        putId(output, reservationDomainId.value());
        putId(output, pulsarCellId.value());
        output.put(ledgerIdCompatibilityNamespaceId.bytes().toByteArray());
        output.put(sliceAssignmentId.bytes().toByteArray());
        output.putLong(startInclusive).putLong(endInclusive);
        RegistryWriterRowV1.putU16(output, lifecycle.code());
        output.put(new byte[RESERVED_ZERO_BYTES]);
        return CanonicalBytes.copyOf(output.array());
    }

    static VirtualLedgerSliceAssignmentV1 decode(ByteBuffer input) {
        requireMagic(input, ROW_MAGIC);
        if (RegistryWriterRowV1.readU16(input) != 1
                || RegistryWriterRowV1.readU16(input) != ProtocolKindV1.PULSAR.code()) {
            throw invalid("NVA1 schema or protocol differs");
        }
        DeploymentId deploymentId = new DeploymentId(readId(input));
        ReservationDomainId reservationDomainId = new ReservationDomainId(readId(input));
        PulsarCellId pulsarCellId = new PulsarCellId(readId(input));
        Sha256Digest namespaceId = readDigest(input);
        Sha256Digest assignmentId = readDigest(input);
        long start = input.getLong();
        long end = input.getLong();
        VirtualLedgerSliceLifecycleV1 lifecycle =
                VirtualLedgerSliceLifecycleV1.fromCode(RegistryWriterRowV1.readU16(input));
        for (int index = 0; index < RESERVED_ZERO_BYTES; index++) {
            if (input.get() != 0) {
                throw new RegistryValidationException(
                        RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL, "NVA1 reserved bytes must be zero");
            }
        }
        return new VirtualLedgerSliceAssignmentV1(
                deploymentId, reservationDomainId, pulsarCellId, namespaceId, assignmentId, start, end, lifecycle);
    }

    private static Sha256Digest deriveAssignmentId(
            DeploymentId deploymentId,
            ReservationDomainId reservationDomainId,
            PulsarCellId cellId,
            Sha256Digest namespaceId,
            long start,
            long end) {
        ByteBuffer output = ByteBuffer.allocate(ID_MAGIC.length + 16 + 16 + 32 + 16 + 8 + 8);
        output.put(ID_MAGIC);
        putId(output, deploymentId.value());
        putId(output, reservationDomainId.value());
        output.put(namespaceId.bytes().toByteArray());
        putId(output, cellId.value());
        output.putLong(start).putLong(end);
        return Sha256Digest.hash(CanonicalBytes.copyOf(output.array()));
    }

    private static void requireGeometry(long start, long end) {
        if (start < RESERVED_START_INCLUSIVE || end > RESERVED_END_INCLUSIVE || end < start) {
            throw invalid("slice lies outside the reserved virtual-ledger interval");
        }
        if ((start - RESERVED_START_INCLUSIVE) % SLICE_SIZE != 0 || end - start != SLICE_SIZE - 1) {
            throw invalid("slice must be one aligned 2^40 interval");
        }
    }

    private static void putId(ByteBuffer output, Id128 value) {
        output.putLong(value.highBits()).putLong(value.lowBits());
    }

    private static Id128 readId(ByteBuffer input) {
        return new Id128(input.getLong(), input.getLong());
    }

    private static Sha256Digest readDigest(ByteBuffer input) {
        byte[] value = new byte[Sha256Digest.LENGTH];
        input.get(value);
        return Sha256Digest.copyOf(value);
    }

    private static void requireMagic(ByteBuffer input, byte[] expected) {
        for (byte value : expected) {
            if (input.get() != value) {
                throw new RegistryValidationException(
                        RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL, "NVA1 magic differs");
            }
        }
    }

    private static RegistryValidationException invalid(String message) {
        return new RegistryValidationException(RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID, message);
    }
}
