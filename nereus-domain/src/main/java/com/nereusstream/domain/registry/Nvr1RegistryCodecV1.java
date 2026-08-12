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
import com.nereusstream.domain.identity.ReservationDomainId;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Strict NVR1 canonical codec. */
public final class Nvr1RegistryCodecV1 {
    private static final byte[] MAGIC = "NVR1".getBytes(StandardCharsets.US_ASCII);

    public CanonicalBytes encode(PulsarVirtualLedgerRegistryV1 value) {
        PulsarVirtualLedgerRegistryValidatorV1.validate(value);
        int length = Math.addExact(
                PulsarVirtualLedgerRegistryValidatorV1.FIXED_HEADER_BYTES,
                Math.addExact(
                        Math.multiplyExact(value.writers().size(), RegistryWriterRowV1.BYTES),
                        Math.multiplyExact(value.assignments().size(), VirtualLedgerSliceAssignmentV1.BYTES)));
        ByteBuffer output = ByteBuffer.allocate(length);
        output.put(MAGIC);
        RegistryWriterRowV1.putU16(output, 1);
        putId(output, value.deploymentId().value());
        putId(output, value.reservationDomainId().value());
        output.put(value.instanceId().bytes().toByteArray());
        output.put(value.ledgerIdCompatibilityNamespaceId().bytes().toByteArray());
        output.putLong(VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE);
        output.putLong(VirtualLedgerSliceAssignmentV1.RESERVED_END_INCLUSIVE);
        RegistryWriterRowV1.putU16(output, VirtualLedgerSliceAssignmentV1.SLICE_EXPONENT);
        output.putInt(PulsarVirtualLedgerRegistryValidatorV1.MAX_REGISTRY_BYTES);
        RegistryWriterRowV1.putU16(output, PulsarVirtualLedgerRegistryValidatorV1.MAX_ASSIGNMENTS_EVER);
        RegistryWriterRowV1.putU16(output, VirtualLedgerSliceAssignmentV1.BYTES);
        RegistryWriterRowV1.putU16(output, PulsarVirtualLedgerRegistryValidatorV1.MAX_WRITER_COUNT);
        RegistryWriterRowV1.putU16(output, RegistryWriterRowV1.BYTES);
        output.putLong(value.registryEpoch());
        RegistryWriterRowV1.putU16(output, value.admissionEvidence().kind());
        RegistryWriterRowV1.putU16(output, value.admissionEvidence().version());
        output.put(value.admissionEvidence().digest().bytes().toByteArray());
        RegistryWriterRowV1.putU16(output, value.writers().size());
        RegistryWriterRowV1.putU16(output, value.assignments().size());
        for (RegistryWriterRowV1 writer : value.writers()) {
            output.put(writer.encode().toByteArray());
        }
        for (VirtualLedgerSliceAssignmentV1 assignment : value.assignments()) {
            output.put(assignment.encode().toByteArray());
        }
        if (output.hasRemaining()) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL, "NVR1 encoder length differs");
        }
        return CanonicalBytes.copyOf(output.array());
    }

    public PulsarVirtualLedgerRegistryV1 decode(CanonicalBytes encoded) {
        if (encoded.length() < PulsarVirtualLedgerRegistryValidatorV1.FIXED_HEADER_BYTES
                || encoded.length() > PulsarVirtualLedgerRegistryValidatorV1.MAX_CANONICAL_REGISTRY_BYTES) {
            throw reject("NVR1 length is outside the canonical v1 bounds");
        }
        try {
            ByteBuffer input = ByteBuffer.wrap(encoded.toByteArray());
            requireMagic(input);
            if (RegistryWriterRowV1.readU16(input) != 1) {
                throw reject("NVR1 schema version differs");
            }
            DeploymentId deploymentId = new DeploymentId(readId(input));
            ReservationDomainId reservationDomainId = new ReservationDomainId(readId(input));
            byte[] instanceBytes = new byte[BookKeeperInstanceIdV1.LENGTH];
            input.get(instanceBytes);
            BookKeeperInstanceIdV1 instanceId = BookKeeperInstanceIdV1.fromBytes(instanceBytes);
            Sha256Digest namespaceId = readDigest(input);
            requireLong(input, VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE, "reserved start");
            requireLong(input, VirtualLedgerSliceAssignmentV1.RESERVED_END_INCLUSIVE, "reserved end");
            requireU16(input, VirtualLedgerSliceAssignmentV1.SLICE_EXPONENT, "slice exponent");
            if (input.getInt() != PulsarVirtualLedgerRegistryValidatorV1.MAX_REGISTRY_BYTES) {
                throw reject("max Registry bytes differs");
            }
            requireU16(input, PulsarVirtualLedgerRegistryValidatorV1.MAX_ASSIGNMENTS_EVER, "assignment count cap");
            requireU16(input, VirtualLedgerSliceAssignmentV1.BYTES, "assignment row bytes");
            requireU16(input, PulsarVirtualLedgerRegistryValidatorV1.MAX_WRITER_COUNT, "writer count cap");
            requireU16(input, RegistryWriterRowV1.BYTES, "writer row bytes");
            long registryEpoch = input.getLong();
            RegistryEvidenceReferenceV1 evidence = new RegistryEvidenceReferenceV1(
                    RegistryWriterRowV1.readU16(input), RegistryWriterRowV1.readU16(input), readDigest(input));
            int writerCount = RegistryWriterRowV1.readU16(input);
            int assignmentCount = RegistryWriterRowV1.readU16(input);
            if (writerCount > PulsarVirtualLedgerRegistryValidatorV1.MAX_WRITER_COUNT) {
                throw new RegistryValidationException(
                        RegistryRejectionCodeV1.REGISTRY_WRITER_COUNT_EXCEEDED, "encoded writer count exceeds 14");
            }
            if (assignmentCount > PulsarVirtualLedgerRegistryValidatorV1.MAX_ASSIGNMENTS_EVER) {
                throw new RegistryValidationException(
                        RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_COUNT_EXCEEDED,
                        "encoded assignment count exceeds 256");
            }
            int expectedLength = Math.addExact(
                    PulsarVirtualLedgerRegistryValidatorV1.FIXED_HEADER_BYTES,
                    Math.addExact(
                            Math.multiplyExact(writerCount, RegistryWriterRowV1.BYTES),
                            Math.multiplyExact(assignmentCount, VirtualLedgerSliceAssignmentV1.BYTES)));
            if (expectedLength != encoded.length()) {
                throw reject("NVR1 counts do not match exact length");
            }
            List<RegistryWriterRowV1> writers = new ArrayList<>(writerCount);
            for (int index = 0; index < writerCount; index++) {
                writers.add(RegistryWriterRowV1.decode(input));
            }
            List<VirtualLedgerSliceAssignmentV1> assignments = new ArrayList<>(assignmentCount);
            for (int index = 0; index < assignmentCount; index++) {
                assignments.add(VirtualLedgerSliceAssignmentV1.decode(input));
            }
            if (input.hasRemaining()) {
                throw reject("NVR1 has trailing bytes");
            }
            PulsarVirtualLedgerRegistryV1 value = new PulsarVirtualLedgerRegistryV1(
                    deploymentId,
                    reservationDomainId,
                    instanceId,
                    namespaceId,
                    registryEpoch,
                    evidence,
                    writers,
                    assignments);
            if (!encode(value).equals(encoded)) {
                throw reject("NVR1 is not canonical on re-encode");
            }
            return value;
        } catch (BufferUnderflowException | ArithmeticException error) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL, "NVR1 is truncated or overflows", error);
        }
    }

    private static void requireMagic(ByteBuffer input) {
        for (byte value : MAGIC) {
            if (input.get() != value) {
                throw reject("NVR1 magic differs");
            }
        }
    }

    private static void requireLong(ByteBuffer input, long expected, String field) {
        if (input.getLong() != expected) {
            throw reject(field + " differs");
        }
    }

    private static void requireU16(ByteBuffer input, int expected, String field) {
        if (RegistryWriterRowV1.readU16(input) != expected) {
            throw reject(field + " differs");
        }
    }

    private static void putId(ByteBuffer output, Id128 id) {
        output.putLong(id.highBits()).putLong(id.lowBits());
    }

    private static Id128 readId(ByteBuffer input) {
        return new Id128(input.getLong(), input.getLong());
    }

    private static Sha256Digest readDigest(ByteBuffer input) {
        byte[] value = new byte[Sha256Digest.LENGTH];
        input.get(value);
        return Sha256Digest.copyOf(value);
    }

    private static RegistryValidationException reject(String message) {
        return new RegistryValidationException(RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL, message);
    }
}
