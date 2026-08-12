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

/** Strict self-framed RAE1 codec; its SHA-256 is the Registry/row evidence reference. */
public final class Rae1RegistryAdmissionEvidenceCodecV1 {
    public static final int FIXED_HEADER_BYTES = 250;
    public static final int MAX_BYTES = FIXED_HEADER_BYTES
            + PulsarVirtualLedgerRegistryValidatorV1.MAX_WRITER_COUNT * RegistryWriterAdmissionV1.BYTES
            + PulsarVirtualLedgerRegistryValidatorV1.MAX_WRITER_COUNT * RegistryWriterRemovalV1.BYTES;
    private static final byte[] MAGIC = "RAE1".getBytes(StandardCharsets.US_ASCII);

    public CanonicalBytes encode(RegistryAdmissionEvidenceV1 value) {
        int length = Math.addExact(
                FIXED_HEADER_BYTES,
                Math.addExact(
                        Math.multiplyExact(value.admittedWriters().size(), RegistryWriterAdmissionV1.BYTES),
                        Math.multiplyExact(value.removedWriters().size(), RegistryWriterRemovalV1.BYTES)));
        ByteBuffer output = ByteBuffer.allocate(length);
        output.put(MAGIC);
        RegistryWriterRowV1.putU16(output, 1);
        putId(output, value.deploymentId().value());
        putId(output, value.reservationDomainId().value());
        output.put(value.instanceId().bytes().toByteArray());
        output.put(value.ledgerIdCompatibilityNamespaceId().bytes().toByteArray());
        output.putLong(value.candidateRegistryEpoch());
        output.put((byte) (value.initialCreate() ? 0 : 1));
        output.put(new byte[3]);
        output.put(value.predecessorRegistryDigest().bytes().toByteArray());
        output.put(value.freshRootProofDigest().bytes().toByteArray());
        output.put(value.adminInterlockDigest().bytes().toByteArray());
        output.put(value.negativeAllocationProofDigest().bytes().toByteArray());
        RegistryWriterRowV1.putU16(output, value.admittedWriters().size());
        RegistryWriterRowV1.putU16(output, value.removedWriters().size());
        value.admittedWriters().forEach(writer -> writer.encodeTo(output));
        value.removedWriters().forEach(removal -> removal.encodeTo(output));
        if (output.hasRemaining()) {
            throw reject("RAE1 encoder length differs");
        }
        return CanonicalBytes.copyOf(output.array());
    }

    public RegistryAdmissionEvidenceV1 decode(CanonicalBytes encoded) {
        if (encoded.length() < FIXED_HEADER_BYTES || encoded.length() > MAX_BYTES) {
            throw reject("RAE1 length is outside the v1 bound");
        }
        try {
            ByteBuffer input = ByteBuffer.wrap(encoded.toByteArray());
            requireMagic(input);
            if (RegistryWriterRowV1.readU16(input) != 1) {
                throw reject("RAE1 schema version differs");
            }
            DeploymentId deploymentId = new DeploymentId(readId(input));
            ReservationDomainId reservationDomainId = new ReservationDomainId(readId(input));
            byte[] instanceBytes = new byte[BookKeeperInstanceIdV1.LENGTH];
            input.get(instanceBytes);
            BookKeeperInstanceIdV1 instanceId = BookKeeperInstanceIdV1.fromBytes(instanceBytes);
            Sha256Digest namespaceId = RegistryWriterAdmissionV1.readDigest(input);
            long epoch = input.getLong();
            int predecessorPresence = Byte.toUnsignedInt(input.get());
            for (int index = 0; index < 3; index++) {
                if (input.get() != 0) {
                    throw reject("RAE1 reserved bytes must be zero");
                }
            }
            Sha256Digest predecessorDigest = RegistryWriterAdmissionV1.readDigest(input);
            if ((predecessorPresence == 0 && !predecessorDigest.isZero())
                    || (predecessorPresence == 1 && predecessorDigest.isZero())
                    || predecessorPresence > 1) {
                throw reject("RAE1 predecessor presence differs from digest");
            }
            Sha256Digest freshRootDigest = RegistryWriterAdmissionV1.readDigest(input);
            Sha256Digest adminDigest = RegistryWriterAdmissionV1.readDigest(input);
            Sha256Digest negativeAllocationDigest = RegistryWriterAdmissionV1.readDigest(input);
            int writerCount = RegistryWriterRowV1.readU16(input);
            int removedCount = RegistryWriterRowV1.readU16(input);
            if (writerCount > PulsarVirtualLedgerRegistryValidatorV1.MAX_WRITER_COUNT
                    || removedCount > PulsarVirtualLedgerRegistryValidatorV1.MAX_WRITER_COUNT) {
                throw new RegistryValidationException(
                        RegistryRejectionCodeV1.REGISTRY_WRITER_COUNT_EXCEEDED,
                        "RAE1 admitted or removed writer count exceeds 14");
            }
            int expectedLength = Math.addExact(
                    FIXED_HEADER_BYTES,
                    Math.addExact(
                            Math.multiplyExact(writerCount, RegistryWriterAdmissionV1.BYTES),
                            Math.multiplyExact(removedCount, RegistryWriterRemovalV1.BYTES)));
            if (expectedLength != encoded.length()) {
                throw reject("RAE1 counts do not match exact length");
            }
            List<RegistryWriterAdmissionV1> writers = new ArrayList<>(writerCount);
            for (int index = 0; index < writerCount; index++) {
                writers.add(RegistryWriterAdmissionV1.decodeFrom(input));
            }
            List<RegistryWriterRemovalV1> removals = new ArrayList<>(removedCount);
            for (int index = 0; index < removedCount; index++) {
                removals.add(RegistryWriterRemovalV1.decodeFrom(input));
            }
            if (input.hasRemaining()) {
                throw reject("RAE1 has trailing bytes");
            }
            RegistryAdmissionEvidenceV1 value = new RegistryAdmissionEvidenceV1(
                    deploymentId,
                    reservationDomainId,
                    instanceId,
                    namespaceId,
                    epoch,
                    predecessorDigest,
                    freshRootDigest,
                    adminDigest,
                    negativeAllocationDigest,
                    writers,
                    removals);
            if (!encode(value).equals(encoded)) {
                throw reject("RAE1 is not canonical on re-encode");
            }
            return value;
        } catch (BufferUnderflowException | ArithmeticException error) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL, "RAE1 is truncated or overflows", error);
        }
    }

    private static void requireMagic(ByteBuffer input) {
        for (byte value : MAGIC) {
            if (input.get() != value) {
                throw reject("RAE1 magic differs");
            }
        }
    }

    private static void putId(ByteBuffer output, Id128 value) {
        output.putLong(value.highBits()).putLong(value.lowBits());
    }

    private static Id128 readId(ByteBuffer input) {
        return new Id128(input.getLong(), input.getLong());
    }

    private static RegistryValidationException reject(String message) {
        return new RegistryValidationException(RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL, message);
    }
}
