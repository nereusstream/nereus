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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;

class Nvr1RegistryCodecV1Test {
    private static final DeploymentId DEPLOYMENT = new DeploymentId(new Id128(1, 2));
    private static final ReservationDomainId RESERVATION = new ReservationDomainId(new Id128(3, 4));
    private static final BookKeeperInstanceIdV1 INSTANCE =
            BookKeeperInstanceIdV1.parse("123e4567-e89b-12d3-a456-426614174000");
    private static final Sha256Digest NAMESPACE = LedgerIdCompatibilityNamespaceV1.derive(INSTANCE);
    private static final RegistryEvidenceReferenceV1 EVIDENCE = evidence("registry-admission");
    private static final Nvr1RegistryCodecV1 CODEC = new Nvr1RegistryCodecV1();

    @Test
    void freezesNli1GoldenAndCanonicalInstanceId() {
        assertThat(NAMESPACE.toHex()).isEqualTo("a4d7103b0cd9cb7392f108b703ac027bd07ac8dadf6f06577791925253ffc1ff");
        assertThat(INSTANCE.bytes().length()).isEqualTo(36);

        assertRejected(
                () -> BookKeeperInstanceIdV1.parse("123E4567-E89B-12D3-A456-426614174000"),
                RegistryRejectionCodeV1.REGISTRY_IDENTITY_INVALID);
        assertRejected(
                () -> BookKeeperInstanceIdV1.parse("00000000-0000-0000-0000-000000000000"),
                RegistryRejectionCodeV1.REGISTRY_IDENTITY_INVALID);
        assertRejected(
                () -> BookKeeperInstanceIdV1.fromBytes(new byte[35]),
                RegistryRejectionCodeV1.REGISTRY_IDENTITY_INVALID);
    }

    @Test
    void roundTripsCanonicalRegistryAndFreezesFixedRows() {
        PulsarVirtualLedgerRegistryV1 registry = registry(1, writers(2), assignments(2));

        CanonicalBytes encoded = CODEC.encode(registry);

        assertThat(encoded.length()).isEqualTo(184 + 2 * 120 + 2 * 192);
        assertThat(CODEC.decode(encoded)).isEqualTo(registry);
        assertThat(registry.assignments().get(0).encode().length()).isEqualTo(192);
        assertThat(registry.writers().get(0).encode().length()).isEqualTo(120);
    }

    @Test
    void reachesExact51016ByteMaximum() {
        PulsarVirtualLedgerRegistryV1 maximum = registry(1, writers(14), assignments(256));

        assertThat(CODEC.encode(maximum).length()).isEqualTo(51_016);

        List<RegistryWriterRowV1> tooMany = new ArrayList<>(writers(14));
        tooMany.add(writer(RegistryWriterKindV1.NATIVE_BOOKKEEPER_LEDGER_ID, 100, "writer-15"));
        tooMany.sort(RegistryWriterRowV1.CANONICAL_ORDER);
        assertRejected(() -> registry(1, tooMany, List.of()), RegistryRejectionCodeV1.REGISTRY_WRITER_COUNT_EXCEEDED);
    }

    @Test
    void rejectsCorruptionUnknownConstantsAndTrailingBytes() {
        byte[] canonical = CODEC.encode(registry(1, writers(2), assignments(1))).toByteArray();

        byte[] badMagic = canonical.clone();
        badMagic[0] = 'X';
        assertNonCanonical(badMagic);

        byte[] unknownSchema = canonical.clone();
        unknownSchema[5] = 2;
        assertNonCanonical(unknownSchema);

        byte[] badFixedLimit = canonical.clone();
        badFixedLimit[127] ^= 1;
        assertNonCanonical(badFixedLimit);

        byte[] trailing = new byte[canonical.length + 1];
        System.arraycopy(canonical, 0, trailing, 0, canonical.length);
        assertNonCanonical(trailing);

        byte[] reserved = canonical.clone();
        reserved[reserved.length - 1] = 1;
        assertNonCanonical(reserved);
    }

    @Test
    void rejectsNonCanonicalWriterAndAssignmentOrder() {
        List<RegistryWriterRowV1> reversedWriters = new ArrayList<>(writers(2));
        java.util.Collections.reverse(reversedWriters);
        assertRejected(() -> registry(1, reversedWriters, List.of()), RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL);

        List<VirtualLedgerSliceAssignmentV1> reversedAssignments = new ArrayList<>(assignments(2));
        java.util.Collections.reverse(reversedAssignments);
        assertRejected(
                () -> registry(1, writers(2), reversedAssignments),
                RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID);
    }

    @Test
    void rejectsDuplicatePrincipalAndMissingWriterKind() {
        RegistryWriterRowV1 nativeWriter =
                writer(RegistryWriterKindV1.NATIVE_BOOKKEEPER_LEDGER_ID, 1, "shared-principal");
        RegistryWriterRowV1 virtualWriter = new RegistryWriterRowV1(
                RegistryWriterKindV1.NEREUS_VIRTUAL_LEDGER_ID,
                1,
                1,
                nativeWriter.principalDigest(),
                1,
                digest("other-interlock"),
                EVIDENCE);
        assertRejected(
                () -> registry(1, List.of(nativeWriter, virtualWriter), List.of()),
                RegistryRejectionCodeV1.REGISTRY_UNAUTHORIZED_WRITER);
        assertRejected(
                () -> registry(1, List.of(nativeWriter), List.of()),
                RegistryRejectionCodeV1.REGISTRY_OMITTED_AUTHORIZED_WRITER);
    }

    @Test
    void rejectsInvalidAssignmentGeometryIdentityAndSecondSlice() {
        VirtualLedgerSliceAssignmentV1 valid = assignments(1).get(0);
        assertRejected(
                () -> VirtualLedgerSliceAssignmentV1.create(
                        DEPLOYMENT,
                        RESERVATION,
                        new PulsarCellId(new Id128(10, 20)),
                        NAMESPACE,
                        VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE + 1,
                        VirtualLedgerSliceLifecycleV1.ACTIVE),
                RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID);
        assertRejected(
                () -> new VirtualLedgerSliceAssignmentV1(
                        valid.deploymentId(),
                        valid.reservationDomainId(),
                        valid.pulsarCellId(),
                        valid.ledgerIdCompatibilityNamespaceId(),
                        digest("wrong-assignment-id"),
                        valid.startInclusive(),
                        valid.endInclusive(),
                        valid.lifecycle()),
                RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID);

        VirtualLedgerSliceAssignmentV1 second = VirtualLedgerSliceAssignmentV1.create(
                DEPLOYMENT,
                RESERVATION,
                valid.pulsarCellId(),
                NAMESPACE,
                valid.startInclusive() + VirtualLedgerSliceAssignmentV1.SLICE_SIZE,
                VirtualLedgerSliceLifecycleV1.ACTIVE);
        assertRejected(
                () -> registry(1, writers(2), List.of(valid, second)),
                RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID);
    }

    private static PulsarVirtualLedgerRegistryV1 registry(
            long epoch, List<RegistryWriterRowV1> writers, List<VirtualLedgerSliceAssignmentV1> assignments) {
        return new PulsarVirtualLedgerRegistryV1(
                DEPLOYMENT, RESERVATION, INSTANCE, NAMESPACE, epoch, EVIDENCE, writers, assignments);
    }

    static List<RegistryWriterRowV1> writers(int count) {
        List<RegistryWriterRowV1> values = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            RegistryWriterKindV1 kind = index % 2 == 0
                    ? RegistryWriterKindV1.NATIVE_BOOKKEEPER_LEDGER_ID
                    : RegistryWriterKindV1.NEREUS_VIRTUAL_LEDGER_ID;
            values.add(writer(kind, index + 1, "writer-" + index));
        }
        values.sort(RegistryWriterRowV1.CANONICAL_ORDER);
        return List.copyOf(values);
    }

    static RegistryWriterRowV1 writer(RegistryWriterKindV1 kind, long generation, String seed) {
        return new RegistryWriterRowV1(
                kind, 1, generation, digest("principal-" + seed), generation, digest("interlock-" + seed), EVIDENCE);
    }

    static List<VirtualLedgerSliceAssignmentV1> assignments(int count) {
        List<VirtualLedgerSliceAssignmentV1> values = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            values.add(VirtualLedgerSliceAssignmentV1.create(
                    DEPLOYMENT,
                    RESERVATION,
                    new PulsarCellId(new Id128(10, index + 1L)),
                    NAMESPACE,
                    VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE
                            + index * VirtualLedgerSliceAssignmentV1.SLICE_SIZE,
                    VirtualLedgerSliceLifecycleV1.ACTIVE));
        }
        values.sort(Comparator.comparingLong(VirtualLedgerSliceAssignmentV1::startInclusive));
        return List.copyOf(values);
    }

    static RegistryEvidenceReferenceV1 evidence(String seed) {
        return new RegistryEvidenceReferenceV1(1, 1, digest(seed));
    }

    static Sha256Digest digest(String seed) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(seed.getBytes(StandardCharsets.UTF_8)));
    }

    private static void assertNonCanonical(byte[] encoded) {
        assertRejected(
                () -> CODEC.decode(CanonicalBytes.copyOf(encoded)), RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL);
    }

    static void assertRejected(Runnable operation, RegistryRejectionCodeV1 expected) {
        assertThatThrownBy(operation::run)
                .isInstanceOfSatisfying(RegistryValidationException.class, failure -> assertThat(failure.code())
                        .isEqualTo(expected));
    }
}
