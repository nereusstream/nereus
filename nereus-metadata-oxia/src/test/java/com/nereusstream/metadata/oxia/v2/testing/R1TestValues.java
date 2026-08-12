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

package com.nereusstream.metadata.oxia.v2.testing;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.registry.BookKeeperInstanceIdV1;
import com.nereusstream.domain.registry.LedgerIdCompatibilityNamespaceV1;
import com.nereusstream.domain.registry.Nvr1RegistryCodecV1;
import com.nereusstream.domain.registry.PulsarVirtualLedgerRegistryV1;
import com.nereusstream.domain.registry.RegistryAdmissionEvidenceV1;
import com.nereusstream.domain.registry.RegistryWriterAdmissionV1;
import com.nereusstream.domain.registry.RegistryWriterKindV1;
import com.nereusstream.domain.registry.RegistryWriterRemovalV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceLifecycleV1;
import com.nereusstream.metadata.oxia.v2.registry.RegistryInterlockSnapshotV1;
import com.nereusstream.metadata.oxia.v2.registry.RegistryMutationRequestV1;
import com.nereusstream.metadata.oxia.v2.registry.RegistryWriterInterlock;
import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/** Exact R1 fixtures shared by deterministic metadata tests. */
public final class R1TestValues {
    public static final DeploymentId DEPLOYMENT = new DeploymentId(new Id128(101, 102));
    public static final ReservationDomainId RESERVATION = new ReservationDomainId(new Id128(103, 104));
    public static final BookKeeperInstanceIdV1 INSTANCE =
            BookKeeperInstanceIdV1.parse("123e4567-e89b-12d3-a456-426614174000");
    public static final Sha256Digest NAMESPACE = LedgerIdCompatibilityNamespaceV1.derive(INSTANCE);

    private R1TestValues() {}

    public static RegistryAdmissionEvidenceV1 initialEvidence(int writerCount) {
        return evidence(1, Sha256Digest.copyOf(new byte[32]), writers(writerCount), List.of());
    }

    public static RegistryAdmissionEvidenceV1 evidence(
            long epoch,
            Sha256Digest predecessorDigest,
            List<RegistryWriterAdmissionV1> writers,
            List<RegistryWriterRemovalV1> removals) {
        return new RegistryAdmissionEvidenceV1(
                DEPLOYMENT,
                RESERVATION,
                INSTANCE,
                NAMESPACE,
                epoch,
                predecessorDigest,
                digest("fresh-root"),
                digest("admin-" + epoch),
                digest("negative-allocation-" + epoch),
                writers,
                removals);
    }

    public static PulsarVirtualLedgerRegistryV1 registry(
            RegistryAdmissionEvidenceV1 evidence, List<VirtualLedgerSliceAssignmentV1> assignments) {
        return new PulsarVirtualLedgerRegistryV1(
                DEPLOYMENT,
                RESERVATION,
                INSTANCE,
                NAMESPACE,
                evidence.candidateRegistryEpoch(),
                evidence.reference(),
                evidence.candidateWriterRows(),
                assignments);
    }

    public static PulsarVirtualLedgerNamespaceRegistryValueV1 storedValue(
            RegistryAdmissionEvidenceV1 evidence, List<VirtualLedgerSliceAssignmentV1> assignments) {
        return PulsarVirtualLedgerNamespaceRegistryValueV1.fromDomain(registry(evidence, assignments));
    }

    public static VirtualLedgerSliceAssignmentV1 assignment(long ordinal) {
        return VirtualLedgerSliceAssignmentV1.create(
                DEPLOYMENT,
                RESERVATION,
                new PulsarCellId(new Id128(201, ordinal + 1)),
                NAMESPACE,
                VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE
                        + ordinal * VirtualLedgerSliceAssignmentV1.SLICE_SIZE,
                VirtualLedgerSliceLifecycleV1.ACTIVE);
    }

    public static List<RegistryWriterAdmissionV1> writers(int count) {
        List<RegistryWriterAdmissionV1> values = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            values.add(new RegistryWriterAdmissionV1(
                    index % 2 == 0
                            ? RegistryWriterKindV1.NATIVE_BOOKKEEPER_LEDGER_ID
                            : RegistryWriterKindV1.NEREUS_VIRTUAL_LEDGER_ID,
                    1,
                    index + 1,
                    digest("principal-" + index),
                    index + 1,
                    digest("interlock-" + index),
                    digest("source-" + index)));
        }
        values.sort(RegistryWriterAdmissionV1.CANONICAL_ORDER);
        return List.copyOf(values);
    }

    public static RegistryWriterRemovalV1 removal(RegistryWriterAdmissionV1 writer) {
        return new RegistryWriterRemovalV1(
                writer,
                digest("fence-" + writer.principalGeneration()),
                digest("drain-" + writer.principalGeneration()),
                digest("revoke-" + writer.principalGeneration()));
    }

    public static RegistryInterlockSnapshotV1 snapshot(RegistryAdmissionEvidenceV1 evidence) {
        return snapshot(evidence, true, true, true, true, true);
    }

    public static RegistryInterlockSnapshotV1 snapshot(
            RegistryAdmissionEvidenceV1 evidence,
            boolean fresh,
            boolean continuity,
            boolean admin,
            boolean revoked,
            boolean negativeAllocation) {
        return new RegistryInterlockSnapshotV1(
                evidence,
                evidence.candidateRegistryEpoch(),
                fresh,
                continuity,
                admin,
                revoked,
                negativeAllocation,
                digest("authority-attestation-" + evidence.candidateRegistryEpoch()));
    }

    public static Sha256Digest digest(String seed) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(seed.getBytes(StandardCharsets.UTF_8)));
    }

    public static Sha256Digest registryDigest(PulsarVirtualLedgerRegistryV1 registry) {
        return Sha256Digest.hash(new Nvr1RegistryCodecV1().encode(registry));
    }

    public static class DeterministicInterlock implements RegistryWriterInterlock {
        private final Map<Sha256Digest, RegistryInterlockSnapshotV1> snapshots = new HashMap<>();
        private int permitCount;

        public void register(RegistryInterlockSnapshotV1 snapshot) {
            snapshots.put(snapshot.evidence().reference().digest(), snapshot);
        }

        public int permitCount() {
            return permitCount;
        }

        @Override
        public <T> CompletionStage<T> withPermit(
                RegistryMutationRequestV1 request,
                Function<RegistryInterlockSnapshotV1, CompletionStage<T>> protectedMutation) {
            permitCount++;
            RegistryInterlockSnapshotV1 snapshot =
                    snapshots.get(request.candidate().admissionEvidence().digest());
            if (snapshot == null) {
                throw new IllegalStateException("no deterministic interlock snapshot for candidate evidence");
            }
            return RegistryWriterInterlock.applyProtected(snapshot, protectedMutation);
        }
    }
}
