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

import static com.nereusstream.domain.registry.Nvr1RegistryCodecV1Test.assertRejected;
import static com.nereusstream.domain.registry.Nvr1RegistryCodecV1Test.digest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.ReservationDomainId;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class RegistryAdmissionEvidenceV1Test {
    private static final DeploymentId DEPLOYMENT = new DeploymentId(new Id128(1, 2));
    private static final ReservationDomainId RESERVATION = new ReservationDomainId(new Id128(3, 4));
    private static final BookKeeperInstanceIdV1 INSTANCE =
            BookKeeperInstanceIdV1.parse("123e4567-e89b-12d3-a456-426614174000");
    private static final Sha256Digest NAMESPACE = LedgerIdCompatibilityNamespaceV1.derive(INSTANCE);
    private static final Rae1RegistryAdmissionEvidenceCodecV1 CODEC = new Rae1RegistryAdmissionEvidenceCodecV1();

    @Test
    void roundTripsInitialEvidenceAndConstructsNonCyclicRows() {
        RegistryAdmissionEvidenceV1 evidence = initialEvidence(admissions(2));
        PulsarVirtualLedgerRegistryV1 candidate = candidate(evidence, List.of());

        evidence.validateCandidate(candidate, null);

        assertThat(CODEC.decode(evidence.canonicalBytes())).isEqualTo(evidence);
        assertThat(candidate.admissionEvidence()).isEqualTo(evidence.reference());
        assertThat(candidate.writers())
                .allSatisfy(writer -> assertThat(writer.evidence()).isEqualTo(evidence.reference()));
        assertThat(evidence.canonicalBytes().toHex())
                .doesNotContain(evidence.reference().digest().toHex());
    }

    @Test
    void freezesMaximumEvidenceLength() {
        RegistryAdmissionEvidenceV1 initial = initialEvidence(admissions(14));
        PulsarVirtualLedgerRegistryV1 predecessor = candidate(initial, List.of());
        List<RegistryWriterRemovalV1> removals = initial.admittedWriters().stream()
                .map(RegistryAdmissionEvidenceV1Test::removal)
                .toList();
        RegistryAdmissionEvidenceV1 maximum =
                evidence(2, Sha256Digest.hash(new Nvr1RegistryCodecV1().encode(predecessor)), admissions(14), removals);

        assertThat(maximum.canonicalBytes().length()).isEqualTo(4_842);
        assertThat(Rae1RegistryAdmissionEvidenceCodecV1.MAX_BYTES).isEqualTo(4_842);
        assertThat(CODEC.decode(maximum.canonicalBytes())).isEqualTo(maximum);
    }

    @Test
    void validatesExactRemovedWriterCut() {
        List<RegistryWriterAdmissionV1> initialWriters = admissions(4);
        RegistryAdmissionEvidenceV1 initialEvidence = initialEvidence(initialWriters);
        PulsarVirtualLedgerRegistryV1 predecessor = candidate(initialEvidence, List.of());
        RegistryWriterAdmissionV1 removed = initialWriters.get(0);
        List<RegistryWriterAdmissionV1> remaining =
                initialWriters.stream().filter(writer -> writer != removed).toList();
        RegistryAdmissionEvidenceV1 successorEvidence = evidence(
                2,
                Sha256Digest.hash(new Nvr1RegistryCodecV1().encode(predecessor)),
                remaining,
                List.of(removal(removed)));
        PulsarVirtualLedgerRegistryV1 successor = candidate(successorEvidence, List.of());

        successorEvidence.validateCandidate(successor, predecessor);

        RegistryAdmissionEvidenceV1 missingProof =
                evidence(2, Sha256Digest.hash(new Nvr1RegistryCodecV1().encode(predecessor)), remaining, List.of());
        PulsarVirtualLedgerRegistryV1 missingProofCandidate = candidate(missingProof, List.of());
        assertRejected(
                () -> missingProof.validateCandidate(missingProofCandidate, predecessor),
                RegistryRejectionCodeV1.REGISTRY_WRITER_LIFECYCLE_VIOLATION);
    }

    @Test
    void rejectsInitialRemovalAndNonCanonicalWriterOrder() {
        List<RegistryWriterAdmissionV1> reversed = new ArrayList<>(admissions(2));
        java.util.Collections.reverse(reversed);
        assertRejected(() -> initialEvidence(reversed), RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL);

        assertRejected(
                () -> evidence(
                        1,
                        Sha256Digest.copyOf(new byte[32]),
                        admissions(2),
                        List.of(removal(admissions(2).get(0)))),
                RegistryRejectionCodeV1.REGISTRY_WRITER_LIFECYCLE_VIOLATION);
    }

    @Test
    void rejectsCorruptionAndEvidenceMismatch() {
        RegistryAdmissionEvidenceV1 evidence = initialEvidence(admissions(2));
        byte[] corrupted = evidence.canonicalBytes().toByteArray();
        corrupted[0] = 'X';
        assertRejected(
                () -> CODEC.decode(com.nereusstream.domain.bytes.CanonicalBytes.copyOf(corrupted)),
                RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL);

        RegistryAdmissionEvidenceV1 other = initialEvidence(admissions(4));
        PulsarVirtualLedgerRegistryV1 candidate = candidate(other, List.of());
        assertThatThrownBy(() -> evidence.validateCandidate(candidate, null))
                .isInstanceOf(RegistryValidationException.class);
    }

    private static RegistryAdmissionEvidenceV1 initialEvidence(List<RegistryWriterAdmissionV1> writers) {
        return evidence(1, Sha256Digest.copyOf(new byte[32]), writers, List.of());
    }

    private static RegistryAdmissionEvidenceV1 evidence(
            long epoch,
            Sha256Digest predecessor,
            List<RegistryWriterAdmissionV1> writers,
            List<RegistryWriterRemovalV1> removals) {
        return new RegistryAdmissionEvidenceV1(
                DEPLOYMENT,
                RESERVATION,
                INSTANCE,
                NAMESPACE,
                epoch,
                predecessor,
                digest("fresh-root"),
                digest("admin-interlock-" + epoch),
                digest("negative-allocation-" + epoch),
                writers,
                removals);
    }

    private static PulsarVirtualLedgerRegistryV1 candidate(
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

    private static List<RegistryWriterAdmissionV1> admissions(int count) {
        List<RegistryWriterAdmissionV1> writers = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            writers.add(new RegistryWriterAdmissionV1(
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
        writers.sort(RegistryWriterAdmissionV1.CANONICAL_ORDER);
        return List.copyOf(writers);
    }

    private static RegistryWriterRemovalV1 removal(RegistryWriterAdmissionV1 writer) {
        return new RegistryWriterRemovalV1(
                writer,
                digest("fence-" + writer.principalGeneration()),
                digest("drain-" + writer.principalGeneration()),
                digest("revoke-" + writer.principalGeneration()));
    }
}
