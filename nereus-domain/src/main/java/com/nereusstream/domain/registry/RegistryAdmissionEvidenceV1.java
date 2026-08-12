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
import com.nereusstream.domain.identity.ReservationDomainId;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Complete immutable RAE1 proof input for one Registry create or successor CAS. */
public record RegistryAdmissionEvidenceV1(
        DeploymentId deploymentId,
        ReservationDomainId reservationDomainId,
        BookKeeperInstanceIdV1 instanceId,
        Sha256Digest ledgerIdCompatibilityNamespaceId,
        long candidateRegistryEpoch,
        Sha256Digest predecessorRegistryDigest,
        Sha256Digest freshRootProofDigest,
        Sha256Digest adminInterlockDigest,
        Sha256Digest negativeAllocationProofDigest,
        List<RegistryWriterAdmissionV1> admittedWriters,
        List<RegistryWriterRemovalV1> removedWriters) {
    public RegistryAdmissionEvidenceV1 {
        Objects.requireNonNull(deploymentId, "deploymentId");
        Objects.requireNonNull(reservationDomainId, "reservationDomainId");
        Objects.requireNonNull(instanceId, "instanceId");
        Objects.requireNonNull(ledgerIdCompatibilityNamespaceId, "ledgerIdCompatibilityNamespaceId");
        Objects.requireNonNull(predecessorRegistryDigest, "predecessorRegistryDigest");
        Objects.requireNonNull(freshRootProofDigest, "freshRootProofDigest");
        Objects.requireNonNull(adminInterlockDigest, "adminInterlockDigest");
        Objects.requireNonNull(negativeAllocationProofDigest, "negativeAllocationProofDigest");
        admittedWriters = List.copyOf(Objects.requireNonNull(admittedWriters, "admittedWriters"));
        removedWriters = List.copyOf(Objects.requireNonNull(removedWriters, "removedWriters"));
        validate(
                instanceId,
                ledgerIdCompatibilityNamespaceId,
                candidateRegistryEpoch,
                predecessorRegistryDigest,
                freshRootProofDigest,
                adminInterlockDigest,
                negativeAllocationProofDigest,
                admittedWriters,
                removedWriters);
    }

    public boolean initialCreate() {
        return predecessorRegistryDigest.isZero();
    }

    public CanonicalBytes canonicalBytes() {
        return new Rae1RegistryAdmissionEvidenceCodecV1().encode(this);
    }

    public RegistryEvidenceReferenceV1 reference() {
        return new RegistryEvidenceReferenceV1(1, 1, Sha256Digest.hash(canonicalBytes()));
    }

    public List<RegistryWriterRowV1> candidateWriterRows() {
        RegistryEvidenceReferenceV1 evidence = reference();
        return admittedWriters.stream()
                .map(writer -> writer.writerRow(evidence))
                .toList();
    }

    public void validateCandidate(PulsarVirtualLedgerRegistryV1 candidate, PulsarVirtualLedgerRegistryV1 predecessor) {
        Objects.requireNonNull(candidate, "candidate");
        if (!deploymentId.equals(candidate.deploymentId())
                || !reservationDomainId.equals(candidate.reservationDomainId())
                || !instanceId.equals(candidate.instanceId())
                || !ledgerIdCompatibilityNamespaceId.equals(candidate.ledgerIdCompatibilityNamespaceId())
                || candidateRegistryEpoch != candidate.registryEpoch()
                || !reference().equals(candidate.admissionEvidence())
                || !candidateWriterRows().equals(candidate.writers())) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_UNAUTHORIZED_WRITER,
                    "RAE1 identity, epoch, reference, or admitted writer set differs from candidate NVR1");
        }
        if (predecessor == null) {
            if (!initialCreate() || candidateRegistryEpoch != 1 || !removedWriters.isEmpty()) {
                throw new RegistryValidationException(
                        RegistryRejectionCodeV1.REGISTRY_WRITER_LIFECYCLE_VIOLATION,
                        "initial RAE1 must have no predecessor or removed writer and must target epoch one");
            }
            PulsarVirtualLedgerRegistryTransitionValidatorV1.validateInitial(candidate);
            return;
        }
        CanonicalBytes predecessorBytes = new Nvr1RegistryCodecV1().encode(predecessor);
        if (!Sha256Digest.hash(predecessorBytes).equals(predecessorRegistryDigest)) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_WRITER_LIFECYCLE_VIOLATION,
                    "RAE1 predecessor digest differs from exact NVR1 predecessor");
        }
        PulsarVirtualLedgerRegistryTransitionValidatorV1.validate(predecessor, candidate);
        validateRemovedWriters(predecessor, candidate);
    }

    private void validateRemovedWriters(
            PulsarVirtualLedgerRegistryV1 predecessor, PulsarVirtualLedgerRegistryV1 candidate) {
        List<RegistryWriterRowV1> removed = predecessor.writers().stream()
                .filter(prior -> candidate.writers().stream().noneMatch(current -> sameWriterIdentity(prior, current)))
                .toList();
        if (removed.size() != removedWriters.size()) {
            lifecycleReject("RAE1 removal proof count differs from predecessor/candidate writer delta");
        }
        for (int index = 0; index < removed.size(); index++) {
            if (!removedWriters.get(index).removedWriter().matches(removed.get(index))) {
                lifecycleReject("RAE1 removal proof does not match the removed writer in canonical order");
            }
        }
    }

    private static boolean sameWriterIdentity(RegistryWriterRowV1 first, RegistryWriterRowV1 second) {
        return first.writerKind() == second.writerKind()
                && first.exclusionContractVersion() == second.exclusionContractVersion()
                && first.principalGeneration() == second.principalGeneration()
                && first.principalDigest().equals(second.principalDigest());
    }

    private static void validate(
            BookKeeperInstanceIdV1 instanceId,
            Sha256Digest namespaceId,
            long epoch,
            Sha256Digest predecessorDigest,
            Sha256Digest freshRootDigest,
            Sha256Digest adminDigest,
            Sha256Digest negativeAllocationDigest,
            List<RegistryWriterAdmissionV1> writers,
            List<RegistryWriterRemovalV1> removals) {
        if (epoch <= 0
                || !namespaceId.equals(LedgerIdCompatibilityNamespaceV1.derive(instanceId))
                || freshRootDigest.isZero()
                || adminDigest.isZero()
                || negativeAllocationDigest.isZero()) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_IDENTITY_INVALID,
                    "RAE1 epoch, namespace, or mandatory interlock proof is invalid");
        }
        if (writers.size() > PulsarVirtualLedgerRegistryValidatorV1.MAX_WRITER_COUNT
                || removals.size() > PulsarVirtualLedgerRegistryValidatorV1.MAX_WRITER_COUNT) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_WRITER_COUNT_EXCEEDED,
                    "RAE1 admitted or removed writer count exceeds 14");
        }
        if (!writers.equals(writers.stream()
                        .sorted(RegistryWriterAdmissionV1.CANONICAL_ORDER)
                        .toList())
                || !removals.equals(removals.stream()
                        .sorted(RegistryWriterRemovalV1.CANONICAL_ORDER)
                        .toList())) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_NON_CANONICAL,
                    "RAE1 writer and removal sections must be canonically sorted");
        }
        Set<String> identities = new HashSet<>();
        Set<String> principals = new HashSet<>();
        for (RegistryWriterAdmissionV1 writer : writers) {
            String identity = writer.writerKind().code()
                    + ":"
                    + writer.principalGeneration()
                    + ":"
                    + writer.principalDigest().toHex();
            if (!identities.add(identity)
                    || !principals.add(writer.principalDigest().toHex())) {
                throw new RegistryValidationException(
                        RegistryRejectionCodeV1.REGISTRY_UNAUTHORIZED_WRITER,
                        "RAE1 has a duplicate writer identity or reused principal");
            }
        }
        if (predecessorDigest.isZero() && !removals.isEmpty()) {
            lifecycleReject("initial RAE1 cannot contain writer-removal proofs");
        }
    }

    private static void lifecycleReject(String message) {
        throw new RegistryValidationException(RegistryRejectionCodeV1.REGISTRY_WRITER_LIFECYCLE_VIOLATION, message);
    }
}
