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
import static com.nereusstream.domain.registry.Nvr1RegistryCodecV1Test.assignments;
import static com.nereusstream.domain.registry.Nvr1RegistryCodecV1Test.evidence;
import static com.nereusstream.domain.registry.Nvr1RegistryCodecV1Test.writers;
import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.ReservationDomainId;
import java.util.List;
import org.junit.jupiter.api.Test;

class PulsarVirtualLedgerRegistryTransitionValidatorV1Test {
    private static final BookKeeperInstanceIdV1 INSTANCE =
            BookKeeperInstanceIdV1.parse("123e4567-e89b-12d3-a456-426614174000");
    private static final DeploymentId DEPLOYMENT = new DeploymentId(new Id128(1, 2));
    private static final ReservationDomainId RESERVATION = new ReservationDomainId(new Id128(3, 4));

    @Test
    void acceptsInitialEpochAndSingleStepLifecycle() {
        PulsarVirtualLedgerRegistryV1 initial = registry(1, assignments(1));
        PulsarVirtualLedgerRegistryTransitionValidatorV1.validateInitial(initial);

        VirtualLedgerSliceAssignmentV1 retiring =
                initial.assignments().get(0).withLifecycle(VirtualLedgerSliceLifecycleV1.RETIRING);
        PulsarVirtualLedgerRegistryV1 successor =
                initial.successor(evidence("next"), writersFor("next"), List.of(retiring));

        assertThat(successor.registryEpoch()).isEqualTo(2);
        assertThat(successor.assignments().get(0).lifecycle()).isEqualTo(VirtualLedgerSliceLifecycleV1.RETIRING);
    }

    @Test
    void rejectsInitialEpochOtherThanOneAndEpochGap() {
        assertRejected(
                () -> PulsarVirtualLedgerRegistryTransitionValidatorV1.validateInitial(registry(2, List.of())),
                RegistryRejectionCodeV1.REGISTRY_EPOCH_INVALID);

        PulsarVirtualLedgerRegistryV1 initial = registry(1, List.of());
        PulsarVirtualLedgerRegistryV1 skipped = registry(3, List.of());
        assertRejected(
                () -> PulsarVirtualLedgerRegistryTransitionValidatorV1.validate(initial, skipped),
                RegistryRejectionCodeV1.REGISTRY_EPOCH_INVALID);
    }

    @Test
    void rejectsAssignmentRemovalAndLifecycleReversal() {
        PulsarVirtualLedgerRegistryV1 initial = registry(1, assignments(1));
        assertRejected(
                () -> PulsarVirtualLedgerRegistryTransitionValidatorV1.validate(initial, registry(2, List.of())),
                RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID);

        VirtualLedgerSliceAssignmentV1 retiring =
                initial.assignments().get(0).withLifecycle(VirtualLedgerSliceLifecycleV1.RETIRING);
        PulsarVirtualLedgerRegistryV1 middle = registry(2, List.of(retiring));
        PulsarVirtualLedgerRegistryV1 reversed =
                registry(3, List.of(retiring.withLifecycle(VirtualLedgerSliceLifecycleV1.ACTIVE)));
        assertRejected(
                () -> PulsarVirtualLedgerRegistryTransitionValidatorV1.validate(middle, reversed),
                RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID);
    }

    @Test
    void rejectsSkippingRetiringAndAddingAlreadyRetiredSlice() {
        PulsarVirtualLedgerRegistryV1 initial = registry(1, assignments(1));
        VirtualLedgerSliceAssignmentV1 retired =
                initial.assignments().get(0).withLifecycle(VirtualLedgerSliceLifecycleV1.RETIRED);
        assertRejected(
                () -> PulsarVirtualLedgerRegistryTransitionValidatorV1.validate(initial, registry(2, List.of(retired))),
                RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID);

        PulsarVirtualLedgerRegistryV1 empty = registry(1, List.of());
        assertRejected(
                () -> PulsarVirtualLedgerRegistryTransitionValidatorV1.validate(empty, registry(2, List.of(retired))),
                RegistryRejectionCodeV1.REGISTRY_ASSIGNMENT_INVALID);
    }

    @Test
    void derivedViewBindsNamespaceEpochAndLifecycle() {
        PulsarVirtualLedgerRegistryV1 initial = registry(1, assignments(1));
        VirtualLedgerSliceViewV1 active =
                initial.sliceView(initial.assignments().get(0).pulsarCellId());
        assertThat(active.registryEpoch()).isEqualTo(1);
        assertThat(active.allocationAllowed()).isTrue();

        VirtualLedgerSliceViewV1 retired = new VirtualLedgerSliceViewV1(
                initial.ledgerIdCompatibilityNamespaceId(),
                2,
                initial.assignments().get(0).withLifecycle(VirtualLedgerSliceLifecycleV1.RETIRED));
        assertThat(retired.allocationAllowed()).isFalse();
    }

    private static PulsarVirtualLedgerRegistryV1 registry(
            long epoch, List<VirtualLedgerSliceAssignmentV1> assignments) {
        return new PulsarVirtualLedgerRegistryV1(
                DEPLOYMENT,
                RESERVATION,
                INSTANCE,
                LedgerIdCompatibilityNamespaceV1.derive(INSTANCE),
                epoch,
                evidence("epoch-" + epoch),
                writersFor("epoch-" + epoch),
                assignments);
    }

    private static List<RegistryWriterRowV1> writersFor(String seed) {
        List<RegistryWriterRowV1> base = writers(2);
        RegistryEvidenceReferenceV1 target = evidence(seed);
        return base.stream()
                .map(writer -> new RegistryWriterRowV1(
                        writer.writerKind(),
                        writer.exclusionContractVersion(),
                        writer.principalGeneration(),
                        writer.principalDigest(),
                        writer.interlockGeneration(),
                        writer.interlockDigest(),
                        target))
                .toList();
    }
}
