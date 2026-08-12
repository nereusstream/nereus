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

package com.nereusstream.metadata.oxia.v2;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.registry.BookKeeperInstanceIdV1;
import com.nereusstream.domain.registry.LedgerIdCompatibilityNamespaceV1;
import com.nereusstream.domain.registry.RegistryAdmissionEvidenceV1;
import com.nereusstream.domain.registry.RegistryWriterAdmissionV1;
import com.nereusstream.domain.registry.RegistryWriterKindV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceLifecycleV1;
import com.nereusstream.metadata.oxia.v2.continuity.RevalidationScheduler;
import com.nereusstream.metadata.oxia.v2.continuity.StoreContinuityState;
import com.nereusstream.metadata.oxia.v2.registry.RegistryInterlockSnapshotV1;
import com.nereusstream.metadata.oxia.v2.registry.RegistryMutationRequestV1;
import com.nereusstream.metadata.oxia.v2.registry.RegistryWriterInterlock;
import com.nereusstream.metadata.spi.model.ConditionalCasOutcome;
import com.nereusstream.metadata.spi.model.CreateMutationOutcome;
import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class R1RegistryOxiaIntegrationTest {
    private static final String IMAGE = "nereus/oxia-o1:37a17bef1720";
    private static final DeploymentId DEPLOYMENT = new DeploymentId(new Id128(301, 302));
    private static final ReservationDomainId RESERVATION = new ReservationDomainId(new Id128(303, 304));
    private static final BookKeeperInstanceIdV1 INSTANCE =
            BookKeeperInstanceIdV1.parse("123e4567-e89b-12d3-a456-426614174000");
    private static final Sha256Digest NAMESPACE = LedgerIdCompatibilityNamespaceV1.derive(INSTANCE);

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("oxia/oxia")).withShards(4);

    @Test
    void exactCreateCasDerivedViewAndRestartUseOneRegistryAuthority() throws Exception {
        String authorityRoot = "/nereus/v2/r1/" + UUID.randomUUID();
        TestInterlock firstInterlock = new TestInterlock();
        RegistryAdmissionEvidenceV1 firstEvidence = evidence(1, Sha256Digest.copyOf(new byte[32]));
        firstInterlock.register(snapshot(firstEvidence));
        var firstValue = storedValue(firstEvidence, List.of());
        com.nereusstream.metadata.spi.model.VersionedRegistrySnapshot applied;

        try (TestScheduler scheduler = new TestScheduler();
                OxiaV2CapabilityStore store = connect(authorityRoot, scheduler, firstInterlock)) {
            var created = store.registryStore()
                    .createRegistry(firstValue)
                    .toCompletableFuture()
                    .join();
            assertThat(created.outcome()).isEqualTo(CreateMutationOutcome.CREATED);

            RegistryAdmissionEvidenceV1 successorEvidence = evidence(2, firstValue.canonicalStoredDigest());
            firstInterlock.register(snapshot(successorEvidence));
            var successor = storedValue(successorEvidence, List.of(assignment(0)));
            var result = store.registryStore()
                    .compareAndSetRegistry(created.exactSnapshot().orElseThrow(), successor)
                    .toCompletableFuture()
                    .join();
            assertThat(result.outcome()).isEqualTo(ConditionalCasOutcome.APPLIED_EXACT);
            applied = result.exactSnapshot().orElseThrow();
            assertThat(applied.sliceView(assignment(0).pulsarCellId()).value().allocationAllowed())
                    .isTrue();
        }

        try (TestScheduler scheduler = new TestScheduler();
                OxiaV2CapabilityStore restarted = connect(authorityRoot, scheduler, new TestInterlock())) {
            var reread = restarted
                    .registryStore()
                    .readRegistry(DEPLOYMENT, RESERVATION, NAMESPACE)
                    .toCompletableFuture()
                    .join();
            assertThat(reread).contains(applied);
            assertThat(restarted.registryReady()).isTrue();
        }
    }

    @Test
    void concurrentExactCreatorsConvergeWithoutDuplicateAuthority() throws Exception {
        String authorityRoot = "/nereus/v2/r1/" + UUID.randomUUID();
        RegistryAdmissionEvidenceV1 evidence = evidence(1, Sha256Digest.copyOf(new byte[32]));
        var candidate = storedValue(evidence, List.of());
        TestInterlock leftInterlock = new TestInterlock();
        TestInterlock rightInterlock = new TestInterlock();
        leftInterlock.register(snapshot(evidence));
        rightInterlock.register(snapshot(evidence));

        try (TestScheduler leftScheduler = new TestScheduler();
                TestScheduler rightScheduler = new TestScheduler();
                OxiaV2CapabilityStore left = connect(authorityRoot, leftScheduler, leftInterlock);
                OxiaV2CapabilityStore right = connect(authorityRoot, rightScheduler, rightInterlock)) {
            var leftResult = left.registryStore().createRegistry(candidate).toCompletableFuture();
            var rightResult = right.registryStore().createRegistry(candidate).toCompletableFuture();
            var first = leftResult.get(30, TimeUnit.SECONDS);
            var second = rightResult.get(30, TimeUnit.SECONDS);

            assertThat(List.of(first.outcome(), second.outcome()))
                    .containsExactlyInAnyOrder(CreateMutationOutcome.CREATED, CreateMutationOutcome.EXISTING_EXACT);
            assertThat(first.exactSnapshot()).isEqualTo(second.exactSnapshot());
        }
    }

    private static OxiaV2CapabilityStore connect(String authorityRoot, TestScheduler scheduler, TestInterlock interlock)
            throws Exception {
        OxiaV2CapabilityStore store = OxiaV2CapabilityStoreFactory.connectR1(
                        new OxiaV2StoreConfiguration(OXIA.getServiceAddress(), "default", authorityRoot),
                        scheduler,
                        interlock)
                .get(30, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (store.continuitySnapshot().state() != StoreContinuityState.READY && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(store.continuitySnapshot().state()).isEqualTo(StoreContinuityState.READY);
        assertThat(store.registryReady()).isTrue();
        return store;
    }

    private static RegistryAdmissionEvidenceV1 evidence(long epoch, Sha256Digest predecessorDigest) {
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
                writers(),
                List.of());
    }

    private static List<RegistryWriterAdmissionV1> writers() {
        List<RegistryWriterAdmissionV1> writers = new ArrayList<>();
        for (int index = 0; index < 2; index++) {
            writers.add(new RegistryWriterAdmissionV1(
                    index == 0
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

    private static PulsarVirtualLedgerNamespaceRegistryValueV1 storedValue(
            RegistryAdmissionEvidenceV1 evidence, List<VirtualLedgerSliceAssignmentV1> assignments) {
        return PulsarVirtualLedgerNamespaceRegistryValueV1.fromDomain(
                new com.nereusstream.domain.registry.PulsarVirtualLedgerRegistryV1(
                        DEPLOYMENT,
                        RESERVATION,
                        INSTANCE,
                        NAMESPACE,
                        evidence.candidateRegistryEpoch(),
                        evidence.reference(),
                        evidence.candidateWriterRows(),
                        assignments));
    }

    private static VirtualLedgerSliceAssignmentV1 assignment(long ordinal) {
        return VirtualLedgerSliceAssignmentV1.create(
                DEPLOYMENT,
                RESERVATION,
                new PulsarCellId(new Id128(401, ordinal + 1)),
                NAMESPACE,
                VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE
                        + ordinal * VirtualLedgerSliceAssignmentV1.SLICE_SIZE,
                VirtualLedgerSliceLifecycleV1.ACTIVE);
    }

    private static RegistryInterlockSnapshotV1 snapshot(RegistryAdmissionEvidenceV1 evidence) {
        return new RegistryInterlockSnapshotV1(
                evidence,
                evidence.candidateRegistryEpoch(),
                true,
                true,
                true,
                true,
                true,
                digest("authority-" + evidence.candidateRegistryEpoch()));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static final class TestInterlock implements RegistryWriterInterlock {
        private final Map<Sha256Digest, RegistryInterlockSnapshotV1> snapshots = new HashMap<>();

        void register(RegistryInterlockSnapshotV1 snapshot) {
            snapshots.put(snapshot.evidence().reference().digest(), snapshot);
        }

        @Override
        public <T> CompletionStage<T> withPermit(
                RegistryMutationRequestV1 request,
                Function<RegistryInterlockSnapshotV1, CompletionStage<T>> protectedMutation) {
            RegistryInterlockSnapshotV1 snapshot =
                    snapshots.get(request.candidate().admissionEvidence().digest());
            if (snapshot == null) {
                throw new IllegalStateException("no interlock snapshot for Registry mutation");
            }
            return RegistryWriterInterlock.applyProtected(snapshot, protectedMutation);
        }
    }

    private static final class TestScheduler implements RevalidationScheduler {
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public boolean request(long clientGeneration, long invalidationEpoch) {
            return !closed.get();
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }
}
