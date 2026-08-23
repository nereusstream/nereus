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

package com.nereusstream.metadata.oxia.v2.allocator;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceLifecycleV1;
import com.nereusstream.domain.registry.allocator.AllocatorModeV1;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient.MutationMode;
import com.nereusstream.metadata.spi.model.ConditionalCasOutcome;
import com.nereusstream.metadata.spi.model.CreateMutationOutcome;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OxiaVirtualLedgerAllocatorStoreTest {
    private DeterministicOxiaConditionalClient client;
    private OxiaVirtualLedgerAllocatorKeys keys;
    private OxiaVirtualLedgerAllocatorStore store;
    private VirtualLedgerCellAllocatorStateV1 initialCell;
    private ManagedLedgerAllocatorHeadV1 initialHead;

    @BeforeEach
    void setUp() {
        client = new DeterministicOxiaConditionalClient();
        keys = new OxiaVirtualLedgerAllocatorKeys("/nereus/m3-allocator-test");
        store = new OxiaVirtualLedgerAllocatorStore(
                () -> {}, keys, client, new ConditionalMutationEngine(client, new MutationFailureClassifier()));
        initialCell = VirtualLedgerCellAllocatorStateV1.initial(AllocatorModeV1.RANGE_LEASED, assignment());
        initialHead = ManagedLedgerAllocatorHeadV1.initial(incarnation(), 7, initialCell.nextSliceLedgerId());
    }

    @Test
    void exactKeysAreVersionedBoundedAndLedgerOrdered() {
        String cell = keys.cellKey(namespace(), assignment().sliceAssignmentId());
        String head = keys.headKey(namespace(), assignment().sliceAssignmentId(), incarnation());
        String first = keys.nodeKey(namespace(), assignment().sliceAssignmentId(), incarnation(), 100);
        String second = keys.nodeKey(namespace(), assignment().sliceAssignmentId(), incarnation(), 101);

        assertThat(cell).endsWith("/cell").contains("/virtual-ledger-allocator/v1/");
        assertThat(head).endsWith("/head").contains(incarnation().value().toHex());
        assertThat(first).isLessThan(second).endsWith("0000000000000000100");
        assertThat(head.getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(512);
    }

    @Test
    void responseLossAfterCellCreateConvergesThroughExactSameKeyReread() {
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);

        var result = store.createCell(initialCell).toCompletableFuture().join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.EXISTING_EXACT);
        assertThat(result.exactSnapshot())
                .get()
                .extracting(snapshot -> snapshot.value())
                .isEqualTo(initialCell);
        assertThat(client.readCount()).isEqualTo(1);
    }

    @Test
    void reservationCasResponseLossConvergesAppliedExactWithoutSecondGrant() {
        var cellPredecessor = store.createCell(initialCell)
                .toCompletableFuture()
                .join()
                .exactSnapshot()
                .orElseThrow();
        var head = store.createHead(namespace(), assignment().sliceAssignmentId(), initialHead)
                .toCompletableFuture()
                .join()
                .exactSnapshot()
                .orElseThrow();
        VirtualLedgerCellAllocatorStateV1 reserved =
                AllocatorProtocolV1.reserve(initialCell, head.value(), digest("request"), 8);
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);

        var applied = store.compareAndSetCell(cellPredecessor, reserved)
                .toCompletableFuture()
                .join();

        assertThat(applied.outcome()).isEqualTo(ConditionalCasOutcome.APPLIED_EXACT);
        assertThat(applied.exactSnapshot())
                .get()
                .extracting(snapshot -> snapshot.value())
                .isEqualTo(reserved);
        assertThat(reserved.nextGrantId()).isEqualTo(initialCell.nextGrantId() + 1);
    }

    @Test
    void immutableNodeCreateResponseLossAcceptsOnlyExactBytes() {
        VirtualLedgerCellAllocatorStateV1 reserved =
                AllocatorProtocolV1.reserve(initialCell, initialHead, digest("request"), 8);
        ManagedLedgerAllocatorHeadV1 installed = AllocatorProtocolV1.installReservedRange(reserved, initialHead);
        VirtualLedgerCandidateNodeV1 node = AllocatorProtocolV1.candidate(installed, digest("descriptor"));
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);

        var result = store.createNode(namespace(), assignment().sliceAssignmentId(), node)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(CreateMutationOutcome.EXISTING_EXACT);
        assertThat(store.readNode(namespace(), assignment().sliceAssignmentId(), incarnation(), node.ledgerId())
                        .toCompletableFuture()
                        .join())
                .get()
                .extracting(snapshot -> snapshot.value())
                .isEqualTo(node);
    }

    @Test
    void responseLossWithoutApplyKeepsExactHeadPredecessor() {
        var predecessor = store.createHead(namespace(), assignment().sliceAssignmentId(), initialHead)
                .toCompletableFuture()
                .join()
                .exactSnapshot()
                .orElseThrow();
        ManagedLedgerAllocatorHeadV1 takeover = AllocatorProtocolV1.takeover(initialHead, 8);
        client.nextMutation(MutationMode.RESPONSE_LOSS_WITHOUT_APPLY);

        var result = store.compareAndSetHead(namespace(), assignment().sliceAssignmentId(), predecessor, takeover)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(ConditionalCasOutcome.PREDECESSOR_UNCHANGED);
        assertThat(result.exactSnapshot())
                .get()
                .extracting(snapshot -> snapshot.value())
                .isEqualTo(initialHead);
    }

    private static VirtualLedgerSliceAssignmentV1 assignment() {
        return VirtualLedgerSliceAssignmentV1.create(
                new DeploymentId(new Id128(1, 2)),
                new ReservationDomainId(new Id128(3, 4)),
                new PulsarCellId(new Id128(5, 6)),
                namespace(),
                VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE,
                VirtualLedgerSliceLifecycleV1.ACTIVE);
    }

    private static ManagedLedgerIncarnationIdV1 incarnation() {
        return new ManagedLedgerIncarnationIdV1(digest("managed-ledger-incarnation"));
    }

    private static Sha256Digest namespace() {
        return digest("compatibility-namespace");
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }
}
