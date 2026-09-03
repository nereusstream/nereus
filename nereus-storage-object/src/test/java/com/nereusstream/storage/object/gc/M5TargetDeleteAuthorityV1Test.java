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

package com.nereusstream.storage.object.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.DeleteTerminalOutcomeV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetKindV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterTicketV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class M5TargetDeleteAuthorityV1Test {
    @Test
    void canonicalOpenAuthorityRoundTripsAtOnePermanentTargetKey() {
        TargetDeleteAuthorityV1 open = open(1);
        CanonicalBytes encoded = M5TargetDeleteAuthorityCodecV1.encodeAuthority(open);

        assertThat(M5TargetDeleteAuthorityCodecV1.decodeAuthority(encoded)).isEqualTo(open);
        assertThat(open.authorityKey())
                .isEqualTo("v2/physical-delete-m5/"
                        + open.target().cellProviderScopeId().digest().toHex()
                        + "/"
                        + open.target().targetIdentitySha256().toHex()
                        + "/authority-v1");
        assertThat(open.authorityRevision()).isEqualTo(1);
        assertThat(open.predecessorAuthoritySha256()).isEmpty();
        assertThat(open.state()).isEqualTo(TargetDeleteAuthorityStateV1.OPEN_V1);
    }

    @Test
    void everyTicketMutationConsumesARevisionAndPreventsCanonicalAba() {
        TargetDeleteAuthorityV1 open = open(1);
        ProofBoundWriterTicketV1 ticket = ticket(open, ProofBoundWriterClassV1.REPLICA_TOPOLOGY_V1, 10);
        TargetDeleteAuthorityV1 acquired = M5TargetDeleteAuthorityStateMachineV1.acquireWriterTicket(open, ticket);
        TargetDeleteAuthorityV1 reconciled = M5TargetDeleteAuthorityStateMachineV1.completeWriterTicket(
                acquired, ticket.operationIdSha256(), open.proofSnapshotDigest());

        assertThat(acquired.authorityRevision()).isEqualTo(2);
        assertThat(reconciled.authorityRevision()).isEqualTo(3);
        assertThat(acquired.predecessorAuthoritySha256())
                .contains(Sha256Digest.hash(M5TargetDeleteAuthorityCodecV1.encodeAuthority(open)));
        assertThat(reconciled.predecessorAuthoritySha256())
                .contains(Sha256Digest.hash(M5TargetDeleteAuthorityCodecV1.encodeAuthority(acquired)));
        assertThat(M5TargetDeleteAuthorityCodecV1.encodeAuthority(reconciled))
                .isNotEqualTo(M5TargetDeleteAuthorityCodecV1.encodeAuthority(open));
    }

    @Test
    void ticketBeforeFenceVetoesCas1AndFenceBeforeTicketRejectsTheWriter() {
        TargetDeleteAuthorityV1 open = open(1);
        TargetDeleteAuthorityV1 ticketed = M5TargetDeleteAuthorityStateMachineV1.acquireWriterTicket(
                open, ticket(open, ProofBoundWriterClassV1.LOGICAL_TRIM_RETENTION_FLOOR_V1, 11));

        assertThatThrownBy(() ->
                        M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(ticketed, digest(20), digest(21)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ticket vetoes CAS-1");

        TargetDeleteAuthorityV1 fenced =
                M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(open, digest(20), digest(21));
        assertThatThrownBy(() -> M5TargetDeleteAuthorityStateMachineV1.acquireWriterTicket(
                        fenced, ticket(open, ProofBoundWriterClassV1.LOGICAL_TRIM_RETENTION_FLOOR_V1, 12)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("OPEN_V1");
        assertThat(fenced.state()).isEqualTo(TargetDeleteAuthorityStateV1.READ_FENCED_V1);
        assertThat(fenced.deleteIntent()).isEmpty();
    }

    @Test
    void competingTicketAndFenceCandidatesBindTheSameExactPredecessor() {
        TargetDeleteAuthorityV1 open = open(1);
        TargetDeleteAuthorityV1 ticketCandidate = M5TargetDeleteAuthorityStateMachineV1.acquireWriterTicket(
                open, ticket(open, ProofBoundWriterClassV1.REFERENCE_SHARED_PHYSICAL_MEMBER_V1, 13));
        TargetDeleteAuthorityV1 fenceCandidate =
                M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(open, digest(22), digest(23));

        assertThat(ticketCandidate.predecessorAuthoritySha256()).isEqualTo(fenceCandidate.predecessorAuthoritySha256());
        assertThat(ticketCandidate.authorityRevision()).isEqualTo(fenceCandidate.authorityRevision());
        assertThat(M5TargetDeleteAuthorityCodecV1.encodeAuthority(ticketCandidate))
                .isNotEqualTo(M5TargetDeleteAuthorityCodecV1.encodeAuthority(fenceCandidate));
    }

    @Test
    void cas2BindsExactIdentityAttemptOwnerAndDispatchToken() {
        TargetDeleteAuthorityV1 fenced =
                M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(open(1), digest(20), digest(21));
        ExactExternalIdentityV1 external = exactPresent(fenced, 30);
        TargetDeleteAuthorityV1 intent = M5TargetDeleteAuthorityStateMachineV1.bindDeleteIntent(
                fenced, external, digest(31), digest(32), digest(33));

        assertThat(intent.state()).isEqualTo(TargetDeleteAuthorityStateV1.DELETE_INTENT_V1);
        assertThat(intent.authorityRevision()).isEqualTo(3);
        assertThat(intent.externalIdentity()).contains(external);
        assertThat(intent.deleteIntent().orElseThrow().deleteAttemptIdSha256()).isEqualTo(digest(31));
        assertThat(intent.deleteIntent().orElseThrow().intentAuthorityRevision())
                .isEqualTo(intent.authorityRevision());
        M5TargetDeleteAuthorityStateMachineV1.requireExactDispatchAuthority(
                intent, intent.deleteIntent().orElseThrow().dispatchTokenSha256());
        assertThatThrownBy(
                        () -> M5TargetDeleteAuthorityStateMachineV1.requireExactDispatchAuthority(intent, digest(34)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dispatch token differs");
    }

    @Test
    void intentCannotBindAnExternalIdentityFromAnotherTargetKind() {
        TargetDeleteAuthorityV1 fenced =
                M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(open(1), digest(20), digest(21));
        ExactExternalIdentityV1 ledgerIdentity = ExactExternalIdentityV1.create(
                PhysicalDeleteTargetKindV1.BOOKKEEPER_LEDGER_V1,
                ExternalIdentityObservationV1.PRESENT_EXACT_V1,
                bytes("ledger-1/fingerprint"));

        assertThatThrownBy(() -> M5TargetDeleteAuthorityStateMachineV1.bindDeleteIntent(
                        fenced, ledgerIdentity, digest(31), digest(32), digest(33)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("another target kind");
    }

    @Test
    void dispatchTakeoverKeepsTheFixedAttemptAndRequiresFencedOldOwner() {
        TargetDeleteAuthorityV1 intent = intent(1);
        TargetDeleteAuthorityV1 takeover =
                M5TargetDeleteAuthorityStateMachineV1.takeOverDispatch(intent, 2, digest(40), digest(41));

        assertThat(takeover.authorityRevision()).isEqualTo(intent.authorityRevision() + 1);
        assertThat(takeover.deleteIntent().orElseThrow().deleteAttemptIdSha256())
                .isEqualTo(intent.deleteIntent().orElseThrow().deleteAttemptIdSha256());
        assertThat(takeover.deleteIntent().orElseThrow().intentAuthorityRevision())
                .isEqualTo(intent.deleteIntent().orElseThrow().intentAuthorityRevision());
        assertThat(takeover.deleteIntent().orElseThrow().dispatchEpoch()).isEqualTo(2);
        assertThat(takeover.deleteIntent().orElseThrow().ownerTakeoverProofSha256())
                .contains(digest(41));
        assertThatThrownBy(
                        () -> M5TargetDeleteAuthorityStateMachineV1.takeOverDispatch(intent, 1, digest(40), digest(41)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly increase");
        assertThatThrownBy(() ->
                        M5TargetDeleteAuthorityStateMachineV1.takeOverDispatch(intent, 2, digest(40), zeroDigest()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be zero");
    }

    @Test
    void doneIsPermanentAndBindsTheExactLastIntent() {
        TargetDeleteAuthorityV1 intent =
                M5TargetDeleteAuthorityStateMachineV1.takeOverDispatch(intent(1), 2, digest(40), digest(41));
        TargetDeleteAuthorityV1 done = M5TargetDeleteAuthorityStateMachineV1.completeDelete(
                intent, DeleteTerminalOutcomeV1.DELETED_EXACT_V1, digest(42), digest(43));

        assertThat(done.state()).isEqualTo(TargetDeleteAuthorityStateV1.DELETE_DONE_V1);
        assertThat(done.deleteDone().orElseThrow().intentCanonicalSha256())
                .isEqualTo(Sha256Digest.hash(M5TargetDeleteAuthorityCodecV1.encodeAuthority(intent)));
        assertThat(done.deleteDone().orElseThrow().deleteAttemptIdSha256())
                .isEqualTo(intent.deleteIntent().orElseThrow().deleteAttemptIdSha256());
        assertThatThrownBy(() -> M5TargetDeleteAuthorityStateMachineV1.acquireWriterTicket(
                        done,
                        new ProofBoundWriterTicketV1(
                                ProofBoundWriterClassV1.REPLICA_TOPOLOGY_V1,
                                digest(50),
                                digest(51),
                                digest(52),
                                digest(53),
                                done.authorityRevision())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("OPEN_V1");
        assertThatThrownBy(() -> M5TargetDeleteAuthorityCodecV1.successor(
                        done,
                        TargetDeleteAuthorityStateV1.DELETE_DONE_V1,
                        done.closedWriterFenceEpoch(),
                        done.proofSnapshotDigest(),
                        List.of(),
                        done.readFence(),
                        done.externalIdentity(),
                        done.deleteIntent(),
                        done.deleteDone()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("permanent");
    }

    @Test
    void exactInitialAbsenceCanOnlyCompleteAsAlreadyAbsent() {
        TargetDeleteAuthorityV1 fenced =
                M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(open(1), digest(20), digest(21));
        ExactExternalIdentityV1 absent = ExactExternalIdentityV1.create(
                fenced.target().targetKind(),
                ExternalIdentityObservationV1.ABSENT_EXACT_V1,
                bytes("expected-object-version/absence-proof"));
        TargetDeleteAuthorityV1 intent = M5TargetDeleteAuthorityStateMachineV1.bindDeleteIntent(
                fenced, absent, digest(31), digest(32), digest(33));

        assertThatThrownBy(() -> M5TargetDeleteAuthorityStateMachineV1.completeDelete(
                        intent, DeleteTerminalOutcomeV1.DELETED_EXACT_V1, digest(42), digest(43)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initially absent");
        assertThat(M5TargetDeleteAuthorityStateMachineV1.completeDelete(
                                intent, DeleteTerminalOutcomeV1.ALREADY_ABSENT_EXACT_V1, digest(42), digest(43))
                        .state())
                .isEqualTo(TargetDeleteAuthorityStateV1.DELETE_DONE_V1);
    }

    @Test
    void codecRejectsTamperTruncationAndTrailingBytes() {
        CanonicalBytes encoded = M5TargetDeleteAuthorityCodecV1.encodeAuthority(intent(1));
        byte[] tampered = encoded.toByteArray();
        tampered[tampered.length - 1] ^= 1;
        byte[] truncated = Arrays.copyOf(tampered, tampered.length - 20);
        byte[] trailing = Arrays.copyOf(encoded.toByteArray(), encoded.length() + 1);

        assertThatThrownBy(() -> M5TargetDeleteAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf(tampered)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> M5TargetDeleteAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf(truncated)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("truncated");
        assertThatThrownBy(() -> M5TargetDeleteAuthorityCodecV1.decodeAuthority(CanonicalBytes.copyOf(trailing)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing bytes");
    }

    @Test
    void closedEnrollmentAndCellBoundTargetIdentityFailClosed() {
        assertThatThrownBy(() -> new ProofBoundWriterEnrollmentV1(
                        List.of(ProofBoundWriterClassV1.REPLICA_TOPOLOGY_V1), digest(1), digest(2), digest(3)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("closed inventory");

        PhysicalDeleteTargetV1 first = target(1);
        PhysicalDeleteTargetV1 second = PhysicalDeleteTargetV1.create(
                new CellProviderScopeId(digest(2)), first.targetKind(), first.exactTargetIdentity());
        assertThat(first.targetIdentitySha256()).isNotEqualTo(second.targetIdentitySha256());
        assertThat(M5TargetDeleteAuthorityKeysV1.authorityKey(first))
                .isNotEqualTo(M5TargetDeleteAuthorityKeysV1.authorityKey(second));
    }

    private static TargetDeleteAuthorityV1 open(int cell) {
        return M5TargetDeleteAuthorityStateMachineV1.open(target(cell), enrollment(), digest(9));
    }

    private static TargetDeleteAuthorityV1 intent(int cell) {
        TargetDeleteAuthorityV1 fenced =
                M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(open(cell), digest(20), digest(21));
        return M5TargetDeleteAuthorityStateMachineV1.bindDeleteIntent(
                fenced, exactPresent(fenced, 30), digest(31), digest(32), digest(33));
    }

    private static ExactExternalIdentityV1 exactPresent(TargetDeleteAuthorityV1 value, int suffix) {
        return ExactExternalIdentityV1.create(
                value.target().targetKind(),
                ExternalIdentityObservationV1.PRESENT_EXACT_V1,
                bytes("object/version-" + suffix + "/length/body/root/footer"));
    }

    private static ProofBoundWriterTicketV1 ticket(
            TargetDeleteAuthorityV1 value, ProofBoundWriterClassV1 writerClass, int suffix) {
        return new ProofBoundWriterTicketV1(
                writerClass,
                digest(suffix),
                digest(suffix + 1),
                digest(suffix + 2),
                digest(suffix + 3),
                value.authorityRevision());
    }

    private static PhysicalDeleteTargetV1 target(int cell) {
        return PhysicalDeleteTargetV1.create(
                new CellProviderScopeId(digest(cell)),
                PhysicalDeleteTargetKindV1.OBJECT_VERSION_V1,
                bytes("cell/" + cell + "/bucket/object/version-7/descriptor"));
    }

    private static ProofBoundWriterEnrollmentV1 enrollment() {
        return new ProofBoundWriterEnrollmentV1(
                List.of(ProofBoundWriterClassV1.values()), digest(4), digest(5), digest(6));
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Sha256Digest digest(int lastByte) {
        byte[] value = new byte[Sha256Digest.LENGTH];
        value[value.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(value);
    }

    private static Sha256Digest zeroDigest() {
        return Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]);
    }
}
