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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.gc.M5PhysicalOrphanProtocolV1.MarkOutcome;
import com.nereusstream.storage.object.gc.M5PhysicalOrphanProtocolV1.OrphanClassDisposition;
import com.nereusstream.storage.object.gc.M5PhysicalOrphanProtocolV1.OrphanScanEvidenceV1;
import com.nereusstream.storage.object.gc.M5PhysicalOrphanProtocolV1.PhysicalOrphanClassV1;
import com.nereusstream.storage.object.gc.M5PhysicalOrphanProtocolV1.PhysicalOrphanObservationV1;
import com.nereusstream.storage.object.gc.M5PhysicalOrphanProtocolV1.RescanOutcome;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class M5PhysicalOrphanProtocolV1Test {
    @Test
    void closedTaxonomyAllowsOnlyThreeClassesToEnterMarkProtocol() {
        assertThat(PhysicalOrphanClassV1.values())
                .containsExactly(
                        PhysicalOrphanClassV1.PHYSICAL_OUTPUT_ORPHAN_CANDIDATE,
                        PhysicalOrphanClassV1.MULTIPART_RESIDUE_CANDIDATE,
                        PhysicalOrphanClassV1.RELEASED_SOURCE_CANDIDATE,
                        PhysicalOrphanClassV1.PERMANENT_METADATA_FENCE,
                        PhysicalOrphanClassV1.ALLOCATOR_NO_REUSE_EVIDENCE,
                        PhysicalOrphanClassV1.UNKNOWN_OR_FOREIGN);
        Map<PhysicalOrphanClassV1, OrphanClassDisposition> dispositions = Arrays.stream(PhysicalOrphanClassV1.values())
                .collect(Collectors.toMap(value -> value, M5PhysicalOrphanProtocolV1::classDisposition));
        assertThat(dispositions)
                .containsEntry(
                        PhysicalOrphanClassV1.PHYSICAL_OUTPUT_ORPHAN_CANDIDATE,
                        OrphanClassDisposition.MAY_ENTER_MARK_PROTOCOL)
                .containsEntry(
                        PhysicalOrphanClassV1.MULTIPART_RESIDUE_CANDIDATE,
                        OrphanClassDisposition.MAY_ENTER_MARK_PROTOCOL)
                .containsEntry(
                        PhysicalOrphanClassV1.RELEASED_SOURCE_CANDIDATE, OrphanClassDisposition.MAY_ENTER_MARK_PROTOCOL)
                .containsEntry(PhysicalOrphanClassV1.PERMANENT_METADATA_FENCE, OrphanClassDisposition.PERMANENT_RETAIN)
                .containsEntry(
                        PhysicalOrphanClassV1.ALLOCATOR_NO_REUSE_EVIDENCE, OrphanClassDisposition.PERMANENT_RETAIN)
                .containsEntry(PhysicalOrphanClassV1.UNKNOWN_OR_FOREIGN, OrphanClassDisposition.QUARANTINE_ONLY);
    }

    @Test
    void completeFirstScanMarksThenExactAuthorityTimeBoundaryAdmitsOnlyAFutureIntentCandidate() {
        PhysicalOrphanObservationV1 observation = observation(PhysicalOrphanClassV1.PHYSICAL_OUTPUT_ORPHAN_CANDIDATE);
        OrphanScanEvidenceV1 evidence = eligibleEvidence(10);
        var first = M5PhysicalOrphanProtocolV1.mark(observation, evidence, 100, 50);
        var repeated = M5PhysicalOrphanProtocolV1.mark(observation, evidence, 100, 50);

        assertThat(first.outcome()).isEqualTo(MarkOutcome.MARKED);
        assertThat(repeated).isEqualTo(first);
        var mark = first.mark().orElseThrow();
        assertThat(mark.graceDeadlineAuthorityTimeMillis()).isEqualTo(150);
        assertThat(M5PhysicalOrphanProtocolV1.rescan(mark, observation, eligibleEvidence(20), 149)
                        .outcome())
                .isEqualTo(RescanOutcome.GRACE_PENDING);
        var ready = M5PhysicalOrphanProtocolV1.rescan(mark, observation, eligibleEvidence(20), 150);
        assertThat(ready.outcome()).isEqualTo(RescanOutcome.FUTURE_INTENT_CANDIDATE);
        assertThat(ready.readyProof().orElseThrow().readyProofRoot().isZero()).isFalse();
    }

    @Test
    void permanentEvidenceNeverMarksAndForeignIdentityOnlyQuarantines() {
        assertThat(M5PhysicalOrphanProtocolV1.mark(
                                observation(PhysicalOrphanClassV1.PERMANENT_METADATA_FENCE),
                                eligibleEvidence(1),
                                10,
                                10)
                        .outcome())
                .isEqualTo(MarkOutcome.PERMANENT_RETAIN);
        assertThat(M5PhysicalOrphanProtocolV1.mark(
                                observation(PhysicalOrphanClassV1.ALLOCATOR_NO_REUSE_EVIDENCE),
                                eligibleEvidence(1),
                                10,
                                10)
                        .outcome())
                .isEqualTo(MarkOutcome.PERMANENT_RETAIN);
        assertThat(M5PhysicalOrphanProtocolV1.mark(
                                observation(PhysicalOrphanClassV1.UNKNOWN_OR_FOREIGN), eligibleEvidence(1), 10, 10)
                        .outcome())
                .isEqualTo(MarkOutcome.QUARANTINE);
    }

    @Test
    void listDiscoveryIncompleteScanAndUnreconciledCreateNeverMark() {
        PhysicalOrphanObservationV1 observation = observation(PhysicalOrphanClassV1.MULTIPART_RESIDUE_CANDIDATE);
        assertThat(M5PhysicalOrphanProtocolV1.mark(observation, evidence(false, false, false, false), 10, 10)
                        .outcome())
                .isEqualTo(MarkOutcome.RETAIN);
        assertThat(M5PhysicalOrphanProtocolV1.mark(observation, evidence(false, true, true, false), 10, 10)
                        .outcome())
                .isEqualTo(MarkOutcome.RETAIN);
        assertThat(M5PhysicalOrphanProtocolV1.mark(observation, evidence(false, true, false, true), 10, 10)
                        .outcome())
                .isEqualTo(MarkOutcome.RETAIN);
    }

    @Test
    void liveOwnerIsAdoptedAndContradictoryOwnershipQuarantines() {
        PhysicalOrphanObservationV1 observation = observation(PhysicalOrphanClassV1.RELEASED_SOURCE_CANDIDATE);
        assertThat(M5PhysicalOrphanProtocolV1.mark(observation, evidence(true, true, true, true), 10, 10)
                        .outcome())
                .isEqualTo(MarkOutcome.ADOPT_LIVE_OWNER);
        OrphanScanEvidenceV1 contradictory = new OrphanScanEvidenceV1(
                true, true, true, true, true, true, digest(1), digest(2), digest(3), digest(4));
        assertThat(M5PhysicalOrphanProtocolV1.mark(observation, contradictory, 10, 10)
                        .outcome())
                .isEqualTo(MarkOutcome.QUARANTINE);
    }

    @Test
    void rescanRejectsChangedIdentityAndAdoptsANewLiveOwner() {
        PhysicalOrphanObservationV1 observation = observation(PhysicalOrphanClassV1.PHYSICAL_OUTPUT_ORPHAN_CANDIDATE);
        var mark = M5PhysicalOrphanProtocolV1.mark(observation, eligibleEvidence(1), 10, 10)
                .mark()
                .orElseThrow();
        PhysicalOrphanObservationV1 changed = new PhysicalOrphanObservationV1(
                observation.cellProviderScopeId(),
                observation.orphanClass(),
                observation.physicalIdentity(),
                observation.canonicalLength() + 1,
                observation.physicalIdentityRoot(),
                observation.exactContentRoot(),
                observation.immutableProviderVersionRoot());

        assertThat(M5PhysicalOrphanProtocolV1.rescan(mark, changed, eligibleEvidence(2), 20)
                        .outcome())
                .isEqualTo(RescanOutcome.QUARANTINE);
        var forged = new M5PhysicalOrphanProtocolV1.OrphanMarkV1(
                mark.observation(),
                mark.firstScanInventoryRoot(),
                mark.firstTaskManifestVersionsRoot(),
                mark.firstFenceRoot(),
                mark.firstResponseLossReconciliationRoot(),
                mark.firstAuthorityTimeMillis(),
                mark.graceDeadlineAuthorityTimeMillis(),
                digest(99));
        assertThat(M5PhysicalOrphanProtocolV1.rescan(forged, observation, eligibleEvidence(2), 20)
                        .outcome())
                .isEqualTo(RescanOutcome.QUARANTINE);
        assertThat(M5PhysicalOrphanProtocolV1.rescan(mark, observation, evidence(true, true, true, true), 20)
                        .outcome())
                .isEqualTo(RescanOutcome.ADOPT_LIVE_OWNER);
    }

    @Test
    void pureProtocolExposesNoDeleteOrIntentMutationApi() {
        assertThat(Arrays.stream(M5PhysicalOrphanProtocolV1.class.getDeclaredMethods())
                        .filter(method -> Modifier.isPublic(method.getModifiers()))
                        .map(method -> method.getName())
                        .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder("classDisposition", "mark", "rescan");
    }

    private static PhysicalOrphanObservationV1 observation(PhysicalOrphanClassV1 orphanClass) {
        return new PhysicalOrphanObservationV1(
                new CellProviderScopeId(digest(90)),
                orphanClass,
                "cell/90/object/segment-1",
                4096,
                digest(91),
                Optional.of(digest(92)),
                Optional.of(digest(93)));
    }

    private static OrphanScanEvidenceV1 eligibleEvidence(int offset) {
        return new OrphanScanEvidenceV1(
                false,
                true,
                true,
                true,
                true,
                true,
                digest(offset),
                digest(offset + 1),
                digest(offset + 2),
                digest(offset + 3));
    }

    private static OrphanScanEvidenceV1 evidence(
            boolean ownerPresent, boolean completeScan, boolean referenceAbsent, boolean responseLossReconciled) {
        return new OrphanScanEvidenceV1(
                ownerPresent,
                !ownerPresent,
                completeScan,
                referenceAbsent,
                responseLossReconciled,
                true,
                digest(1),
                digest(2),
                digest(3),
                digest(4));
    }

    private static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }
}
