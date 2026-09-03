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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

/** Pure M5 orphan classification and mark/grace/rescan protocol with no external delete capability. */
public final class M5PhysicalOrphanProtocolV1 {
    private static final int MAX_PHYSICAL_IDENTITY_BYTES = 4096;
    private static final byte[] MARK_DOMAIN = "NEREUS-M5-ORPHAN-MARK-V1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] READY_DOMAIN = "NEREUS-M5-ORPHAN-READY-V1".getBytes(StandardCharsets.US_ASCII);

    private M5PhysicalOrphanProtocolV1() {}

    public static OrphanClassDisposition classDisposition(PhysicalOrphanClassV1 orphanClass) {
        Objects.requireNonNull(orphanClass, "orphanClass");
        return switch (orphanClass) {
            case PHYSICAL_OUTPUT_ORPHAN_CANDIDATE, MULTIPART_RESIDUE_CANDIDATE, RELEASED_SOURCE_CANDIDATE ->
                OrphanClassDisposition.MAY_ENTER_MARK_PROTOCOL;
            case PERMANENT_METADATA_FENCE, ALLOCATOR_NO_REUSE_EVIDENCE -> OrphanClassDisposition.PERMANENT_RETAIN;
            case UNKNOWN_OR_FOREIGN -> OrphanClassDisposition.QUARANTINE_ONLY;
        };
    }

    public static MarkResult mark(
            PhysicalOrphanObservationV1 observation,
            OrphanScanEvidenceV1 evidence,
            long firstAuthorityTimeMillis,
            long requiredGraceMillis) {
        Objects.requireNonNull(observation, "observation");
        Objects.requireNonNull(evidence, "evidence");
        if (firstAuthorityTimeMillis < 0 || requiredGraceMillis <= 0) {
            throw new IllegalArgumentException("authority time and required grace must be finite and positive");
        }
        return switch (classDisposition(observation.orphanClass())) {
            case PERMANENT_RETAIN -> MarkResult.permanentRetain();
            case QUARANTINE_ONLY -> MarkResult.quarantine();
            case MAY_ENTER_MARK_PROTOCOL ->
                markCandidate(observation, evidence, firstAuthorityTimeMillis, requiredGraceMillis);
        };
    }

    public static RescanResult rescan(
            OrphanMarkV1 mark,
            PhysicalOrphanObservationV1 currentObservation,
            OrphanScanEvidenceV1 currentEvidence,
            long currentAuthorityTimeMillis) {
        Objects.requireNonNull(mark, "mark");
        Objects.requireNonNull(currentObservation, "currentObservation");
        Objects.requireNonNull(currentEvidence, "currentEvidence");
        if (currentAuthorityTimeMillis < 0) {
            throw new IllegalArgumentException("authority time must be non-negative");
        }
        if (!validMark(mark)
                || classDisposition(currentObservation.orphanClass()) == OrphanClassDisposition.QUARANTINE_ONLY
                || !mark.observation().equals(currentObservation)) {
            return RescanResult.quarantine();
        }
        if (classDisposition(currentObservation.orphanClass()) == OrphanClassDisposition.PERMANENT_RETAIN) {
            return RescanResult.permanentRetain();
        }
        if (contradictory(currentEvidence)) {
            return RescanResult.quarantine();
        }
        if (currentEvidence.authoritativeOwnerPresent()) {
            return RescanResult.adoptLiveOwner();
        }
        if (currentAuthorityTimeMillis < mark.graceDeadlineAuthorityTimeMillis()) {
            return RescanResult.gracePending();
        }
        if (!eligibleEvidence(currentEvidence)) {
            return RescanResult.retain();
        }
        Sha256Digest rescanRoot = readyRoot(mark, currentEvidence, currentAuthorityTimeMillis);
        return RescanResult.futureIntentCandidate(new OrphanReadyProofV1(
                mark.markRoot(),
                currentEvidence.scanInventoryRoot(),
                currentEvidence.taskManifestVersionsRoot(),
                currentEvidence.fenceRoot(),
                currentEvidence.responseLossReconciliationRoot(),
                currentAuthorityTimeMillis,
                rescanRoot));
    }

    private static boolean validMark(OrphanMarkV1 mark) {
        if (classDisposition(mark.observation().orphanClass()) != OrphanClassDisposition.MAY_ENTER_MARK_PROTOCOL) {
            return false;
        }
        OrphanScanEvidenceV1 firstEvidence = new OrphanScanEvidenceV1(
                false,
                true,
                true,
                true,
                true,
                true,
                mark.firstScanInventoryRoot(),
                mark.firstTaskManifestVersionsRoot(),
                mark.firstFenceRoot(),
                mark.firstResponseLossReconciliationRoot());
        return mark.markRoot()
                .equals(markRoot(
                        mark.observation(),
                        firstEvidence,
                        mark.firstAuthorityTimeMillis(),
                        mark.graceDeadlineAuthorityTimeMillis()));
    }

    private static MarkResult markCandidate(
            PhysicalOrphanObservationV1 observation,
            OrphanScanEvidenceV1 evidence,
            long firstAuthorityTimeMillis,
            long requiredGraceMillis) {
        if (contradictory(evidence)) {
            return MarkResult.quarantine();
        }
        if (evidence.authoritativeOwnerPresent()) {
            return MarkResult.adoptLiveOwner();
        }
        if (!eligibleEvidence(evidence)) {
            return MarkResult.retain();
        }
        long deadline;
        try {
            deadline = Math.addExact(firstAuthorityTimeMillis, requiredGraceMillis);
        } catch (ArithmeticException overflow) {
            return MarkResult.retain();
        }
        Sha256Digest root = markRoot(observation, evidence, firstAuthorityTimeMillis, deadline);
        return MarkResult.marked(new OrphanMarkV1(
                observation,
                evidence.scanInventoryRoot(),
                evidence.taskManifestVersionsRoot(),
                evidence.fenceRoot(),
                evidence.responseLossReconciliationRoot(),
                firstAuthorityTimeMillis,
                deadline,
                root));
    }

    private static boolean eligibleEvidence(OrphanScanEvidenceV1 evidence) {
        return evidence.authoritativeOwnerAbsentOrReleased()
                && evidence.completeReferenceScan()
                && evidence.everyReferenceAbsent()
                && evidence.responseLossPathsReconciled()
                && evidence.currentFencesExact();
    }

    private static boolean contradictory(OrphanScanEvidenceV1 evidence) {
        return evidence.authoritativeOwnerPresent() && evidence.authoritativeOwnerAbsentOrReleased()
                || evidence.everyReferenceAbsent() && !evidence.completeReferenceScan();
    }

    private static Sha256Digest markRoot(
            PhysicalOrphanObservationV1 observation,
            OrphanScanEvidenceV1 evidence,
            long firstAuthorityTimeMillis,
            long deadline) {
        return hash(output -> {
            writeBytes(output, MARK_DOMAIN);
            writeObservation(output, observation);
            writeEvidence(output, evidence);
            output.writeLong(firstAuthorityTimeMillis);
            output.writeLong(deadline);
        });
    }

    private static Sha256Digest readyRoot(OrphanMarkV1 mark, OrphanScanEvidenceV1 evidence, long authorityTimeMillis) {
        return hash(output -> {
            writeBytes(output, READY_DOMAIN);
            writeBytes(output, mark.markRoot().bytes().toByteArray());
            writeObservation(output, mark.observation());
            writeEvidence(output, evidence);
            output.writeLong(authorityTimeMillis);
        });
    }

    private static void writeObservation(DataOutputStream output, PhysicalOrphanObservationV1 value)
            throws IOException {
        writeBytes(output, value.cellProviderScopeId().digest().bytes().toByteArray());
        writeString(output, value.orphanClass().name());
        writeString(output, value.physicalIdentity());
        output.writeLong(value.canonicalLength());
        writeBytes(output, value.physicalIdentityRoot().bytes().toByteArray());
        writeOptionalDigest(output, value.exactContentRoot());
        writeOptionalDigest(output, value.immutableProviderVersionRoot());
    }

    private static void writeEvidence(DataOutputStream output, OrphanScanEvidenceV1 value) throws IOException {
        output.writeBoolean(value.authoritativeOwnerPresent());
        output.writeBoolean(value.authoritativeOwnerAbsentOrReleased());
        output.writeBoolean(value.completeReferenceScan());
        output.writeBoolean(value.everyReferenceAbsent());
        output.writeBoolean(value.responseLossPathsReconciled());
        output.writeBoolean(value.currentFencesExact());
        writeBytes(output, value.scanInventoryRoot().bytes().toByteArray());
        writeBytes(output, value.taskManifestVersionsRoot().bytes().toByteArray());
        writeBytes(output, value.fenceRoot().bytes().toByteArray());
        writeBytes(output, value.responseLossReconciliationRoot().bytes().toByteArray());
    }

    private static void writeOptionalDigest(DataOutputStream output, Optional<Sha256Digest> value) throws IOException {
        output.writeBoolean(value.isPresent());
        if (value.isPresent()) {
            writeBytes(output, value.orElseThrow().bytes().toByteArray());
        }
    }

    private static Sha256Digest hash(Encoder encoder) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                encoder.encode(output);
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory orphan protocol encoding failed", impossible);
        }
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        writeBytes(output, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    public enum PhysicalOrphanClassV1 {
        PHYSICAL_OUTPUT_ORPHAN_CANDIDATE,
        MULTIPART_RESIDUE_CANDIDATE,
        RELEASED_SOURCE_CANDIDATE,
        PERMANENT_METADATA_FENCE,
        ALLOCATOR_NO_REUSE_EVIDENCE,
        UNKNOWN_OR_FOREIGN
    }

    public enum OrphanClassDisposition {
        MAY_ENTER_MARK_PROTOCOL,
        PERMANENT_RETAIN,
        QUARANTINE_ONLY
    }

    public enum MarkOutcome {
        MARKED,
        ADOPT_LIVE_OWNER,
        RETAIN,
        PERMANENT_RETAIN,
        QUARANTINE
    }

    public enum RescanOutcome {
        FUTURE_INTENT_CANDIDATE,
        GRACE_PENDING,
        ADOPT_LIVE_OWNER,
        RETAIN,
        PERMANENT_RETAIN,
        QUARANTINE
    }

    public record PhysicalOrphanObservationV1(
            CellProviderScopeId cellProviderScopeId,
            PhysicalOrphanClassV1 orphanClass,
            String physicalIdentity,
            long canonicalLength,
            Sha256Digest physicalIdentityRoot,
            Optional<Sha256Digest> exactContentRoot,
            Optional<Sha256Digest> immutableProviderVersionRoot) {
        public PhysicalOrphanObservationV1 {
            Objects.requireNonNull(cellProviderScopeId, "cellProviderScopeId");
            Objects.requireNonNull(orphanClass, "orphanClass");
            Objects.requireNonNull(physicalIdentity, "physicalIdentity");
            Objects.requireNonNull(physicalIdentityRoot, "physicalIdentityRoot");
            Objects.requireNonNull(exactContentRoot, "exactContentRoot");
            Objects.requireNonNull(immutableProviderVersionRoot, "immutableProviderVersionRoot");
            int identityBytes = physicalIdentity.getBytes(StandardCharsets.UTF_8).length;
            if (physicalIdentity.isBlank()
                    || identityBytes > MAX_PHYSICAL_IDENTITY_BYTES
                    || canonicalLength < 0
                    || physicalIdentityRoot.isZero()) {
                throw new IllegalArgumentException("invalid bounded physical orphan observation");
            }
        }
    }

    public record OrphanScanEvidenceV1(
            boolean authoritativeOwnerPresent,
            boolean authoritativeOwnerAbsentOrReleased,
            boolean completeReferenceScan,
            boolean everyReferenceAbsent,
            boolean responseLossPathsReconciled,
            boolean currentFencesExact,
            Sha256Digest scanInventoryRoot,
            Sha256Digest taskManifestVersionsRoot,
            Sha256Digest fenceRoot,
            Sha256Digest responseLossReconciliationRoot) {
        public OrphanScanEvidenceV1 {
            Objects.requireNonNull(scanInventoryRoot, "scanInventoryRoot");
            Objects.requireNonNull(taskManifestVersionsRoot, "taskManifestVersionsRoot");
            Objects.requireNonNull(fenceRoot, "fenceRoot");
            Objects.requireNonNull(responseLossReconciliationRoot, "responseLossReconciliationRoot");
            if (scanInventoryRoot.isZero()
                    || taskManifestVersionsRoot.isZero()
                    || fenceRoot.isZero()
                    || responseLossReconciliationRoot.isZero()) {
                throw new IllegalArgumentException("orphan scan evidence roots must be non-zero");
            }
        }
    }

    public record OrphanMarkV1(
            PhysicalOrphanObservationV1 observation,
            Sha256Digest firstScanInventoryRoot,
            Sha256Digest firstTaskManifestVersionsRoot,
            Sha256Digest firstFenceRoot,
            Sha256Digest firstResponseLossReconciliationRoot,
            long firstAuthorityTimeMillis,
            long graceDeadlineAuthorityTimeMillis,
            Sha256Digest markRoot) {
        public OrphanMarkV1 {
            Objects.requireNonNull(observation, "observation");
            Objects.requireNonNull(firstScanInventoryRoot, "firstScanInventoryRoot");
            Objects.requireNonNull(firstTaskManifestVersionsRoot, "firstTaskManifestVersionsRoot");
            Objects.requireNonNull(firstFenceRoot, "firstFenceRoot");
            Objects.requireNonNull(firstResponseLossReconciliationRoot, "firstResponseLossReconciliationRoot");
            Objects.requireNonNull(markRoot, "markRoot");
            if (firstAuthorityTimeMillis < 0
                    || graceDeadlineAuthorityTimeMillis <= firstAuthorityTimeMillis
                    || firstScanInventoryRoot.isZero()
                    || firstTaskManifestVersionsRoot.isZero()
                    || firstFenceRoot.isZero()
                    || firstResponseLossReconciliationRoot.isZero()
                    || markRoot.isZero()) {
                throw new IllegalArgumentException("invalid orphan mark authority-time boundary");
            }
        }
    }

    public record OrphanReadyProofV1(
            Sha256Digest markRoot,
            Sha256Digest rescanInventoryRoot,
            Sha256Digest rescanTaskManifestVersionsRoot,
            Sha256Digest rescanFenceRoot,
            Sha256Digest rescanResponseLossReconciliationRoot,
            long rescanAuthorityTimeMillis,
            Sha256Digest readyProofRoot) {
        public OrphanReadyProofV1 {
            Objects.requireNonNull(markRoot, "markRoot");
            Objects.requireNonNull(rescanInventoryRoot, "rescanInventoryRoot");
            Objects.requireNonNull(rescanTaskManifestVersionsRoot, "rescanTaskManifestVersionsRoot");
            Objects.requireNonNull(rescanFenceRoot, "rescanFenceRoot");
            Objects.requireNonNull(rescanResponseLossReconciliationRoot, "rescanResponseLossReconciliationRoot");
            Objects.requireNonNull(readyProofRoot, "readyProofRoot");
            if (rescanAuthorityTimeMillis < 0
                    || markRoot.isZero()
                    || rescanInventoryRoot.isZero()
                    || rescanTaskManifestVersionsRoot.isZero()
                    || rescanFenceRoot.isZero()
                    || rescanResponseLossReconciliationRoot.isZero()
                    || readyProofRoot.isZero()) {
                throw new IllegalArgumentException("invalid orphan rescan proof");
            }
        }
    }

    public record MarkResult(MarkOutcome outcome, Optional<OrphanMarkV1> mark) {
        public MarkResult {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(mark, "mark");
            if ((outcome == MarkOutcome.MARKED) != mark.isPresent()) {
                throw new IllegalArgumentException("only MARKED carries an orphan mark");
            }
        }

        static MarkResult marked(OrphanMarkV1 mark) {
            return new MarkResult(MarkOutcome.MARKED, Optional.of(mark));
        }

        static MarkResult adoptLiveOwner() {
            return new MarkResult(MarkOutcome.ADOPT_LIVE_OWNER, Optional.empty());
        }

        static MarkResult retain() {
            return new MarkResult(MarkOutcome.RETAIN, Optional.empty());
        }

        static MarkResult permanentRetain() {
            return new MarkResult(MarkOutcome.PERMANENT_RETAIN, Optional.empty());
        }

        static MarkResult quarantine() {
            return new MarkResult(MarkOutcome.QUARANTINE, Optional.empty());
        }
    }

    public record RescanResult(RescanOutcome outcome, Optional<OrphanReadyProofV1> readyProof) {
        public RescanResult {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(readyProof, "readyProof");
            if ((outcome == RescanOutcome.FUTURE_INTENT_CANDIDATE) != readyProof.isPresent()) {
                throw new IllegalArgumentException("only FUTURE_INTENT_CANDIDATE carries a rescan proof");
            }
        }

        static RescanResult futureIntentCandidate(OrphanReadyProofV1 proof) {
            return new RescanResult(RescanOutcome.FUTURE_INTENT_CANDIDATE, Optional.of(proof));
        }

        static RescanResult gracePending() {
            return new RescanResult(RescanOutcome.GRACE_PENDING, Optional.empty());
        }

        static RescanResult adoptLiveOwner() {
            return new RescanResult(RescanOutcome.ADOPT_LIVE_OWNER, Optional.empty());
        }

        static RescanResult retain() {
            return new RescanResult(RescanOutcome.RETAIN, Optional.empty());
        }

        static RescanResult permanentRetain() {
            return new RescanResult(RescanOutcome.PERMANENT_RETAIN, Optional.empty());
        }

        static RescanResult quarantine() {
            return new RescanResult(RescanOutcome.QUARANTINE, Optional.empty());
        }
    }

    @FunctionalInterface
    private interface Encoder {
        void encode(DataOutputStream output) throws IOException;
    }
}
