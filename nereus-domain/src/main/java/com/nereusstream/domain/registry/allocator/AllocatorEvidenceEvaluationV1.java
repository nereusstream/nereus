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

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Production-parser result for complete raw ADR-0094 evidence, including honest non-selection outcomes. */
public final class AllocatorEvidenceEvaluationV1 {
    public enum Status {
        STRICT_SELECTED,
        RANGE_SELECTED,
        NONE_QUALIFIED,
        BOTH_QUALIFIED
    }

    private final AllocatorEvidenceSourceTupleV1 sourceTuple;
    private final Map<AllocatorEvidenceAttachmentKindV1, Sha256Digest> attachmentSha256;
    private final Status status;
    private final List<AllocatorEvidenceCandidateV1> qualifiedCandidates;
    private final Optional<AllocatorEvidenceCandidateV1> selectedCandidate;
    private final List<AllocatorNativeRelativeMetricsV1> selectedRows;
    private final long tests;
    private final long testFailures;
    private final long testErrors;
    private final long testSkips;

    private AllocatorEvidenceEvaluationV1(
            AllocatorEvidenceSourceTupleV1 sourceTuple,
            Map<AllocatorEvidenceAttachmentKindV1, Sha256Digest> attachmentSha256,
            Status status,
            List<AllocatorEvidenceCandidateV1> qualifiedCandidates,
            Optional<AllocatorEvidenceCandidateV1> selectedCandidate,
            List<AllocatorNativeRelativeMetricsV1> selectedRows,
            AllocatorJUnitEvidenceV1.Counts junit) {
        this.sourceTuple = Objects.requireNonNull(sourceTuple, "sourceTuple");
        this.attachmentSha256 = Map.copyOf(attachmentSha256);
        this.status = Objects.requireNonNull(status, "status");
        this.qualifiedCandidates = List.copyOf(qualifiedCandidates);
        this.selectedCandidate = Objects.requireNonNull(selectedCandidate, "selectedCandidate");
        this.selectedRows = List.copyOf(selectedRows);
        this.tests = junit.tests();
        this.testFailures = junit.failures();
        this.testErrors = junit.errors();
        this.testSkips = junit.skipped();
        if (selectionEligible() != (this.selectedCandidate.isPresent() && this.selectedRows.size() == 8)) {
            throw new IllegalArgumentException("allocator raw evaluation selection/row inventory differs");
        }
    }

    static AllocatorEvidenceEvaluationV1 from(
            AllocatorEvidenceSourceTupleV1 sourceTuple,
            Map<AllocatorEvidenceAttachmentKindV1, Sha256Digest> attachmentSha256,
            AllocatorRawEvidenceValidatorV1.SelectionComputation computation) {
        Status status = Status.valueOf(computation.status().name());
        return new AllocatorEvidenceEvaluationV1(
                sourceTuple,
                attachmentSha256,
                status,
                computation.qualifiedCandidates(),
                computation.selectedCandidate(),
                computation.selectedRows(),
                computation.junit());
    }

    public boolean selectionEligible() {
        return status == Status.STRICT_SELECTED || status == Status.RANGE_SELECTED;
    }

    public AllocatorEvidenceSourceTupleV1 sourceTuple() {
        return sourceTuple;
    }

    public Map<AllocatorEvidenceAttachmentKindV1, Sha256Digest> attachmentSha256() {
        return attachmentSha256;
    }

    public Status status() {
        return status;
    }

    public List<AllocatorEvidenceCandidateV1> qualifiedCandidates() {
        return qualifiedCandidates;
    }

    public Optional<AllocatorEvidenceCandidateV1> selectedCandidate() {
        return selectedCandidate;
    }

    public List<AllocatorNativeRelativeMetricsV1> selectedRows() {
        return selectedRows;
    }

    public long tests() {
        return tests;
    }

    public long testFailures() {
        return testFailures;
    }

    public long testErrors() {
        return testErrors;
    }

    public long testSkips() {
        return testSkips;
    }
}
