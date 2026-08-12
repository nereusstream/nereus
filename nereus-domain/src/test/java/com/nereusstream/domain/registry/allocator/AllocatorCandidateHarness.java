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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Deterministic evidence-only allocator model. It is deliberately absent from production sources and SPI. */
final class AllocatorCandidateHarness {
    enum Mode {
        STRICT_SERIALIZED,
        RANGE_LEASED
    }

    enum Outcome {
        APPLIED_EXACT,
        EXISTING_EXACT,
        DEFINITIVE_CONFLICT,
        INDETERMINATE
    }

    enum Cut {
        NONE,
        APPLIED_RESPONSE_LOST,
        NOT_APPLIED_RESPONSE_LOST
    }

    record AllocationState(long visibleHead, long priorGrantId, long nextLedgerId) {}

    record Reservation(
            long managedLedgerIncarnation,
            long grantId,
            long rangeStart,
            long rangeEndExclusive,
            AllocationState expectedState) {}

    record Candidate(long ledgerId, long grantId, long creatorOwnerEpoch, long expectedPredecessor, String digest) {}

    record Head(
            long managedLedgerIncarnation,
            long ownerEpoch,
            long visibleHead,
            long grantId,
            long rangeStart,
            long rangeEndExclusive,
            long nextLedgerId) {}

    private final Mode mode;
    private long cellNextLedgerId;
    private long nextGrantId = 1;
    private Reservation reserved;
    private Head head;
    private final Map<Long, Candidate> candidates = new HashMap<>();

    AllocatorCandidateHarness(Mode mode, long firstLedgerId, long managedLedgerIncarnation, long ownerEpoch) {
        this.mode = Objects.requireNonNull(mode, "mode");
        this.cellNextLedgerId = firstLedgerId;
        this.head = new Head(managedLedgerIncarnation, ownerEpoch, -1, 0, 0, 0, firstLedgerId);
    }

    Head head() {
        return head;
    }

    Optional<Reservation> reserved() {
        return Optional.ofNullable(reserved);
    }

    Optional<Candidate> candidate(long ledgerId) {
        return Optional.ofNullable(candidates.get(ledgerId));
    }

    Outcome reserve(long rangeSize, Cut cut) {
        if (rangeSize <= 0 || (mode == Mode.STRICT_SERIALIZED && rangeSize != 1)) {
            return Outcome.DEFINITIVE_CONFLICT;
        }
        if (reserved != null) {
            return reserved.expectedState().equals(allocationState())
                    ? Outcome.EXISTING_EXACT
                    : Outcome.DEFINITIVE_CONFLICT;
        }
        if (cut == Cut.NOT_APPLIED_RESPONSE_LOST) {
            return Outcome.INDETERMINATE;
        }
        long rangeEnd = Math.addExact(cellNextLedgerId, rangeSize);
        reserved = new Reservation(
                head.managedLedgerIncarnation(), nextGrantId++, cellNextLedgerId, rangeEnd, allocationState());
        cellNextLedgerId = rangeEnd;
        return cut == Cut.APPLIED_RESPONSE_LOST ? Outcome.INDETERMINATE : Outcome.APPLIED_EXACT;
    }

    Outcome installReserved(Cut cut) {
        if (reserved == null) {
            return Outcome.DEFINITIVE_CONFLICT;
        }
        if (head.managedLedgerIncarnation() != reserved.managedLedgerIncarnation()
                || !allocationState().equals(reserved.expectedState())) {
            return Outcome.DEFINITIVE_CONFLICT;
        }
        if (cut == Cut.NOT_APPLIED_RESPONSE_LOST) {
            return Outcome.INDETERMINATE;
        }
        head = new Head(
                head.managedLedgerIncarnation(),
                head.ownerEpoch(),
                head.visibleHead(),
                reserved.grantId(),
                reserved.rangeStart(),
                reserved.rangeEndExclusive(),
                reserved.rangeStart());
        return cut == Cut.APPLIED_RESPONSE_LOST ? Outcome.INDETERMINATE : Outcome.APPLIED_EXACT;
    }

    Outcome reconcileInstall() {
        if (reserved == null) {
            return Outcome.DEFINITIVE_CONFLICT;
        }
        if (head.grantId() == reserved.grantId()
                && head.rangeStart() == reserved.rangeStart()
                && head.rangeEndExclusive() == reserved.rangeEndExclusive()) {
            return Outcome.EXISTING_EXACT;
        }
        return installReserved(Cut.NONE);
    }

    Outcome clearReserved(Cut cut) {
        if (reserved == null) {
            return Outcome.EXISTING_EXACT;
        }
        if (head.grantId() != reserved.grantId()) {
            return Outcome.DEFINITIVE_CONFLICT;
        }
        if (cut == Cut.NOT_APPLIED_RESPONSE_LOST) {
            return Outcome.INDETERMINATE;
        }
        reserved = null;
        return cut == Cut.APPLIED_RESPONSE_LOST ? Outcome.INDETERMINATE : Outcome.APPLIED_EXACT;
    }

    Outcome takeover(long newOwnerEpoch) {
        if (newOwnerEpoch <= head.ownerEpoch()) {
            return Outcome.DEFINITIVE_CONFLICT;
        }
        head = new Head(
                head.managedLedgerIncarnation(),
                newOwnerEpoch,
                head.visibleHead(),
                head.grantId(),
                head.rangeStart(),
                head.rangeEndExclusive(),
                head.nextLedgerId());
        return Outcome.APPLIED_EXACT;
    }

    Candidate createCandidate(String digest) {
        if (head.grantId() == 0 || head.nextLedgerId() >= head.rangeEndExclusive()) {
            throw new IllegalStateException("no installed range capacity");
        }
        long ledgerId = head.nextLedgerId();
        Candidate candidate = new Candidate(ledgerId, head.grantId(), head.ownerEpoch(), head.visibleHead(), digest);
        Candidate existing = candidates.putIfAbsent(ledgerId, candidate);
        if (existing != null && !existing.equals(candidate)) {
            throw new IllegalStateException("candidate conflict");
        }
        return existing == null ? candidate : existing;
    }

    Outcome publishCandidate(Candidate candidate, Cut cut) {
        Candidate exact = candidates.get(candidate.ledgerId());
        if (!candidate.equals(exact)
                || candidate.grantId() != head.grantId()
                || candidate.creatorOwnerEpoch() != head.ownerEpoch()
                || candidate.expectedPredecessor() != head.visibleHead()
                || candidate.ledgerId() != head.nextLedgerId()) {
            return Outcome.DEFINITIVE_CONFLICT;
        }
        if (cut == Cut.NOT_APPLIED_RESPONSE_LOST) {
            return Outcome.INDETERMINATE;
        }
        head = new Head(
                head.managedLedgerIncarnation(),
                head.ownerEpoch(),
                candidate.ledgerId(),
                head.grantId(),
                head.rangeStart(),
                head.rangeEndExclusive(),
                Math.addExact(candidate.ledgerId(), 1));
        return cut == Cut.APPLIED_RESPONSE_LOST ? Outcome.INDETERMINATE : Outcome.APPLIED_EXACT;
    }

    Outcome reconcilePublished(Candidate candidate) {
        if (head.visibleHead() == candidate.ledgerId() && head.nextLedgerId() == candidate.ledgerId() + 1) {
            return Outcome.EXISTING_EXACT;
        }
        return publishCandidate(candidate, Cut.NONE);
    }

    Outcome burnStaleCandidate(long ledgerId) {
        Candidate candidate = candidates.get(ledgerId);
        if (candidate == null
                || ledgerId != head.nextLedgerId()
                || candidate.creatorOwnerEpoch() >= head.ownerEpoch()) {
            return Outcome.DEFINITIVE_CONFLICT;
        }
        head = new Head(
                head.managedLedgerIncarnation(),
                head.ownerEpoch(),
                head.visibleHead(),
                head.grantId(),
                head.rangeStart(),
                head.rangeEndExclusive(),
                Math.addExact(ledgerId, 1));
        return Outcome.APPLIED_EXACT;
    }

    private AllocationState allocationState() {
        return new AllocationState(head.visibleHead(), head.grantId(), head.nextLedgerId());
    }
}
