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

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import com.nereusstream.domain.registry.allocator.AllocatorEvidenceCandidateV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceScheduleV1;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Exact pre-run workload dimensions and deterministic request stream accepted by ADR 0094. */
final class M3AllocatorWorkloadPlan {
    static final int BROKER_ACTORS = 4;
    static final int WORKER_THREADS = 96;
    static final int WARM_UP_SECONDS = 10;
    static final int MEASURED_SECONDS = 30;
    static final int STEADY_MEASURED_SECONDS = 20;
    static final int STORM_MEASURED_SECONDS = 10;
    static final long XORSHIFT64_STAR_SEED = AllocatorEvidenceScheduleV1.SEED;
    static final int BYTE_PAYLOAD_BYTES = 64 * 1024;
    static final long AGE_ADVANCE_NANOS = 1_000_000_000L;
    static final List<Integer> ACTIVE_POPULATIONS = List.of(10_000, 100_000);
    static final List<Integer> METADATA_LATENCY_P99_MILLIS = List.of(1, 5, 10, 25);
    static final List<Integer> OFFERED_RATES = List.of(200, 250, 333, 500, 750, 1000);
    static final List<Long> RANGE_SIZES = List.copyOf(AllocatorEvidenceCandidateV1.RANGE_SIZES);
    static final List<Integer> ARRIVAL_JITTER_MICROS = List.of(0, 125, -125, 250, -250, 500, -500, 0);

    private M3AllocatorWorkloadPlan() {}

    static Iterable<PlannedRequest> requests(int activePopulation, int offeredRate) {
        requireDimension(ACTIVE_POPULATIONS, activePopulation, "active population");
        requireDimension(OFFERED_RATES, offeredRate, "offered rate");
        return () -> new RequestIterator(activePopulation, offeredRate);
    }

    static int requestCount(int offeredRate) {
        requireDimension(OFFERED_RATES, offeredRate, "offered rate");
        return Math.multiplyExact(WARM_UP_SECONDS + MEASURED_SECONDS, offeredRate);
    }

    static int measuredRequestCount(int offeredRate) {
        requireDimension(OFFERED_RATES, offeredRate, "offered rate");
        return Math.multiplyExact(MEASURED_SECONDS, offeredRate);
    }

    static List<Candidate> candidates() {
        List<Candidate> candidates = new ArrayList<>(1 + RANGE_SIZES.size());
        candidates.add(new Candidate("STRICT_SERIALIZED", 1));
        RANGE_SIZES.forEach(range -> candidates.add(new Candidate("RANGE_LEASED", range)));
        return List.copyOf(candidates);
    }

    private static Phase phase(long ordinal, int offeredRate) {
        long warmUpRequests = (long) WARM_UP_SECONDS * offeredRate;
        // The first twenty measured seconds run at 0.5R (10R requests), then the
        // final ten-second storm runs at 2R (20R requests). The closed interval
        // therefore retains exactly 30R requests while exposing the 2x storm.
        long steadyRequests = (long) offeredRate * STEADY_MEASURED_SECONDS / 2;
        if (ordinal < warmUpRequests) {
            return Phase.WARM_UP;
        }
        return ordinal - warmUpRequests < steadyRequests ? Phase.MEASURED_STEADY : Phase.MEASURED_STORM;
    }

    private static Trigger trigger(long ordinal) {
        int scheduleIndex = Math.toIntExact(ordinal % 10);
        if (scheduleIndex < 5) {
            return Trigger.ENTRY;
        }
        return scheduleIndex < 8 ? Trigger.BYTE : Trigger.AGE;
    }

    private static <T> void requireDimension(List<T> allowed, T value, String name) {
        if (!allowed.contains(value)) {
            throw new IllegalArgumentException(name + " differs from ADR 0094");
        }
    }

    enum Phase {
        WARM_UP,
        MEASURED_STEADY,
        MEASURED_STORM
    }

    enum Trigger {
        ENTRY,
        BYTE,
        AGE
    }

    record Candidate(String mode, long rangeSize) {}

    record PlannedRequest(
            long requestOrdinal,
            int actorId,
            int ledgerIndex,
            Trigger trigger,
            Phase phase,
            long arrivalOffsetMicros) {}

    private static final class RequestIterator implements Iterator<PlannedRequest> {
        private final int activePopulation;
        private final int offeredRate;
        private final long requestCount;
        private final AllocatorEvidenceScheduleV1.Cursor ledgers;
        private final AllocatorEvidenceScheduleV1.ArrivalCursor arrivals;
        private long ordinal;

        RequestIterator(int activePopulation, int offeredRate) {
            this.activePopulation = activePopulation;
            this.offeredRate = offeredRate;
            this.requestCount = requestCount(offeredRate);
            ledgers = AllocatorEvidenceScheduleV1.ledgerCursor(activePopulation);
            arrivals = AllocatorEvidenceScheduleV1.arrivalCursor(offeredRate);
        }

        @Override
        public boolean hasNext() {
            return ordinal < requestCount;
        }

        @Override
        public PlannedRequest next() {
            if (!hasNext()) {
                throw new NoSuchElementException("allocator workload request stream exhausted");
            }
            long exactOrdinal = ordinal++;
            Phase exactPhase = phase(exactOrdinal, offeredRate);
            return new PlannedRequest(
                    exactOrdinal,
                    AllocatorEvidenceScheduleV1.actorId(exactOrdinal),
                    ledgers.nextLedgerIndex(),
                    trigger(exactOrdinal),
                    exactPhase,
                    arrivals.nextOfferedTimestampMicros());
        }
    }
}
