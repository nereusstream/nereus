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

/** Exact deterministic actor/trigger/ledger/arrival schedule shared by the formal runner and raw validator. */
public final class AllocatorEvidenceScheduleV1 {
    public static final long SEED = 0x4e45524555534d33L;
    private static final long MULTIPLIER = 0x2545F4914F6CDD1DL;
    private static final int[] JITTER_MICROS = {0, 125, -125, 250, -250, 500, -500, 0};

    private AllocatorEvidenceScheduleV1() {}

    public static int actorId(long requestOrdinal) {
        return (int) (requestOrdinal & 3);
    }

    public static AllocatorRawEvidenceEventV1.TriggerKind trigger(long requestOrdinal) {
        int slot = (int) (requestOrdinal % 10);
        return slot < 5
                ? AllocatorRawEvidenceEventV1.TriggerKind.ENTRY
                : slot < 8 ? AllocatorRawEvidenceEventV1.TriggerKind.BYTE : AllocatorRawEvidenceEventV1.TriggerKind.AGE;
    }

    /** Relative offered timestamp across 10s warm-up + 30s measured interval, including the final 10s 2x storm. */
    public static long offeredTimestampMicros(long requestOrdinal, int offeredRate) {
        if (requestOrdinal < 0 || requestOrdinal > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("request ordinal is outside the formal evidence bound");
        }
        ArrivalCursor cursor = arrivalCursor(offeredRate);
        long value = 0;
        for (long ordinal = 0; ordinal <= requestOrdinal; ordinal++) {
            value = cursor.nextOfferedTimestampMicros();
        }
        return value;
    }

    private static long offeredTimestampMicrosWithoutClamp(long requestOrdinal, int offeredRate) {
        long warmupRequests = Math.multiplyExact(offeredRate, 10L);
        long base;
        if (requestOrdinal < warmupRequests) {
            base = requestOrdinal * 1_000_000L / offeredRate;
        } else {
            long measured = requestOrdinal - warmupRequests;
            long halfRateRequests = Math.multiplyExact(offeredRate, 10L);
            base = measured < halfRateRequests
                    ? 10_000_000L + measured * 2_000_000L / offeredRate
                    : 30_000_000L + (measured - halfRateRequests) * 500_000L / offeredRate;
        }
        return Math.max(0, base + JITTER_MICROS[(int) (requestOrdinal & 7)]);
    }

    public static Cursor ledgerCursor(int activeManagedLedgers) {
        if (!AllocatorEvidenceContextV1.POPULATIONS.contains(activeManagedLedgers)) {
            throw new IllegalArgumentException("allocator evidence population is not frozen");
        }
        return new Cursor(activeManagedLedgers);
    }

    public static ArrivalCursor arrivalCursor(int offeredRate) {
        if (!AllocatorEvidenceContextV1.OFFERED_RATES.contains(offeredRate)) {
            throw new IllegalArgumentException("allocator evidence offered rate is not frozen");
        }
        return new ArrivalCursor(offeredRate);
    }

    public static final class ArrivalCursor {
        private final int offeredRate;
        private long ordinal;
        private long prior = -1;

        private ArrivalCursor(int offeredRate) {
            this.offeredRate = offeredRate;
        }

        public long nextOfferedTimestampMicros() {
            long jittered = offeredTimestampMicrosWithoutClamp(ordinal, offeredRate);
            long clamped = Math.max(prior + 1, jittered);
            ordinal++;
            prior = clamped;
            return clamped;
        }
    }

    public static final class Cursor {
        private final int bound;
        private long state = SEED;

        private Cursor(int bound) {
            this.bound = bound;
        }

        public int nextLedgerIndex() {
            long threshold = Long.remainderUnsigned(-Integer.toUnsignedLong(bound), Integer.toUnsignedLong(bound));
            while (true) {
                long value = next();
                if (Long.compareUnsigned(value, threshold) >= 0) {
                    return (int) Long.remainderUnsigned(value, Integer.toUnsignedLong(bound));
                }
            }
        }

        private long next() {
            long value = state;
            value ^= value >>> 12;
            value ^= value << 25;
            value ^= value >>> 27;
            state = value;
            return value * MULTIPLIER;
        }
    }
}
