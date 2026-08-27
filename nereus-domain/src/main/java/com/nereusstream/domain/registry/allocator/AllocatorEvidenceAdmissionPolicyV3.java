/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

/** Versioned ADR-0108 evidence-workload admission contract; production activation remains owned by M6. */
public final class AllocatorEvidenceAdmissionPolicyV3 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_EVIDENCE_ADMISSION_V3";
    public static final int ACTOR_COUNT = 4;
    public static final int MAX_ASYNC_OUTSTANDING_PER_ACTOR = 64;
    public static final int MAX_GLOBAL_OUTSTANDING = 256;
    public static final int MAX_ROLLOVER_OUTSTANDING_PER_BINDING = 1;
    public static final int PRE_ADMISSION_QUEUE_RATE_MULTIPLIER = 2;
    public static final int MAX_FROZEN_RATE = 1_000;
    public static final int ROLLOVER_P99_MILLIS = 250;
    public static final int EXACT_DERIVED_OUTSTANDING_PER_ACTOR = 63;

    static {
        int exact = exactOutstandingPerActor(MAX_FROZEN_RATE, ROLLOVER_P99_MILLIS, ACTOR_COUNT);
        if (exact != EXACT_DERIVED_OUTSTANDING_PER_ACTOR
                || MAX_ASYNC_OUTSTANDING_PER_ACTOR < exact
                || Math.multiplyExact(ACTOR_COUNT, MAX_ASYNC_OUTSTANDING_PER_ACTOR) != MAX_GLOBAL_OUTSTANDING) {
            throw new IllegalStateException("allocator V3 evidence admission derivation differs from ADR 0108");
        }
    }

    private AllocatorEvidenceAdmissionPolicyV3() {}

    public static int exactOutstandingPerActor(int offeredRate, int completionP99Millis, int actorCount) {
        if (offeredRate <= 0 || completionP99Millis <= 0 || actorCount <= 0) {
            throw new IllegalArgumentException("allocator V3 admission derivation inputs must be positive");
        }
        long numerator = Math.multiplyExact((long) offeredRate, completionP99Millis);
        long denominator = Math.multiplyExact(1_000L, actorCount);
        return Math.toIntExact(Math.floorDiv(Math.addExact(numerator, denominator - 1), denominator));
    }

    public static int preAdmissionQueueCapacity(int offeredRate) {
        if (offeredRate <= 0) {
            throw new IllegalArgumentException("allocator V3 offered rate must be positive");
        }
        return Math.multiplyExact(PRE_ADMISSION_QUEUE_RATE_MULTIPLIER, offeredRate);
    }
}
