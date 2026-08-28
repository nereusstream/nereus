/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

/** Versioned ADR-0137 evidence admission boundary; production activation remains owned by M6. */
public final class AllocatorEvidenceAdmissionPolicyV5 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_EVIDENCE_ADMISSION_V5";
    public static final int ACTOR_COUNT = AllocatorEvidenceAdmissionPolicyV4.ACTOR_COUNT;
    public static final int MAX_ASYNC_OUTSTANDING_PER_ACTOR = 128;
    public static final int MAX_GLOBAL_OUTSTANDING = 512;
    public static final int MAX_ROLLOVER_OUTSTANDING_PER_BINDING =
            AllocatorEvidenceAdmissionPolicyV4.MAX_ROLLOVER_OUTSTANDING_PER_BINDING;
    public static final int PRE_ADMISSION_QUEUE_RATE_MULTIPLIER =
            AllocatorEvidenceAdmissionPolicyV4.PRE_ADMISSION_QUEUE_RATE_MULTIPLIER;
    public static final int OFFER_HORIZON_SECONDS = AllocatorEvidenceAdmissionPolicyV4.OFFER_HORIZON_SECONDS;
    public static final int TERMINAL_ADMISSION_DRAIN_SECONDS =
            AllocatorEvidenceAdmissionPolicyV4.TERMINAL_ADMISSION_DRAIN_SECONDS;
    public static final int CLEANUP_GRACE_SECONDS = AllocatorEvidenceAdmissionPolicyV4.CLEANUP_GRACE_SECONDS;
    public static final int MAX_STORM_RATE_MULTIPLIER = 2;
    public static final int MAX_STORM_RATE =
            Math.multiplyExact(AllocatorEvidenceAdmissionPolicyV3.MAX_FROZEN_RATE, MAX_STORM_RATE_MULTIPLIER);
    public static final int EXACT_STORM_OUTSTANDING_PER_ACTOR = 125;

    static {
        int exact = AllocatorEvidenceAdmissionPolicyV3.exactOutstandingPerActor(
                MAX_STORM_RATE, AllocatorEvidenceAdmissionPolicyV3.ROLLOVER_P99_MILLIS, ACTOR_COUNT);
        if (exact != EXACT_STORM_OUTSTANDING_PER_ACTOR
                || MAX_ASYNC_OUTSTANDING_PER_ACTOR < exact
                || Math.multiplyExact(ACTOR_COUNT, MAX_ASYNC_OUTSTANDING_PER_ACTOR) != MAX_GLOBAL_OUTSTANDING) {
            throw new IllegalStateException("allocator V5 storm admission derivation differs from ADR 0137");
        }
    }

    private AllocatorEvidenceAdmissionPolicyV5() {}

    public static int preAdmissionQueueCapacity(int offeredRate) {
        return AllocatorEvidenceAdmissionPolicyV4.preAdmissionQueueCapacity(offeredRate);
    }
}
