/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

/** Versioned ADR-0125 evidence admission boundary; production activation remains owned by M6. */
public final class AllocatorEvidenceAdmissionPolicyV4 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_EVIDENCE_ADMISSION_V4";
    public static final int ACTOR_COUNT = AllocatorEvidenceAdmissionPolicyV3.ACTOR_COUNT;
    public static final int MAX_ASYNC_OUTSTANDING_PER_ACTOR =
            AllocatorEvidenceAdmissionPolicyV3.MAX_ASYNC_OUTSTANDING_PER_ACTOR;
    public static final int MAX_GLOBAL_OUTSTANDING = AllocatorEvidenceAdmissionPolicyV3.MAX_GLOBAL_OUTSTANDING;
    public static final int MAX_ROLLOVER_OUTSTANDING_PER_BINDING =
            AllocatorEvidenceAdmissionPolicyV3.MAX_ROLLOVER_OUTSTANDING_PER_BINDING;
    public static final int PRE_ADMISSION_QUEUE_RATE_MULTIPLIER =
            AllocatorEvidenceAdmissionPolicyV3.PRE_ADMISSION_QUEUE_RATE_MULTIPLIER;
    public static final int OFFER_HORIZON_SECONDS = 40;
    public static final int TERMINAL_ADMISSION_DRAIN_SECONDS = 2;
    public static final int CLEANUP_GRACE_SECONDS = 5;

    private AllocatorEvidenceAdmissionPolicyV4() {}

    public static int preAdmissionQueueCapacity(int offeredRate) {
        return AllocatorEvidenceAdmissionPolicyV3.preAdmissionQueueCapacity(offeredRate);
    }
}
