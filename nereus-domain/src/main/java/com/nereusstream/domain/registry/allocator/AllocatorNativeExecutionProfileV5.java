/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.charset.StandardCharsets;

/** Source-bound ADR-0137 profile; only bounded admission differs from the V4 execution profile. */
public final class AllocatorNativeExecutionProfileV5 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_NATIVE_EXECUTION_PROFILE_V5";
    public static final String MODEL = AllocatorNativeExecutionProfileV4.MODEL;
    public static final int NATIVE_BRIDGE_WORKERS = AllocatorNativeExecutionProfileV4.NATIVE_BRIDGE_WORKERS;
    public static final int NATIVE_BRIDGE_QUEUE_CAPACITY =
            AllocatorNativeExecutionProfileV4.NATIVE_BRIDGE_QUEUE_CAPACITY;
    public static final int HIDDEN_DISPATCH_QUEUE = AllocatorNativeExecutionProfileV4.HIDDEN_DISPATCH_QUEUE;
    public static final String SCHEDULE_SCHEMA = AllocatorNativeExecutionProfileV4.SCHEDULE_SCHEMA;

    private AllocatorNativeExecutionProfileV5() {}

    public static Sha256Digest scheduleDigest() {
        return AllocatorNativeExecutionProfileV4.scheduleDigest();
    }

    public static Sha256Digest executionProfileDigest() {
        return Sha256Digest.hash(
                CanonicalBytes.copyOf(executionProfileCanonicalText().getBytes(StandardCharsets.UTF_8)));
    }

    public static String executionProfileCanonicalText() {
        return SCHEMA + '\n'
                + "nativeExecutionModel=" + MODEL + '\n'
                + "nativeBridgeWorkers=" + NATIVE_BRIDGE_WORKERS + '\n'
                + "nativeBridgeQueueCapacity=" + NATIVE_BRIDGE_QUEUE_CAPACITY + '\n'
                + "hiddenDispatchQueue=" + HIDDEN_DISPATCH_QUEUE + '\n'
                + "actorCount=" + AllocatorEvidenceAdmissionPolicyV5.ACTOR_COUNT + '\n'
                + "maxOutstandingPerActor="
                + AllocatorEvidenceAdmissionPolicyV5.MAX_ASYNC_OUTSTANDING_PER_ACTOR + '\n'
                + "maxGlobalOutstanding=" + AllocatorEvidenceAdmissionPolicyV5.MAX_GLOBAL_OUTSTANDING + '\n'
                + "maxOutstandingPerBinding="
                + AllocatorEvidenceAdmissionPolicyV5.MAX_ROLLOVER_OUTSTANDING_PER_BINDING + '\n'
                + "offerHorizonSeconds=" + AllocatorEvidenceAdmissionPolicyV5.OFFER_HORIZON_SECONDS + '\n'
                + "terminalAdmissionDrainSeconds="
                + AllocatorEvidenceAdmissionPolicyV5.TERMINAL_ADMISSION_DRAIN_SECONDS + '\n'
                + "cleanupGraceSeconds=" + AllocatorEvidenceAdmissionPolicyV5.CLEANUP_GRACE_SECONDS + '\n'
                + "scheduleProfileSha256=" + scheduleDigest().toHex() + '\n';
    }
}
