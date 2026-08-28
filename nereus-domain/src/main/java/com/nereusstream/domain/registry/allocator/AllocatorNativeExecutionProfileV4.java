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

/** Source-bound ADR-0125 execution profile; its workload schedule is byte-identical to the V3 schedule. */
public final class AllocatorNativeExecutionProfileV4 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_NATIVE_EXECUTION_PROFILE_V4";
    public static final String MODEL = AllocatorNativeExecutionProfileV3.MODEL;
    public static final int NATIVE_BRIDGE_WORKERS = AllocatorNativeExecutionProfileV3.NATIVE_BRIDGE_WORKERS;
    public static final int NATIVE_BRIDGE_QUEUE_CAPACITY =
            AllocatorNativeExecutionProfileV3.NATIVE_BRIDGE_QUEUE_CAPACITY;
    public static final int HIDDEN_DISPATCH_QUEUE = AllocatorNativeExecutionProfileV3.HIDDEN_DISPATCH_QUEUE;
    public static final String SCHEDULE_SCHEMA = AllocatorNativeExecutionProfileV3.SCHEDULE_SCHEMA;

    private AllocatorNativeExecutionProfileV4() {}

    public static Sha256Digest scheduleDigest() {
        return AllocatorNativeExecutionProfileV3.scheduleDigest();
    }

    public static Sha256Digest executionProfileDigest() {
        return digest(executionProfileCanonicalText());
    }

    public static String executionProfileCanonicalText() {
        return SCHEMA + '\n'
                + "nativeExecutionModel=" + MODEL + '\n'
                + "nativeBridgeWorkers=" + NATIVE_BRIDGE_WORKERS + '\n'
                + "nativeBridgeQueueCapacity=" + NATIVE_BRIDGE_QUEUE_CAPACITY + '\n'
                + "hiddenDispatchQueue=" + HIDDEN_DISPATCH_QUEUE + '\n'
                + "actorCount=" + AllocatorEvidenceAdmissionPolicyV4.ACTOR_COUNT + '\n'
                + "maxOutstandingPerActor="
                + AllocatorEvidenceAdmissionPolicyV4.MAX_ASYNC_OUTSTANDING_PER_ACTOR + '\n'
                + "maxGlobalOutstanding=" + AllocatorEvidenceAdmissionPolicyV4.MAX_GLOBAL_OUTSTANDING + '\n'
                + "maxOutstandingPerBinding="
                + AllocatorEvidenceAdmissionPolicyV4.MAX_ROLLOVER_OUTSTANDING_PER_BINDING + '\n'
                + "offerHorizonSeconds=" + AllocatorEvidenceAdmissionPolicyV4.OFFER_HORIZON_SECONDS + '\n'
                + "terminalAdmissionDrainSeconds="
                + AllocatorEvidenceAdmissionPolicyV4.TERMINAL_ADMISSION_DRAIN_SECONDS + '\n'
                + "cleanupGraceSeconds=" + AllocatorEvidenceAdmissionPolicyV4.CLEANUP_GRACE_SECONDS + '\n'
                + "scheduleProfileSha256=" + scheduleDigest().toHex() + '\n';
    }

    private static Sha256Digest digest(String canonical) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(canonical.getBytes(StandardCharsets.UTF_8)));
    }
}
