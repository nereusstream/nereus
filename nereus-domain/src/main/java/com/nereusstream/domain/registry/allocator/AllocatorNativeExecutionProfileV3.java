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

/** Source-bound ADR-0109 Native executor and frozen workload-schedule identity. */
public final class AllocatorNativeExecutionProfileV3 {
    public static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_NATIVE_EXECUTION_PROFILE_V3";
    public static final String MODEL = "PINNED_MANAGED_LEDGER_ASYNC_CHAIN_V1";
    public static final int NATIVE_BRIDGE_WORKERS = 0;
    public static final int NATIVE_BRIDGE_QUEUE_CAPACITY = 0;
    public static final int HIDDEN_DISPATCH_QUEUE = 0;
    public static final String SCHEDULE_SCHEMA = "NEREUS_V2_M3_ALLOCATOR_WORKLOAD_SCHEDULE_V3";

    private AllocatorNativeExecutionProfileV3() {}

    public static Sha256Digest scheduleDigest() {
        return digest(scheduleCanonicalText());
    }

    public static Sha256Digest executionProfileDigest() {
        return digest(executionProfileCanonicalText());
    }

    public static String scheduleCanonicalText() {
        return SCHEDULE_SCHEMA + '\n'
                + "actorCount=4\n"
                + "warmupSeconds=10\n"
                + "steadyMeasuredSeconds=20\n"
                + "stormMeasuredSeconds=10\n"
                + "steadyRateNumerator=1\n"
                + "steadyRateDenominator=2\n"
                + "stormRateNumerator=2\n"
                + "entryTriggerPerTen=5\n"
                + "byteTriggerPerTen=3\n"
                + "ageTriggerPerTen=2\n"
                + "arrivalJitterMicros=0,125,-125,250,-250,500,-500,0\n"
                + "populations=10000,100000\n"
                + "latenciesMillis=1,5,10,25\n"
                + "fixedRatesDescending=1000,750,500,333,250,200\n";
    }

    public static String executionProfileCanonicalText() {
        return SCHEMA + '\n'
                + "nativeExecutionModel=" + MODEL + '\n'
                + "nativeBridgeWorkers=" + NATIVE_BRIDGE_WORKERS + '\n'
                + "nativeBridgeQueueCapacity=" + NATIVE_BRIDGE_QUEUE_CAPACITY + '\n'
                + "hiddenDispatchQueue=" + HIDDEN_DISPATCH_QUEUE + '\n'
                + "actorCount=" + AllocatorEvidenceAdmissionPolicyV3.ACTOR_COUNT + '\n'
                + "maxOutstandingPerActor="
                + AllocatorEvidenceAdmissionPolicyV3.MAX_ASYNC_OUTSTANDING_PER_ACTOR + '\n'
                + "maxGlobalOutstanding=" + AllocatorEvidenceAdmissionPolicyV3.MAX_GLOBAL_OUTSTANDING + '\n'
                + "maxOutstandingPerBinding="
                + AllocatorEvidenceAdmissionPolicyV3.MAX_ROLLOVER_OUTSTANDING_PER_BINDING + '\n'
                + "scheduleProfileSha256=" + scheduleDigest().toHex() + '\n';
    }

    private static Sha256Digest digest(String canonical) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(canonical.getBytes(StandardCharsets.UTF_8)));
    }
}
