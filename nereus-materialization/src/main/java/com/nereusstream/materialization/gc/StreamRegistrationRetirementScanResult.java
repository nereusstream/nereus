/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/** Exact status counts from one complete 64-shard registration-retirement pass. */
public record StreamRegistrationRetirementScanResult(
        int shardsScanned,
        long registrationsScanned,
        Map<StreamRegistrationRetirementStatus, Long> statusCounts) {
    public StreamRegistrationRetirementScanResult {
        if (shardsScanned != 64) {
            throw new IllegalArgumentException("a complete registration-retirement pass must scan 64 shards");
        }
        if (registrationsScanned < 0) {
            throw new IllegalArgumentException("registrationsScanned must be non-negative");
        }
        Objects.requireNonNull(statusCounts, "statusCounts");
        EnumMap<StreamRegistrationRetirementStatus, Long> exact =
                new EnumMap<>(StreamRegistrationRetirementStatus.class);
        long total = 0;
        for (StreamRegistrationRetirementStatus status : StreamRegistrationRetirementStatus.values()) {
            Long count = statusCounts.get(status);
            if (count == null || count < 0) {
                throw new IllegalArgumentException(
                        "statusCounts must contain every status with a non-negative count");
            }
            exact.put(status, count);
            total = Math.addExact(total, count);
        }
        if (statusCounts.size() != exact.size() || total != registrationsScanned) {
            throw new IllegalArgumentException(
                    "registration-retirement status counts do not match the completed pass");
        }
        statusCounts = Collections.unmodifiableMap(exact);
    }

    public long count(StreamRegistrationRetirementStatus status) {
        return statusCounts.get(Objects.requireNonNull(status, "status"));
    }
}
