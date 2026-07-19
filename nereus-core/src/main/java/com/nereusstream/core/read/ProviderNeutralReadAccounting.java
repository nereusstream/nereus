/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.PhysicalReadStats;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ResolvedRange;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Exact one-stat-per-resolved-target accounting validation shared by every physical reader. */
public final class ProviderNeutralReadAccounting {
    private ProviderNeutralReadAccounting() { }

    public static void validate(List<ResolvedRange> ranges, PhysicalReadResult result) {
        Map<Checksum, Integer> expectedRanges = new HashMap<>();
        ranges.forEach(range -> expectedRanges.merge(
                ReadTargetIdentities.sha256(range.readTarget()), 1, Math::addExact));
        Map<Checksum, Integer> observedRanges = new HashMap<>();
        long statsReturned = 0;
        long batchBytes = 0;
        try {
            for (PhysicalReadStats stats : result.rangeStats()) {
                Checksum identity = stats.targetIdentity();
                int expectedCount = expectedRanges.getOrDefault(identity, 0);
                if (expectedCount == 0) {
                    throw invariant("WAL reader reported accounting for an unknown resolved range");
                }
                int observedCount = observedRanges.merge(identity, 1, Math::addExact);
                if (observedCount > expectedCount) {
                    throw invariant("WAL reader reported duplicate accounting for one resolved range");
                }
                stats.amplificationBytes();
                statsReturned = Math.addExact(statsReturned, stats.returnedPayloadBytes());
            }
            for (ReadBatch batch : result.batches()) {
                Checksum sourceIdentity = batch.source().targetIdentity();
                if (!sourceIdentity.equals(ReadTargetIdentities.sha256(batch.source().target()))) {
                    throw invariant("WAL reader returned a batch with a non-canonical source identity");
                }
                if (!observedRanges.containsKey(sourceIdentity)) {
                    throw invariant("WAL reader omitted read accounting for a returned batch");
                }
                batchBytes = Math.addExact(batchBytes, batch.payload().length);
            }
        } catch (ArithmeticException e) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "WAL reader accounting overflows",
                    e);
        }
        if (statsReturned != batchBytes) {
            throw invariant("WAL reader returned-byte accounting does not match batches");
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
