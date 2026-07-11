/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class Phase15ApiContractTest {
    @Test
    void targetUnionDoesNotConfuseDurableRepresentationWithRuntimeSupport() {
        BookKeeperEntryRangeReadTarget target = new BookKeeperEntryRangeReadTarget(
                1, "primary", 7, 9, 3,
                BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                new Checksum(ChecksumType.SHA256, "11".repeat(32)));
        assertThat(target.lastEntryIdInclusive()).isEqualTo(11);
        assertThatThrownBy(() -> ResolvedObjectRange.from(new ResolvedRange(
                new OffsetRange(0, 3), 0, target, PayloadFormat.OPAQUE_RECORD_BATCH,
                3, 3, 10, List.of(), Optional.empty(), 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void objectCompatibilityViewIsCheckedAndLossless() {
        EntryIndexRef index = new EntryIndexRef(EntryIndexLocation.OBJECT_FOOTER,
                Optional.empty(), Optional.empty(), Optional.empty(), 20, 4,
                new Checksum(ChecksumType.CRC32C, "11111111"));
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1, new ObjectId("object"), new ObjectKey("key"), ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1", "OPAQUE_SLICE", "slice", 10, 5,
                new Checksum(ChecksumType.CRC32C, "22222222"), index);
        ResolvedRange generic = new ResolvedRange(new OffsetRange(0, 1), 0, target,
                PayloadFormat.OPAQUE_RECORD_BATCH, 1, 1, 5, List.of(), Optional.empty(), 1);
        assertThat(ResolvedObjectRange.from(generic).objectKey()).isEqualTo(target.objectKey());
    }

    @Test
    void recoveryAndLifecycleValuesEnforceBounds() {
        assertThat(new AppendAttemptId("run/1").value()).isEqualTo("run/1");
        assertThatThrownBy(() -> new AppendAttemptId("x".repeat(257)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AppendRecoveryOptions(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SealOptions(Duration.ofSeconds(1), " "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new DeleteOptions(Duration.ofSeconds(1), "cleanup").reason()).isEqualTo("cleanup");
    }

    @Test
    void appendExceptionCarriesExactAttemptIdentity() {
        AppendAttemptId id = new AppendAttemptId("run/2");
        NereusException failure = new NereusException(
                ErrorCode.TIMEOUT, true, "uncertain", null, AppendOutcome.MAY_HAVE_COMMITTED, id);
        assertThat(failure.appendAttemptId()).contains(id);
        assertThatThrownBy(() -> new NereusException(
                ErrorCode.TIMEOUT, true, "invalid", null, AppendOutcome.KNOWN_NOT_COMMITTED, id))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
