/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.core.append.GenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.append.ProtectedGenerationZero;
import com.nereusstream.core.append.ProtectedStableAppend;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** BK_ONLY physical-reference publisher; Object upload authorization is deliberately unavailable. */
public final class BookKeeperGenerationZeroPhysicalReferencePublisher
        implements GenerationZeroPhysicalReferencePublisher {
    private final BookKeeperPrimaryPhysicalReferenceAdapter adapter;

    public BookKeeperGenerationZeroPhysicalReferencePublisher(
            BookKeeperPrimaryPhysicalReferenceAdapter adapter) {
        this.adapter = Objects.requireNonNull(adapter, "adapter");
    }

    @Override
    public CompletableFuture<Void> authorizeUpload(
            AppendSession session,
            PhysicalObjectIdentity object,
            Duration timeout) {
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                false,
                "BOOKKEEPER_WAL_ONLY has no Object WAL upload path",
                AppendOutcome.KNOWN_NOT_COMMITTED));
    }

    @Override
    public CompletableFuture<ProtectedStableAppend> protectBeforeHead(
            PreparedStableAppend append,
            Duration timeout) {
        PreparedStableAppend exact = Objects.requireNonNull(append, "append");
        if (!(exact.request().readTarget() instanceof BookKeeperEntryRangeReadTarget target)) {
            return CompletableFuture.failedFuture(invariant(
                    "BK_ONLY stable append carries a non-BookKeeper target",
                    AppendOutcome.KNOWN_NOT_COMMITTED));
        }
        return adapter.protectBeforeHead(exact, target, timeout);
    }

    @Override
    public CompletableFuture<ProtectedGenerationZero> protectVisibleIndex(
            MaterializedGenerationZero append,
            Duration timeout) {
        MaterializedGenerationZero exact = Objects.requireNonNull(append, "append");
        if (!(exact.committedAppend().readTarget() instanceof BookKeeperEntryRangeReadTarget target)) {
            return CompletableFuture.failedFuture(invariant(
                    "BK_ONLY generation zero carries a non-BookKeeper target",
                    AppendOutcome.KNOWN_COMMITTED));
        }
        return adapter.protectVisibleIndex(exact, target, timeout);
    }

    private static NereusException invariant(String message, AppendOutcome outcome) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, outcome);
    }
}
