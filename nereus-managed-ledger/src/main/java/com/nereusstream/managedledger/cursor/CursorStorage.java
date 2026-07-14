/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Protocol-neutral durable cursor storage over the F3 root/snapshot contract. */
public interface CursorStorage extends AutoCloseable {
    CompletableFuture<CursorHandle> open(
            CursorOwnerSession owner, String cursorName, CursorOpenRequest request);

    CompletableFuture<List<CursorHandle>> claimAndLoadActiveCursors(
            CursorOwnerSession owner);

    CompletableFuture<CursorMutationResult> cumulativeAck(
            CursorHandle handle, CursorAckRequest request);

    CompletableFuture<CursorMutationResult> individualAck(
            CursorHandle handle, List<CursorAckRequest> requests);

    CompletableFuture<CursorMutationResult> reset(
            CursorHandle handle, CursorResetRequest request);

    CompletableFuture<CursorMutationResult> clearBacklog(
            CursorHandle handle, long observedCommittedEndOffset);

    CompletableFuture<CursorMutationResult> mutateCursorProperties(
            CursorHandle handle, CursorPropertyMutation mutation);

    CompletableFuture<CursorMutationResult> flushPositionProperties(
            CursorHandle handle, Map<String, Long> stagedProperties);

    CompletableFuture<Void> delete(CursorOwnerSession owner, String cursorName);

    CompletableFuture<CursorRetentionView> retentionView(CursorOwnerSession owner);

    @Override
    void close();
}
