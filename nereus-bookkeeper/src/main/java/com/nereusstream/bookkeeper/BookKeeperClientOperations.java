/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;

/** Narrow public-API adapter used to fake deterministic provider outcomes without mocking BookKeeper internals. */
public interface BookKeeperClientOperations {
    CompletableFuture<WriteAdvHandle> createAdvanced(
            long ledgerId,
            BookKeeperWalConfiguration configuration,
            byte[] password,
            Map<String, byte[]> customMetadata,
            BookKeeperOperationDeadline deadline);

    CompletableFuture<ReadHandle> open(
            long ledgerId,
            BookKeeperDigestType digestType,
            byte[] password,
            boolean recovery,
            BookKeeperOperationDeadline deadline);

    CompletableFuture<Long> write(
            WriteAdvHandle handle, long entryId, ByteBuf entry, BookKeeperOperationDeadline deadline);

    CompletableFuture<LedgerEntries> readUnconfirmed(
            ReadHandle handle, long firstEntryId, long lastEntryIdInclusive, BookKeeperOperationDeadline deadline);

    CompletableFuture<LedgerMetadata> metadata(long ledgerId, BookKeeperOperationDeadline deadline);
    CompletableFuture<Void> delete(long ledgerId, BookKeeperOperationDeadline deadline);
}
