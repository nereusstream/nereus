/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.nereusstream.api.StreamMetadata;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;

/** Package-private read surface shared by read-only and ManagedCursor facades. */
interface NereusCursorLedgerView {
    NereusManagedLedgerRuntime runtime();
    String getName();
    Map<String, String> getProperties();
    StreamMetadata currentMetadata();
    CompletableFuture<StreamMetadata> refreshMetadata();
    CompletableFuture<Entry> readAt(long offset, StreamMetadata metadata);
    Position readPosition(long offset, StreamMetadata metadata);
    Position normalizeInclusiveMax(Position position, StreamMetadata metadata);
}
