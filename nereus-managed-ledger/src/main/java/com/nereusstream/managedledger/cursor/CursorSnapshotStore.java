/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.concurrent.CompletableFuture;

/** Immutable object-store persistence for full F3 cursor acknowledgement snapshots. */
public interface CursorSnapshotStore extends AutoCloseable {
    CompletableFuture<CursorSnapshotReference> write(CursorSnapshotWriteRequest request);

    CompletableFuture<CursorAckState> read(
            CursorSnapshotReference reference, CursorIdentity expectedIdentity);

    @Override
    void close();
}
