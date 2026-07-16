/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.metadata.oxia.VersionedCursorState;
import java.util.concurrent.CompletableFuture;

/** F4-protected immutable object-store persistence for full F3 cursor acknowledgement snapshots. */
public interface CursorSnapshotStore extends AutoCloseable {
    CompletableFuture<CursorSnapshotPublication> prepareWrite(
            CursorSnapshotWriteRequest request,
            CursorSnapshotWriteAuthority authority);

    CompletableFuture<Void> completeWrite(
            CursorSnapshotPublication publication,
            VersionedCursorState publishedRoot);

    CompletableFuture<CursorAckState> read(
            CursorSnapshotReference reference, CursorIdentity expectedIdentity);

    @Override
    void close();
}
