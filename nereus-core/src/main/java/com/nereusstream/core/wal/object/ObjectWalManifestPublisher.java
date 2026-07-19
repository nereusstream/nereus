/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal.object;

import com.nereusstream.api.AppendSession;
import com.nereusstream.objectstore.wal.WalWriteResult;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Publishes the durable Object-WAL manifest before common head commit. */
@FunctionalInterface
public interface ObjectWalManifestPublisher {
    CompletableFuture<Void> publish(WalWriteResult result, AppendSession session, Duration timeout);

    static ObjectWalManifestPublisher noOp() {
        return (result, session, timeout) -> CompletableFuture.completedFuture(null);
    }
}
