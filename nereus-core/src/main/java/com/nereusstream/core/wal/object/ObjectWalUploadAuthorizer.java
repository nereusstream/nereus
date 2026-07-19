/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal.object;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Provider retry guard installed at Object-WAL composition time. */
@FunctionalInterface
public interface ObjectWalUploadAuthorizer {
    CompletableFuture<Void> authorize(
            ObjectPreparedPrimaryAppend prepared,
            PhysicalObjectIdentity object,
            ObjectKey key,
            int providerAttempt,
            Duration timeout);

    static ObjectWalUploadAuthorizer noOp() {
        return (prepared, object, key, attempt, timeout) -> CompletableFuture.completedFuture(null);
    }
}
