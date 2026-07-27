/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import java.util.concurrent.CompletableFuture;

/** Resolves and later revalidates one exact authoritative COMMITTED source cut. */
public interface CommittedSourceSetResolver {
    CompletableFuture<CommittedSourceSetResolution> resolve(
            StreamId streamId, OffsetRange coverage);

    CompletableFuture<Void> revalidate(CommittedSourceSetResolution expected);
}
