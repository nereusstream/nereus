/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import java.util.concurrent.CompletableFuture;

/** Internal semantic-view read surface; compatibility APIs remain fixed to COMMITTED. */
@Deprecated(forRemoval = true)
public interface StreamViewReader extends AutoCloseable {
    CompletableFuture<ViewReadResult> read(
            StreamId streamId, long startOffset, ReadView view, ReadOptions options);

    @Override
    void close();
}
