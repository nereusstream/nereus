/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadView;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Explicit NTC1-only facade preventing sparse rows from crossing into the ordinary committed reader. */
public final class TopicCompactedObjectReader implements CompactedObjectReader {
    private final CompactedObjectReader delegate;

    public TopicCompactedObjectReader(CompactedObjectReader delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public CompletableFuture<CompactedObjectReadResult> read(CompactedObjectReadRequest request) {
        if (request == null || request.view() != ReadView.TOPIC_COMPACTED) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.UNSUPPORTED_READ_TARGET,
                    false,
                    "topic-compacted reader accepts only the NTC1 semantic view"));
        }
        return delegate.read(request);
    }
}
