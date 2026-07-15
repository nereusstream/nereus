/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.StreamId;

/** Creates the lightweight stream-scoped exact-reader facade used by a shared worker. */
@FunctionalInterface
public interface ExactSourceRangeReaderFactory {
    ExactSourceRangeReader forStream(StreamId streamId);
}
