/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ReadOptions;
import java.util.concurrent.CompletableFuture;

/** Reads one task-frozen generation without resolving or substituting another generation. */
public interface ExactSourceRangeReader {
    CompletableFuture<ExactSourceRead> read(
            SourceGeneration expected,
            ReadOptions options);
}
