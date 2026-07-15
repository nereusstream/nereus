/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Full format/content validation supplied by the exact compacted-object reader. */
@FunctionalInterface
public interface MaterializationFormatVerifier {
    CompletableFuture<Void> verify(
            MaterializationTask task,
            MaterializationOutput output,
            Duration timeout);
}
