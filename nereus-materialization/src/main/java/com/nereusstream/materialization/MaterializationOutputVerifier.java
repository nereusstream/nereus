/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Exact immutable-object HEAD plus complete logical-format verification before publication. */
@FunctionalInterface
public interface MaterializationOutputVerifier {
    CompletableFuture<Void> verify(
            MaterializationTask task,
            MaterializationOutput output,
            Duration timeout);
}
