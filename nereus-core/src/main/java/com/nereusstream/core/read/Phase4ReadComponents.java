/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import java.util.Objects;

/** Exact generation-aware read pipeline installed atomically with Phase 4 append enablement. */
public record Phase4ReadComponents(
        GenerationReadResolver resolver,
        ReadTargetReaderRegistry readers,
        GenerationReadFailureHandler failureHandler,
        GenerationReadRetryPolicy retryPolicy) {
    public Phase4ReadComponents(
            GenerationReadResolver resolver,
            ReadTargetReaderRegistry readers,
            GenerationReadFailureHandler failureHandler) {
        this(
                resolver,
                readers,
                failureHandler,
                GenerationReadRetryPolicy.defaults());
    }

    public Phase4ReadComponents {
        Objects.requireNonNull(resolver, "resolver");
        Objects.requireNonNull(readers, "readers");
        Objects.requireNonNull(failureHandler, "failureHandler");
        Objects.requireNonNull(retryPolicy, "retryPolicy");
    }
}
