/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Provider-owned durable protection protocol selected by exact read-target type. */
public interface PrimaryPhysicalReferenceAdapter<T extends ReadTarget> {
    ReadTargetType targetType();
    Class<T> targetClass();

    CompletableFuture<ProtectedStableAppend> protectBeforeHead(
            PreparedStableAppend append,
            T target,
            Duration timeout);

    CompletableFuture<ProtectedGenerationZero> protectVisibleIndex(
            MaterializedGenerationZero append,
            T target,
            Duration timeout);
}
