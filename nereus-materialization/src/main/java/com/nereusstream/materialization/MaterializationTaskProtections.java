/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Exact protection handles converged to one durable materialization-task version. */
public record MaterializationTaskProtections(
        VersionedMaterializationTask task,
        List<ObjectProtection> sources,
        Optional<ObjectProtection> output) {
    public MaterializationTaskProtections {
        Objects.requireNonNull(task, "task");
        sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
        if (sources.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("sources cannot contain null");
        }
        output = Objects.requireNonNull(output, "output");
    }
}
