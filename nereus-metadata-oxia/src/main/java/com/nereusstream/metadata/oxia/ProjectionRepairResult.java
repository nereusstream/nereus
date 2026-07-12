/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.Objects;

public record ProjectionRepairResult(
        ProjectionRepairStatus virtualLedger,
        ProjectionRepairStatus positionIndex) {
    public ProjectionRepairResult {
        Objects.requireNonNull(virtualLedger, "virtualLedger");
        Objects.requireNonNull(positionIndex, "positionIndex");
    }
}
