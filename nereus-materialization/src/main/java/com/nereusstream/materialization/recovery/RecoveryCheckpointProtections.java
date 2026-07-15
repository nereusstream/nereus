/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import com.nereusstream.core.physical.ObjectProtection;
import java.util.List;
import java.util.Objects;

/** Permanent root-owned protections established after one recovery-root CAS. */
public record RecoveryCheckpointProtections(
        ObjectProtection checkpointObject,
        List<ObjectProtection> checkpointTargets) {
    public RecoveryCheckpointProtections {
        Objects.requireNonNull(checkpointObject, "checkpointObject");
        checkpointTargets = List.copyOf(Objects.requireNonNull(
                checkpointTargets, "checkpointTargets"));
    }
}
