/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.Objects;

/** Result at the stable head-commit boundary. */
public record StableAppendResult(ReachableCommittedAppend reachableAppend, boolean headAdvancedByThisCall) {
    public StableAppendResult { Objects.requireNonNull(reachableAppend, "reachableAppend"); }
}
