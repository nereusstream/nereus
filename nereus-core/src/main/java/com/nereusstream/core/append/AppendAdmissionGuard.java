/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import java.util.concurrent.CompletableFuture;

/** Product admission hook invoked on the serialized append lane before any primary object preparation or IO. */
@FunctionalInterface
public interface AppendAdmissionGuard {
    CompletableFuture<Void> admit(AppendAdmissionRequest request);

    static AppendAdmissionGuard noOp() {
        return ignored -> CompletableFuture.completedFuture(null);
    }
}
