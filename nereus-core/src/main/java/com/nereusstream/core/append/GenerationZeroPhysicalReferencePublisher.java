/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.AppendSession;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Establishes exact permanent physical-reference vetoes around generation-zero visibility cuts. */
public interface GenerationZeroPhysicalReferencePublisher {
    /** Revalidates the durable append owner and absent-or-exact-ACTIVE root before one provider PUT. */
    CompletableFuture<Void> authorizeUpload(
            AppendSession session,
            PhysicalObjectIdentity object,
            Duration timeout);

    CompletableFuture<ProtectedStableAppend> protectBeforeHead(
            PreparedStableAppend append,
            Duration timeout);

    CompletableFuture<ProtectedGenerationZero> protectVisibleIndex(
            MaterializedGenerationZero append,
            Duration timeout);
}
