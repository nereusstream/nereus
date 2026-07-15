/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ReadView;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import java.util.concurrent.CompletableFuture;

/** Resolves the exact whole-object identity needed by the durable reader-pin handshake. */
@FunctionalInterface
public interface PhysicalObjectIdentityResolver {
    CompletableFuture<PhysicalObjectIdentity> resolve(
            ObjectSliceReadTarget target, ReadView view);
}
