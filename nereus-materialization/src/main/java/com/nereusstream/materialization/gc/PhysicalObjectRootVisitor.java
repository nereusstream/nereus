/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface PhysicalObjectRootVisitor {
    CompletableFuture<Void> visit(VersionedPhysicalObjectRoot root);
}
