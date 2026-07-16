/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import java.util.concurrent.CompletableFuture;

/**
 * Resolves one opaque live-projection subject without loading or owning the
 * protocol facade that created it.
 */
public interface GenerationProjectionAuthorityReader {
    CompletableFuture<GenerationProjectionAuthoritySnapshot> capture(
            LiveProjectionSubject subject);
}
