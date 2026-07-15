/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.nereusstream.api.ObjectKey;
import java.util.concurrent.CompletableFuture;

/** Durable owner/root authorization invoked immediately before each provider transmission. */
@FunctionalInterface
public interface PutObjectAttemptGuard {
    CompletableFuture<Void> authorize(ObjectKey key, int providerAttemptNumber);
}
