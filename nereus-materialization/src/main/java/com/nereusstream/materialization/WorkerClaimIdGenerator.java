/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

/** Generates a cryptographically strong id once for a new worker claim attempt. */
@FunctionalInterface
public interface WorkerClaimIdGenerator {
    String next();
}
