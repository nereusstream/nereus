/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

/** Generates one fresh random identity for each NRC1 build attempt. */
@FunctionalInterface
public interface RecoveryCheckpointAttemptIdGenerator {
    String next();
}
