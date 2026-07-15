/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

/**
 * Module-neutral semantic verifier for embedded canonical metadata records.
 * Implementations live above object-store and must decode through the authoritative metadata codec registry.
 */
public interface RecoveryCheckpointVerifier {
    void verifyPublication(
            RecoveryCheckpointWriteRequest header,
            RecoveryCheckpointPublication publication);

    void verifyEntry(
            RecoveryCheckpointWriteRequest header,
            RecoveryCheckpointEntry entry);
}
