/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

/** Internal signal used by deterministic projection-store backends for a lost single-key condition. */
final class ProjectionMetadataConditionFailedException extends RuntimeException {
    ProjectionMetadataConditionFailedException(String message) {
        super(message);
    }
}
