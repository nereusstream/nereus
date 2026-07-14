/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

/** Typed absent-create or exact-version condition failure for F3 metadata. */
public final class CursorMetadataConditionFailedException extends RuntimeException {
    public CursorMetadataConditionFailedException(String message) {
        super(message);
    }

    public CursorMetadataConditionFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
