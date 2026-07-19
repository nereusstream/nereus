/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

/** Typed put-if-absent/version/delete condition failure for BookKeeper metadata. */
public final class BookKeeperMetadataConditionFailedException extends RuntimeException {
    public BookKeeperMetadataConditionFailedException(String message) {
        super(message);
    }

    public BookKeeperMetadataConditionFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
