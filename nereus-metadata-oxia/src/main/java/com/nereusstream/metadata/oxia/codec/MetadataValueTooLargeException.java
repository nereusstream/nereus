/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

/** Typed signal that a canonical metadata payload cannot fit the frozen value bound. */
public final class MetadataValueTooLargeException extends MetadataCodecException {
    public MetadataValueTooLargeException(String message) {
        super(message);
    }
}
