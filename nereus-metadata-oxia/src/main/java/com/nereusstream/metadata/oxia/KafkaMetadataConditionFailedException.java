/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

public final class KafkaMetadataConditionFailedException extends RuntimeException {
    public KafkaMetadataConditionFailedException(String message) { super(message); }
    public KafkaMetadataConditionFailedException(String message, Throwable cause) { super(message, cause); }
}
