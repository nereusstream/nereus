/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;

/** Permanent immutable compacted-object schema, metadata, ordering, or checksum violation. */
public final class CompactedObjectFormatException extends NereusException {
    public CompactedObjectFormatException(String message) {
        super(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, message);
    }

    public CompactedObjectFormatException(String message, Throwable cause) {
        super(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, message, cause);
    }
}
