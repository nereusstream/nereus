/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;

/** Permanent NRC1 layout, ordering, identity, or checksum violation. */
public final class RecoveryCheckpointFormatException extends NereusException {
    public RecoveryCheckpointFormatException(String message) {
        super(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, message);
    }

    public RecoveryCheckpointFormatException(String message, Throwable cause) {
        super(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, message, cause);
    }
}
