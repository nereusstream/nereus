/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;

/** Typed conditional-create collision while preserving the public Phase 1 error mapping. */
public final class ObjectAlreadyExistsException extends NereusException {
    public ObjectAlreadyExistsException(String message) {
        super(ErrorCode.OBJECT_UPLOAD_FAILED, false, message);
    }
}
