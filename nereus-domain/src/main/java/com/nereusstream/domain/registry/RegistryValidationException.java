/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry;

import java.util.Objects;

/** A fail-closed Registry-v1 validation error with a stable machine code. */
public final class RegistryValidationException extends IllegalArgumentException {
    private final RegistryRejectionCodeV1 code;

    public RegistryValidationException(RegistryRejectionCodeV1 code, String message) {
        super(message);
        this.code = Objects.requireNonNull(code, "code");
    }

    public RegistryValidationException(RegistryRejectionCodeV1 code, String message, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "code");
    }

    public RegistryRejectionCodeV1 code() {
        return code;
    }
}
