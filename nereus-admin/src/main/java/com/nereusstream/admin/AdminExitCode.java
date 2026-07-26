/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

public enum AdminExitCode {
    SUCCESS(0),
    INVALID_ARGUMENT(2),
    CONFIGURATION_ERROR(3),
    CONDITION_FAILED(4),
    TIMEOUT(5),
    PROVIDER_ERROR(6),
    INTERNAL_ERROR(10);

    private final int code;

    AdminExitCode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
