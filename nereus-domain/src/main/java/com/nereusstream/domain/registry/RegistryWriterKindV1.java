/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry;

/** Complete Registry-v1 ledger-ID writer discriminator. */
public enum RegistryWriterKindV1 {
    NATIVE_BOOKKEEPER_LEDGER_ID(1),
    NEREUS_VIRTUAL_LEDGER_ID(2);

    private final int code;

    RegistryWriterKindV1(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static RegistryWriterKindV1 fromCode(int code) {
        return switch (code) {
            case 1 -> NATIVE_BOOKKEEPER_LEDGER_ID;
            case 2 -> NEREUS_VIRTUAL_LEDGER_ID;
            default ->
                throw new RegistryValidationException(
                        RegistryRejectionCodeV1.REGISTRY_UNAUTHORIZED_WRITER, "unknown Registry writer kind: " + code);
        };
    }
}
