/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Closed typed reference to immutable Registry admission evidence. */
public record RegistryEvidenceReferenceV1(int kind, int version, Sha256Digest digest) {
    public static final int REGISTRY_ADMISSION_EVIDENCE = 1;
    public static final int VERSION = 1;

    public RegistryEvidenceReferenceV1 {
        Objects.requireNonNull(digest, "digest");
        if (kind != REGISTRY_ADMISSION_EVIDENCE || version != VERSION || digest.isZero()) {
            throw new RegistryValidationException(
                    RegistryRejectionCodeV1.REGISTRY_IDENTITY_INVALID,
                    "Registry evidence reference must be closed kind/version 1 with non-zero digest");
        }
    }
}
