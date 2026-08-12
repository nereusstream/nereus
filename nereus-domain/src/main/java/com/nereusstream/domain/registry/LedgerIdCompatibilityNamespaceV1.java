/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Exact NLI1 compatibility-namespace derivation. */
public final class LedgerIdCompatibilityNamespaceV1 {
    private static final byte[] MAGIC = "NLI1".getBytes(StandardCharsets.US_ASCII);

    private LedgerIdCompatibilityNamespaceV1() {}

    public static Sha256Digest derive(BookKeeperInstanceIdV1 instanceId) {
        Objects.requireNonNull(instanceId, "instanceId");
        ByteBuffer output = ByteBuffer.allocate(MAGIC.length + Integer.BYTES + BookKeeperInstanceIdV1.LENGTH);
        output.put(MAGIC)
                .putInt(BookKeeperInstanceIdV1.LENGTH)
                .put(instanceId.bytes().toByteArray());
        return Sha256Digest.hash(CanonicalBytes.copyOf(output.array()));
    }
}
