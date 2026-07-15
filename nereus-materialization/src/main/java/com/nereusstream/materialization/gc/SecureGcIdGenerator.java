/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.keys.DeterministicIds;
import java.security.SecureRandom;
import java.util.Objects;

/** Secure random 128-bit-or-stronger lowercase-base32 GC identity generator. */
public final class SecureGcIdGenerator implements GcIdGenerator {
    private final SecureRandom random;

    public SecureGcIdGenerator() {
        this(new SecureRandom());
    }

    SecureGcIdGenerator(SecureRandom random) {
        this.random = Objects.requireNonNull(random, "random");
    }

    @Override
    public String next() {
        byte[] entropy = new byte[16];
        random.nextBytes(entropy);
        return DeterministicIds.randomRunIdHash(entropy);
    }
}
