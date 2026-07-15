/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.PublicationId;
import com.nereusstream.api.keys.DeterministicIds;
import java.security.SecureRandom;
import java.util.Objects;

/** Cryptographically strong default publication-attempt identity generator. */
public final class SecurePublicationIdGenerator implements PublicationIdGenerator {
    private final SecureRandom random;

    public SecurePublicationIdGenerator() {
        this(new SecureRandom());
    }

    SecurePublicationIdGenerator(SecureRandom random) {
        this.random = Objects.requireNonNull(random, "random");
    }

    @Override
    public PublicationId next() {
        byte[] entropy = new byte[16];
        random.nextBytes(entropy);
        return new PublicationId(DeterministicIds.randomRunIdHash(entropy));
    }
}
