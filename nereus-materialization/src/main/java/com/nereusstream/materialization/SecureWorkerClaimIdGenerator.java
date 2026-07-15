/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.keys.DeterministicIds;
import java.security.SecureRandom;
import java.util.Objects;

/** Default 128-bit worker-claim identity generator. */
public final class SecureWorkerClaimIdGenerator implements WorkerClaimIdGenerator {
    private final SecureRandom random;

    public SecureWorkerClaimIdGenerator() {
        this(new SecureRandom());
    }

    SecureWorkerClaimIdGenerator(SecureRandom random) {
        this.random = Objects.requireNonNull(random, "random");
    }

    @Override
    public String next() {
        byte[] entropy = new byte[16];
        random.nextBytes(entropy);
        return DeterministicIds.randomRunIdHash(entropy);
    }
}
