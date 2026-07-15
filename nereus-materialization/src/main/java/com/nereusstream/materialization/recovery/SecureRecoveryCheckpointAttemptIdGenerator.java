/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import com.nereusstream.api.keys.DeterministicIds;
import java.security.SecureRandom;
import java.util.Objects;

/** Cryptographically strong 128-bit checkpoint-attempt identity generator. */
public final class SecureRecoveryCheckpointAttemptIdGenerator
        implements RecoveryCheckpointAttemptIdGenerator {
    private final SecureRandom random;

    public SecureRecoveryCheckpointAttemptIdGenerator() {
        this(new SecureRandom());
    }

    SecureRecoveryCheckpointAttemptIdGenerator(SecureRandom random) {
        this.random = Objects.requireNonNull(random, "random");
    }

    @Override
    public String next() {
        byte[] entropy = new byte[16];
        random.nextBytes(entropy);
        return DeterministicIds.randomRunIdHash(entropy);
    }
}
