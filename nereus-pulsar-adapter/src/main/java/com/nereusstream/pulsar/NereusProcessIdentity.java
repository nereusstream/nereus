/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.keys.DeterministicIds;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;

/** One process-run identity generated before StreamStorageConfig is built. */
public record NereusProcessIdentity(String processRunId, String writerId) {
    private static final Pattern PROCESS_RUN_ID = Pattern.compile("[a-z2-7]{26,128}");

    public NereusProcessIdentity {
        Objects.requireNonNull(processRunId, "processRunId");
        Objects.requireNonNull(writerId, "writerId");
        if (!PROCESS_RUN_ID.matcher(processRunId).matches()) {
            throw new IllegalArgumentException(
                    "processRunId must be lowercase base32 with at least 128 bits");
        }
        if (!writerId.equals("pulsar-f2/" + processRunId)) {
            throw new IllegalArgumentException("writerId must equal pulsar-f2/{processRunId}");
        }
    }

    public static NereusProcessIdentity generate(SecureRandom random) {
        Objects.requireNonNull(random, "random");
        byte[] bytes = new byte[16];
        try {
            random.nextBytes(bytes);
            String processRunId = DeterministicIds.randomRunIdHash(bytes);
            return new NereusProcessIdentity(processRunId, "pulsar-f2/" + processRunId);
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
    }
}
