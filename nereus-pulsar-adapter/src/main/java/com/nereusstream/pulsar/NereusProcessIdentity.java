/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.regex.Pattern;

/** One process-run identity generated before StreamStorageConfig is built. */
public record NereusProcessIdentity(String processRunId, String writerId) {
    private static final Pattern PROCESS_RUN_ID = Pattern.compile("[A-Za-z0-9_-]{22}");

    public NereusProcessIdentity {
        Objects.requireNonNull(processRunId, "processRunId");
        Objects.requireNonNull(writerId, "writerId");
        if (!PROCESS_RUN_ID.matcher(processRunId).matches()) {
            throw new IllegalArgumentException("processRunId must be a URL-safe 128-bit identifier");
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
            String processRunId = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
            return new NereusProcessIdentity(processRunId, "pulsar-f2/" + processRunId);
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
    }
}
