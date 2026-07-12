/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.SecureRandom;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class NereusProcessIdentityTest {
    @Test
    void generatesUrlSafeIdentityAndZerosTemporaryEntropy() {
        CapturingSecureRandom random = new CapturingSecureRandom();

        NereusProcessIdentity identity = NereusProcessIdentity.generate(random);

        assertThat(identity.processRunId()).hasSize(22).matches("[A-Za-z0-9_-]{22}");
        assertThat(identity.writerId()).isEqualTo("pulsar-f2/" + identity.processRunId());
        assertThat(random.supplied).containsOnly((byte) 0);
    }

    @Test
    void rejectsIdentityMismatch() {
        assertThatThrownBy(() -> new NereusProcessIdentity(
                "AAAAAAAAAAAAAAAAAAAAAA", "pulsar-f2/different"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("writerId");
    }

    private static final class CapturingSecureRandom extends SecureRandom {
        private byte[] supplied;

        @Override
        public void nextBytes(byte[] bytes) {
            Arrays.fill(bytes, (byte) 7);
            supplied = bytes;
        }
    }
}
