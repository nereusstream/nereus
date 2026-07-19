/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.SecureRandom;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class NereusProcessIdentityTest {
    @Test
    void generatesLowercaseBase32IdentityAndZerosTemporaryEntropy() {
        CapturingSecureRandom random = new CapturingSecureRandom();

        NereusProcessIdentity identity = NereusProcessIdentity.generate(random);

        assertThat(identity.processRunId()).hasSize(52).matches("[a-z2-7]{52}");
        assertThat(identity.writerId()).isEqualTo("pulsar-f2/" + identity.processRunId());
        assertThat(random.supplied).containsOnly((byte) 0);
    }

    @Test
    void rejectsIdentityMismatch() {
        assertThatThrownBy(() -> new NereusProcessIdentity(
                "aaaaaaaaaaaaaaaaaaaaaaaaaa", "pulsar-f2/different"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("writerId");
    }

    @Test
    void rejectsLegacyBase64UrlIdentity() {
        assertThatThrownBy(() -> new NereusProcessIdentity(
                "AAAAAAAAAAAAAAAAAAAAAA", "pulsar-f2/AAAAAAAAAAAAAAAAAAAAAA"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lowercase base32");
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
