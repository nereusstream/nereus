/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class M5BookKeeperNativeCreateSpecV2Test {
    private static final String INSTANCE = "10203040-0000-0000-0000-000000000001";
    private static final Sha256Digest TASK = Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1}));

    @Test
    void canonicalMaximumScopeRoundTripsWithoutEndpointOrOwnerInPhysicalNamespace() {
        var runs = IntStream.rangeClosed(1, 256)
                .mapToObj(M5BookKeeperNativeCreateSpecV2Test::run)
                .toList();
        var spec = M5BookKeeperNativeCreateSpecV2.of(INSTANCE, TASK, runs);
        assertThat(spec.encode().length()).isLessThan(M5BookKeeperNativeCreateSpecV2.MAX_BYTES);
        assertThat(M5BookKeeperNativeCreateSpecV2.decode(spec.encode())).isEqualTo(spec);
        assertThat(spec.namespace())
                .isEqualTo(M5BookKeeperNativeCreateSpecV2.of(
                                INSTANCE, Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {2})), List.of(run(300)))
                        .namespace());
        assertThat(spec.namespace())
                .isNotEqualTo(M5BookKeeperNativeCreateSpecV2.namespace("10203040-0000-0000-0000-000000000002"));
    }

    @Test
    void rejectTruncationTrailingBytesAndUnsupportedVersionBeforeAdmission() {
        byte[] encoded = M5BookKeeperNativeCreateSpecV2.of(INSTANCE, TASK, List.of(run(1)))
                .encode()
                .toByteArray();
        for (int length = 0; length < encoded.length; length++) {
            var truncated = CanonicalBytes.copyOf(Arrays.copyOf(encoded, length));
            assertThatThrownBy(() -> M5BookKeeperNativeCreateSpecV2.decode(truncated))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        var trailing = CanonicalBytes.copyOf(Arrays.copyOf(encoded, encoded.length + 1));
        assertThatThrownBy(() -> M5BookKeeperNativeCreateSpecV2.decode(trailing))
                .isInstanceOf(IllegalArgumentException.class);
        encoded[4] = 1;
        assertThatThrownBy(() -> M5BookKeeperNativeCreateSpecV2.decode(CanonicalBytes.copyOf(encoded)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectDuplicateUnorderedEmptyAndOversizedRunAdmission() {
        assertThatThrownBy(() -> M5BookKeeperNativeCreateSpecV2.of(INSTANCE, TASK, List.of(run(1), run(1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new M5BookKeeperNativeCreateSpecV2(INSTANCE, TASK, List.of(run(2), run(1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> M5BookKeeperNativeCreateSpecV2.of(INSTANCE, TASK, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        var oversized = IntStream.rangeClosed(1, 257)
                .mapToObj(M5BookKeeperNativeCreateSpecV2Test::run)
                .toList();
        assertThatThrownBy(() -> M5BookKeeperNativeCreateSpecV2.of(INSTANCE, TASK, oversized))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nativeIdentityAndCallbackErrorsAreFailClosed() {
        assertThatThrownBy(() -> M5BookKeeperNativeCreateSpecV2.namespace("1-0-0-0-1"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(M5BookKeeperNativeCreateGuardV2.failure(0, "/invalid")).isNotNull();
        assertThat(M5BookKeeperNativeCreateGuardV2.failure(-123456, "/invalid")).isNotNull();
    }

    private static RunLedgerConfigurationV1 run(int id) {
        return new RunLedgerConfigurationV1(
                new CellProviderScopeId(TASK),
                new StorageRunId(new Id128(0, id)),
                3,
                3,
                2,
                BookKeeperDigestTypeV1.CRC32C,
                TASK);
    }
}
