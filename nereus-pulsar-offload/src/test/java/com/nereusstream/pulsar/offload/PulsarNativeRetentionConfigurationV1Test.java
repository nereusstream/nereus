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

package com.nereusstream.pulsar.offload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.retention.M5PulsarRetentionConfigurationV1;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.junit.jupiter.api.Test;

class PulsarNativeRetentionConfigurationV1Test {
    @Test
    void installedCopyKeepsSdkResourcesAndFreezesRetentionAndClockOnly() {
        var source =
                new ManagedLedgerConfig().setRetentionTime(7, TimeUnit.SECONDS).setRetentionSizeInMB(2);
        var copy = source.copyWithFrozenRetention();
        assertThat(copy).isNotSameAs(source);
        assertThat(copy.getRetentionTimeMillis()).isEqualTo(7_000);
        assertThat(copy.getRetentionSizeInMB()).isEqualTo(2);
        assertThat(copy.getClock()).isSameAs(source.getClock());
        source.setRetentionTime(0, TimeUnit.SECONDS).setRetentionSizeInMB(0);
        assertThat(copy.getRetentionTimeMillis()).isEqualTo(7_000);
        assertThatThrownBy(() -> copy.setRetentionTime(0, TimeUnit.SECONDS)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> copy.setRetentionSizeInMB(0)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> copy.setClock(Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)))
                .isInstanceOf(IllegalStateException.class);
        copy.setMaxEntriesPerLedger(123);
        assertThat(copy.getMaxEntriesPerLedger()).isEqualTo(123);
    }

    @Test
    void nativeRetentionUsesStrictTimeOrRemainingSizeAndNeverCrossesConsumedOrCurrentLedger() {
        long[] sizes = {1_048_576, 1_048_576, 1_048_576};
        long[] timestamps = {100, 200, 300};
        assertThat(retained(sizes, timestamps, 2, -1, -1, 1_000)).isZero();
        assertThat(retained(sizes, timestamps, 2, -60_000, -1, 1_000)).isZero();
        assertThat(retained(sizes, timestamps, 2, 900, -1, 1_000)).isZero();
        assertThat(retained(sizes, timestamps, 2, 899, -1, 1_000)).isEqualTo(1);
        assertThat(retained(sizes, timestamps, 2, -1, 2, 1_000)).isEqualTo(1);
        assertThat(retained(sizes, timestamps, 2, 0, -1, 1_000)).isEqualTo(2);
        assertThat(retained(sizes, timestamps, 1, -1, 0, 1_000)).isEqualTo(1);
        assertThat(retained(sizes, new long[] {-1, 200, 300}, 2, 0, -1, 1_000)).isZero();
        assertThat(retained(sizes, timestamps, 2, 0, 0, 50)).isEqualTo(2);
        assertThatThrownBy(() -> retained(new long[] {-1, 2}, new long[] {100, 200}, 1, 0, 0, 1_000))
                .isInstanceOf(IllegalStateException.class);
    }

    private static int retained(long[] sizes, long[] timestamps, int consumed, long time, long size, long now) {
        var scope =
                Sha256Digest.hash(CanonicalUtf8.fromString("unit-policy-scope").bytes());
        var policy = new M5PulsarRetentionConfigurationV1(time, size, 1, scope, Optional.of(scope));
        return PulsarNativeCursorFloorScanV3.retainedLedgerIndex(sizes, timestamps, consumed, policy, now);
    }
}
