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

package com.nereusstream.metadata.oxia.v2.retention;

import static com.nereusstream.metadata.oxia.v2.retention.M5PermanentDoneOxiaIntegrationTest.Fixture.await;
import static com.nereusstream.metadata.oxia.v2.retention.M5ReadFencedOxiaIntegrationTest.decode;
import static com.nereusstream.metadata.oxia.v2.retention.M5ReadFencedOxiaIntegrationTest.external;
import static com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2.digest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.metadata.oxia.v2.retention.M5PermanentDoneOxiaIntegrationTest.Fixture;
import com.nereusstream.metadata.oxia.v2.retention.M5ReadFencedOxiaIntegrationTest.RecoveryFixture;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1.Outcome;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Same native container and data, separate JVMs; checkpoint carries only route/key/hash/version identities. */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class M5ReadFencedOxiaRestartTest {
    @Test
    void writeBeforeServerRestart() throws Exception {
        try (var fixture = new RecoveryFixture(Fixture.root())) {
            var old = fixture.fence();
            var context = fixture.next(old, true);
            var snapshot = fixture.snapshot(decode(old).authorityRevision() + 1);
            fixture.nativeFixture.faults.loseCas = true;
            var result = await(fixture.coordinator.refreshIdentityRead(old, context, snapshot));
            assertThat(result.outcome()).isEqualTo(Outcome.APPLIED_EXACT);
            assertThat(fixture.nativeFixture.faults.loseCas).isFalse();
            var stored = result.observed().orElseThrow();
            M5ReadFencedOxiaIntegrationTest.verifyRefresh(old, stored, context);
            Files.write(
                    checkpoint(),
                    List.of(
                            fixture.nativeFixture.root,
                            stored.key(),
                            stored.canonicalStoredSha256().toHex(),
                            stored.metadataVersion().value().toHex()));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(4);
        try (var fixture = new RecoveryFixture(lines.get(0))) {
            // Read the surviving exact authority before creating any successor fact or performing a mutation.
            var old = await(fixture.nativeFixture.route.read(lines.get(1))).orElseThrow();
            assertThat(old.canonicalStoredSha256().toHex()).isEqualTo(lines.get(2));
            assertThat(old.metadataVersion().value().toHex()).isEqualTo(lines.get(3));
            assertThat(decode(old)
                            .readFence()
                            .orElseThrow()
                            .observationContext()
                            .observationEpoch())
                    .isEqualTo(2);
            var late = external(old);
            var context = fixture.next(old, true);
            var snapshot = fixture.snapshot(decode(old).authorityRevision() + 1);
            var fresh = await(fixture.coordinator.refreshIdentityRead(old, context, snapshot))
                    .observed()
                    .orElseThrow();
            M5ReadFencedOxiaIntegrationTest.verifyRefresh(old, fresh, context);
            assertThatThrownBy(() -> fixture.coordinator.bindDeleteIntent(fresh, late, digest("late-after-restart")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(await(fixture.coordinator.bindDeleteIntent(old, late, digest("old-after-restart")))
                            .outcome())
                    .isEqualTo(Outcome.DEFINITIVE_CONFLICT);
            assertThat(await(fixture.coordinator.bindDeleteIntent(fresh, external(fresh), digest("after-restart")))
                            .exactCandidateIsAuthoritative())
                    .isTrue();
        }
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.readFenced.oxia.restartCheckpoint"));
    }
}
