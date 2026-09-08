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
import com.nereusstream.storage.object.gc.DeleteObservationContextV2;
import com.nereusstream.storage.object.gc.DeleteRecoveryVetoV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1.Outcome;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Same native container and data, separate JVMs; checkpoint carries only route/key/hash/version identities. */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class M5ReadFencedOxiaRestartTest {
    @Test
    void writeBeforeServerRestart() throws Exception {
        try (var fixture = new RecoveryFixture(Fixture.root())) {
            var fenced = fixture.fence();
            var intent = await(fixture.coordinator.bindDeleteIntent(fenced, external(fenced), digest("intent")))
                    .observed()
                    .orElseThrow();
            var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                    fixture.nativeFixture.route,
                    fixture.syntheticOwner,
                    expected -> CompletableFuture.completedFuture(ExternalIdentityObservationV1.PRESENT_EXACT_V1));
            var refreshed = await(coordinator.refreshDispatch(intent, fixture.next(fenced, true), fixture.snapshot(4)))
                    .observed()
                    .orElseThrow();
            Files.write(
                    intentCheckpoint(),
                    List.of(
                            fixture.nativeFixture.root,
                            refreshed.key(),
                            refreshed.canonicalStoredSha256().toHex(),
                            refreshed.metadataVersion().value().toHex()));
        }
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
            var capability =
                    await(fixture.facts.read(context.capability().key())).orElseThrow();
            await(fixture.facts.compareAndSet(
                    Optional.of(capability), capability.key(), capability.canonicalStoredBytes()));
            assertThatThrownBy(() ->
                            await(fixture.coordinator.bindDeleteIntent(stored, external(stored), digest("revoked"))))
                    .hasRootCauseMessage("eligibility authority changed: " + capability.key());
            var vetoed = await(fixture.nativeFixture.route.read(stored.key())).orElseThrow();
            M5ReadFencedOxiaIntegrationTest.verifyVeto(
                    stored, vetoed, DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED);

            Files.write(
                    checkpoint(),
                    List.of(
                            fixture.nativeFixture.root,
                            vetoed.key(),
                            vetoed.canonicalStoredSha256().toHex(),
                            vetoed.metadataVersion().value().toHex()));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var intentLines = Files.readAllLines(intentCheckpoint());
        assertThat(intentLines).hasSize(4);
        try (var fixture = new RecoveryFixture(intentLines.get(0))) {
            var stored =
                    await(fixture.nativeFixture.route.read(intentLines.get(1))).orElseThrow();
            assertThat(stored.canonicalStoredSha256().toHex()).isEqualTo(intentLines.get(2));
            assertThat(stored.metadataVersion().value().toHex()).isEqualTo(intentLines.get(3));
            var previous = M5TargetDeleteAuthorityStateMachineV1.dispatchContext(decode(stored));
            assertThat(previous.observationEpoch()).isEqualTo(2);
            assertThat(decode(stored).deleteIntent().orElseThrow().dispatchEpoch())
                    .isEqualTo(2);
            var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(
                    fixture.nativeFixture.route,
                    fixture.syntheticOwner,
                    expected -> CompletableFuture.completedFuture(ExternalIdentityObservationV1.ABSENT_EXACT_V1));
            var successor = new DeleteObservationContextV2(
                    3, previous.coordinatorOwner(), previous.capability(), Optional.empty());
            var refreshed = await(coordinator.refreshDispatch(stored, successor, fixture.snapshot(5)))
                    .observed()
                    .orElseThrow();
            var done = await(coordinator.completeAbsent(refreshed)).observed().orElseThrow();
            assertThat(await(coordinator.compactDone(done)).exactTerminalIsAuthoritative())
                    .isTrue();
        }
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
            assertThat(decode(old).recoveryVeto().orElseThrow().reason())
                    .isEqualTo(DeleteRecoveryVetoV2.Reason.ELIGIBILITY_FACTS_REJECTED);
            assertThatThrownBy(() -> fixture.coordinator.bindDeleteIntent(old, external(old), digest("before-repair")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("qualified observation refresh");
            var late = external(old);
            var context = fixture.next(old, true);
            var snapshot = fixture.snapshot(decode(old).authorityRevision() + 1);
            var fresh = await(fixture.coordinator.refreshIdentityRead(old, context, snapshot))
                    .observed()
                    .orElseThrow();
            M5ReadFencedOxiaIntegrationTest.verifyRefresh(old, fresh, context);
            assertThatThrownBy(() -> fixture.coordinator.bindDeleteIntent(fresh, late, digest("late-after-restart")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> fixture.coordinator.bindDeleteIntent(old, late, digest("old-after-restart")))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(await(fixture.coordinator.bindDeleteIntent(fresh, external(fresh), digest("after-restart")))
                            .exactCandidateIsAuthoritative())
                    .isTrue();
        }
    }

    private static Path intentCheckpoint() {
        return Path.of(checkpoint().toString() + ".intent");
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.readFenced.oxia.restartCheckpoint"));
    }
}
