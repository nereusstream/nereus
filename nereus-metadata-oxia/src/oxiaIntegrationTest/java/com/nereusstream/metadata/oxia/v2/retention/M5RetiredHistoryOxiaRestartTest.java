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

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerCoordinatorV1;
import com.nereusstream.storage.object.retention.M5RetiredBatchHistoryCoordinatorV2.Outcome;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Two separate JVM invocations; the source-locked runner must restart the same server container between them. */
class M5RetiredHistoryOxiaRestartTest {
    @Test
    void writeBeforeServerRestart() throws Exception {
        try (var fixture = M5RetiredHistoryOxiaIntegrationTest.Fixture.create(1, 2, 3)) {
            var firstWorker = worker(fixture, "before-history");
            fixture.retire(2);
            fixture.retire(3);
            assertThat(fixture.fold(2)).isEqualTo(Outcome.APPLIED_EXACT);
            assertThat(fixture.fold(3)).isEqualTo(Outcome.APPLIED_EXACT);
            assertThat(firstWorker.isCurrent()).isFalse();
            var admittedWorker = worker(fixture, "after-history");
            var exact = fixture.exact();
            var authority = fixture.authority();
            Files.write(
                    checkpoint(),
                    List.of(
                            fixture.root,
                            exact.canonicalStoredSha256().toHex(),
                            authority.retiredHistory().sha256().toHex(),
                            Long.toString(authority.retiredHistory().count()),
                            Long.toString(authority.lastActivationOrdinal()),
                            java.util.HexFormat.of()
                                    .formatHex(exact.metadataVersion().value().toByteArray()),
                            Long.toString(authority.authorityGeneration()),
                            Sha256Digest.hash(admittedWorker.worker().encode()).toHex()));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(8);
        assertThat(lines.get(0)).matches("/nereus/v2/m5/history/[a-f0-9-]{36}");
        try (var fixture = new M5RetiredHistoryOxiaIntegrationTest.Fixture(lines.get(0))) {
            assertThat(fixture.exact().canonicalStoredSha256().toHex()).isEqualTo(lines.get(1));
            var authority = fixture.authority();
            assertThat(authority.retiredHistory().sha256().toHex()).isEqualTo(lines.get(2));
            assertThat(authority.retiredHistory().count())
                    .isEqualTo(Long.parseLong(lines.get(3)))
                    .isEqualTo(2);
            assertThat(authority.lastActivationOrdinal())
                    .isEqualTo(Long.parseLong(lines.get(4)))
                    .isEqualTo(3);
            assertThat(java.util.HexFormat.of()
                            .formatHex(fixture.exact().metadataVersion().value().toByteArray()))
                    .isEqualTo(lines.get(5));
            assertThat(authority.authorityGeneration()).isEqualTo(Long.parseLong(lines.get(6)));
            var storedWorker = authority.materializationWorker().orElseThrow();
            assertThat(authority.wireVersion()).isEqualTo(5);
            assertThat(authority.logicalTrim()).isPresent();
            assertThat(storedWorker.identity().workerEpoch()).isEqualTo(2);
            assertThat(Sha256Digest.hash(storedWorker.encode()).toHex()).isEqualTo(lines.get(7));
            var workers =
                    new M5MaterializationWorkerCoordinatorV1(fixture.route.rawControlMetadata(), 7, fixture.binding);
            workers.requireCurrent(storedWorker);
            assertThat(authority.batchSlots()).hasSize(1);
            fixture.requireRejected(2);
            fixture.requireRejected(3);
            fixture.admit(4);
            assertThat(fixture.authority().lastActivationOrdinal()).isEqualTo(4);
            assertThat(fixture.authority().materializationWorker()).contains(storedWorker);
            var nextWorker = worker(fixture, "after-server-restart");
            assertThat(nextWorker.worker().identity().workerEpoch()).isEqualTo(3);
            assertThat(nextWorker.isCurrent()).isTrue();
            System.out.println(
                    "M5_NATIVE_OXIA_WORKER_RESTART epoch=3 wire=" + authority.wireVersion() + " hash=" + lines.get(7));
        }
    }

    /** Native metadata/restart only; source, owner, namespace and plan facts are explicit synthetic fixtures. */
    private static M5MaterializationWorkerCoordinatorV1.Admission worker(
            M5RetiredHistoryOxiaIntegrationTest.Fixture fixture, String task) {
        var workers = new M5MaterializationWorkerCoordinatorV1(fixture.route.rawControlMetadata(), 7, fixture.binding);
        var proposal = workers.propose(
                        fixture.authority().selectorProjection(),
                        digest("synthetic-cell"),
                        digest("synthetic-scope"),
                        1,
                        digest("synthetic-native-worker"),
                        digest("synthetic-native-namespace"))
                .orElseThrow();
        return proposal.admit(digest("synthetic-source-cut"), digest("synthetic-plan"), digest(task))
                .orElseThrow();
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(value).bytes());
    }

    private static Path checkpoint() {
        String value = System.getProperty("nereus.m5.history.oxia.restartCheckpoint");
        if (value == null || value.isBlank() || "UNSET".equals(value)) {
            throw new IllegalStateException("missing owned real Oxia history restart checkpoint path");
        }
        return Path.of(value);
    }
}
