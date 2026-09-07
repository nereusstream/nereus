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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.M5GcQuotaOxiaIntegrationTest.QFixture;
import com.nereusstream.metadata.oxia.v2.retention.M5PermanentDoneOxiaIntegrationTest.Fixture;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.Result;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Same native container/data, separate JVMs; checkpoint carries identities/hashes, never reconstructed authority. */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class M5GcQuotaOxiaRestartTest {
    @Test
    void writeBeforeServerRestart() throws Exception {
        String root = Fixture.root();
        try (var first = new QFixture(root);
                var inspector = new QFixture(root)) {
            first.initialize(3);
            first.complete(1);
            assertThat(first.settle(1)).isEqualTo(Result.SETTLED);
            var intent = first.intent(2);
            first.faults.stopAfterWrites = 1;
            assertThatThrownBy(
                            () -> Fixture.await(first.route.quota().reserve(M5GcQuotaOxiaIntegrationTest.resource(3))))
                    .hasRootCauseMessage("injected quota client stopped");
            var head = Fixture.await(inspector.route.quota().snapshot()).head();
            assertThat(head.pending()).isPresent();
            Files.write(
                    checkpoint(),
                    List.of(
                            root,
                            Sha256Digest.hash(head.encode()).toHex(),
                            intent.canonicalStoredSha256().toHex(),
                            intent.metadataVersion().value().toHex(),
                            Fixture.await(inspector.route.read(M5GcQuotaOxiaIntegrationTest.resource(1)
                                            .authorityKey()))
                                    .orElseThrow()
                                    .canonicalStoredSha256()
                                    .toHex(),
                            Sha256Digest.hash(Fixture.await(inspector.faults.client.read(inspector.layout.nativeKey(
                                                    M5GcQuotaRecordsV2.entryKey(M5GcQuotaOxiaIntegrationTest.resource(2)
                                                            .sha256()))))
                                            .orElseThrow()
                                            .storedBytes())
                                    .toHex()));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(6);
        try (var q = new QFixture(lines.get(0))) {
            var before = Fixture.await(q.route.quota().snapshot());
            assertThat(Sha256Digest.hash(before.head().encode()).toHex()).isEqualTo(lines.get(1));
            var intent = Fixture.await(q.route.read(
                            M5GcQuotaOxiaIntegrationTest.resource(2).authorityKey()))
                    .orElseThrow();
            assertThat(intent.canonicalStoredSha256().toHex()).isEqualTo(lines.get(2));
            assertThat(intent.metadataVersion().value().toHex()).isEqualTo(lines.get(3));
            assertThat(Fixture.await(q.route.read(
                                    M5GcQuotaOxiaIntegrationTest.resource(1).authorityKey()))
                            .orElseThrow()
                            .canonicalStoredSha256()
                            .toHex())
                    .isEqualTo(lines.get(4));
            assertThat(Sha256Digest.hash(Fixture.await(q.faults.client.read(q.layout.nativeKey(
                                            M5GcQuotaRecordsV2.entryKey(M5GcQuotaOxiaIntegrationTest.resource(2)
                                                    .sha256()))))
                                    .orElseThrow()
                                    .storedBytes())
                            .toHex())
                    .isEqualTo(lines.get(5));
            // This exact existing intent completes while the recovered head is still full with another pending grant.
            q.complete(2);
            assertThat(Fixture.await(q.route.quota().snapshot()).head().pending())
                    .isEqualTo(before.head().pending());
            assertThat(q.reserve(4)).isEqualTo(Result.EXHAUSTED);
            assertThat(q.settle(2)).isEqualTo(Result.SETTLED);
            assertThat(q.reserve(1)).isEqualTo(Result.SETTLED);
            assertThat(q.reserve(2)).isEqualTo(Result.SETTLED);
            assertThat(q.reserve(4)).isEqualTo(Result.GRANTED);
            assertThat(q.reserve(5)).isEqualTo(Result.EXHAUSTED);
            assertThat(Fixture.await(
                            q.route.quota().expand(before.head().capacityBytes() + q.layout.reservationCharge())))
                    .isTrue();
            assertThat(q.reserve(5)).isEqualTo(Result.GRANTED);
            assertThat(Fixture.await(q.route.quota().snapshot()).head().settledResources())
                    .isEqualTo(2);
            assertThat(Fixture.await(q.route.quota().snapshot()).head().reservedResources())
                    .isEqualTo(3);
            assertThat(Fixture.await(q.route.quota().snapshot()).head().pending())
                    .isEmpty();
        }
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.quota.oxia.restartCheckpoint"));
    }
}
