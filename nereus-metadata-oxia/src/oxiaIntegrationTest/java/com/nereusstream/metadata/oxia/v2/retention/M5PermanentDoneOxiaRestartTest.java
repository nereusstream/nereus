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
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.M5PermanentDoneOxiaIntegrationTest.Fixture;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Independent JVMs and retained native server data. Synthetic proofs exercise storage and permanent rejection only. */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class M5PermanentDoneOxiaRestartTest {
    @Test
    void writeBeforeServerRestart() throws Exception {
        try (var fixture = new Fixture(Fixture.root())) {
            var done = fixture.complete(1);
            var exactDone =
                    Fixture.await(fixture.route.read(done.authorityKey())).orElseThrow();
            var pendingResource = SyntheticDeleteAuthorityFixturesV2.resource(99);
            var phases = SyntheticDeleteAuthorityFixturesV2.phases(pendingResource);
            var current =
                    Optional
                            .<com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue>
                                    empty();
            for (var phase : phases.subList(0, 3)) {
                current = Optional.of(fixture.persist(current, phase));
            }
            var pending = current.orElseThrow();
            Files.write(
                    checkpoint(),
                    List.of(
                            fixture.root,
                            Base64.getEncoder()
                                    .encodeToString(
                                            done.resource().canonicalBytes().toByteArray()),
                            Sha256Digest.hash(done.encode()).toHex(),
                            HexFormat.of()
                                    .formatHex(
                                            exactDone.metadataVersion().value().toByteArray()),
                            Base64.getEncoder()
                                    .encodeToString(
                                            pendingResource.canonicalBytes().toByteArray()),
                            pending.canonicalStoredSha256().toHex(),
                            HexFormat.of()
                                    .formatHex(pending.metadataVersion().value().toByteArray())));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(7);
        var doneResource = PhysicalResourceIdCodecV2.decode(
                CanonicalBytes.copyOf(Base64.getDecoder().decode(lines.get(1))));
        var pendingResource = PhysicalResourceIdCodecV2.decode(
                CanonicalBytes.copyOf(Base64.getDecoder().decode(lines.get(4))));
        try (var fixture = new Fixture(lines.get(0))) {
            // The checkpoint supplies identities/hashes only; current done and intent must come from native Oxia.
            var done = Fixture.await(fixture.coordinator.inspect(doneResource.authorityKey()))
                    .orElseThrow();
            assertThat(done.compactDone()).isPresent();
            assertThat(done.exactStoredValue().canonicalStoredSha256().toHex()).isEqualTo(lines.get(2));
            assertThat(HexFormat.of()
                            .formatHex(done.exactStoredValue()
                                    .metadataVersion()
                                    .value()
                                    .toByteArray()))
                    .isEqualTo(lines.get(3));
            fixture.rejectRediscovery(1);
            var pending = Fixture.await(fixture.coordinator.inspect(pendingResource.authorityKey()))
                    .orElseThrow();
            assertThat(pending.state()).isEqualTo(TargetDeleteAuthorityStateV1.DELETE_INTENT_V1);
            assertThat(pending.exactStoredValue().canonicalStoredSha256().toHex())
                    .isEqualTo(lines.get(5));
            assertThat(HexFormat.of()
                            .formatHex(pending.exactStoredValue()
                                    .metadataVersion()
                                    .value()
                                    .toByteArray()))
                    .isEqualTo(lines.get(6));
            var completed = fixture.persist(
                    Optional.of(pending.exactStoredValue()),
                    SyntheticDeleteAuthorityFixturesV2.phases(pendingResource).get(3));
            assertThat(Fixture.await(fixture.coordinator.compactDone(completed)).exactCandidateIsAuthoritative())
                    .isTrue();
            fixture.rejectRediscovery(99);
            assertThat(fixture.done(1)).isEqualTo(done.compactDone().orElseThrow());
        }
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.done.oxia.restartCheckpoint"));
    }
}
