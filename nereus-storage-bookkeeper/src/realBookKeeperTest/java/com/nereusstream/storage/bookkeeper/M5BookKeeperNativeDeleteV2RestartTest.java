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

import static com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteV2RealTest.await;
import static com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteV2RealTest.connect;
import static com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteV2RealTest.sealed;
import static com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteV2RealTest.spec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.DeleteOutcome;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Separate JVMs read the surviving epoch and ledger before mutating; checkpoint stores input and identity hashes. */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class M5BookKeeperNativeDeleteV2RestartTest {
    @Test
    void writeBeforeServerRestart() throws Exception {
        var spec = spec();
        try (var client = connect(spec)) {
            var handle = sealed(client, spec);
            await(client.fenceCreates());
            var authority = client.deleteAuthority(handle);
            var previous = await(authority.claim(Optional.empty(), UUID.randomUUID()));
            var current = await(authority.claim(Optional.of(previous), UUID.randomUUID()));
            var target = await(client.captureExactTarget(handle)).exactTarget().orElseThrow();
            Files.write(
                    checkpoint(),
                    List.of(
                            spec.encode().toHex(),
                            Long.toString(handle.ledgerIdentity().ledgerId()),
                            Sha256Digest.hash(current.encode()).toHex(),
                            Integer.toString(current.nativeVersion()),
                            target.metadataSha256().toHex(),
                            previous.owner().toString(),
                            Sha256Digest.hash(previous.encode()).toHex()));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(7);
        var spec = M5BookKeeperNativeCreateSpecV2.decode(
                CanonicalBytes.copyOf(HexFormat.of().parseHex(lines.get(0))));
        var run = spec.configurations().get(0);
        var handle = new RunLedgerHandleV1(
                run.providerScopeId(),
                run.runId(),
                new BookKeeperLedgerIdentity(Long.parseLong(lines.get(1))),
                run.configurationDigest());
        try (var client = connect(spec)) {
            var authority = client.deleteAuthority(handle);
            var current = await(authority.read()).orElseThrow();
            assertThat(Sha256Digest.hash(current.encode()).toHex()).isEqualTo(lines.get(2));
            assertThat(current.nativeVersion()).isEqualTo(Integer.parseInt(lines.get(3)));
            var target = await(client.captureExactTarget(handle)).exactTarget().orElseThrow();
            assertThat(target.metadataSha256().toHex()).isEqualTo(lines.get(4));
            var previous = new M5BookKeeperNativeDeleteAuthorityV2.Snapshot(
                    current.resource(), UUID.fromString(lines.get(5)), 1, current.capabilitySha256(), 0);
            assertThat(Sha256Digest.hash(previous.encode()).toHex()).isEqualTo(lines.get(6));
            assertThatThrownBy(() -> await(authority.deleteExact(previous, target)))
                    .hasRootCauseMessage("native delete owner is fenced");
            assertThat(await(client.captureExactTarget(handle)).exactTarget()).contains(target);
            assertThat(await(authority.deleteExact(current, target)).outcome())
                    .isEqualTo(DeleteOutcome.AUTHORITATIVELY_ABSENT);
            assertThat(await(authority.read())).contains(current);
        }
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.nativeDelete.restartCheckpoint"));
    }
}
