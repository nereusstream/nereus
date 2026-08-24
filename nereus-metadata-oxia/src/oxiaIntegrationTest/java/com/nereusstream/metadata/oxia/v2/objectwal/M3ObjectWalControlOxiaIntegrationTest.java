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

package com.nereusstream.metadata.oxia.v2.objectwal;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Exact-byte client-reconnect and persistence evidence path against a source-locked O1 server. */
class M3ObjectWalControlOxiaIntegrationTest {
    private static final String POINTER = "v2/object-wal/shards/0000000007/current";
    private static final String ROOT = "v2/object-wal/shards/0000000007/runs/00000000000000000011/root";
    private static final CanonicalBytes FIRST = bytes("root-v1");
    private static final CanonicalBytes SECOND = bytes("root-v2");

    @Test
    void exactCreateCasClientReconnectAndCellIsolationUseTheLockedO1Backend() throws Exception {
        String leftCell = "/nereus/v2/m3/object-wal/cells/left-" + UUID.randomUUID();
        String rightCell = "/nereus/v2/m3/object-wal/cells/right-" + UUID.randomUUID();

        try (AsyncOxiaClient client = connect()) {
            var left = new OxiaCanonicalControlMetadataStore(client, leftCell, 7);
            var right = new OxiaCanonicalControlMetadataStore(client, rightCell, 7);

            assertThat(left.putIfAbsent(ROOT, FIRST)).isEqualTo(ControlMutationOutcome.APPLIED);
            assertThat(left.compareAndSet(POINTER, Optional.empty(), FIRST)).isEqualTo(ControlMutationOutcome.APPLIED);
            assertThat(left.compareAndSet(POINTER, Optional.of(FIRST), SECOND))
                    .isEqualTo(ControlMutationOutcome.APPLIED);
            assertThat(left.compareAndSet(POINTER, Optional.of(FIRST), bytes("loser")))
                    .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);

            assertThat(right.get(ROOT)).isEmpty();
            assertThat(right.putIfAbsent(ROOT, SECOND)).isEqualTo(ControlMutationOutcome.APPLIED);
        }

        try (AsyncOxiaClient restartedClient = connect()) {
            var left = new OxiaCanonicalControlMetadataStore(restartedClient, leftCell, 7);
            var right = new OxiaCanonicalControlMetadataStore(restartedClient, rightCell, 7);
            assertThat(left.get(ROOT)).contains(FIRST);
            assertThat(left.get(POINTER)).contains(SECOND);
            assertThat(right.get(ROOT)).contains(SECOND);
        }
    }

    private static AsyncOxiaClient connect() throws Exception {
        String serviceAddress = System.getProperty("nereus.m3.objectwal.oxia.serviceAddress");
        if (serviceAddress == null || serviceAddress.isBlank() || "UNSET".equals(serviceAddress)) {
            throw new IllegalStateException("missing exact-source Oxia service address");
        }
        return OxiaClientBuilder.create(serviceAddress)
                .namespace("default")
                .asyncClient()
                .get(30, TimeUnit.SECONDS);
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }
}
