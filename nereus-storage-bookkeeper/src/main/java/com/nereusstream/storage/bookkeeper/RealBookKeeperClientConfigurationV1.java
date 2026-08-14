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

import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import java.util.Objects;
import org.apache.bookkeeper.conf.ClientConfiguration;

/** Closed projection from the admitted M2 capability to the exact BookKeeper 4.18 client configuration. */
public final class RealBookKeeperClientConfigurationV1 {
    private RealBookKeeperClientConfigurationV1() {}

    public static ClientConfiguration from(String metadataServiceUri, BookKeeperCapabilitySnapshotV1 capability) {
        Objects.requireNonNull(metadataServiceUri, "metadataServiceUri");
        Objects.requireNonNull(capability, "capability");
        if (!metadataServiceUri.matches("zk://[^/]+/[^/]+")) {
            throw new IllegalArgumentException("metadata service URI must name one explicit ZooKeeper ledger root");
        }
        if (capability.protocolMode() != BookKeeperProtocolModeV1.V3) {
            throw new IllegalArgumentException("M2 requires BookKeeper protocol v3");
        }

        BookKeeperTimeoutClassV1 timeouts = capability.timeoutClass();
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setMetadataServiceUri(metadataServiceUri);
        configuration.setUseV2WireProtocol(false);
        configuration.setPreserveMdcForTaskExecution(false);
        configuration.setClientConnectTimeoutMillis(exactInt(timeouts.connectMillis(), "connect timeout"));
        configuration.setAddEntryTimeout(exactSeconds(timeouts.addMillis(), "add timeout"));
        configuration.setReadEntryTimeout(exactSeconds(timeouts.readMillis(), "read timeout"));
        configuration.setNettyMaxFrameSizeBytes(capability.clientFrameLimitBytes());
        return configuration;
    }

    private static int exactSeconds(long millis, String name) {
        if (millis % 1_000 != 0) {
            throw new IllegalArgumentException(name + " must be an exact whole-second BookKeeper timeout");
        }
        return exactInt(millis / 1_000, name);
    }

    private static int exactInt(long value, String name) {
        if (value <= 0 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " is outside the BookKeeper integer range");
        }
        return Math.toIntExact(value);
    }
}
