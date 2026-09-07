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
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.util.ZkUtils;

/** Owned real native namespace connection. Assignment closes unbound M5 creates, not existing append/read handles. */
public final class M5BookKeeperNamespaceAuthorityV2 implements NativePhysicalNamespaceAuthorityV2, AutoCloseable {
    private final BookKeeper client;
    private final M5BookKeeperNamespaceGateV2 gate;

    private M5BookKeeperNamespaceAuthorityV2(BookKeeper client, M5BookKeeperNamespaceGateV2 gate) {
        this.client = client;
        this.gate = gate;
    }

    /** May initialize ordinary BK metadata and the permanent unbound creation gate; performs no ledger allocation. */
    public static M5BookKeeperNamespaceAuthorityV2 connect(
            String metadataServiceUri, BookKeeperCapabilitySnapshotV1 capability) throws Exception {
        var configuration = RealBookKeeperClientConfigurationV1.from(metadataServiceUri, capability);
        var client = (BookKeeper) org.apache.bookkeeper.client.api.BookKeeper.newBuilder(configuration)
                .build();
        try {
            var driver = (ZKMetadataClientDriver) client.getMetadataClientDriver();
            String root = URI.create(metadataServiceUri).getPath();
            String instance =
                    new String(driver.getZk().getData(root + "/INSTANCEID", false, null), StandardCharsets.US_ASCII);
            var gate = new M5BookKeeperNamespaceGateV2(driver.getZk(), root, instance, ZkUtils.getACLs(configuration));
            gate.initializeUnbound().get();
            return new M5BookKeeperNamespaceAuthorityV2(client, gate);
        } catch (Exception failure) {
            client.close();
            throw failure;
        }
    }

    @Override
    public PhysicalResourceIdV2.Namespace namespace() {
        return gate.namespace();
    }

    @Override
    public CompletionStage<Optional<PhysicalNamespaceAuthorityBindingV2>> readBinding() {
        return gate.read().thenApply(M5BookKeeperNamespaceGateV2.Snapshot::binding);
    }

    @Override
    public CompletionStage<PhysicalNamespaceAuthorityBindingV2> bind(MetadataNamespaceIdentityV2 metadataNamespace) {
        return gate.bind(new PhysicalNamespaceAuthorityBindingV2(namespace(), metadataNamespace));
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
