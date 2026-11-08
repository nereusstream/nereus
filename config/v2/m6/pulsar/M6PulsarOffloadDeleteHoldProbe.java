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

package com.nereusstream.pulsar.broker;

import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectIdentityKind;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectVersion;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2;
import io.oxia.client.api.OxiaClientBuilder;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public final class M6PulsarOffloadDeleteHoldProbe {
    public static void main(String[] args) throws Exception {
        var cell = PulsarM6BookKeeperCellConfig.read(Path.of(args[0]));
        var objects = cell.objectScope().orElseThrow();
        var metadata = cell.metadata();
        try (var oxia = OxiaClientBuilder.create(metadata.serviceAddress()).namespace(metadata.namespace())
                .asyncClient().get(10, TimeUnit.SECONDS)) {
            var exact = new Oxia09ExactMetadataTransactionStoreV1(oxia);
            var provider = PulsarM6AsyncObjectScope.open(objects, cell.metadataIdentity()).get(10, TimeUnit.SECONDS);
            try {
                var physical = provider.physical(objects);
                var resource = new ObjectVersion(physical.namespace(), CanonicalUtf8.fromString(args[1]),
                        ObjectIdentityKind.IMMUTABLE_VERSION, CanonicalUtf8.fromString(args[2]));
                var namespace = OxiaPhysicalMetadataNamespaceV2.connect(oxia, cell.metadataIdentity())
                        .toCompletableFuture().get(10, TimeUnit.SECONDS);
                var route = namespace.openAuthorityRoute(physical, exact).toCompletableFuture().get(10, TimeUnit.SECONDS);
                if ("held".equals(args[3])) {
                    var facts = route.readResourceFact(resource).toCompletableFuture().get(5, TimeUnit.SECONDS);
                    if (M5TargetDeleteStoredValueV2.decode(facts.authority()).state()
                            != TargetDeleteAuthorityStateV1.DELETE_INTENT_V1) {
                        throw new AssertionError("Original S3 version lost its INTENT during the held response");
                    }
                    System.out.println("PULSAR_M6_ORIGINAL_OBJECT_DELETE_HELD " + resource.sha256().toHex()
                            + " " + facts.reservation().canonicalStoredSha256().toHex());
                } else {
                    var done = route.read(resource.authorityKey()).toCompletableFuture()
                            .get(5, TimeUnit.SECONDS).orElseThrow();
                    var current = physical.readBinding().toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
                    var layout = M5GcQuotaRecordsV2.Layout.withPhysicalMembers(current.authorityRoot(), physical.namespace());
                    var entry = exact.read(layout.nativeKey(M5GcQuotaRecordsV2.entryKey(resource.sha256())))
                            .toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
                    var reservation = M5GcQuotaRecordsV2.Entry.decode(entry.canonicalStoredBytes());
                    layout.verify(reservation);
                    var terminal = M5TargetDeleteStoredValueV2.decode(done);
                    if (!terminal.resource().equals(resource) || terminal.compactDone().isEmpty()
                            || !reservation.resource().equals(resource.sha256()) || !reservation.settled()) {
                        throw new AssertionError("Original S3 version did not reach exact DONE and settlement");
                    }
                    System.out.println("PULSAR_M6_ORIGINAL_OBJECT_DELETE_SETTLED " + resource.sha256().toHex());
                }
            } finally {
                provider.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }
}
