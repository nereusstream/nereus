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

import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.metadata.oxia.v2.OxiaV2StoreConfiguration;
import com.nereusstream.pulsar.offload.PulsarNativeReadCellBudgetV1;
import com.nereusstream.pulsar.offload.PulsarNativeWriterRegistryV2;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.bookkeeper.BookKeeperV3Crc32cAddPayloadLimitV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;

record PulsarM6BookKeeperCellConfig(
        NamespaceName namespace,
        MetadataNamespaceIdentityV2 metadataIdentity,
        OxiaV2StoreConfiguration metadata,
        PulsarProtocolCellIdentity cell,
        BookKeeperCapabilitySnapshotV1 bookKeeper,
        String bookKeeperUri,
        String bookKeeperNamespace,
        long bookKeeperQuota,
        PulsarNativeReadCellBudgetV1.Usage share,
        PulsarNativeReadCellBudgetV1.ReadLimits readLimits,
        M5ClosedWriterRegistryV1 writers,
        StorageProfileV1 profile,
        Optional<PulsarM6AsyncObjectScope.Configuration> objectScope,
        Map<TopicName, Long> generations) {

    static PulsarM6BookKeeperCellConfig read(Path path) throws IOException {
        var raw = new Properties();
        try (var input = Files.newBufferedReader(path)) {
            raw.load(input);
        }
        var namespace = NamespaceName.get(required(raw, "pulsarNamespace"));
        Map<TopicName, Long> topics = new HashMap<>();
        for (var key : raw.stringPropertyNames()) {
            if (key.startsWith("binding.")) {
                var topic = TopicName.get(key.substring("binding.".length()));
                if (!topic.isPersistent()
                        || SystemTopicNames.isSystemTopic(topic)
                        || !namespace.equals(topic.getNamespaceObject())
                        || topics.putIfAbsent(topic, positive(raw, key)) != null) {
                    throw new IllegalArgumentException("Invalid or duplicate topic in Cell profile");
                }
            }
        }
        if (topics.isEmpty() || topics.size() > 1024) {
            throw new IllegalArgumentException("Cell requires 1 to 1024 explicitly admitted bindings");
        }
        var metadata = MetadataNamespaceIdentityV2.decode(CanonicalBytes.copyOf(hex(raw, "metadataIdentity")));
        var cell = new PulsarProtocolCellIdentity(
                new DeploymentId(id(raw, "deploymentId")),
                new ReservationDomainId(id(raw, "reservationDomainId")),
                new PulsarCellId(id(raw, "pulsarCellId")));
        int clientFrame = integer(raw, "bkClientFrameBytes");
        int serverFrame = integer(raw, "bkServerFrameBytes");
        var capability = new BookKeeperCapabilitySnapshotV1(
                new CellProviderScopeId(digest(raw, "bkProviderScope")),
                required(raw, "bkClientCommit"),
                digest(raw, "bkClientJar"),
                required(raw, "bkServerCommit"),
                digest(raw, "bkServerImage"),
                BookKeeperProtocolModeV1.V3,
                clientFrame,
                serverFrame,
                BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(clientFrame, serverFrame),
                true,
                integer(raw, "bkEnsemble"),
                integer(raw, "bkWriteQuorum"),
                integer(raw, "bkAckQuorum"),
                BookKeeperDigestTypeV1.CRC32C,
                true,
                true,
                new BookKeeperTimeoutClassV1(
                        integer(raw, "bkAddTimeoutMs"),
                        integer(raw, "bkReadTimeoutMs"),
                        integer(raw, "bkRequestTimeoutMs"),
                        integer(raw, "bkConnectTimeoutMs")),
                "bk-k0-no-auth:v1",
                digest(raw, "bkConfigurationDigest"));
        Map<Class<?>, Sha256Digest> source = new HashMap<>();
        for (var writer : PulsarNativeWriterRegistryV2.implementationOwners()) {
            source.put(writer, digest(raw, "writer." + writer.getName()));
        }
        var writers = PulsarNativeWriterRegistryV2.create(
                new CapabilityBinding(positive(raw, "writerCapabilityVersion"), digest(raw, "writerCapabilityDigest")),
                source);
        var profile = StorageProfileV1.valueOf(raw.getProperty("storageProfile", "BOOKKEEPER_WAL_ONLY"));
        var objectScope =
                switch (profile) {
                    case BOOKKEEPER_WAL_ONLY -> Optional.<PulsarM6AsyncObjectScope.Configuration>empty();
                    case BOOKKEEPER_WAL_ASYNC_OBJECT ->
                        Optional.of(PulsarM6AsyncObjectScope.Configuration.read(
                                key -> required(raw, key), raw.getProperty("bookKeeperRetention", "RETAIN_BK")));
                    default ->
                        throw new IllegalArgumentException(
                                "Native BookKeeper Cell profile is not admitted: " + profile);
                };
        var share = new PulsarNativeReadCellBudgetV1.Usage(
                positive(raw, "shareOwners"), positive(raw, "shareReadSlots"),
                positive(raw, "shareEncodedBytes"), positive(raw, "shareDecodedBytes"));
        var readLimits = new PulsarNativeReadCellBudgetV1.ReadLimits(
                integer(raw, "readMaximumEntries"),
                positive(raw, "readMaximumEntryBytes"),
                positive(raw, "readMaximumBatchBytes"));
        if (objectScope
                .flatMap(PulsarM6AsyncObjectScope.Configuration::bookKeeperDelete)
                .isPresent()) {
            // One factory, metadata and M4 writers, source preparation, read owner and replacement workspace.
            long frames = Math.multiplyExact((long) readLimits.maximumEntries(), capability.clientFrameLimitBytes());
            long minimumEncoded = Math.multiplyExact(share.readSlots(), frames);
            long minimumDecoded = Math.multiplyExact(
                    share.readSlots(),
                    Math.addExact(
                            Math.multiplyExact(4L, frames),
                            Math.multiplyExact(2L, readLimits.maximumOwnedBatchBytes())));
            if (share.owners() < 6
                    || share.readSlots() > 65_536
                    || share.encodedBytes() < minimumEncoded
                    || share.decodedBytes() < minimumDecoded) {
                throw new IllegalArgumentException(
                        "Verified Native deletion requires capacity for its original M4 and SDK actors");
            }
        }
        return new PulsarM6BookKeeperCellConfig(
                namespace,
                metadata,
                new OxiaV2StoreConfiguration(
                        required(raw, "oxiaAddress"), required(raw, "oxiaNamespace"), required(raw, "authorityRoot")),
                cell,
                capability,
                required(raw, "bkMetadataUri"),
                required(raw, "bkNamespaceIdentity"),
                positive(raw, "bkQuotaBytes"),
                share,
                readLimits,
                writers,
                profile,
                objectScope,
                Map.copyOf(topics));
    }

    private static String required(Properties raw, String name) {
        var value = raw.getProperty(name);
        if (value == null || value.isBlank() || !value.equals(value.strip())) {
            throw new IllegalArgumentException("Missing or noncanonical Cell setting: " + name);
        }
        return value;
    }

    private static byte[] hex(Properties raw, String name) {
        return HexFormat.of().parseHex(required(raw, name));
    }

    private static Id128 id(Properties raw, String name) {
        return Id128.fromBytes(hex(raw, name));
    }

    private static Sha256Digest digest(Properties raw, String name) {
        return Sha256Digest.copyOf(hex(raw, name));
    }

    private static long positive(Properties raw, String name) {
        long value = Long.parseLong(required(raw, name));
        if (value < 1) {
            throw new IllegalArgumentException("Non-positive Cell setting: " + name);
        }
        return value;
    }

    private static int integer(Properties raw, String name) {
        return Math.toIntExact(positive(raw, name));
    }
}
