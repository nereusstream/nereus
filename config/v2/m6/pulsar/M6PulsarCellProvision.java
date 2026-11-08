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

import com.nereusstream.domain.aggregate.*;
import com.nereusstream.domain.bytes.*;
import com.nereusstream.domain.codec.Nta1CodecV1;
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.protocol.*;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import com.nereusstream.pulsar.offload.PulsarNativeBindingV1;
import com.nereusstream.pulsar.offload.PulsarNativeWriterRegistryV2;
import io.oxia.client.api.OxiaClientBuilder;
import java.nio.file.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

class M6PulsarCellProvision {
    public static void main(String[] args) throws Exception {
        var path = Path.of(args[1]);
        var props = new Properties();
        try (var r = Files.newBufferedReader(path)) {
            props.load(r);
        }
        props.setProperty("oxiaAddress", args[0]);
        boolean asyncObject = args.length == 6;
        if (asyncObject) {
            props.setProperty("storageProfile", "BOOKKEEPER_WAL_ASYNC_OBJECT");
            props.setProperty("s3Endpoint", args[5]);
            props.setProperty("s3Region", "us-east-1");
            props.setProperty("s3Bucket", "m6-native-pulsar-offload");
            props.setProperty("s3KeyPrefix", "m6-native-pulsar");
            props.setProperty("offloadBlockClass", "latency-1mib");
            props.setProperty("offloadStagingDirectory", path.resolveSibling("offload-staging").toString());
            props.setProperty("s3EncryptionScope", "NONE");
            props.setProperty("s3SecurityScope", "m6-pulsar-test");
            props.setProperty("s3CredentialScope", "m6-pulsar-fixture:minio");
            props.setProperty("s3QuotaScope", "m6-pulsar-native-birth");
            props.setProperty("s3OperatorOwner", "m6-fixture");
            props.setProperty("s3DeleteCapability", "minio-version-match-v1");
            props.setProperty("vaultEndpoint", args[4]);
            props.setProperty("vaultMount", "transit");
            props.setProperty("vaultKey", "nereus-kafka-wal");
            props.setProperty("vaultTokenEnvironment", "NEREUS_M6_PULSAR_TEST_VAULT_TOKEN");
        }
        try (var nativeBookKeeper = new org.apache.bookkeeper.client.BookKeeper(
                new org.apache.bookkeeper.conf.ClientConfiguration().setMetadataServiceUri(props.getProperty("bkMetadataUri")))) {
            var driver = (org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver) nativeBookKeeper.getMetadataClientDriver();
            var root = java.net.URI.create(props.getProperty("bkMetadataUri")).getPath();
            var instance = new String(driver.getZk().getData(root + "/INSTANCEID", false, null),
                    java.nio.charset.StandardCharsets.US_ASCII);
            props.setProperty("bkNamespaceIdentity", "bookkeeper-instance:" + instance);
        }
        for (var writer : PulsarNativeWriterRegistryV2.implementationOwners()) {
            var relative = writer.getName().replace('.', '/') + ".java";
            try (var modules = Files.list(Path.of(args[2]))) {
                var exact = modules.map(module -> module.resolve("src/main/java").resolve(relative))
                        .filter(Files::isRegularFile).findFirst().orElseThrow();
                props.setProperty("writer." + writer.getName(),
                        Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(exact))).toHex());
            }
        }
        try (var oxia = OxiaClientBuilder.create(args[0]).namespace("default").asyncClient().get(15, TimeUnit.SECONDS)) {
            var ns = OxiaPhysicalMetadataNamespaceV2.provision(oxia).toCompletableFuture().get(15, TimeUnit.SECONDS);
            props.setProperty("metadataIdentity", ns.identity().encode().toHex());
            if (asyncObject) {
                String bucket = props.getProperty("s3Bucket");
                try (var s3 = software.amazon.awssdk.services.s3.S3Client.builder()
                        .credentialsProvider(software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider
                                .create())
                        .endpointOverride(java.net.URI.create(args[3])).forcePathStyle(true)
                        .region(software.amazon.awssdk.regions.Region.US_EAST_1).build()) {
                    s3.createBucket(r -> r.bucket(bucket));
                    s3.putBucketVersioning(r -> r.bucket(bucket).versioningConfiguration(v -> v.status("Enabled")));
                    var object = com.nereusstream.storage.object.s3.S3M5PhysicalNamespaceAuthorityV2.provision(
                            s3, bucket, ns.identity());
                    props.setProperty("s3ServiceIdentity", object.namespace().serviceIdentity().value());
                    props.setProperty("s3ContainerIdentity", object.namespace().containerIdentity().value());
                    ns.openAuthorityRoute(object,
                            new com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1(oxia))
                            .thenCompose(route -> route.initialize(64L * 1024 * 1024))
                            .toCompletableFuture().get(20, TimeUnit.SECONDS);
                }
            }
            try (var writer = Files.newBufferedWriter(path)) {
                props.store(writer, null);
            }
            var config = PulsarM6BookKeeperCellConfig.read(path);
            try (var bk = com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2.connect(
                    config.bookKeeperUri(), config.bookKeeper())) {
                bk.bind(ns.identity()).toCompletableFuture().get(15, TimeUnit.SECONDS);
            }
            for (var row : config.generations().entrySet()) {
                var topic = row.getKey();
                var incarnation = new PulsarTopicIncarnationIdentity(
                        PulsarPersistenceName.fromString(topic.getPersistenceNamingEncoding()),
                        PulsarTopicName.fromString(topic.toString()), new PulsarBindingGeneration(row.getValue()));
                var cell = config.cell();
                var bindingId = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
                var aggregate = new TopicBindingAggregateV1(TopicBindingAggregateV1.SCHEMA_VERSION,
                        new TopicBindingV1(ProtocolKindV1.PULSAR, bindingId, cell, incarnation),
                        new InitialStorageEpochV1(DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 0), 0,
                                config.profile(), ProfileOriginV1.TOPIC_EXPLICIT,
                                new PolicyCatalogDigest(config.bookKeeper().configurationDigest()),
                                FrameEncodingPolicyValueV1.none()));
                var bytes = Nta1CodecV1.encode(aggregate);
                var actual = PulsarNativeBindingV1.createNativeOnly(oxia, ns.identity(),
                        config.metadata().authorityRoot(),
                        new AggregatePublicationCandidate(aggregate, bytes, Sha256Digest.hash(bytes)), config.writers())
                        .toCompletableFuture().get(30, TimeUnit.SECONDS);
                if (asyncObject) {
                    var observer = actual.readCellBudget(config.share(),
                            java.util.Map.of(actual.bindingIdentity(), config.share()), config.readLimits());
                    props.setProperty("probeCellOwnerKey", observer.ownerKey());
                }
                System.out.println("BINDING " + actual.bindingIdentity());
            }
            if (asyncObject) {
                try (var writer = Files.newBufferedWriter(path)) {
                    props.store(writer, null);
                }
            }
        }
    }
}
