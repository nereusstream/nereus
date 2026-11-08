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
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.codec.Nta1CodecV1;
import com.nereusstream.domain.protocol.*;
import com.nereusstream.domain.registry.*;
import com.nereusstream.metadata.oxia.v2.*;
import com.nereusstream.metadata.oxia.v2.continuity.*;
import com.nereusstream.metadata.oxia.v2.registry.*;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import com.nereusstream.pulsar.offload.PulsarNativeBindingV1;
import com.nereusstream.pulsar.offload.PulsarNativeWriterRegistryV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport;
import com.nereusstream.storage.object.s3.S3M5PhysicalNamespaceAuthorityV2;
import io.oxia.client.api.OxiaClientBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Provisions a dedicated test Cell on real services before any native writer may start. */
final class M6PulsarObjectWalProvision {
    private static final Duration TIMEOUT = Duration.ofSeconds(25);

    public static void main(String[] args) throws Exception {
        var path = Path.of(args[1]);
        var props = new Properties();
        try (var input = Files.newBufferedReader(path)) {
            props.load(input);
        }
        props.setProperty("oxiaAddress", args[0]);
        for (var owner : PulsarNativeWriterRegistryV2.implementationOwners()) {
            String relative = owner.getName().replace('.', '/') + ".java";
            try (var modules = Files.list(Path.of(args[2]))) {
                Path source = modules.map(module -> module.resolve("src/main/java").resolve(relative))
                        .filter(Files::isRegularFile).findFirst().orElseThrow();
                props.setProperty("writer." + owner.getName(),
                        Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(source))).toHex());
            }
        }
        try (var bk = new org.apache.bookkeeper.client.BookKeeper(
                    new org.apache.bookkeeper.conf.ClientConfiguration()
                            .setMetadataServiceUri(props.getProperty("bkMetadataUri")))) {
            var driver = (org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver) bk.getMetadataClientDriver();
            String root = java.net.URI.create(props.getProperty("bkMetadataUri")).getPath();
            var instance = BookKeeperInstanceIdV1.fromBytes(driver.getZk().getData(root + "/INSTANCEID", false, null));
            props.setProperty("bkNamespaceIdentity", "bookkeeper-instance:" + instance.value());
        }
        try (var oxia = OxiaClientBuilder.create(args[0]).namespace("default").asyncClient().get(25, TimeUnit.SECONDS)) {
            var metadata = await(OxiaPhysicalMetadataNamespaceV2.provision(oxia));
            props.setProperty("metadataIdentity", metadata.identity().encode().toHex());
            store(path, props);
            var base = PulsarM6BookKeeperCellConfig.read(path);
            String servers = props.getProperty("objectWalBookKeeperZkServers");
            String ledgerRoot = props.getProperty("objectWalBookKeeperLedgerRoot");
            String nativeAuth = PulsarM6ObjectWalNamespace.NATIVE_ID_PRINCIPAL + ":" + java.util.UUID.randomUUID();
            var lock = lockFreshBookKeeper(servers, ledgerRoot, nativeAuth);
            props.setProperty("objectWalNamespaceProof", lock.proof().toHex());
            var namespace = LedgerIdCompatibilityNamespaceV1.derive(lock.instance());
            var cell = base.cell();
            var runtimeWitness = digest(String.join("/", props.getProperty("objectWalDomainJarSha256"),
                    props.getProperty("objectWalMetadataSpiJarSha256"),
                    props.getProperty("objectWalMetadataOxiaJarSha256")));
            var evidence = new RegistryAdmissionEvidenceV1(cell.deploymentId(), cell.reservationDomainId(),
                    lock.instance(), namespace, 1, Sha256Digest.copyOf(new byte[32]),
                    digest("fresh-layout/" + lock.proof().toHex()), lock.proof(),
                    digest("actual-noauth-create-and-instance-write/" + lock.proof().toHex()),
                    List.of(new RegistryWriterAdmissionV1(RegistryWriterKindV1.NATIVE_BOOKKEEPER_LEDGER_ID, 1,
                                    1, lock.nativePrincipal(), lock.generation(), lock.proof(),
                                    base.bookKeeper().clientArtifactSha256()),
                            new RegistryWriterAdmissionV1(RegistryWriterKindV1.NEREUS_VIRTUAL_LEDGER_ID, 1,
                            1, digest("native-object-writer/" + cell.cellId().value().toHex()), lock.generation(),
                            lock.proof(), runtimeWitness)), List.of());
            var slice = VirtualLedgerSliceAssignmentV1.create(cell.deploymentId(), cell.reservationDomainId(),
                    cell.cellId(), namespace, VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE,
                    VirtualLedgerSliceLifecycleV1.ACTIVE);
            var exactRegistry = PulsarVirtualLedgerNamespaceRegistryValueV1.fromDomain(new PulsarVirtualLedgerRegistryV1(
                    cell.deploymentId(), cell.reservationDomainId(), lock.instance(), namespace, 1,
                    evidence.reference(), evidence.candidateWriterRows(), List.of(slice)));
            var ready = new CompletableFuture<Void>();
            var scheduler = new RevalidationScheduler() {
                @Override public boolean request(long generation, long epoch) { ready.complete(null); return true; }
                @Override public void close() {}
            };
            RegistryWriterInterlock interlock = new RegistryWriterInterlock() {
                @Override
                public <T> CompletionStage<T> withPermit(RegistryMutationRequestV1 request,
                        Function<RegistryInterlockSnapshotV1, CompletionStage<T>> mutation) {
                    try {
                        if (request.predecessor().isPresent() || !request.candidate().equals(exactRegistry.domainValue())
                                || !PulsarM6ObjectWalNamespace.read(servers, ledgerRoot, TIMEOUT).equals(lock)) {
                            throw new IllegalStateException("Fresh Object-only writer interlock changed");
                        }
                        return mutation.apply(new RegistryInterlockSnapshotV1(evidence, lock.generation(),
                                true, true, true, true, true, lock.proof())).thenApply(result -> {
                            try {
                                if (!PulsarM6ObjectWalNamespace.read(servers, ledgerRoot, TIMEOUT).equals(lock)) {
                                    throw new IllegalStateException("Object-only writer interlock changed during R1 CAS");
                                }
                                return result;
                            } catch (Exception failure) {
                                throw new java.util.concurrent.CompletionException(failure);
                            }
                        });
                    } catch (Exception failure) {
                        return CompletableFuture.failedFuture(failure);
                    }
                }
            };
            try (var capabilities = await(OxiaV2CapabilityStoreFactory.connectR1(base.metadata(), scheduler, interlock))) {
                await(ready);
                if (!capabilities.registryReady()) {
                    throw new IllegalStateException("Fixture did not connect real R1 capability");
                }
                var created = await(capabilities.registryStore().createRegistry(exactRegistry));
                var registry = created.exactSnapshot().orElseThrow();
                if (!registry.value().equals(exactRegistry)) {
                    throw new IllegalStateException("Fixture R1 readback differs from the original Registry");
                }
                proveNativeBookKeeperAllocation(servers, ledgerRoot, nativeAuth);
                props.setProperty("objectWalRegistrySha256", registry.value().canonicalStoredDigest().toHex());
                Files.write(path.resolveSibling("registry-RAE1.bin"), evidence.canonicalBytes().toByteArray());
                Files.write(path.resolveSibling("registry-NVR1.bin"), registry.value().canonicalStoredBytes().toByteArray());
                props.setProperty("storageProfile", "OBJECT_WAL");
                store(path, props);
                var configured = PulsarM6BookKeeperCellConfig.read(path);
                var settings = configured.objectWal().orElseThrow();
                var allocator = new PulsarM6ObjectWalAllocator(oxia, () -> {
                    if (capabilities.continuitySnapshot().state() != StoreContinuityState.READY) {
                        throw new IllegalStateException("Fixture R1 is not ready");
                    }
                }, configured.metadata().authorityRoot(), cell, lock, settings.registry(), configured.bookKeeper().clientArtifactSha256(),
                        settings.selectionBytes(), settings.artifacts());
                var exactSlice = await(allocator.readSlice());
                var initialCell = await(allocator.createInitialCell(exactSlice));
                provisionProvider(metadata, settings.provider(), cell);
                var incarnations = new Properties();
                for (var row : configured.generations().entrySet()) {
                    var topic = row.getKey();
                    var incarnation = new PulsarTopicIncarnationIdentity(
                            PulsarPersistenceName.fromString(topic.getPersistenceNamingEncoding()),
                            PulsarTopicName.fromString(topic.toString()), new PulsarBindingGeneration(row.getValue()));
                    var binding = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
                    var aggregate = new TopicBindingAggregateV1(TopicBindingAggregateV1.SCHEMA_VERSION,
                            new TopicBindingV1(ProtocolKindV1.PULSAR, binding, cell, incarnation),
                            new InitialStorageEpochV1(DeterministicTopicIdsV1.deriveStorageEpochId(binding, 0), 0,
                                    StorageProfileV1.OBJECT_WAL, ProfileOriginV1.TOPIC_EXPLICIT,
                                    new PolicyCatalogDigest(configured.bookKeeper().configurationDigest()),
                                    FrameEncodingPolicyCatalogV1.zstdFastIfSmaller()));
                    var bytes = Nta1CodecV1.encode(aggregate);
                    await(capabilities.pulsarTopicAuthorityCoordinator().activate(
                            new AggregatePublicationCandidate(aggregate, bytes, Sha256Digest.hash(bytes))));
                    var actual = await(PulsarNativeBindingV1.readObjectWal(oxia, metadata.identity(),
                            configured.metadata().authorityRoot(), incarnation));
                    var ledger = PulsarM6ObjectWalAllocator.incarnation(actual);
                    incarnations.setProperty(topic.toString(), ledger.value().toHex());
                    await(allocator.createInitialHead(initialCell, exactSlice, ledger));
                    await(PulsarM6ObjectWalCursorStore.provision(new Oxia09ExactMetadataTransactionStoreV1(oxia),
                            configured.metadata().authorityRoot(), ledger.value()));
                }
                store(path.resolveSibling("object-wal-incarnations.properties"), incarnations);
                System.out.println("PULSAR_OBJECT_WAL_ACTUAL_NAMESPACE_AND_PROVIDER_PROVISIONED");
            }
        }
    }

    private static void provisionProvider(OxiaPhysicalMetadataNamespaceV2 metadata,
            PulsarM6ObjectWalProvider.Configuration config, PulsarProtocolCellIdentity cell) throws Exception {
        try (var s3 = S3Client.builder().endpointOverride(config.endpoint()).forcePathStyle(true)
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create()).region(Region.US_EAST_1).build()) {
            s3.createBucket(request -> request.bucket(config.bucket()));
            s3.putBucketVersioning(request -> request.bucket(config.bucket())
                    .versioningConfiguration(version -> version.status("Enabled")));
            var provider = S3M5PhysicalNamespaceAuthorityV2.provision(s3, config.bucket(), metadata.identity());
            var profile = new M5CellProviderProfileV2(
                    new PhysicalNamespaceAuthorityBindingV2(provider.namespace(), metadata.identity()), cell,
                    "m6-native-pulsar-object", new M5CellProviderProfileV2.References("fixture-isolated",
                            "m6pulsar", "NONE", "pulsar-object-wal", "m6-fixture", "minio-version-match-v1"),
                    S3C1ObjectProviderTransport.ADAPTER_VERSION, config.commitment(),
                    S3C1ObjectProviderTransport.EVIDENCED_MAXIMUM_OBJECT_BYTES,
                    S3C1ObjectProviderTransport.EVIDENCED_MAXIMUM_RANGE_BYTES,
                    S3C1ObjectProviderTransport.DEFAULT_MAXIMUM_LIST_PAGE_KEYS,
                    S3C1ObjectProviderTransport.MAXIMUM_DELETE_VERSION_TOKEN_BYTES);
            await(metadata.openAuthorityRoute(provider).thenCompose(route -> route.initialize(64L * 1024 * 1024)));
            await(metadata.openCellProviderScopes(provider).thenCompose(scopes -> scopes.prepare(profile).admit()));
            Files.write(config.capability(), profile.encode().toByteArray());
        }
    }

    private static PulsarM6ObjectWalNamespace.Locked lockFreshBookKeeper(String servers, String root, String auth)
            throws Exception {
        var config = new ServerConfiguration();
        config.setMetadataServiceUri("zk://" + servers + root);
        if (!BookKeeperAdmin.initNewCluster(config)) {
            throw new IllegalStateException("Fixture may only seal a newly created dedicated namespace");
        }
        var connected = new CompletableFuture<Void>();
        try (var zk = new ZooKeeper(servers, 25_000, event -> {
            if (event.getState() == KeeperState.SyncConnected) {
                connected.complete(null);
            }
        })) {
            await(connected);
            freeze(zk, root, auth);
            var locked = PulsarM6ObjectWalNamespace.read(zk, root);
            try {
                zk.create(root + "/" + PulsarM6ObjectWalNamespace.NATIVE_ID_NODE + "/forbidden-test",
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        org.apache.zookeeper.CreateMode.PERSISTENT);
                throw new IllegalStateException("Real ZooKeeper did not exclude native compatibility writes");
            } catch (KeeperException.NoAuthException expected) {
                // Actual backend rejected create under the protected namespace.
            }
            try {
                zk.setData(root + "/INSTANCEID", locked.instance().bytes().toByteArray(), -1);
                throw new IllegalStateException("Real ZooKeeper did not exclude INSTANCEID mutation");
            } catch (KeeperException.NoAuthException expected) {
                // Actual backend rejected INSTANCEID mutation.
            }
            return locked;
        }
    }

    private static void freeze(ZooKeeper zk, String path, String auth) throws Exception {
        var id = new org.apache.zookeeper.data.Id("digest",
                org.apache.zookeeper.server.auth.DigestAuthenticationProvider.generateDigest(auth));
        var allowed = new java.util.ArrayList<>(ZooDefs.Ids.READ_ACL_UNSAFE);
        allowed.add(new org.apache.zookeeper.data.ACL(ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE, id));
        zk.create(path + "/" + PulsarM6ObjectWalNamespace.NATIVE_ID_NODE, new byte[0], allowed,
                org.apache.zookeeper.CreateMode.PERSISTENT);
        freezeNodes(zk, path);
    }

    private static void freezeNodes(ZooKeeper zk, String path) throws Exception {
        for (String child : zk.getChildren(path, false)) {
            if (!child.equals(PulsarM6ObjectWalNamespace.NATIVE_ID_NODE)) {
                freezeNodes(zk, path + "/" + child);
            }
        }
        zk.setACL(path, ZooDefs.Ids.READ_ACL_UNSAFE, -1);
    }

    private static void proveNativeBookKeeperAllocation(String servers, String root, String auth) throws Exception {
        var connected = new CompletableFuture<Void>();
        try (var zk = new ZooKeeper(servers, 25_000, event -> {
            if (event.getState() == KeeperState.SyncConnected) {
                connected.complete(null);
            }
        })) {
            await(connected);
            zk.addAuthInfo("digest", auth.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            try (var writer = new org.apache.bookkeeper.meta.ZkLedgerIdGenerator(zk, root,
                    PulsarM6ObjectWalNamespace.NATIVE_ID_NODE, ZooDefs.Ids.READ_ACL_UNSAFE)) {
                var id = new CompletableFuture<Long>();
                writer.generateLedgerId((result, ledger) -> {
                    if (result == org.apache.bookkeeper.client.BKException.Code.OK && ledger != null) {
                        id.complete(ledger);
                    } else {
                        id.completeExceptionally(new IllegalStateException("Actual BK ID allocation failed: " + result));
                    }
                });
                long ledger = await(id);
                if (ledger < 0 || ledger >= Integer.MAX_VALUE) {
                    throw new IllegalStateException("Native BookKeeper generated a ledger outside its bounded interval");
                }
            }
            try {
                zk.create(root + "/L" + VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE,
                        new byte[0], ZooDefs.Ids.READ_ACL_UNSAFE, org.apache.zookeeper.CreateMode.PERSISTENT);
                throw new IllegalStateException("Qualified BookKeeper principal could create a reserved ledger");
            } catch (KeeperException.NoAuthException expected) {
                System.out.println("PULSAR_OBJECT_WAL_REAL_BOOKKEEPER_ID_WRITER_EXCLUDED_FROM_RESERVED_RANGE");
            }
        }
    }

    private static void store(Path path, Properties props) throws Exception {
        try (var out = Files.newBufferedWriter(path)) {
            props.store(out, null);
        }
    }

    private static Sha256Digest digest(String input) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(input).bytes());
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(25, TimeUnit.SECONDS);
    }
}
