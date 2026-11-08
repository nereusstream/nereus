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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.pulsar.offload.PulsarNativeBindingV1;
import com.nereusstream.pulsar.offload.PulsarNativeBookKeeperReclamationV2;
import com.nereusstream.pulsar.offload.PulsarNativeBookKeeperReplacementV2;
import com.nereusstream.pulsar.offload.PulsarNativeManagedLedgerFactoryV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1.Caps;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1;
import com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport;
import io.oxia.client.api.OxiaClientBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.MetaStoreImpl;
import org.apache.bookkeeper.mledger.proto.BookKeeperDeleteState;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/** Explicit operator entry for replacing one sealed BK source after the serving Broker has drained the Cell. */
public final class PulsarM6BookKeeperReplacement {
    private PulsarM6BookKeeperReplacement() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "Expected Cell configuration, original Broker configuration, exact topic name, and ledger ID");
        }
        var cell = PulsarM6BookKeeperCellConfig.read(Path.of(args[0]));
        ServiceConfiguration broker = PulsarConfigurationLoader.create(args[1], ServiceConfiguration.class);
        if (!cell.bookKeeperUri().equals(broker.getBookkeeperMetadataStoreUrl())
                || !Path.of(args[0])
                        .toRealPath()
                        .equals(Path.of((String) broker.getProperty("nereusPulsarCellConfig"))
                                .toRealPath())) {
            throw new IllegalArgumentException("Original Broker configuration differs from the admitted Cell");
        }
        run(cell, TopicName.get(args[2]), Long.parseLong(args[3]), broker.getManagedLedgerReadEntryTimeoutSeconds());
    }

    private static void run(PulsarM6BookKeeperCellConfig cell, TopicName topic, long ledgerId, long readTimeout)
            throws Exception {
        var objects = cell.objectScope()
                .orElseThrow(() ->
                        new IllegalArgumentException("Native replacement requires the asynchronous Object profile"));
        var policy = objects.bookKeeperDelete()
                .orElseThrow(() ->
                        new IllegalArgumentException("Native replacement requires explicit DELETE_AFTER_VERIFIED"));
        Long generation = cell.generations().get(topic);
        if (generation == null || ledgerId < 0) {
            throw new IllegalArgumentException("Replacement requires an admitted topic and exact original ledger");
        }
        var actualJar = Path.of(BookKeeper.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
        if (!Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(actualJar)))
                .equals(cell.bookKeeper().clientArtifactSha256())) {
            throw new IllegalArgumentException("Loaded BookKeeper client differs from the admitted Cell artifact");
        }
        var incarnation = new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString(topic.getPersistenceNamingEncoding()),
                PulsarTopicName.fromString(topic.toString()),
                new PulsarBindingGeneration(generation));
        var oxia = cell.metadata();
        String origin = "oxia-nereus://" + oxia.serviceAddress() + "/" + oxia.namespace();
        try (var client = OxiaClientBuilder.create(oxia.serviceAddress())
                        .namespace(oxia.namespace())
                        .asyncClient()
                        .get(30, TimeUnit.SECONDS);
                var metadata = (MetadataStoreExtended) new PulsarP1OxiaMetadataStoreProvider()
                        .create(origin, MetadataStoreConfig.builder().build(), true);
                var bookKeeper =
                        M5BookKeeperNamespaceAuthorityV2.connectExisting(cell.bookKeeperUri(), cell.bookKeeper())) {
            var binding = PulsarNativeBindingV1.read(client, cell.metadataIdentity(), oxia.authorityRoot(), incarnation)
                    .toCompletableFuture()
                    .get();
            if (!binding.authority().aggregate().binding().cellIdentity().equals(cell.cell())
                    || binding.authority().aggregate().initialEpoch().storageProfile() != cell.profile()) {
                throw new IllegalArgumentException("Replacement Binding differs from the admitted Cell");
            }
            binding.requireBookKeeperDeleteAuditEnrollment(cell.writers())
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            var physical = bookKeeper.readBinding().toCompletableFuture().get().orElseThrow();
            if (!physical.metadataNamespace().equals(cell.metadataIdentity())
                    || !physical.physicalNamespace().serviceIdentity().value().equals(cell.bookKeeperNamespace())) {
                throw new IllegalArgumentException("Replacement BookKeeper namespace differs from the admitted Cell");
            }
            policy.requireNativeBudget(bookKeeper).toCompletableFuture().get(30, TimeUnit.SECONDS);
            var key = new OxiaV2AuthorityKeys(oxia.authorityRoot()).aggregateKey(incarnation);
            var aggregate =
                    new M5PulsarAggregateRetirementCoordinatorV1(new Oxia09ExactMetadataTransactionStoreV1(client));
            var limits = new java.util.HashMap<
                    com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity,
                    com.nereusstream.pulsar.offload.PulsarNativeReadCellBudgetV1.Usage>();
            for (var row : cell.generations().entrySet()) {
                var name = row.getKey();
                var actual = new PulsarTopicIncarnationIdentity(
                        PulsarPersistenceName.fromString(name.getPersistenceNamingEncoding()),
                        PulsarTopicName.fromString(name.toString()),
                        new PulsarBindingGeneration(row.getValue()));
                var member = PulsarNativeBindingV1.read(client, cell.metadataIdentity(), oxia.authorityRoot(), actual)
                        .toCompletableFuture()
                        .get();
                if (!member.authority().aggregate().binding().cellIdentity().equals(cell.cell())
                        || member.authority().aggregate().initialEpoch().storageProfile() != cell.profile()) {
                    throw new IllegalArgumentException("Replacement Cell contains a different Binding");
                }
                limits.put(member.bindingIdentity(), cell.share());
            }
            var share = cell.share();
            long size = limits.size();
            var cellLimit = new com.nereusstream.pulsar.offload.PulsarNativeReadCellBudgetV1.Usage(
                    Math.multiplyExact(size, share.owners()), Math.multiplyExact(size, share.readSlots()),
                    Math.multiplyExact(size, share.encodedBytes()), Math.multiplyExact(size, share.decodedBytes()));
            var budget = binding.readCellBudget(cellLimit, Map.copyOf(limits), cell.readLimits())
                    .admit()
                    .get();
            var worker = Executors.newFixedThreadPool(2);
            // The M4 owner lineage crosses three actual SDK factories; its serial loop must outlive each.
            var readEventLoop = Executors.newSingleThreadExecutor();
            try {
                var actualRetention = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(aggregate
                                .read(key)
                                .toCompletableFuture()
                                .get()
                                .orElseThrow()
                                .canonicalStoredBytes())
                        .nativeRetention()
                        .orElseThrow();
                if (actualRetention.installedByFactoryScope().isEmpty()) {
                    throw new IllegalStateException("Original Broker has not installed native retention");
                }
                var replacement = new PulsarNativeBookKeeperReplacementV2(
                        binding,
                        policy.drainAdmission(),
                        budget,
                        cell.writers(),
                        aggregate,
                        key,
                        worker,
                        readEventLoop,
                        Math.toIntExact(share.readSlots()),
                        () -> openScope(
                                cell,
                                metadata,
                                bookKeeper,
                                binding,
                                budget,
                                aggregate,
                                key,
                                worker,
                                actualRetention,
                                readTimeout),
                        ledgerId);
                long cap = S3C1ObjectProviderTransport.EVIDENCED_MAXIMUM_OBJECT_BYTES;
                // The existing planner reserves one full-object bound and one UNKNOWN slot per planned output.
                int admittedOutputs = Math.toIntExact(Math.min(514, share.encodedBytes() / cap));
                var released = replacement
                        .prepare(
                                objects.staging(),
                                cap,
                                new Caps(
                                        1,
                                        share.decodedBytes(),
                                        share.encodedBytes(),
                                        512,
                                        admittedOutputs,
                                        2,
                                        admittedOutputs))
                        .get();
                var attempt = requireNativeMarker(metadata, topic, ledgerId, BookKeeperDeleteState.BK_DELETE_INTENT);
                System.out.println("Native BK replacement confirmed RELEASED batch "
                        + released.release().batch().batchIdSha256().toHex() + " and INTENT for ledger " + ledgerId);
                if (!released.originalScope().bookKeeperGcGrace().equals(Optional.of(policy.grace()))) {
                    throw new IllegalStateException("Original BookKeeper birth has a different admitted grace");
                }
                VersionedValue physicalDone;
                try (var original = M5BookKeeperNamespaceAuthorityV2.observePulsarScope(
                        cell.bookKeeperUri(), cell.bookKeeper(), released.originalScope())) {
                    var objectObserver = PulsarM6AsyncObjectScope.open(objects, cell.metadataIdentity())
                            .get();
                    var gc = new PulsarNativeBookKeeperReclamationV2(
                            binding,
                            binding.m4Control(),
                            released.resource(),
                            released.release(),
                            aggregate,
                            cell.writers(),
                            worker);
                    gc.open(
                                    aggregate
                                            .read(key)
                                            .toCompletableFuture()
                                            .get()
                                            .orElseThrow(),
                                    metadata,
                                    nativeConfig(cell, actualRetention, readTimeout),
                                    original,
                                    objectObserver.physical(objects),
                                    budget)
                            .get();
                    physicalDone = gc.reclaim(policy.grace().durationMillis()).get();
                    gc.closeAsync().get();
                    objectObserver.closeAsync().get();
                }
                replacement.completeNativeDone(physicalDone).get();
                if (!attempt.equals(
                        requireNativeMarker(metadata, topic, ledgerId, BookKeeperDeleteState.BK_DELETE_DONE))) {
                    throw new IllegalStateException("Native BookKeeper DONE changed the original offload UUID");
                }
                budget.closeCell().get();
                System.out.println("Native BK replacement reached exact RELEASED and DONE for ledger " + ledgerId);
            } finally {
                readEventLoop.shutdown();
                worker.shutdown();
            }
        }
    }

    private static java.util.UUID requireNativeMarker(
            MetadataStoreExtended metadata, TopicName topic, long ledgerId, BookKeeperDeleteState expected)
            throws Exception {
        var stored = metadata.get("/managed-ledgers/" + topic.getPersistenceNamingEncoding())
                .get()
                .orElseThrow();
        var source = new MetaStoreImpl(null, null)
                .parseManagedLedgerInfo(stored.getValue()).getLedgerInfosList().stream()
                        .filter(ledger -> ledger.getLedgerId() == ledgerId)
                        .findFirst()
                        .orElseThrow()
                        .getOffloadContext();
        if (!source.hasUidMsb()
                || !source.hasUidLsb()
                || !source.isComplete()
                || !source.isBookkeeperDeleted()
                || source.getBookkeeperDeleteState() != expected) {
            throw new IllegalStateException("Original Native BK deletion marker was not visible: " + expected);
        }
        return new java.util.UUID(source.getUidMsb(), source.getUidLsb());
    }

    private static ManagedLedgerConfig nativeConfig(
            PulsarM6BookKeeperCellConfig cell,
            com.nereusstream.storage.object.retention.M5PulsarRetentionConfigurationV1 retention,
            long readTimeout) {
        long minute = TimeUnit.MINUTES.toMillis(1);
        if (retention.retentionTimeMillis() % minute != 0) {
            throw new IllegalArgumentException("Stored Broker retention is not representable in native minutes");
        }
        return new ManagedLedgerConfig()
                .setEnsembleSize(cell.bookKeeper().ensembleSize())
                .setWriteQuorumSize(cell.bookKeeper().writeQuorumSize())
                .setAckQuorumSize(cell.bookKeeper().ackQuorumSize())
                .setReadEntryTimeoutSeconds(readTimeout)
                .setDigestType(DigestType.CRC32C)
                .setPassword("")
                .setRetentionTime(Math.toIntExact(retention.retentionTimeMillis() / minute), TimeUnit.MINUTES)
                .setRetentionSizeInMB(retention.retentionSizeInMB());
    }

    private static CompletableFuture<PulsarNativeBookKeeperReplacementV2.Scope> openScope(
            PulsarM6BookKeeperCellConfig cell,
            MetadataStoreExtended metadata,
            M5BookKeeperNamespaceAuthorityV2 bk,
            PulsarNativeBindingV1 binding,
            com.nereusstream.pulsar.offload.PulsarNativeReadCellBudgetV1 budget,
            M5PulsarAggregateRetirementCoordinatorV1 aggregate,
            String key,
            ExecutorService worker,
            com.nereusstream.storage.object.retention.M5PulsarRetentionConfigurationV1 retention,
            long readTimeout) {
        var objects = cell.objectScope().orElseThrow();
        return PulsarM6AsyncObjectScope.open(objects, cell.metadataIdentity()).thenCompose(provider -> {
            var config = nativeConfig(cell, retention, readTimeout).setLedgerOffloader(provider.offloader());
            var factoryConfig = new ManagedLedgerFactoryConfig();
            factoryConfig.setMaxCacheSize(0);
            return PulsarNativeManagedLedgerFactoryV2.openEnrolledOffload(
                            binding,
                            metadata,
                            cell.bookKeeperUri(),
                            bk,
                            budget,
                            cell.bookKeeperQuota(),
                            factoryConfig,
                            cell.writers(),
                            aggregate,
                            key,
                            provider.offloader(),
                            objects.physicalNamespace(),
                            S3C1ObjectProviderTransport.EVIDENCED_MAXIMUM_OBJECT_BYTES,
                            worker,
                            binding::revalidate,
                            objects.bookKeeperDelete().map(PulsarM6AsyncObjectScope.BookKeeperDeletePolicy::grace))
                    .thenCompose(factory -> factory.openLedgerAsync(config, () -> binding.revalidate()
                                    .thenApply(unused -> true)
                                    .toCompletableFuture())
                            .thenApply(ledger -> new PulsarNativeBookKeeperReplacementV2.Scope(
                                    factory,
                                    ledger,
                                    provider.offloader(),
                                    objects.physicalNamespace(),
                                    () -> factory.closeAsync().thenCompose(unused -> provider.closeAsync())))
                            .exceptionallyCompose(error -> factory.closeAsync()
                                    .thenCompose(unused -> provider.closeAsync())
                                    .handle((unused, cleanup) -> {
                                        if (cleanup != null) {
                                            error.addSuppressed(cleanup);
                                        }
                                        throw new CompletionException(error);
                                    })));
        });
    }
}
