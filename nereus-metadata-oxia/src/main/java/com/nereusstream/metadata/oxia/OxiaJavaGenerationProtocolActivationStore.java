/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Production exact-key/CAS adapter for the cluster generation activation authority. */
public final class OxiaJavaGenerationProtocolActivationStore
        implements GenerationProtocolActivationStore {
    private final F4MetadataStoreSupport support;
    private final String activatingBrokerRunId;
    private final List<ReferenceDomainVersionRecord> requiredReferenceDomains;

    public static OxiaJavaGenerationProtocolActivationStore usingSharedRuntime(
            OxiaClientConfiguration clientConfig,
            SharedOxiaClientRuntime runtime,
            Clock clock,
            String activatingBrokerRunId,
            List<ReferenceDomainVersionRecord> requiredReferenceDomains) {
        Objects.requireNonNull(clientConfig, "clientConfig");
        Objects.requireNonNull(runtime, "runtime");
        runtime.requireCompatible(clientConfig);
        return new OxiaJavaGenerationProtocolActivationStore(
                runtime.client(),
                clock,
                activatingBrokerRunId,
                requiredReferenceDomains);
    }

    OxiaJavaGenerationProtocolActivationStore(
            PartitionedOxiaClient client,
            Clock clock,
            String activatingBrokerRunId,
            List<ReferenceDomainVersionRecord> requiredReferenceDomains) {
        this.support = new F4MetadataStoreSupport(client, clock);
        this.activatingBrokerRunId = requireBase32Id(
                activatingBrokerRunId, "activatingBrokerRunId");
        this.requiredReferenceDomains = canonicalDomains(
                requiredReferenceDomains);
    }

    @Override
    public CompletableFuture<Optional<VersionedGenerationProtocolActivation>> get(
            String cluster) {
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.get(
                        keys.generationProtocolActivationKey(),
                        keys.generationProtocolActivationPartitionKey(),
                        GenerationProtocolActivationRecord.class)
                .thenApply(optional -> optional.map(item -> activation(keys, item)));
    }

    @Override
    public CompletableFuture<VersionedGenerationProtocolActivation> getOrCreate(
            String cluster) {
        F4Keyspace keys = new F4Keyspace(cluster);
        return get(cluster).thenCompose(existing -> {
            if (existing.isPresent()) {
                return CompletableFuture.completedFuture(existing.orElseThrow());
            }
            long now = support.now();
            GenerationProtocolActivationRecord bootstrap =
                    new GenerationProtocolActivationRecord(
                            1,
                            GenerationProtocolActivationRecord.PROTOCOL_VERSION,
                            GenerationProtocolActivationLifecycle.PREPARED,
                            false,
                            false,
                            false,
                            0,
                            requiredReferenceDomains,
                            GenerationBackfillProofRecord.incomplete(0),
                            GenerationBackfillProofRecord.incomplete(0),
                            GenerationBackfillProofRecord.incomplete(0),
                            "",
                            activatingBrokerRunId,
                            now,
                            0,
                            now,
                            0);
            CompletableFuture<VersionedGenerationProtocolActivation> create =
                    support.create(
                                    keys.generationProtocolActivationKey(),
                                    keys.generationProtocolActivationPartitionKey(),
                                    bootstrap,
                                    GenerationProtocolActivationRecord.class)
                            .thenApply(item -> activation(keys, item));
            return create.handle((created, failure) -> {
                if (failure == null) {
                    return CompletableFuture.completedFuture(created);
                }
                return get(cluster).thenApply(value -> value.orElseThrow(() ->
                        F4MetadataStoreSupport.invariant(
                                "generation activation disappeared after create conflict",
                                F4MetadataStoreSupport.unwrap(failure))));
            }).thenCompose(java.util.function.Function.identity());
        });
    }

    @Override
    public CompletableFuture<VersionedGenerationProtocolActivation> compareAndSet(
            String cluster,
            GenerationProtocolActivationRecord replacement,
            long expectedVersion) {
        F4Keyspace keys = new F4Keyspace(cluster);
        GenerationProtocolActivationRecord exact = Objects.requireNonNull(
                replacement, "replacement");
        if (exact.metadataVersion() != 0) {
            throw new IllegalArgumentException(
                    "replacement metadataVersion must be zero");
        }
        return get(cluster).thenCompose(optional -> {
            VersionedGenerationProtocolActivation current = optional.orElseThrow(() ->
                    new F4MetadataConditionFailedException(
                            "generation activation is absent"));
            if (current.metadataVersion() != expectedVersion) {
                return CompletableFuture.failedFuture(
                        new F4MetadataConditionFailedException(
                                "generation activation version mismatch"));
            }
            GenerationProtocolActivationTransitions.requireValidReplacement(
                    current.value(), exact);
            CompletableFuture<VersionedGenerationProtocolActivation> cas =
                    support.compareAndSet(
                                    keys.generationProtocolActivationKey(),
                                    keys.generationProtocolActivationPartitionKey(),
                                    exact,
                                    GenerationProtocolActivationRecord.class,
                                    expectedVersion)
                            .thenApply(item -> activation(keys, item));
            return recoverWrite(cas, () -> get(cluster).thenApply(value ->
                    value.orElseThrow(() -> new F4MetadataConditionFailedException(
                            "generation activation disappeared after uncertain CAS"))), exact, expectedVersion);
        });
    }

    @Override
    public void close() {
        support.close();
    }

    private CompletableFuture<VersionedGenerationProtocolActivation> recoverWrite(
            CompletableFuture<VersionedGenerationProtocolActivation> write,
            java.util.function.Supplier<CompletableFuture<VersionedGenerationProtocolActivation>> reload,
            GenerationProtocolActivationRecord desired,
            long expectedVersion) {
        return write.handle((result, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(result);
            }
            Throwable original = F4MetadataStoreSupport.unwrap(failure);
            return reload.get().thenCompose(current -> {
                boolean sameDesired = current.value()
                        .withMetadataVersion(0)
                        .equals(desired);
                if (sameDesired
                        && (expectedVersion < 0
                                || current.metadataVersion() > expectedVersion)) {
                    return CompletableFuture.completedFuture(current);
                }
                return CompletableFuture.failedFuture(original);
            });
        }).thenCompose(java.util.function.Function.identity());
    }

    private static VersionedGenerationProtocolActivation activation(
            F4Keyspace keys,
            F4MetadataStoreSupport.Decoded<GenerationProtocolActivationRecord> item) {
        if (!item.key().equals(keys.generationProtocolActivationKey())) {
            throw F4MetadataStoreSupport.invariant(
                    "generation activation key/value identity mismatch");
        }
        return new VersionedGenerationProtocolActivation(
                item.key(),
                item.value(),
                item.version(),
                item.durableSha256());
    }

    private static List<ReferenceDomainVersionRecord> canonicalDomains(
            List<ReferenceDomainVersionRecord> supplied) {
        List<ReferenceDomainVersionRecord> domains = List.copyOf(
                Objects.requireNonNull(supplied, "requiredReferenceDomains"));
        new GenerationProtocolActivationRecord(
                1,
                1,
                GenerationProtocolActivationLifecycle.PREPARED,
                false,
                false,
                false,
                0,
                domains,
                GenerationBackfillProofRecord.incomplete(0),
                GenerationBackfillProofRecord.incomplete(0),
                GenerationBackfillProofRecord.incomplete(0),
                "",
                "a".repeat(26),
                0,
                0,
                0,
                0);
        return domains;
    }

    private static String requireBase32Id(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.length() < 26
                || value.length() > 128
                || !value.matches("[a-z2-7]+")) {
            throw new IllegalArgumentException(
                    name + " must be lowercase base32 and encode at least 128 bits");
        }
        return value;
    }
}
