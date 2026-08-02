/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.kafka.compaction;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.GenerationId;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.read.GenerationReadConstraint;
import com.nereusstream.materialization.GenerationCommitResult;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.KafkaCompactionCoverageActivationMode;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Reconstructs the unique gap-free NTC2 generation path named by a binding coverage digest.
 */
public final class KafkaActivatedGenerationSetResolver implements KafkaActivatedGenerationAuthority {
    static final int SCAN_PAGE_SIZE = 512;
    static final int MAX_CANDIDATES = 4_096;
    static final int MAX_SEARCH_NODES = 16_384;
    private static final int MAX_REPAIR_CAS_ATTEMPTS = 8;

    private static final Comparator<GenerationCommitResult> PATH_ORDER = Comparator.comparingLong(
                    (GenerationCommitResult value) -> value.coverage().endOffset())
            .thenComparingLong(value -> value.generation().value())
            .thenComparing(GenerationCommitResult::indexKey);

    private final String cluster;
    private final GenerationMetadataStore generations;
    private final ReadTargetCodecRegistry targetCodecs;
    private final Optional<RepairContext> repair;

    public KafkaActivatedGenerationSetResolver(String cluster, GenerationMetadataStore generations) {
        this(cluster, generations, ReadTargetCodecRegistry.phase15(), Optional.empty());
    }

    KafkaActivatedGenerationSetResolver(
            String cluster, GenerationMetadataStore generations, ReadTargetCodecRegistry targetCodecs) {
        this(cluster, generations, targetCodecs, Optional.empty());
    }

    public KafkaActivatedGenerationSetResolver(
            String cluster,
            GenerationMetadataStore generations,
            KafkaPartitionMetadataStore partitions,
            PhysicalObjectMetadataStore physicalObjects,
            ObjectStore objectStore,
            Clock clock) {
        this(
                cluster,
                generations,
                ReadTargetCodecRegistry.phase15(),
                Optional.of(new RepairContext(partitions, physicalObjects, objectStore, clock)));
    }

    private KafkaActivatedGenerationSetResolver(
            String cluster,
            GenerationMetadataStore generations,
            ReadTargetCodecRegistry targetCodecs,
            Optional<RepairContext> repair) {
        this.cluster = requireText(cluster, "cluster");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.targetCodecs = Objects.requireNonNull(targetCodecs, "targetCodecs");
        this.repair = Objects.requireNonNull(repair, "repair");
    }

    @Override
    public CompletableFuture<GenerationReadConstraint> resolve(
            StreamId streamId, KafkaCompactionCoverageRecord coverage) {
        return resolveGenerationSet(streamId, coverage).thenApply(KafkaActivatedGenerationSetResolver::readConstraint);
    }

    /**
     * Returns the canonical generation set named by one activated coverage root.
     *
     * <p>The read path normally consumes {@link GenerationReadConstraint}; compaction publication
     * recovery additionally needs the exact previous set so it can reproduce EXTEND/REPLACE
     * activation deterministically after a process restart.
     */
    public CompletableFuture<KafkaCompactionGenerationSet> resolveGenerationSet(
            StreamId streamId, KafkaCompactionCoverageRecord coverage) {
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        KafkaCompactionCoverageRecord exactCoverage = Objects.requireNonNull(coverage, "coverage");
        if (exactCoverage.coverageVersion() != 1) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "activated generation discovery requires non-empty Kafka coverage"));
        }
        OffsetRange range = new OffsetRange(exactCoverage.startOffset(), exactCoverage.endOffset());
        return scan(
                        exactStream,
                        Math.addExact(range.startOffset(), 1),
                        range.endOffset(),
                        Optional.empty(),
                        new ArrayList<>())
                .thenApply(wrappers -> select(
                        exactStream,
                        range,
                        sha256(exactCoverage.generationSetSha256()),
                        sha256(exactCoverage.policySha256()),
                        wrappers));
    }

    @Override
    public CompletableFuture<VersionedKafkaPartitionBinding> repairIfQuarantined(
            KafkaPartitionId partition, StreamId streamId, VersionedKafkaPartitionBinding binding, Duration timeout) {
        KafkaPartitionId exactPartition = Objects.requireNonNull(partition, "partition");
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        VersionedKafkaPartitionBinding exactBinding = Objects.requireNonNull(binding, "binding");
        Duration exactTimeout = Objects.requireNonNull(timeout, "timeout");
        if (exactTimeout.isZero() || exactTimeout.isNegative()) {
            throw new IllegalArgumentException("repair timeout must be positive");
        }
        if (!exactBinding.value().identity().equals(exactPartition)
                || !exactBinding.value().streamId().equals(exactStream.value())) {
            return CompletableFuture.failedFuture(
                    invariant("mandatory NTC2 repair received another partition or stream binding"));
        }
        KafkaCompactionCoverageRecord coverage = exactBinding.value().compactionCoverage();
        if (coverage.coverageVersion() == 0) {
            return CompletableFuture.completedFuture(exactBinding);
        }
        OffsetRange range = new OffsetRange(coverage.startOffset(), coverage.endOffset());
        Checksum expectedSet = sha256(coverage.generationSetSha256());
        Checksum expectedPolicy = sha256(coverage.policySha256());
        return scan(
                        exactStream,
                        Math.addExact(range.startOffset(), 1),
                        range.endOffset(),
                        Optional.empty(),
                        new ArrayList<>())
                .thenCompose(wrappers -> {
                    try {
                        select(exactStream, range, expectedSet, expectedPolicy, wrappers);
                        return CompletableFuture.completedFuture(exactBinding);
                    } catch (NereusException unavailable) {
                        if (repair.isEmpty()) {
                            return CompletableFuture.failedFuture(unavailable);
                        }
                        RepairableSelection selected = selectRepairable(
                                exactStream, range, expectedSet, expectedPolicy, wrappers, unavailable);
                        if (selected.quarantined().isEmpty()) {
                            return CompletableFuture.failedFuture(unavailable);
                        }
                        return repairSelection(exactPartition, exactBinding, selected, exactTimeout);
                    }
                });
    }

    private RepairableSelection selectRepairable(
            StreamId streamId,
            OffsetRange coverage,
            Checksum expectedSetSha256,
            Checksum expectedPolicySha256,
            List<VersionedGenerationCandidate> wrappers,
            NereusException originalFailure) {
        Map<Long, List<GenerationCommitResult>> byStart = new HashMap<>();
        Map<String, VersionedGenerationIndex> byKey = new HashMap<>();
        for (VersionedGenerationCandidate candidate : wrappers) {
            if (!(candidate instanceof VersionedGenerationIndex index)) {
                continue;
            }
            Optional<GenerationCommitResult> admitted =
                    admitRepairable(streamId, coverage, expectedPolicySha256, index);
            admitted.ifPresent(value -> {
                byStart.computeIfAbsent(value.coverage().startOffset(), ignored -> new ArrayList<>())
                        .add(value);
                byKey.put(value.indexKey(), index);
            });
        }
        byStart.values().forEach(values -> values.sort(PATH_ORDER));
        Search search = new Search(expectedSetSha256);
        search(streamId, coverage, byStart, coverage.startOffset(), new ArrayList<>(), search);
        if (search.matches.size() != 1) {
            throw originalFailure;
        }
        KafkaCompactionGenerationSet historical = search.matches.get(0);
        List<VersionedGenerationIndex> actual = historical.generations().stream()
                .map(value -> byKey.get(value.indexKey()))
                .toList();
        if (actual.stream().anyMatch(Objects::isNull)) {
            throw invariant("repairable NTC2 path lost an exact generation wrapper");
        }
        return new RepairableSelection(
                historical,
                actual.stream()
                        .filter(value -> value.value().lifecycle() == GenerationLifecycle.QUARANTINED)
                        .toList());
    }

    private Optional<GenerationCommitResult> admitRepairable(
            StreamId streamId, OffsetRange coverage, Checksum expectedPolicySha256, VersionedGenerationIndex index) {
        GenerationIndexRecord record = index.value();
        if ((record.lifecycle() != GenerationLifecycle.COMMITTED
                        && record.lifecycle() != GenerationLifecycle.QUARANTINED)
                || !validCandidate(streamId, coverage, expectedPolicySha256, record)) {
            return Optional.empty();
        }
        Checksum historicalDigest = index.durableValueSha256();
        long historicalVersion = index.metadataVersion();
        if (record.lifecycle() == GenerationLifecycle.QUARANTINED) {
            Optional<PriorIndexIdentity> prior = priorIndexIdentity(record.stateReason());
            if (prior.isEmpty() || record.committedAtMillis() <= 0) {
                return Optional.empty();
            }
            historicalVersion = prior.orElseThrow().metadataVersion();
            historicalDigest = prior.orElseThrow().durableValueSha256();
        }
        return Optional.of(commitResult(streamId, index, historicalVersion, historicalDigest));
    }

    private CompletableFuture<VersionedKafkaPartitionBinding> repairSelection(
            KafkaPartitionId partition,
            VersionedKafkaPartitionBinding binding,
            RepairableSelection selected,
            Duration timeout) {
        CompletableFuture<List<VersionedGenerationIndex>> repaired =
                CompletableFuture.completedFuture(new ArrayList<>());
        for (GenerationCommitResult historical : selected.historical().generations()) {
            VersionedGenerationIndex actual = selected.quarantined().stream()
                    .filter(value -> value.key().equals(historical.indexKey()))
                    .findFirst()
                    .orElse(null);
            repaired = repaired.thenCompose(values -> {
                if (actual == null) {
                    return generations
                            .getCandidateByKey(
                                    cluster,
                                    selected.historical().streamId(),
                                    ReadView.TOPIC_COMPACTED,
                                    historical.indexKey())
                            .thenApply(candidate -> {
                                VersionedGenerationCandidate present = candidate.orElseThrow(() ->
                                        invariant("committed NTC2 path member " + "disappeared during" + " repair"));
                                if (!(present instanceof VersionedGenerationIndex committed)
                                        || committed.value().lifecycle() != GenerationLifecycle.COMMITTED
                                        || committed.metadataVersion() != historical.indexMetadataVersion()
                                        || !committed.durableValueSha256().equals(historical.indexRecordSha256())) {
                                    throw invariant("committed NTC2 path member changed during repair");
                                }
                                values.add(committed);
                                return values;
                            });
                }
                return repairGeneration(actual, timeout).thenApply(committed -> {
                    values.add(committed);
                    return values;
                });
            });
        }
        return repaired.thenCompose(values -> {
            KafkaCompactionGenerationSet desired = KafkaCompactionGenerationSet.of(values.stream()
                    .map(value -> commitResult(
                            selected.historical().streamId(),
                            value,
                            value.metadataVersion(),
                            value.durableValueSha256()))
                    .toList());
            return activateRepairCoverage(partition, binding.value().compactionCoverage(), desired, 0);
        });
    }

    private CompletableFuture<VersionedGenerationIndex> repairGeneration(
            VersionedGenerationIndex quarantined, Duration timeout) {
        ObjectSliceReadTarget target =
                (ObjectSliceReadTarget) targetCodecs.decode(quarantined.value().readTarget());
        return verifyPhysicalRepair(target, timeout)
                .thenCompose(ignored -> reactivateRoot(target, 0))
                .thenCompose(ignored -> reactivateGeneration(quarantined, 0));
    }

    private CompletableFuture<Void> verifyPhysicalRepair(ObjectSliceReadTarget target, Duration timeout) {
        RepairContext context = repair.orElseThrow();
        return context.physicalObjects()
                .getRoot(cluster, ObjectKeyHash.from(target.objectKey()))
                .thenCompose(optional -> {
                    VersionedPhysicalObjectRoot root =
                            optional.orElseThrow(() -> invariant("mandatory NTC2 repair physical root disappeared"));
                    requireRepairRoot(target, root.value());
                    Checksum expected =
                            new Checksum(ChecksumType.CRC32C, root.value().storageChecksumValue());
                    return context.objectStore()
                            .headObject(target.objectKey(), new HeadObjectOptions(timeout))
                            .thenCompose(head -> {
                                if (head.objectLength() != root.value().objectLength()
                                        || !head.checksum().equals(expected)
                                        || (!root.value().etag().isEmpty()
                                                && !head.etag()
                                                        .orElse("")
                                                        .equals(root.value().etag()))) {
                                    throw invariant(
                                            "mandatory NTC2 repaired HEAD differs from quarantined " + "identity");
                                }
                                return context.objectStore()
                                        .readRange(
                                                target.objectKey(),
                                                0,
                                                root.value().objectLength(),
                                                new RangeReadOptions(Optional.of(expected), timeout))
                                        .thenAccept(read -> {
                                            if (!root.value().contentSha256().isEmpty()
                                                    && !sha256(read.payload())
                                                            .value()
                                                            .equals(root.value().contentSha256())) {
                                                throw invariant("mandatory NTC2 repaired content "
                                                        + "SHA-256 differs from"
                                                        + " quarantined identity");
                                            }
                                        });
                            });
                });
    }

    private CompletableFuture<VersionedPhysicalObjectRoot> reactivateRoot(ObjectSliceReadTarget target, int attempt) {
        if (attempt >= MAX_REPAIR_CAS_ATTEMPTS) {
            return CompletableFuture.failedFuture(
                    invariant("mandatory NTC2 physical-root repair exhausted CAS retries"));
        }
        RepairContext context = repair.orElseThrow();
        return context.physicalObjects()
                .getRoot(cluster, ObjectKeyHash.from(target.objectKey()))
                .thenCompose(optional -> {
                    VersionedPhysicalObjectRoot current =
                            optional.orElseThrow(() -> invariant("mandatory NTC2 repair physical root disappeared"));
                    requireRepairRoot(target, current.value());
                    if (current.value().lifecycle() == PhysicalObjectLifecycle.ACTIVE) {
                        return CompletableFuture.completedFuture(current);
                    }
                    PhysicalObjectRootRecord replacement = activeRoot(current.value());
                    return context.physicalObjects()
                            .compareAndSetRoot(cluster, replacement, current.metadataVersion())
                            .exceptionallyCompose(failure -> retryableCondition(failure)
                                    ? reactivateRoot(target, attempt + 1)
                                    : CompletableFuture.failedFuture(unwrap(failure)));
                });
    }

    private CompletableFuture<VersionedGenerationIndex> reactivateGeneration(
            VersionedGenerationIndex expected, int attempt) {
        if (attempt >= MAX_REPAIR_CAS_ATTEMPTS) {
            return CompletableFuture.failedFuture(invariant("mandatory NTC2 generation repair exhausted CAS retries"));
        }
        GenerationIndexRecord record = expected.value();
        return generations
                .getCandidateByKey(cluster, new StreamId(record.streamId()), ReadView.TOPIC_COMPACTED, expected.key())
                .thenCompose(optional -> {
                    VersionedGenerationIndex current = (VersionedGenerationIndex) optional.orElseThrow(
                            () -> invariant("mandatory NTC2 generation disappeared during repair"));
                    if (current.value().lifecycle() == GenerationLifecycle.COMMITTED) {
                        return CompletableFuture.completedFuture(current);
                    }
                    if (current.value().lifecycle() != GenerationLifecycle.QUARANTINED
                            || !samePublicationIdentity(expected.value(), current.value())) {
                        return CompletableFuture.failedFuture(
                                invariant("mandatory NTC2 generation changed during physical repair"));
                    }
                    GenerationIndexRecord replacement = committedRecord(
                            current.value(),
                            0,
                            Math.max(
                                    repair.orElseThrow().clock().millis(),
                                    current.value().stateChangedAtMillis()));
                    return generations
                            .compareAndSetIndex(cluster, replacement, current.metadataVersion())
                            .exceptionallyCompose(failure -> retryableCondition(failure)
                                    ? reactivateGeneration(expected, attempt + 1)
                                    : CompletableFuture.failedFuture(unwrap(failure)));
                });
    }

    private CompletableFuture<VersionedKafkaPartitionBinding> activateRepairCoverage(
            KafkaPartitionId partition,
            KafkaCompactionCoverageRecord expectedCoverage,
            KafkaCompactionGenerationSet desired,
            int attempt) {
        if (attempt >= MAX_REPAIR_CAS_ATTEMPTS) {
            return CompletableFuture.failedFuture(invariant("mandatory NTC2 binding repair exhausted CAS retries"));
        }
        RepairContext context = repair.orElseThrow();
        return context.partitions().get(partition).thenCompose(optional -> {
            VersionedKafkaPartitionBinding current =
                    optional.orElseThrow(() -> invariant("mandatory NTC2 repair binding disappeared"));
            KafkaCompactionCoverageRecord coverage = current.value().compactionCoverage();
            if (coverage.coverageVersion() == 1
                    && coverage.startOffset() == desired.coverage().startOffset()
                    && coverage.endOffset() == desired.coverage().endOffset()
                    && java.util.Arrays.equals(coverage.generationSetSha256(), desired.digestBytes())) {
                return CompletableFuture.completedFuture(current);
            }
            if (!coverage.equals(expectedCoverage)) {
                return CompletableFuture.failedFuture(
                        invariant("mandatory NTC2 binding changed during physical repair"));
            }
            long now = Math.max(context.clock().millis(), current.value().updatedAtMillis());
            return context.partitions()
                    .activateCompactionCoverage(
                            current,
                            KafkaCompactionCoverageActivationMode.REPLACE,
                            current.value().observedStableEndOffset(),
                            desired.coverage().startOffset(),
                            desired.coverage().endOffset(),
                            desired.digestBytes(),
                            coverage.policySha256(),
                            now)
                    .exceptionallyCompose(failure -> retryableCondition(failure)
                            ? activateRepairCoverage(partition, expectedCoverage, desired, attempt + 1)
                            : CompletableFuture.failedFuture(unwrap(failure)));
        });
    }

    private CompletableFuture<List<VersionedGenerationCandidate>> scan(
            StreamId streamId,
            long minimumEnd,
            long maximumEnd,
            Optional<F4ScanToken> continuation,
            List<VersionedGenerationCandidate> accumulated) {
        int remaining = MAX_CANDIDATES - accumulated.size();
        if (remaining <= 0) {
            return limit("Kafka activated generation scan exceeded its candidate bound");
        }
        return generations
                .scanIndex(
                        cluster,
                        streamId,
                        ReadView.TOPIC_COMPACTED,
                        minimumEnd,
                        maximumEnd,
                        continuation,
                        Math.min(SCAN_PAGE_SIZE, remaining))
                .thenCompose(page -> appendPage(streamId, minimumEnd, maximumEnd, accumulated, page));
    }

    private CompletableFuture<List<VersionedGenerationCandidate>> appendPage(
            StreamId streamId,
            long minimumEnd,
            long maximumEnd,
            List<VersionedGenerationCandidate> accumulated,
            GenerationScanPage page) {
        if (accumulated.size() + page.values().size() > MAX_CANDIDATES) {
            return limit("Kafka activated generation scan exceeded its candidate bound");
        }
        accumulated.addAll(page.values());
        if (page.continuation().isEmpty()) {
            return CompletableFuture.completedFuture(List.copyOf(accumulated));
        }
        if (accumulated.size() == MAX_CANDIDATES) {
            return limit("Kafka activated generation scan has additional candidates");
        }
        return scan(streamId, minimumEnd, maximumEnd, page.continuation(), accumulated);
    }

    private KafkaCompactionGenerationSet select(
            StreamId streamId,
            OffsetRange coverage,
            Checksum expectedSetSha256,
            Checksum expectedPolicySha256,
            List<VersionedGenerationCandidate> wrappers) {
        Map<Long, List<GenerationCommitResult>> byStart = new HashMap<>();
        for (VersionedGenerationCandidate candidate : wrappers) {
            if (!(candidate instanceof VersionedGenerationIndex index)) {
                continue;
            }
            Optional<GenerationCommitResult> admitted = admit(streamId, coverage, expectedPolicySha256, index);
            admitted.ifPresent(
                    value -> byStart.computeIfAbsent(value.coverage().startOffset(), ignored -> new ArrayList<>())
                            .add(value));
        }
        byStart.values().forEach(values -> values.sort(PATH_ORDER));
        Search search = new Search(expectedSetSha256);
        search(streamId, coverage, byStart, coverage.startOffset(), new ArrayList<>(), search);
        if (search.matches.isEmpty()) {
            throw invariant("Kafka binding generation-set digest has no exact committed NTC2 path"
                    + " [coverage="
                    + coverage.startOffset()
                    + ".."
                    + coverage.endOffset()
                    + ", expectedSetSha256="
                    + expectedSetSha256.value()
                    + ", expectedPolicySha256="
                    + expectedPolicySha256.value()
                    + ", candidates="
                    + describeCandidates(streamId, coverage, expectedPolicySha256, wrappers)
                    + "]");
        }
        if (search.matches.size() != 1) {
            throw invariant("Kafka binding generation-set digest resolves to multiple committed NTC2 paths");
        }
        return search.matches.get(0);
    }

    private String describeCandidates(
            StreamId streamId,
            OffsetRange coverage,
            Checksum expectedPolicySha256,
            List<VersionedGenerationCandidate> wrappers) {
        List<String> descriptions = new ArrayList<>();
        for (VersionedGenerationCandidate candidate : wrappers) {
            if (descriptions.size() == 8) {
                descriptions.add("...+" + (wrappers.size() - descriptions.size()));
                break;
            }
            if (!(candidate instanceof VersionedGenerationIndex index)) {
                descriptions.add(candidate.getClass().getSimpleName());
                continue;
            }
            GenerationIndexRecord record = index.value();
            descriptions.add("{generation="
                    + record.generation()
                    + ", range="
                    + record.offsetStart()
                    + ".."
                    + record.offsetEnd()
                    + ", lifecycle="
                    + record.lifecycle()
                    + ", stream="
                    + record.streamId().equals(streamId.value())
                    + ", withinCoverage="
                    + (record.offsetStart() >= coverage.startOffset() && record.offsetEnd() <= coverage.endOffset())
                    + ", policy="
                    + record.policySha256().equals(expectedPolicySha256.value())
                    + ", targetIdentity="
                    + record.targetIdentitySha256().equals(record.readTarget().identityChecksumValue())
                    + ", payload="
                    + record.payloadFormat()
                    + "}");
        }
        return descriptions.toString();
    }

    private static GenerationReadConstraint readConstraint(KafkaCompactionGenerationSet selected) {
        List<GenerationReadConstraint.Identity> identities = selected.generations().stream()
                .map(value -> new GenerationReadConstraint.Identity(
                        value.coverage(),
                        value.generation().value(),
                        value.publicationId(),
                        value.indexKey(),
                        value.indexMetadataVersion(),
                        value.indexRecordSha256()))
                .toList();
        return new GenerationReadConstraint(
                selected.streamId(), ReadView.TOPIC_COMPACTED, selected.coverage(), identities);
    }

    private Optional<GenerationCommitResult> admit(
            StreamId streamId, OffsetRange coverage, Checksum expectedPolicySha256, VersionedGenerationIndex index) {
        GenerationIndexRecord record = index.value();
        if (record.lifecycle() != GenerationLifecycle.COMMITTED
                || !validCandidate(streamId, coverage, expectedPolicySha256, record)) {
            return Optional.empty();
        }
        return Optional.of(commitResult(streamId, index, index.metadataVersion(), index.durableValueSha256()));
    }

    private boolean validCandidate(
            StreamId streamId, OffsetRange coverage, Checksum expectedPolicySha256, GenerationIndexRecord record) {
        if (!record.streamId().equals(streamId.value())
                || record.readViewId() != ReadView.TOPIC_COMPACTED.wireId()
                || record.offsetStart() < coverage.startOffset()
                || record.offsetEnd() > coverage.endOffset()
                || !record.policySha256().equals(expectedPolicySha256.value())
                || !record.targetIdentitySha256().equals(record.readTarget().identityChecksumValue())
                || !record.payloadFormat().equals(PayloadFormat.KAFKA_RECORD_BATCH.name())) {
            return false;
        }
        ReadTarget decoded;
        try {
            decoded = targetCodecs.decode(record.readTarget());
        } catch (RuntimeException failure) {
            return false;
        }
        if (!(decoded instanceof ObjectSliceReadTarget target)
                || target.objectType() != ObjectType.STREAM_COMPACTED_OBJECT
                || !target.physicalFormat().equals(CompactedObjectFormatV2.TOPIC_COMPACTED_PHYSICAL_FORMAT)
                || !target.logicalFormat().equals(CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT)) {
            return false;
        }
        return true;
    }

    private static GenerationCommitResult commitResult(
            StreamId streamId, VersionedGenerationIndex index, long metadataVersion, Checksum durableValueSha256) {
        GenerationIndexRecord record = index.value();
        return new GenerationCommitResult(
                streamId,
                ReadView.TOPIC_COMPACTED,
                new OffsetRange(record.offsetStart(), record.offsetEnd()),
                new GenerationId(record.generation()),
                new PublicationId(record.publicationId()),
                index.key(),
                metadataVersion,
                durableValueSha256,
                false);
    }

    private static GenerationIndexRecord committedRecord(
            GenerationIndexRecord current, long metadataVersion, long stateChangedAtMillis) {
        return new GenerationIndexRecord(
                current.schemaVersion(),
                current.streamId(),
                current.readViewId(),
                current.offsetStart(),
                current.offsetEnd(),
                current.generation(),
                current.publicationId(),
                current.taskId(),
                GenerationLifecycle.COMMITTED,
                current.sourceSetSha256(),
                current.policySha256(),
                current.readTarget(),
                current.targetIdentitySha256(),
                current.materializationPolicySha256(),
                current.payloadFormat(),
                current.sourceRecordCount(),
                current.outputRecordCount(),
                current.entryCount(),
                current.logicalBytes(),
                current.cumulativeSizeAtStart(),
                current.cumulativeSizeAtEnd(),
                current.firstCommitVersion(),
                current.lastCommitVersion(),
                current.schemaRefs(),
                current.projectionRef(),
                current.createdAtMillis(),
                current.committedAtMillis(),
                "",
                stateChangedAtMillis,
                metadataVersion);
    }

    private static PhysicalObjectRootRecord activeRoot(PhysicalObjectRootRecord current) {
        return new PhysicalObjectRootRecord(
                current.schemaVersion(),
                current.objectKeyHash(),
                current.objectKey(),
                current.objectId(),
                current.objectKindId(),
                current.objectLength(),
                current.storageChecksumType(),
                current.storageChecksumValue(),
                current.contentSha256(),
                current.etag(),
                PhysicalObjectLifecycle.ACTIVE,
                Math.addExact(current.lifecycleEpoch(), 1),
                current.createdAtMillis(),
                current.orphanNotBeforeMillis(),
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                "",
                "",
                0);
    }

    private static void requireRepairRoot(ObjectSliceReadTarget target, PhysicalObjectRootRecord root) {
        Checksum targetChecksum = target.sliceChecksum();
        if ((root.lifecycle() != PhysicalObjectLifecycle.QUARANTINED
                        && root.lifecycle() != PhysicalObjectLifecycle.ACTIVE)
                || !root.objectKey().equals(target.objectKey().value())
                || !root.objectId().isEmpty()
                        && !root.objectId().equals(target.objectId().value())
                || target.objectOffset() != 0
                || root.objectLength() != target.objectLength()
                || !root.storageChecksumType().equals(targetChecksum.type().name())
                || !root.storageChecksumValue().equals(targetChecksum.value())) {
            throw invariant("mandatory NTC2 repair root differs from the activated target identity");
        }
    }

    private static boolean samePublicationIdentity(GenerationIndexRecord expected, GenerationIndexRecord actual) {
        return expected.streamId().equals(actual.streamId())
                && expected.readViewId() == actual.readViewId()
                && expected.offsetStart() == actual.offsetStart()
                && expected.offsetEnd() == actual.offsetEnd()
                && expected.generation() == actual.generation()
                && expected.publicationId().equals(actual.publicationId())
                && expected.taskId().equals(actual.taskId())
                && expected.readTarget().equals(actual.readTarget())
                && expected.targetIdentitySha256().equals(actual.targetIdentitySha256())
                && expected.policySha256().equals(actual.policySha256());
    }

    private static Optional<PriorIndexIdentity> priorIndexIdentity(String reason) {
        String versionMarker = "|prior-index-version=";
        String digestMarker = "|prior-index-sha256=";
        int versionStart = reason.indexOf(versionMarker);
        int digestStart = reason.indexOf(digestMarker);
        if (versionStart < 0 || digestStart <= versionStart + versionMarker.length()) {
            return Optional.empty();
        }
        try {
            long version = Long.parseLong(reason.substring(versionStart + versionMarker.length(), digestStart));
            Checksum digest = new Checksum(ChecksumType.SHA256, reason.substring(digestStart + digestMarker.length()));
            if (version < 0) {
                return Optional.empty();
            }
            return Optional.of(new PriorIndexIdentity(version, digest));
        } catch (IllegalArgumentException invalid) {
            return Optional.empty();
        }
    }

    private void search(
            StreamId streamId,
            OffsetRange coverage,
            Map<Long, List<GenerationCommitResult>> byStart,
            long cursor,
            ArrayList<GenerationCommitResult> path,
            Search search) {
        if (++search.nodes > MAX_SEARCH_NODES) {
            throw new NereusException(
                    ErrorCode.METADATA_LIMIT_EXCEEDED,
                    false,
                    "Kafka activated generation path search exceeded its bound");
        }
        if (cursor == coverage.endOffset()) {
            KafkaCompactionGenerationSet generationSet = KafkaCompactionGenerationSet.of(path);
            if (generationSet.streamId().equals(streamId)
                    && generationSet.coverage().equals(coverage)
                    && generationSet.digestSha256().equals(search.expectedSha256)) {
                search.matches.add(generationSet);
            }
            return;
        }
        if (path.size() == KafkaCompactionGenerationSet.MAX_GENERATIONS) {
            return;
        }
        for (GenerationCommitResult candidate : byStart.getOrDefault(cursor, List.of())) {
            path.add(candidate);
            search(streamId, coverage, byStart, candidate.coverage().endOffset(), path, search);
            path.remove(path.size() - 1);
        }
    }

    private static Checksum sha256(byte[] value) {
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(value));
    }

    private static Checksum sha256(ByteBuffer value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(value.asReadOnlyBuffer());
            return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static boolean retryableCondition(Throwable failure) {
        return unwrap(failure) instanceof F4MetadataConditionFailedException;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> CompletableFuture<T> limit(String message) {
        return CompletableFuture.failedFuture(new NereusException(ErrorCode.METADATA_LIMIT_EXCEEDED, false, message));
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.length() > 4_096) {
            throw new IllegalArgumentException(name + " must be non-blank and bounded");
        }
        return value;
    }

    private static final class Search {
        private final Checksum expectedSha256;
        private final ArrayList<KafkaCompactionGenerationSet> matches = new ArrayList<>();
        private int nodes;

        private Search(Checksum expectedSha256) {
            this.expectedSha256 = Objects.requireNonNull(expectedSha256, "expectedSha256");
        }
    }

    private record RepairableSelection(
            KafkaCompactionGenerationSet historical, List<VersionedGenerationIndex> quarantined) {
        private RepairableSelection {
            Objects.requireNonNull(historical, "historical");
            quarantined = List.copyOf(Objects.requireNonNull(quarantined, "quarantined"));
        }
    }

    private record PriorIndexIdentity(long metadataVersion, Checksum durableValueSha256) {
        private PriorIndexIdentity {
            Objects.requireNonNull(durableValueSha256, "durableValueSha256");
        }
    }

    private record RepairContext(
            KafkaPartitionMetadataStore partitions,
            PhysicalObjectMetadataStore physicalObjects,
            ObjectStore objectStore,
            Clock clock) {
        private RepairContext {
            Objects.requireNonNull(partitions, "partitions");
            Objects.requireNonNull(physicalObjects, "physicalObjects");
            Objects.requireNonNull(objectStore, "objectStore");
            Objects.requireNonNull(clock, "clock");
        }
    }
}
