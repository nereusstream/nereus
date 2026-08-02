/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.kafka.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.GenerationId;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.read.GenerationReadConstraint;
import com.nereusstream.materialization.GenerationCommitResult;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import java.lang.reflect.Proxy;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class KafkaActivatedGenerationSetResolverTest {
    private static final String CLUSTER = "cluster";
    private static final String POLICY = "c".repeat(64);
    private static final StreamId STREAM = new StreamId("activated-generation-stream");

    @Test
    void resolvesOnlyTheUniqueGapFreePathNamedByTheBindingDigest() {
        VersionedGenerationIndex first = generation(0, 10, 1, "a", "1", POLICY, 11);
        VersionedGenerationIndex second = generation(10, 20, 2, "b", "2", POLICY, 12);
        VersionedGenerationIndex newerUnactivated = generation(0, 20, 9, "d", "9", POLICY, 19);
        KafkaCompactionGenerationSet activated =
                KafkaCompactionGenerationSet.of(List.of(commit(first), commit(second)));
        KafkaActivatedGenerationSetResolver resolver =
                new KafkaActivatedGenerationSetResolver(CLUSTER, store(List.of(newerUnactivated, second, first)));

        GenerationReadConstraint constraint =
                resolver.resolve(STREAM, coverage(activated, POLICY)).join();
        KafkaCompactionGenerationSet recovered = resolver.resolveGenerationSet(STREAM, coverage(activated, POLICY))
                .join();

        assertThat(recovered).isEqualTo(activated);
        assertThat(constraint.coverage()).isEqualTo(new OffsetRange(0, 20));
        assertThat(constraint.identities())
                .extracting(GenerationReadConstraint.Identity::generation)
                .containsExactly(1L, 2L);
        assertThat(constraint.identities())
                .extracting(GenerationReadConstraint.Identity::indexKey)
                .containsExactly(first.key(), second.key());
    }

    @Test
    void failsClosedWhenNoSamePolicyPathMatchesTheActivatedDigest() {
        VersionedGenerationIndex first = generation(0, 10, 1, "a", "1", POLICY, 11);
        VersionedGenerationIndex second = generation(10, 20, 2, "b", "2", POLICY, 12);
        KafkaCompactionGenerationSet activated =
                KafkaCompactionGenerationSet.of(List.of(commit(first), commit(second)));
        VersionedGenerationIndex wrongPolicy = generation(10, 20, 2, "b", "2", "d".repeat(64), 12);
        KafkaActivatedGenerationSetResolver resolver =
                new KafkaActivatedGenerationSetResolver(CLUSTER, store(List.of(first, wrongPolicy)));

        assertThatThrownBy(() ->
                        resolver.resolve(STREAM, coverage(activated, POLICY)).join())
                .hasRootCauseInstanceOf(NereusException.class)
                .rootCause()
                .hasMessageContaining("no exact committed NTC2 path");
    }

    @Test
    void generationSetFactoryRejectsAPathGapBeforeHashing() {
        VersionedGenerationIndex first = generation(0, 10, 1, "a", "1", POLICY, 11);
        VersionedGenerationIndex gap = generation(11, 20, 2, "b", "2", POLICY, 12);

        assertThatThrownBy(() -> KafkaCompactionGenerationSet.of(List.of(commit(first), commit(gap))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gap-free");
    }

    private static KafkaCompactionCoverageRecord coverage(KafkaCompactionGenerationSet generations, String policy) {
        return new KafkaCompactionCoverageRecord(
                1,
                generations.coverage().startOffset(),
                generations.coverage().endOffset(),
                7,
                generations.digestBytes(),
                HexFormat.of().parseHex(policy),
                1_000);
    }

    private static GenerationCommitResult commit(VersionedGenerationIndex index) {
        GenerationIndexRecord value = index.value();
        return new GenerationCommitResult(
                STREAM,
                ReadView.TOPIC_COMPACTED,
                new OffsetRange(value.offsetStart(), value.offsetEnd()),
                new GenerationId(value.generation()),
                new PublicationId(value.publicationId()),
                index.key(),
                index.metadataVersion(),
                index.durableValueSha256(),
                false);
    }

    private static VersionedGenerationIndex generation(
            long start,
            long end,
            long generation,
            String publicationSeed,
            String checksumSeed,
            String policy,
            long metadataVersion) {
        Checksum crc32c = new Checksum(ChecksumType.CRC32C, "00000001");
        EntryIndexRef footer = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                100,
                10,
                crc32c);
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                new ObjectId("object-" + generation),
                new ObjectKey("f9/activated/object-" + generation),
                ObjectType.STREAM_COMPACTED_OBJECT,
                CompactedObjectFormatV2.TOPIC_COMPACTED_PHYSICAL_FORMAT,
                CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT,
                "slice-" + generation,
                0,
                110,
                crc32c,
                footer);
        var encoded = ReadTargetCodecRegistry.phase15().encode(target);
        GenerationIndexRecord record = new GenerationIndexRecord(
                1,
                STREAM.value(),
                ReadView.TOPIC_COMPACTED.wireId(),
                start,
                end,
                generation,
                publicationSeed.repeat(26),
                "task-" + generation,
                GenerationLifecycle.COMMITTED,
                "a".repeat(64),
                policy,
                encoded,
                encoded.identityChecksumValue(),
                policy,
                PayloadFormat.KAFKA_RECORD_BATCH.name(),
                Math.toIntExact(end - start),
                1,
                1,
                1,
                0,
                end - start,
                1,
                1,
                List.of(),
                "",
                100,
                110,
                "",
                110,
                metadataVersion);
        return new VersionedGenerationIndex(
                String.format("/generation/%019d/%019d", end, generation),
                record,
                metadataVersion,
                new Checksum(ChecksumType.SHA256, checksumSeed.repeat(64)));
    }

    private static GenerationMetadataStore store(List<VersionedGenerationIndex> values) {
        List<VersionedGenerationCandidate> ordered = values.stream()
                .sorted(Comparator.comparing(VersionedGenerationIndex::key))
                .map(value -> (VersionedGenerationCandidate) value)
                .toList();
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "scanIndex" ->
                        CompletableFuture.completedFuture(new GenerationScanPage(ordered, Optional.empty()));
                    case "close" -> null;
                    case "toString" -> "activated-generation-store";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
