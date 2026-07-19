/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.GenerationPublicationTestSupport.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.PhysicalReadStats;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.read.GenerationReadResolver;
import com.nereusstream.core.read.MetadataPhysicalObjectIdentityResolver;
import com.nereusstream.core.read.ReadTargetReader;
import com.nereusstream.core.read.ReadTargetReaderKey;
import com.nereusstream.core.read.ReadTargetReaderRegistry;
import com.nereusstream.metadata.oxia.GenerationIndexValidator;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class NormalPathCommittedObjectGenerationReadVerifierTest {
    @Test
    void retirementProofAcquiresNormalPinsAndReadsTheExactGenerationEndToEnd() {
        try (GenerationPublicationTestSupport.Context context = GenerationPublicationTestSupport.context()) {
            context.committer(context.generations(), GenerationPublicationTestSupport.successfulGuard())
                    .publish(context.task(), context.output())
                    .join();
            createCoveringCheckpoint(context);

            RecordingReader reader = new RecordingReader(
                    ReadTargetReaderKey.from(context.output().readTarget()));
            RecordingPins pins = new RecordingPins();
            ReadTargetReaderRegistry readers = new ReadTargetReaderRegistry(List.of(reader));
            GenerationReadResolver resolver = new GenerationReadResolver(
                    CLUSTER,
                    context.l0Store(),
                    context.generations(),
                    GenerationIndexValidator.phase15Targets(),
                    readers,
                    new MetadataPhysicalObjectIdentityResolver(
                            CLUSTER, context.l0Store(), context.physical()),
                    pins,
                    100,
                    GenerationPublicationTestSupport.CLOCK,
                    Runnable::run);
            NormalPathCommittedObjectGenerationReadVerifier liveReads =
                    new NormalPathCommittedObjectGenerationReadVerifier(
                            resolver, readers, 1, 1_024, context.scheduler());
            CommittedObjectGenerationAuthority authority = new CommittedObjectGenerationAuthority(
                    CLUSTER,
                    context.generations(),
                    context.physical(),
                    liveReads,
                    2,
                    Duration.ofSeconds(10),
                    context.scheduler());

            CommittedObjectGenerationProof proof = authority.prove(
                            context.task().streamId(), context.task().coverage(), 1)
                    .join()
                    .orElseThrow();

            assertThat(proof.target()).isEqualTo(context.output().readTarget());
            assertThat(reader.reads).hasValue(2);
            assertThat(pins.acquires).hasValue(2);
            assertThat(pins.releases).hasValue(2);

            reader.readable.set(false);
            assertThatThrownBy(() -> authority.prove(
                            context.task().streamId(), context.task().coverage(), 1).join())
                    .hasRootCauseInstanceOf(com.nereusstream.api.NereusException.class);
            assertThat(pins.acquires).hasValue(3);
            assertThat(pins.releases).hasValue(3);
        }
    }

    private static void createCoveringCheckpoint(GenerationPublicationTestSupport.Context context) {
        var created = context.generations().getOrCreateMaterializationCheckpoint(
                CLUSTER,
                context.task().streamId(),
                context.task().policy().policyId(),
                context.task().policy().policyVersion(),
                context.task().policyDigestSha256()).join();
        MaterializationCheckpointRecord checkpoint = created.value();
        context.generations().compareAndSetMaterializationCheckpoint(
                CLUSTER,
                new MaterializationCheckpointRecord(
                        checkpoint.schemaVersion(),
                        checkpoint.streamId(),
                        checkpoint.policyId(),
                        checkpoint.policyVersion(),
                        checkpoint.policySha256(),
                        2,
                        context.task().taskSequence(),
                        context.task().taskSequence(),
                        context.task().taskId(),
                        2_000,
                        0),
                created.metadataVersion()).join();
    }

    private static final class RecordingReader implements ReadTargetReader {
        private final ReadTargetReaderKey key;
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicBoolean readable = new AtomicBoolean(true);

        private RecordingReader(ReadTargetReaderKey key) {
            this.key = key;
        }

        @Override
        public ReadTargetReaderKey key() {
            return key;
        }

        @Override
        public long reservationBytes(ResolvedRange range) {
            return range.logicalBytes();
        }

        @Override
        public CompletableFuture<PhysicalReadResult> readPhysicalWithStats(
                StreamId streamId,
                long startOffset,
                List<ResolvedRange> ranges,
                com.nereusstream.api.ReadOptions options) {
            reads.incrementAndGet();
            if (!readable.get()) {
                return CompletableFuture.completedFuture(new PhysicalReadResult(List.of(), List.of()));
            }
            ResolvedRange range = ranges.getFirst();
            long end = Math.min(range.offsetRange().endOffset(), startOffset + options.maxRecords());
            var batchRange = new com.nereusstream.api.OffsetRange(startOffset, end);
            byte[] payload = new byte[Math.toIntExact(batchRange.recordCount())];
            var identity = ReadTargetIdentities.sha256(range.readTarget());
            ReadBatch batch = new ReadBatch(
                    batchRange,
                    range.payloadFormat(),
                    payload,
                    range.schemaRefs(),
                    range.projectionRef(),
                    new ReadSourceRef(
                            range.offsetRange(),
                            range.generation(),
                            range.commitVersion(),
                            range.readTarget(),
                            identity));
            return CompletableFuture.completedFuture(new PhysicalReadResult(
                    List.of(batch),
                    List.of(new PhysicalReadStats(
                            identity,
                            range.logicalBytes(),
                            0,
                            payload.length,
                            0,
                            payload.length))));
        }
    }

    private static final class RecordingPins implements ObjectReadPinManager {
        private final AtomicInteger acquires = new AtomicInteger();
        private final AtomicInteger releases = new AtomicInteger();

        @Override
        public CompletableFuture<ObjectReadLease> acquire(
                PhysicalObjectIdentity object,
                long maximumReadDeadlineMillis,
                SelectionRevalidator selectionRevalidator) {
            acquires.incrementAndGet();
            return selectionRevalidator.revalidate().thenApply(ignored -> new ObjectReadLease() {
                private final AtomicBoolean released = new AtomicBoolean();

                @Override
                public PhysicalObjectIdentity object() {
                    return object;
                }

                @Override
                public String leaseId() {
                    return "retirement-read";
                }

                @Override
                public long maximumReadDeadlineMillis() {
                    return maximumReadDeadlineMillis;
                }

                @Override
                public CompletableFuture<Void> release() {
                    if (released.compareAndSet(false, true)) {
                        releases.incrementAndGet();
                    }
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public boolean isReleased() {
                    return released.get();
                }
            });
        }

        @Override
        public void close() {
        }
    }
}
