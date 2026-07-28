/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedReader;
import com.nereusstream.objectstore.compacted.ParquetRangedCompactedObjectReader;
import com.nereusstream.objectstore.compacted.ParquetRangedCompactedObjectWriter;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectRow;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerifier;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectWriteRequest;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectWriteResult;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RangedMaterializationFormatVerifierTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void verifiesNcp2AgainstExactTaskAndRejectsChangedPolicy()
            throws Exception {
        try (GenerationPublicationTestSupport.Context context =
                        GenerationPublicationTestSupport.context();
                StagingFileManager staging = staging();
                LocalFileObjectStore store =
                        new LocalFileObjectStore(
                                temporaryDirectory.resolve("objects"))) {
            MaterializationTask task = kafkaTask(context.task());
            byte[] payload = new byte[100];
            java.util.Arrays.fill(payload, (byte) 7);
            RangedCompactedObjectWriteRequest request =
                    new RangedCompactedObjectWriteRequest(
                            GenerationPublicationTestSupport.CLUSTER,
                            task.streamId(),
                            task.coverage(),
                            GenerationPublicationTestSupport.CLAIM_ID,
                            task.sourceSetSha256(),
                            task.policyDigestSha256(),
                            PayloadFormat.KAFKA_RECORD_BATCH,
                            CompactedObjectFormatV2
                                    .KAFKA_LOGICAL_FORMAT,
                            2,
                            1,
                            payload.length,
                            payload.length,
                            task.policy()
                                    .targetRowGroupRecords(),
                            task.policy().compression(),
                            "nereus-kafka-test");

            try (RangedCompactedObjectWriteResult written =
                    new ParquetRangedCompactedObjectWriter(
                                    staging, Runnable::run)
                            .write(
                                    request,
                                    publisher(new RangedCompactedObjectRow(
                                            0,
                                            2,
                                            0,
                                            ByteBuffer.wrap(payload),
                                            Crc32cChecksums.intValue(
                                                    Crc32cChecksums
                                                            .checksum(
                                                                    payload)),
                                            OptionalLong.empty())))
                            .join()) {
                store.putObject(
                                written.objectKey(),
                                written.stagingFile(),
                                new PutObjectOptions(
                                        "application/vnd.apache.parquet",
                                        written.storageCrc32c(),
                                        true,
                                        Map.of(),
                                        Duration.ofSeconds(10)))
                        .join();
                MaterializationOutput output =
                        output(task, request, written);
                MaterializationOutputVerifier verifier =
                        new DefaultMaterializationOutputVerifier(
                                store,
                                new RangedMaterializationFormatVerifier(
                                        new RangedCompactedObjectVerifier(
                                                store,
                                                new ParquetRangedCompactedObjectReader(
                                                        store,
                                                        Runnable::run),
                                                new ParquetKafkaTopicCompactedReader(
                                                        store,
                                                        Runnable::run))));

                verifier.verify(
                                task,
                                output,
                                Duration.ofSeconds(10))
                        .join();

                MaterializationPolicy changedPolicy =
                        MaterializationPolicyFactory
                                .kafkaLosslessCommitted(
                                        2,
                                        16,
                                        1_000,
                                        1_000_000,
                                        128,
                                        "UNCOMPRESSED");
                MaterializationTask changedTask =
                        MaterializationTask.create(
                                task.streamId(),
                                task.coverage(),
                                task.sources(),
                                changedPolicy);
                MaterializationOutput relabelled =
                        new MaterializationOutput(
                                changedTask.taskId(),
                                output.streamId(),
                                output.view(),
                                output.coverage(),
                                output.outputAttemptId(),
                                output.objectId(),
                                output.objectKey(),
                                output.objectKeyHash(),
                                output.objectLength(),
                                output.storageCrc32c(),
                                output.contentSha256(),
                                output.etag(),
                                output.physicalFormat(),
                                output.logicalFormat(),
                                output.readTarget(),
                                output.targetIdentitySha256(),
                                output.entryIndexRef(),
                                output.sourceRecordCount(),
                                output.outputRecordCount(),
                                output.entryCount(),
                                output.logicalBytes(),
                                output.schemaRefs(),
                                output.cumulativeSizeAtStart(),
                                output.cumulativeSizeAtEnd(),
                                changedTask.sourceSetSha256(),
                                output.projectionRef());

                assertThatThrownBy(() -> verifier.verify(
                                        changedTask,
                                        relabelled,
                                        Duration.ofSeconds(10))
                                .join())
                        .satisfies(failure ->
                                assertThat(findNereus(failure).code())
                                        .isEqualTo(
                                                ErrorCode
                                                        .OBJECT_CHECKSUM_MISMATCH));
            }
        }
    }

    private static MaterializationTask kafkaTask(
            MaterializationTask template) {
        SourceGeneration source = template.sources().get(0);
        SourceGeneration kafkaSource = new SourceGeneration(
                source.view(),
                source.range(),
                source.generation(),
                source.commitVersion(),
                source.indexKey(),
                source.indexMetadataVersion(),
                source.indexRecordSha256(),
                source.readTarget(),
                source.targetIdentitySha256(),
                source.materializationPolicySha256(),
                PayloadFormat.KAFKA_RECORD_BATCH,
                source.projectionRef(),
                source.recordCount(),
                1,
                source.logicalBytes(),
                List.of(),
                source.cumulativeSizeAtStart(),
                source.cumulativeSizeAtEnd());
        return MaterializationTask.create(
                template.streamId(),
                template.coverage(),
                List.of(kafkaSource),
                MaterializationPolicyFactory.kafkaLosslessCommitted(
                        2,
                        16,
                        1_000,
                        1_000_000,
                        128,
                        "ZSTD"));
    }

    private static MaterializationOutput output(
            MaterializationTask task,
            RangedCompactedObjectWriteRequest request,
            RangedCompactedObjectWriteResult written) {
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                written.objectId(),
                written.objectKey(),
                ObjectType.STREAM_COMPACTED_OBJECT,
                written.physicalFormat(),
                request.logicalFormat(),
                task.taskId(),
                0,
                written.objectLength(),
                written.storageCrc32c(),
                written.entryIndexRef());
        Checksum targetIdentity = new Checksum(
                ChecksumType.SHA256,
                ReadTargetCodecRegistry.phase15()
                        .encode(target)
                        .identityChecksumValue());
        return new MaterializationOutput(
                task.taskId(),
                task.streamId(),
                task.view(),
                task.coverage(),
                request.outputAttemptId(),
                written.objectId(),
                written.objectKey(),
                ObjectKeyHash.from(written.objectKey()),
                written.objectLength(),
                written.storageCrc32c(),
                written.contentSha256(),
                "",
                written.physicalFormat(),
                request.logicalFormat(),
                target,
                targetIdentity,
                written.entryIndexRef(),
                Math.toIntExact(request.sourceRecordCount()),
                Math.toIntExact(written.outputRecordCount()),
                written.outputEntryCount(),
                request.logicalBytes(),
                List.of(),
                0,
                request.cumulativeSizeAtEnd(),
                task.sourceSetSha256(),
                task.sources().get(0).projectionRef());
    }

    private StagingFileManager staging() throws Exception {
        Path directory =
                Files.createDirectory(
                        temporaryDirectory.resolve("staging"));
        Files.setPosixFilePermissions(
                directory,
                PosixFilePermissions.fromString("rwx------"));
        return new StagingFileManager(
                directory,
                32L << 20,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run);
    }

    private static Flow.Publisher<RangedCompactedObjectRow>
            publisher(RangedCompactedObjectRow row) {
        return subscriber -> subscriber.onSubscribe(
                new Flow.Subscription() {
                    private boolean complete;

                    @Override
                    public void request(long count) {
                        if (complete) {
                            return;
                        }
                        complete = true;
                        subscriber.onNext(row);
                        subscriber.onComplete();
                    }

                    @Override
                    public void cancel() {
                        complete = true;
                    }
                });
    }

    private static NereusException findNereus(
            Throwable supplied) {
        Throwable current = supplied;
        while (current != null
                && !(current instanceof NereusException)) {
            current = current.getCause();
        }
        assertThat(current).isInstanceOf(NereusException.class);
        return (NereusException) current;
    }
}
