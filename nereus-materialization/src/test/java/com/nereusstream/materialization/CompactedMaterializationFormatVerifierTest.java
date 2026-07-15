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
import com.nereusstream.objectstore.compacted.CompactedObjectRow;
import com.nereusstream.objectstore.compacted.CompactedObjectVerificationRequest;
import com.nereusstream.objectstore.compacted.CompactedObjectVerifier;
import com.nereusstream.objectstore.compacted.CompactedObjectWriteRequest;
import com.nereusstream.objectstore.compacted.CompactedObjectWriteResult;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectReader;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectWriter;
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
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompactedMaterializationFormatVerifierTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void verifiesFullObjectAgainstTaskAndRejectsAChangedPolicyIdentity() throws Exception {
        try (GenerationPublicationTestSupport.Context context =
                        GenerationPublicationTestSupport.context();
                StagingFileManager staging = staging();
                LocalFileObjectStore store =
                        new LocalFileObjectStore(temporaryDirectory.resolve("objects"))) {
            MaterializationTask task = context.task();
            MaterializationOutput template = context.output();
            byte[] first = new byte[40];
            byte[] second = new byte[60];
            java.util.Arrays.fill(first, (byte) 1);
            java.util.Arrays.fill(second, (byte) 2);
            CompactedObjectWriteRequest writeRequest = new CompactedObjectWriteRequest(
                    GenerationPublicationTestSupport.CLUSTER,
                    task.view(),
                    task.streamId(),
                    task.coverage(),
                    template.outputAttemptId(),
                    task.sourceSetSha256(),
                    task.policyDigestSha256(),
                    PayloadFormat.valueOf(template.logicalFormat()),
                    template.logicalFormat(),
                    Optional.of(GenerationPublicationTestSupport.PROJECTION_SHA),
                    template.sourceRecordCount(),
                    template.outputRecordCount(),
                    template.entryCount(),
                    template.logicalBytes(),
                    template.schemaRefs(),
                    template.cumulativeSizeAtStart(),
                    template.cumulativeSizeAtEnd(),
                    task.policy().targetRowGroupRecords(),
                    task.policy().compression(),
                    "nereus-materialization-test",
                    Optional.empty());

            try (CompactedObjectWriteResult written = new ParquetCompactedObjectWriter(staging, Runnable::run)
                    .write(writeRequest, publisher(List.of(row(0, first), row(1, second))))
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
                ObjectSliceReadTarget target = target(writeRequest, written);
                MaterializationOutput output = output(task.taskId(), template, target, written);
                CompactedObjectVerifier objectVerifier = new CompactedObjectVerifier(
                        store,
                        new ParquetCompactedObjectReader(store, Runnable::run));
                MaterializationOutputVerifier verifier = new DefaultMaterializationOutputVerifier(
                        store,
                        new CompactedMaterializationFormatVerifier(objectVerifier));

                verifier.verify(task, output, Duration.ofSeconds(10)).join();
                objectVerifier.verifyExact(
                                new CompactedObjectVerificationRequest(
                                        task.streamId(),
                                        task.view(),
                                        task.coverage(),
                                        target,
                                        writeRequest.payloadFormat(),
                                        written.storageCrc32c(),
                                        written.contentSha256(),
                                        Duration.ofSeconds(10)),
                                writeRequest)
                        .join();

                MaterializationPolicy changedPolicy = new MaterializationPolicy(
                        task.policy().policyId(),
                        task.policy().policyVersion() + 1,
                        task.policy().view(),
                        task.policy().taskKind(),
                        task.policy().targetPhysicalFormat(),
                        task.policy().minMergeSourceRanges(),
                        task.policy().maxSourceRanges(),
                        task.policy().maxRangeRecords(),
                        task.policy().targetObjectBytes(),
                        task.policy().targetRowGroupRecords(),
                        "UNCOMPRESSED",
                        task.policy().topicCompaction());
                MaterializationTask changedTask = MaterializationTask.create(
                        task.streamId(), task.coverage(), task.sources(), changedPolicy);
                MaterializationOutput relabelled = output(
                        changedTask.taskId(), template, target, written);
                assertThatThrownBy(() -> verifier.verify(
                                        changedTask,
                                        relabelled,
                                        Duration.ofSeconds(10))
                                .join())
                        .satisfies(failure -> assertThat(findNereus(failure).code())
                                .isEqualTo(ErrorCode.OBJECT_CHECKSUM_MISMATCH));
            }
        }
    }

    private MaterializationOutput output(
            String taskId,
            MaterializationOutput template,
            ObjectSliceReadTarget target,
            CompactedObjectWriteResult written) {
        Checksum targetIdentity = new Checksum(
                ChecksumType.SHA256,
                ReadTargetCodecRegistry.phase15().encode(target).identityChecksumValue());
        return new MaterializationOutput(
                taskId,
                template.streamId(),
                template.view(),
                template.coverage(),
                template.outputAttemptId(),
                written.objectId(),
                written.objectKey(),
                ObjectKeyHash.from(written.objectKey()),
                written.objectLength(),
                written.storageCrc32c(),
                written.contentSha256(),
                "",
                written.physicalFormat(),
                template.logicalFormat(),
                target,
                targetIdentity,
                written.entryIndexRef(),
                template.sourceRecordCount(),
                template.outputRecordCount(),
                template.entryCount(),
                template.logicalBytes(),
                template.schemaRefs(),
                template.cumulativeSizeAtStart(),
                template.cumulativeSizeAtEnd(),
                template.sourceSetSha256(),
                template.projectionRef());
    }

    private static ObjectSliceReadTarget target(
            CompactedObjectWriteRequest request,
            CompactedObjectWriteResult written) {
        return new ObjectSliceReadTarget(
                1,
                written.objectId(),
                written.objectKey(),
                ObjectType.STREAM_COMPACTED_OBJECT,
                written.physicalFormat(),
                request.logicalFormat(),
                "0-2",
                0,
                written.objectLength(),
                written.storageCrc32c(),
                written.entryIndexRef());
    }

    private StagingFileManager staging() throws Exception {
        Path directory = Files.createDirectory(temporaryDirectory.resolve("staging"));
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
        return new StagingFileManager(
                directory,
                32L << 20,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run);
    }

    private static CompactedObjectRow row(long offset, byte[] payload) {
        return new CompactedObjectRow(
                offset,
                ByteBuffer.wrap(payload),
                Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)),
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty());
    }

    private static Flow.Publisher<CompactedObjectRow> publisher(List<CompactedObjectRow> rows) {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private int index;
            private boolean complete;

            @Override
            public void request(long count) {
                if (complete) {
                    return;
                }
                subscriber.onNext(rows.get(index++));
                if (index == rows.size()) {
                    complete = true;
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
                complete = true;
            }
        });
    }

    private static NereusException findNereus(Throwable supplied) {
        Throwable current = supplied;
        while (current != null && !(current instanceof NereusException)) {
            current = current.getCause();
        }
        assertThat(current).isInstanceOf(NereusException.class);
        return (NereusException) current;
    }
}
