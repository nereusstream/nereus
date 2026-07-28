/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatException;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectMetadata;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerificationRequest;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerifier;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Task-aware bridge from the generic lossless publication protocol to strict full-file NCP2
 * verification.
 */
public final class RangedMaterializationFormatVerifier
        implements MaterializationFormatVerifier {
    private final RangedCompactedObjectVerifier verifier;

    public RangedMaterializationFormatVerifier(
            RangedCompactedObjectVerifier verifier) {
        this.verifier = Objects.requireNonNull(verifier, "verifier");
    }

    @Override
    public CompletableFuture<Void> verify(
            MaterializationTask task,
            MaterializationOutput output,
            Duration timeout) {
        try {
            MaterializationTask exactTask =
                    Objects.requireNonNull(task, "task");
            MaterializationOutput exactOutput =
                    Objects.requireNonNull(output, "output");
            if (exactTask.view() != ReadView.COMMITTED
                    || exactTask.taskKind()
                            != TaskKind.LOSSLESS_REWRITE
                    || !exactTask.policy().targetPhysicalFormat()
                            .equals(
                                    MaterializationPolicy
                                            .KAFKA_COMMITTED_FORMAT)
                    || !(exactOutput.readTarget()
                            instanceof ObjectSliceReadTarget target)
                    || exactOutput.payloadFormat()
                            != PayloadFormat.KAFKA_RECORD_BATCH) {
                throw new CompactedObjectFormatException(
                        "ranged materialization verification requires a lossless NCP2 task/output");
            }
            RangedCompactedObjectVerificationRequest request =
                    new RangedCompactedObjectVerificationRequest(
                            exactOutput.streamId(),
                            exactOutput.view(),
                            exactOutput.coverage(),
                            target,
                            exactOutput.payloadFormat(),
                            exactOutput.storageCrc32c(),
                            exactOutput.contentSha256(),
                            timeout);
            return verifier.verify(request).thenAccept(metadata ->
                    requireAgreement(
                            exactTask, exactOutput, metadata));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static void requireAgreement(
            MaterializationTask task,
            MaterializationOutput output,
            RangedCompactedObjectMetadata metadata) {
        if (!task.taskId().equals(output.taskId())
                || metadata.view() != ReadView.COMMITTED
                || output.view() != ReadView.COMMITTED
                || !metadata.streamId().equals(task.streamId())
                || !metadata.sourceCoverage().equals(task.coverage())
                || !metadata.sourceSetSha256()
                        .equals(task.sourceSetSha256())
                || !metadata.policySha256()
                        .equals(task.policyDigestSha256())
                || !metadata.outputAttemptId()
                        .equals(output.outputAttemptId())
                || metadata.payloadFormat()
                        != PayloadFormat.KAFKA_RECORD_BATCH
                || !metadata.logicalFormat().equals(
                        CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT)
                || !metadata.rangeModel().equals(
                        CompactedObjectFormatV2.RANGE_MODEL)
                || metadata.sourceRecordCount()
                        != output.sourceRecordCount()
                || metadata.outputRecordCount()
                        != output.outputRecordCount()
                || metadata.entryCount() != output.entryCount()
                || metadata.logicalBytes() != output.logicalBytes()
                || metadata.cumulativeSizeAtEnd()
                        != output.cumulativeSizeAtEnd()
                || !metadata.compression()
                        .equals(task.policy().compression())
                || metadata.targetRowGroupRecords()
                        != task.policy().targetRowGroupRecords()
                || metadata.topicCompaction().isPresent()
                || !output.physicalFormat().equals(
                        CompactedObjectFormatV2
                                .COMMITTED_PHYSICAL_FORMAT)
                || !output.physicalFormat().equals(
                        task.policy().targetPhysicalFormat())
                || !output.logicalFormat().equals(
                        CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT)) {
            throw new CompactedObjectFormatException(
                    "NCP2 metadata does not match task/output publication facts");
        }
    }
}
