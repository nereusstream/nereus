/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatException;
import com.nereusstream.objectstore.compacted.CompactedObjectMetadata;
import com.nereusstream.objectstore.compacted.CompactedObjectVerificationRequest;
import com.nereusstream.objectstore.compacted.CompactedObjectVerifier;
import com.nereusstream.objectstore.compacted.TopicCompactionFormatSpec;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Task-aware production bridge from durable materialization facts to full NCP1/NTC1 verification. */
public final class CompactedMaterializationFormatVerifier implements MaterializationFormatVerifier {
    private final CompactedObjectVerifier verifier;

    public CompactedMaterializationFormatVerifier(CompactedObjectVerifier verifier) {
        this.verifier = Objects.requireNonNull(verifier, "verifier");
    }

    @Override
    public CompletableFuture<Void> verify(
            MaterializationTask task,
            MaterializationOutput output,
            Duration timeout) {
        CompactedObjectVerificationRequest request;
        try {
            Objects.requireNonNull(task, "task");
            Objects.requireNonNull(output, "output");
            if (!(output.readTarget() instanceof ObjectSliceReadTarget target)) {
                throw new CompactedObjectFormatException(
                        "materialization output is not a compacted object-slice target");
            }
            request = new CompactedObjectVerificationRequest(
                    output.streamId(),
                    output.view(),
                    output.coverage(),
                    target,
                    PayloadFormat.valueOf(output.logicalFormat()),
                    output.storageCrc32c(),
                    output.contentSha256(),
                    timeout);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return verifier.verify(request).thenApply(metadata -> {
            requireAgreement(task, output, metadata);
            return null;
        });
    }

    private static void requireAgreement(
            MaterializationTask task,
            MaterializationOutput output,
            CompactedObjectMetadata metadata) {
        Optional<TopicCompactionFormatSpec> topicSpec = task.policy().topicCompaction().map(spec ->
                new TopicCompactionFormatSpec(
                        spec.strategyId(),
                        spec.strategyVersion(),
                        spec.keyCodecId()));
        if (!task.taskId().equals(output.taskId())
                || metadata.view() != task.view()
                || !metadata.streamId().equals(task.streamId())
                || !metadata.sourceCoverage().equals(task.coverage())
                || !metadata.sourceSetSha256().equals(task.sourceSetSha256())
                || !metadata.policySha256().equals(task.policyDigestSha256())
                || !metadata.outputAttemptId().equals(output.outputAttemptId())
                || !metadata.logicalFormat().equals(output.logicalFormat())
                || !metadata.projectionIdentitySha256().map(ignored -> true)
                        .equals(output.projectionRef().map(ignored -> true))
                || metadata.sourceRecordCount() != output.sourceRecordCount()
                || metadata.outputRecordCount() != output.outputRecordCount()
                || metadata.entryCount() != output.entryCount()
                || metadata.logicalBytes() != output.logicalBytes()
                || metadata.cumulativeSizeAtEnd() != output.cumulativeSizeAtEnd()
                || !metadata.compression().equals(task.policy().compression())
                || metadata.targetRowGroupRecords() != task.policy().targetRowGroupRecords()
                || !metadata.topicCompaction().equals(topicSpec)
                || !output.physicalFormat().equals(task.policy().targetPhysicalFormat())) {
            throw new CompactedObjectFormatException(
                    "compacted object metadata does not match task/output publication facts");
        }
    }
}
