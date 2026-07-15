/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import java.util.List;
import java.util.Objects;

/** Immutable logical materialization plan; workflow lifecycle lives in its durable task root. */
public record MaterializationTask(
        String taskId,
        StreamId streamId,
        ReadView view,
        TaskKind taskKind,
        OffsetRange coverage,
        List<SourceGeneration> sources,
        Checksum sourceSetSha256,
        MaterializationPolicy policy,
        Checksum policyDigestSha256) {
    public MaterializationTask {
        taskId = requireText(taskId, "taskId");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(taskKind, "taskKind");
        Objects.requireNonNull(coverage, "coverage");
        if (coverage.isEmpty()) {
            throw new IllegalArgumentException("task coverage cannot be empty");
        }
        sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
        Objects.requireNonNull(policy, "policy");
        requireSha256(sourceSetSha256, "sourceSetSha256");
        requireSha256(policyDigestSha256, "policyDigestSha256");
        if (sources.isEmpty() || sources.size() > policy.maxSourceRanges()) {
            throw new IllegalArgumentException("task source count is outside policy limits");
        }
        if (view != policy.view() || taskKind != policy.taskKind()
                || coverage.recordCount() > policy.maxRangeRecords()) {
            throw new IllegalArgumentException("task view/kind/coverage does not match policy");
        }
        if (!sources.equals(MaterializationCanonical.canonicalSources(sources))) {
            throw new IllegalArgumentException("task sources are not in canonical order");
        }
        long cursor = coverage.startOffset();
        long previousCommitVersion = 0;
        for (SourceGeneration source : sources) {
            if (source.view() != taskKind.sourceView()
                    || source.range().startOffset() != cursor
                    || source.commitVersion() < previousCommitVersion) {
                throw new IllegalArgumentException(
                        "task sources must use the task-kind source view and be gap-free");
            }
            cursor = source.range().endOffset();
            previousCommitVersion = source.commitVersion();
        }
        if (cursor != coverage.endOffset()) {
            throw new IllegalArgumentException("task source coverage does not match task coverage");
        }
        Checksum expectedSourceSet = MaterializationCanonical.sourceSetDigest(sources);
        Checksum expectedPolicy = policy.digestSha256();
        if (!sourceSetSha256.equals(expectedSourceSet)
                || !policyDigestSha256.equals(expectedPolicy)) {
            throw new IllegalArgumentException("task source/policy digest does not match canonical fields");
        }
        String expectedTaskId = MaterializationCanonical.taskId(
                streamId, view, taskKind, coverage, sourceSetSha256, policyDigestSha256);
        if (!taskId.equals(expectedTaskId)) {
            throw new IllegalArgumentException("taskId does not match canonical task identity");
        }
    }

    public static MaterializationTask create(
            StreamId streamId,
            OffsetRange coverage,
            List<SourceGeneration> sources,
            MaterializationPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        List<SourceGeneration> canonical = MaterializationCanonical.canonicalSources(sources);
        Checksum sourceSet = MaterializationCanonical.sourceSetDigest(canonical);
        Checksum policyDigest = policy.digestSha256();
        String taskId = MaterializationCanonical.taskId(
                streamId,
                policy.view(),
                policy.taskKind(),
                coverage,
                sourceSet,
                policyDigest);
        return new MaterializationTask(
                taskId,
                streamId,
                policy.view(),
                policy.taskKind(),
                coverage,
                canonical,
                sourceSet,
                policy,
                policyDigest);
    }

    public long taskSequence() {
        return sources.get(sources.size() - 1).commitVersion();
    }

    public ReadView sourceView() {
        return taskKind.sourceView();
    }

    private static void requireSha256(Checksum value, String field) {
        Objects.requireNonNull(value, field);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(field + " must be SHA256");
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
