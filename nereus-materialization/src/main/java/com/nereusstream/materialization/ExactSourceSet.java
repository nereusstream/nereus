/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadView;
import java.util.List;
import java.util.Objects;

/** Canonical gap-free source identities for one exact logical range. */
public record ExactSourceSet(
        ReadView view,
        OffsetRange coverage,
        List<SourceGeneration> sources,
        Checksum sourceSetSha256) {
    public ExactSourceSet {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(coverage, "coverage");
        if (coverage.isEmpty()) {
            throw new IllegalArgumentException("exact source-set coverage cannot be empty");
        }
        sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
        Objects.requireNonNull(sourceSetSha256, "sourceSetSha256");
        if (sources.isEmpty()
                || sourceSetSha256.type() != ChecksumType.SHA256
                || !sources.equals(MaterializationCanonical.canonicalSources(sources))) {
            throw new IllegalArgumentException("exact source set is empty or non-canonical");
        }
        long nextOffset = coverage.startOffset();
        long previousCommitVersion = 0;
        long previousCumulativeBytes = sources.get(0).cumulativeSizeAtStart();
        for (SourceGeneration source : sources) {
            Objects.requireNonNull(source, "source");
            if (source.view() != view
                    || source.range().startOffset() != nextOffset
                    || source.commitVersion() < previousCommitVersion
                    || source.cumulativeSizeAtStart() != previousCumulativeBytes) {
                throw new IllegalArgumentException(
                        "exact sources must be same-view, gap-free, and monotonically versioned");
            }
            nextOffset = source.range().endOffset();
            previousCommitVersion = source.commitVersion();
            previousCumulativeBytes = source.cumulativeSizeAtEnd();
        }
        if (nextOffset != coverage.endOffset()
                || !sourceSetSha256.equals(MaterializationCanonical.sourceSetDigest(sources))) {
            throw new IllegalArgumentException(
                    "exact source coverage or canonical source-set digest changed");
        }
    }

    public static ExactSourceSet create(
            ReadView view, OffsetRange coverage, List<SourceGeneration> sources) {
        List<SourceGeneration> canonical =
                MaterializationCanonical.canonicalSources(
                        Objects.requireNonNull(sources, "sources"));
        return new ExactSourceSet(
                view,
                coverage,
                canonical,
                MaterializationCanonical.sourceSetDigest(canonical));
    }
}
