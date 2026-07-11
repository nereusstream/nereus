/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.Objects;

/** Process-local proof that a commit was validated against one append-only head anchor. */
public final class ReachableCommittedAppend {
    private final CommittedAppend committedAppend;
    private final String observedHeadCommitId;
    private final long observedHeadOffsetEnd;
    private final long observedHeadCumulativeSize;
    private final long observedHeadCommitVersion;

    private ReachableCommittedAppend(CommittedAppend append, String headId, long headEnd, long headSize, long headVersion) {
        this.committedAppend = Objects.requireNonNull(append, "append");
        this.observedHeadCommitId = Objects.requireNonNull(headId, "headId");
        this.observedHeadOffsetEnd = headEnd;
        this.observedHeadCumulativeSize = headSize;
        this.observedHeadCommitVersion = headVersion;
        if (headEnd < append.range().endOffset() || headSize < append.cumulativeSize()
                || headVersion < append.commitVersion()) throw new IllegalArgumentException("invalid reachability proof");
    }
    /** Internal adapter factory; callers must supply a head anchor they have validated. */
    public static ReachableCommittedAppend verified(
            CommittedAppend append, String headId, long headEnd, long headSize, long headVersion) {
        return new ReachableCommittedAppend(append, headId, headEnd, headSize, headVersion);
    }
    public CommittedAppend committedAppend() { return committedAppend; }
    public String observedHeadCommitId() { return observedHeadCommitId; }
    public long observedHeadOffsetEnd() { return observedHeadOffsetEnd; }
    public long observedHeadCumulativeSize() { return observedHeadCumulativeSize; }
    public long observedHeadCommitVersion() { return observedHeadCommitVersion; }
}
