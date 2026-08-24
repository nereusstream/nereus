/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.object.recovery;

import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Monotonic cumulative counters shared by primary, checkpoint-fallback, and retry paths. Working-set bytes count
 * Root-governed canonical regions once; JVM allocation/copy amplification is separate D3 evidence.
 */
public final class CumulativeRecoveryBudget {
    private final RecoveryEnvelopeLimits limits;
    private final LongSupplier nanoTime;
    private final long startedAtNanos;
    private int liveRoots;
    private int predecessorRuns;
    private int listPages;
    private long listedKeys;
    private long listedKeyBytes;
    private int headRequests;
    private int rangeGetRequests;
    private int fullGetRequests;
    private long canonicalBodyBytes;
    private long decodedContexts;
    private long decodedFrames;
    private long decodedCommitSets;
    private long workingMemoryBytes;
    private int currentConcurrency;
    private int retryAttempts;

    public CumulativeRecoveryBudget(RecoveryEnvelopeLimits limits, LongSupplier nanoTime) {
        this.limits = Objects.requireNonNull(limits, "limits");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.startedAtNanos = nanoTime.getAsLong();
    }

    public synchronized void chargeRoot(boolean predecessor, long canonicalBytes) {
        checkWallTime();
        int nextLiveRoots = addInt(liveRoots, 1, limits.maxLiveRoots(), "live roots");
        int nextPredecessorRuns = predecessorRuns;
        if (predecessor) {
            nextPredecessorRuns = addInt(predecessorRuns, 1, limits.maxPredecessorRuns(), "predecessor runs");
        }
        long nextCanonicalBodyBytes =
                addLong(canonicalBodyBytes, canonicalBytes, limits.maxCanonicalBodyBytes(), "canonical body bytes");
        liveRoots = nextLiveRoots;
        predecessorRuns = nextPredecessorRuns;
        canonicalBodyBytes = nextCanonicalBodyBytes;
    }

    /** Charges canonical control metadata without pretending it was a Provider HEAD/GET request. */
    public synchronized void chargeControlMetadata(long canonicalBytes) {
        checkWallTime();
        canonicalBodyBytes = addLong(
                canonicalBodyBytes, canonicalBytes, limits.maxCanonicalBodyBytes(), "canonical control metadata bytes");
    }

    public synchronized void chargeListPage(int keys, long keyBytes) {
        checkWallTime();
        int nextListPages = addInt(listPages, 1, limits.maxListPages(), "LIST pages");
        long nextListedKeys = addLong(listedKeys, keys, limits.maxListedKeys(), "LIST keys");
        long nextListedKeyBytes = addLong(listedKeyBytes, keyBytes, limits.maxListedKeyBytes(), "LIST key bytes");
        listPages = nextListPages;
        listedKeys = nextListedKeys;
        listedKeyBytes = nextListedKeyBytes;
    }

    /**
     * Atomically reserves the caller bounds, clamped to the cumulative remainder, before the first LIST request.
     * Successful callers settle to the observed counts; an abandoned reservation remains fully consumed.
     */
    public synchronized ListReservation reserveList(int maximumPages, long maximumKeys, long maximumCanonicalKeyBytes) {
        if (maximumPages <= 0 || maximumKeys <= 0 || maximumCanonicalKeyBytes <= 0) {
            throw new IllegalArgumentException("LIST reservation bounds must be positive");
        }
        checkWallTime();
        int remainingPages = Math.subtractExact(limits.maxListPages(), listPages);
        long remainingKeys = Math.subtractExact(limits.maxListedKeys(), listedKeys);
        long remainingKeyBytes = Math.subtractExact(limits.maxListedKeyBytes(), listedKeyBytes);
        if (remainingPages <= 0) {
            throw new RecoveryEnvelopeExceededException("LIST pages");
        }
        if (remainingKeys <= 0) {
            throw new RecoveryEnvelopeExceededException("LIST keys");
        }
        if (remainingKeyBytes <= 0) {
            throw new RecoveryEnvelopeExceededException("LIST key bytes");
        }
        int reservedPages = Math.min(maximumPages, remainingPages);
        long reservedKeys = Math.min(maximumKeys, remainingKeys);
        long reservedKeyBytes = Math.min(maximumCanonicalKeyBytes, remainingKeyBytes);
        listPages = Math.addExact(listPages, reservedPages);
        listedKeys = Math.addExact(listedKeys, reservedKeys);
        listedKeyBytes = Math.addExact(listedKeyBytes, reservedKeyBytes);
        return new ListReservation(this, reservedPages, reservedKeys, reservedKeyBytes);
    }

    /** Reserves the complete cumulative LIST remainder; callers cannot replace Root-persisted caps. */
    public synchronized ListReservation reserveRemainingList() {
        checkWallTime();
        int remainingPages = Math.subtractExact(limits.maxListPages(), listPages);
        long remainingKeys = Math.subtractExact(limits.maxListedKeys(), listedKeys);
        long remainingKeyBytes = Math.subtractExact(limits.maxListedKeyBytes(), listedKeyBytes);
        if (remainingPages <= 0 || remainingKeys <= 0 || remainingKeyBytes <= 0) {
            throw new RecoveryEnvelopeExceededException("LIST cumulative remainder");
        }
        return reserveList(remainingPages, remainingKeys, remainingKeyBytes);
    }

    public synchronized void chargeHead() {
        checkWallTime();
        headRequests = addInt(headRequests, 1, limits.maxHeadRequests(), "HEAD requests");
    }

    public synchronized void chargeRangeGet(long expectedBytes) {
        checkWallTime();
        int nextRequests = addInt(rangeGetRequests, 1, limits.maxRangeGetRequests(), "range GET requests");
        long nextCanonicalBodyBytes =
                addLong(canonicalBodyBytes, expectedBytes, limits.maxCanonicalBodyBytes(), "canonical body bytes");
        rangeGetRequests = nextRequests;
        canonicalBodyBytes = nextCanonicalBodyBytes;
    }

    public synchronized void chargeFullGet(long expectedBytes) {
        checkWallTime();
        int nextRequests = addInt(fullGetRequests, 1, limits.maxFullGetRequests(), "full GET requests");
        long nextCanonicalBodyBytes =
                addLong(canonicalBodyBytes, expectedBytes, limits.maxCanonicalBodyBytes(), "canonical body bytes");
        fullGetRequests = nextRequests;
        canonicalBodyBytes = nextCanonicalBodyBytes;
    }

    public synchronized void chargeDecoded(long contexts, long frames, long commitSets) {
        checkWallTime();
        long nextContexts = addLong(decodedContexts, contexts, limits.maxDecodedContexts(), "decoded contexts");
        long nextFrames = addLong(decodedFrames, frames, limits.maxDecodedFrames(), "decoded frames");
        long nextCommitSets =
                addLong(decodedCommitSets, commitSets, limits.maxDecodedCommitSets(), "decoded commit sets");
        decodedContexts = nextContexts;
        decodedFrames = nextFrames;
        decodedCommitSets = nextCommitSets;
    }

    public synchronized void acquireWorkingSet(long bytes) {
        checkWallTime();
        long nextWorkingMemoryBytes =
                addLong(workingMemoryBytes, bytes, limits.maxWorkingMemoryBytes(), "working memory bytes");
        int nextConcurrency = addInt(currentConcurrency, 1, limits.maxConcurrency(), "recovery concurrency");
        workingMemoryBytes = nextWorkingMemoryBytes;
        currentConcurrency = nextConcurrency;
    }

    /** One owner-open composite spool reservation; nested page/LIST/prefix/frame steps must reuse this slot. */
    public synchronized CompositeWorkingSetLease acquireCompositeWorkingSet(long exactBytes) {
        acquireWorkingSet(exactBytes);
        return new CompositeWorkingSetLease(this, exactBytes);
    }

    public synchronized void releaseWorkingSet(long bytes) {
        if (bytes < 0 || bytes > workingMemoryBytes || currentConcurrency <= 0) {
            throw new IllegalArgumentException("working-set release does not match acquired recovery work");
        }
        workingMemoryBytes = Math.subtractExact(workingMemoryBytes, bytes);
        currentConcurrency = Math.decrementExact(currentConcurrency);
        checkWallTime();
    }

    /** Extends an already acquired spool without inventing a second concurrent recovery operation. */
    public synchronized void growWorkingSet(long bytes) {
        if (bytes <= 0 || currentConcurrency <= 0) {
            throw new IllegalArgumentException("working-set growth requires one live positive lease");
        }
        checkWallTime();
        workingMemoryBytes = addLong(workingMemoryBytes, bytes, limits.maxWorkingMemoryBytes(), "working memory bytes");
    }

    /** Releases bytes from one still-live spool without releasing its sole concurrency slot. */
    public synchronized void shrinkWorkingSet(long bytes) {
        if (bytes <= 0 || bytes > workingMemoryBytes || currentConcurrency <= 0) {
            throw new IllegalArgumentException("working-set shrink does not match one live recovery lease");
        }
        workingMemoryBytes = Math.subtractExact(workingMemoryBytes, bytes);
        checkWallTime();
    }

    public static final class CompositeWorkingSetLease implements AutoCloseable {
        private final CumulativeRecoveryBudget owner;
        private long heldBytes;
        private boolean closed;

        private CompositeWorkingSetLease(CumulativeRecoveryBudget owner, long heldBytes) {
            this.owner = owner;
            this.heldBytes = heldBytes;
        }

        public synchronized void grow(long bytes) {
            requireOpen();
            owner.growWorkingSet(bytes);
            heldBytes = Math.addExact(heldBytes, bytes);
        }

        public synchronized void shrink(long bytes) {
            requireOpen();
            if (bytes <= 0 || bytes > heldBytes) {
                throw new IllegalArgumentException("composite working-set shrink is invalid");
            }
            owner.shrinkWorkingSet(bytes);
            heldBytes = Math.subtractExact(heldBytes, bytes);
        }

        @Override
        public synchronized void close() {
            requireOpen();
            owner.releaseWorkingSet(heldBytes);
            closed = true;
        }

        private void requireOpen() {
            if (closed) {
                throw new IllegalStateException("composite working-set lease was already closed");
            }
        }
    }

    public synchronized void chargeRetry() {
        checkWallTime();
        retryAttempts = addInt(retryAttempts, 1, limits.maxRetryAttempts(), "retry attempts");
    }

    /** Fallback is observable but cannot reset, replace, or copy the existing budget. */
    public synchronized Snapshot enterFallback() {
        checkWallTime();
        return snapshot();
    }

    public synchronized Snapshot snapshot() {
        checkWallTime();
        return new Snapshot(
                liveRoots,
                predecessorRuns,
                listPages,
                listedKeys,
                listedKeyBytes,
                headRequests,
                rangeGetRequests,
                fullGetRequests,
                canonicalBodyBytes,
                decodedContexts,
                decodedFrames,
                decodedCommitSets,
                workingMemoryBytes,
                currentConcurrency,
                retryAttempts,
                elapsedNanos());
    }

    private int addInt(int current, int delta, int maximum, String counter) {
        if (delta < 0) {
            throw new IllegalArgumentException("negative recovery counter delta");
        }
        int candidate;
        try {
            candidate = Math.addExact(current, delta);
        } catch (ArithmeticException failure) {
            throw new RecoveryEnvelopeExceededException(counter + " arithmetic overflow");
        }
        if (candidate > maximum) {
            throw new RecoveryEnvelopeExceededException(counter);
        }
        return candidate;
    }

    private long addLong(long current, long delta, long maximum, String counter) {
        if (delta < 0) {
            throw new IllegalArgumentException("negative recovery counter delta");
        }
        long candidate;
        try {
            candidate = Math.addExact(current, delta);
        } catch (ArithmeticException failure) {
            throw new RecoveryEnvelopeExceededException(counter + " arithmetic overflow");
        }
        if (candidate > maximum) {
            throw new RecoveryEnvelopeExceededException(counter);
        }
        return candidate;
    }

    public synchronized void checkWallTime() {
        if (elapsedNanos() > limits.maxWallTimeNanos()) {
            throw new RecoveryEnvelopeExceededException("wall time");
        }
    }

    private synchronized void settleListReservation(
            ListReservation reservation, int actualPages, long actualKeys, long actualCanonicalKeyBytes) {
        if (reservation.settled) {
            throw new IllegalStateException("LIST reservation was settled twice");
        }
        if (actualPages <= 0
                || actualPages > reservation.maximumPages
                || actualKeys < 0
                || actualKeys > reservation.maximumKeys
                || actualCanonicalKeyBytes < 0
                || actualCanonicalKeyBytes > reservation.maximumCanonicalKeyBytes) {
            throw new IllegalArgumentException("LIST result exceeds its pre-call reservation");
        }
        checkWallTime();
        listPages = Math.subtractExact(listPages, reservation.maximumPages - actualPages);
        listedKeys = Math.subtractExact(listedKeys, reservation.maximumKeys - actualKeys);
        listedKeyBytes =
                Math.subtractExact(listedKeyBytes, reservation.maximumCanonicalKeyBytes - actualCanonicalKeyBytes);
        reservation.settled = true;
    }

    private long elapsedNanos() {
        long now = nanoTime.getAsLong();
        if (now < startedAtNanos) {
            throw new RecoveryEnvelopeExceededException("monotonic clock regression");
        }
        try {
            return Math.subtractExact(now, startedAtNanos);
        } catch (ArithmeticException failure) {
            throw new RecoveryEnvelopeExceededException("wall-time arithmetic overflow");
        }
    }

    public static final class ListReservation {
        private final CumulativeRecoveryBudget owner;
        private final int maximumPages;
        private final long maximumKeys;
        private final long maximumCanonicalKeyBytes;
        private boolean settled;

        private ListReservation(
                CumulativeRecoveryBudget owner, int maximumPages, long maximumKeys, long maximumCanonicalKeyBytes) {
            this.owner = owner;
            this.maximumPages = maximumPages;
            this.maximumKeys = maximumKeys;
            this.maximumCanonicalKeyBytes = maximumCanonicalKeyBytes;
        }

        public int maximumPages() {
            return maximumPages;
        }

        public long maximumKeys() {
            return maximumKeys;
        }

        public long maximumCanonicalKeyBytes() {
            return maximumCanonicalKeyBytes;
        }

        public void settle(int actualPages, long actualKeys, long actualCanonicalKeyBytes) {
            owner.settleListReservation(this, actualPages, actualKeys, actualCanonicalKeyBytes);
        }
    }

    public record Snapshot(
            int liveRoots,
            int predecessorRuns,
            int listPages,
            long listedKeys,
            long listedKeyBytes,
            int headRequests,
            int rangeGetRequests,
            int fullGetRequests,
            long canonicalBodyBytes,
            long decodedContexts,
            long decodedFrames,
            long decodedCommitSets,
            long workingMemoryBytes,
            int currentConcurrency,
            int retryAttempts,
            long elapsedNanos) {}
}
