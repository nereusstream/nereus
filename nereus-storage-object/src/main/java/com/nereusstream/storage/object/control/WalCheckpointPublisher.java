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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Bounded single-candidate physical checkpoint combiner. It is asynchronous and outside append ACK. */
public final class WalCheckpointPublisher {
    private static final long MAX_CONTROL_METADATA_BYTES = 1024L * 1024;
    private static final Comparator<ProviderResolvedExtentDescriptor> ORDER = Comparator.comparingInt(
                    (ProviderResolvedExtentDescriptor value) ->
                            value.row().laneId().code())
            .thenComparingLong(value -> value.row().laneSequence());

    private final CanonicalControlMetadataStore metadata;
    private final String headKey;
    private final String pageKeyPrefix;
    private final WalRunRootRecord root;
    private final Sha256Digest rootSha256;
    private final WalRunObjectSession objectSession;
    private final ArrayList<ProviderResolvedExtentDescriptor> queue = new ArrayList<>();
    private WalCheckpointHeadV1 head;
    private long queuedBodyBytes;

    WalCheckpointPublisher(
            CanonicalControlMetadataStore metadata,
            String headKey,
            String pageKeyPrefix,
            WalRunRootRecord root,
            WalCheckpointHeadV1 initialHead) {
        this(metadata, headKey, pageKeyPrefix, root, initialHead, null, true);
    }

    /** Production constructor: all adoption/reconciliation reads consume the sole Root-owned recovery budget. */
    public WalCheckpointPublisher(
            CanonicalControlMetadataStore metadata,
            String headKey,
            String pageKeyPrefix,
            WalRunRootRecord root,
            WalCheckpointHeadV1 initialHead,
            WalRunObjectSession objectSession) {
        this(metadata, headKey, pageKeyPrefix, root, initialHead, objectSession, false);
    }

    private WalCheckpointPublisher(
            CanonicalControlMetadataStore metadata,
            String headKey,
            String pageKeyPrefix,
            WalRunRootRecord root,
            WalCheckpointHeadV1 initialHead,
            WalRunObjectSession objectSession,
            boolean isolatedTestFixture) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.root = Objects.requireNonNull(root, "root");
        this.objectSession =
                isolatedTestFixture ? objectSession : Objects.requireNonNull(objectSession, "objectSession");
        if (objectSession != null
                && (!objectSession.rootSha256().equals(WalRunControlCodec.rootSha256(root))
                        || !objectSession.rootRecord().equals(root))) {
            throw new IllegalArgumentException("checkpoint publisher session differs from the exact Root");
        }
        WalRunControlKeys.requireCheckpointHeadKey(headKey, root.shardId(), root.shardRunEpoch());
        WalRunControlKeys.requireCheckpointPagePrefix(pageKeyPrefix, root.shardId(), root.shardRunEpoch());
        this.headKey = headKey;
        this.pageKeyPrefix = pageKeyPrefix;
        this.rootSha256 = WalRunControlCodec.rootSha256(root);
        this.head = Objects.requireNonNull(initialHead, "initialHead");
        requireHeadForRoot(initialHead);
        if (initialHead.pageOrdinal() >= 0) {
            if (objectSession != null) {
                objectSession.requireRecoveredCurrentRoot();
            }
            CanonicalBytes expectedHead = WalRunControlCodec.encodeCheckpointHead(initialHead);
            if (!readControl(headKey, MAX_CONTROL_METADATA_BYTES)
                    .map(expectedHead::equals)
                    .orElse(false)) {
                throw new IllegalArgumentException("non-empty initial checkpoint Head is not the exact stored value");
            }
            if (objectSession == null) {
                new WalCheckpointChainVerifier(metadata, root).verify(initialHead);
            } else {
                objectSession.verifyCheckpointChainStreaming(metadata, initialHead, ignored -> {});
            }
        }
    }

    public synchronized void initializeHead() {
        CanonicalBytes candidate = WalRunControlCodec.encodeCheckpointHead(head);
        reconcileImmutableOrExact(
                headKey, candidate, metadata.putIfAbsent(headKey, candidate), MAX_CONTROL_METADATA_BYTES);
    }

    /** Only physically provider-resolved descriptors enter this queue; binding ACK/frontier is not an input. */
    public synchronized void enqueue(ProviderResolvedExtentDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        if (!descriptor.rootSha256().equals(rootSha256)) {
            throw new IllegalArgumentException("descriptor belongs to a different WalRun Root");
        }
        ProviderResolvedExtentRowV1 row = descriptor.row();
        if (row.laneSequence() <= head.coveredThrough().get(row.laneId())) {
            throw new IllegalArgumentException("descriptor is stale or already checkpoint-covered");
        }
        if (row.directoryPrefixEnd() > root.nwg1AdmissionCaps().maxDirectoryPrefixBytes()
                || row.bodyLength() > root.nwg1AdmissionCaps().maxCanonicalBodyBytes()) {
            throw new IllegalArgumentException("descriptor exceeds the Root-admitted NWG1 caps");
        }
        if (row.providerProof().mode() != root.providerConfiguration().proofMode()
                || row.providerProof().canonicalVersionToken().length()
                        > root.providerConfiguration().proofTokenHardCap()) {
            throw new IllegalArgumentException("descriptor proof differs from the Root-frozen provider mode/cap");
        }
        for (ProviderResolvedExtentDescriptor queued : queue) {
            if (queued.row().laneId() == row.laneId() && queued.row().laneSequence() == row.laneSequence()) {
                throw new IllegalArgumentException("duplicate provider-resolved extent descriptor");
            }
        }
        long nextBytes = Math.addExact(queuedBodyBytes, row.bodyLength());
        if (queue.size() >= root.checkpointPolicy().maxUncheckpointedExtents()
                || nextBytes > root.checkpointPolicy().maxUncheckpointedBytes()) {
            throw new IllegalStateException("uncheckpointed-tail bound reached; checkpoint/backpressure required");
        }
        queue.add(descriptor);
        queuedBodyBytes = nextBytes;
    }

    public synchronized boolean requiresAgeForcing(long nowMillis) {
        for (ProviderResolvedExtentDescriptor descriptor : queue) {
            if (nowMillis < descriptor.providerResolvedAtMillis()) {
                throw new IllegalArgumentException("clock regressed before provider resolution");
            }
            if (nowMillis - descriptor.providerResolvedAtMillis()
                    >= root.checkpointPolicy().maxUncheckpointedAgeMillis()) {
                return true;
            }
        }
        return false;
    }

    /** Publishes at most one immutable page candidate and its exact head transition. */
    public synchronized Optional<WalRunCheckpointPageV1> publishNext() {
        if (queue.isEmpty()) {
            return Optional.empty();
        }
        ArrayList<ProviderResolvedExtentDescriptor> ordered = new ArrayList<>(queue);
        ordered.sort(ORDER);
        ArrayList<ProviderResolvedExtentRowV1> selected = selectContiguousRows(ordered);
        if (selected.isEmpty()) {
            throw new IllegalStateException("queued descriptors contain no contiguous lane successor");
        }
        int rowCap = Math.min(root.checkpointPolicy().maxRowsPerPage(), selected.size());
        selected.subList(rowCap, selected.size()).clear();
        WalRunCheckpointPageV1 page;
        CanonicalBytes pageBytes;
        while (true) {
            LaneSequenceVector covered = advancedVector(head.coveredThrough(), selected);
            page = new WalRunCheckpointPageV1(
                    rootSha256, Math.incrementExact(head.pageOrdinal()), head.pageSha256(), selected, covered);
            try {
                pageBytes = WalRunControlCodec.encodeCheckpointPage(page);
                if (pageBytes.length() > root.checkpointPolicy().maxCanonicalPageBytes()) {
                    throw new IllegalArgumentException("page exceeds Root-frozen canonical byte cap");
                }
                break;
            } catch (IllegalArgumentException overCap) {
                if (selected.size() == 1) {
                    throw overCap;
                }
                selected.remove(selected.size() - 1);
            }
        }
        Sha256Digest pageSha = Sha256Digest.hash(pageBytes);
        String pageKey = pageKey(page.pageOrdinal(), pageSha);
        reconcileImmutableOrExact(
                pageKey,
                pageBytes,
                metadata.putIfAbsent(pageKey, pageBytes),
                root.checkpointPolicy().maxCanonicalPageBytes());
        WalCheckpointHeadV1 candidate = new WalCheckpointHeadV1(
                rootSha256,
                root.shardRunEpoch(),
                head.publisherEpoch(),
                page.pageOrdinal(),
                Optional.of(pageKey),
                Optional.of(pageSha),
                page.coveredThrough());
        CanonicalBytes expectedBytes = WalRunControlCodec.encodeCheckpointHead(head);
        CanonicalBytes candidateBytes = WalRunControlCodec.encodeCheckpointHead(candidate);
        ControlMutationOutcome outcome = metadata.compareAndSet(headKey, Optional.of(expectedBytes), candidateBytes);
        if (outcome == ControlMutationOutcome.APPLIED) {
            head = candidate;
            removeCovered();
            return Optional.of(page);
        }
        Optional<CanonicalBytes> observedBytes = readControl(headKey, MAX_CONTROL_METADATA_BYTES);
        if (outcome == ControlMutationOutcome.RESPONSE_UNKNOWN) {
            if (observedBytes.isPresent() && observedBytes.orElseThrow().equals(candidateBytes)) {
                head = candidate;
                removeCovered();
                return Optional.of(page);
            }
            throw new IllegalStateException("checkpoint head response-unknown did not equal the exact candidate");
        }
        WalCheckpointHeadV1 observed = observedBytes
                .map(WalRunControlCodec::decodeCheckpointHead)
                .orElseThrow(() -> new IllegalStateException("checkpoint head disappeared after CAS conflict"));
        requireAdoptableConflict(observed);
        head = observed;
        removeCovered();
        return Optional.empty();
    }

    /** Fences a publisher while preserving the exact committed page and covered-through vector. */
    public synchronized void takeover(long newPublisherEpoch) {
        if (newPublisherEpoch <= head.publisherEpoch()) {
            throw new IllegalArgumentException("publisher epoch must increase monotonically");
        }
        WalCheckpointHeadV1 candidate = new WalCheckpointHeadV1(
                head.rootSha256(),
                head.shardRunEpoch(),
                newPublisherEpoch,
                head.pageOrdinal(),
                head.pageKey(),
                head.pageSha256(),
                head.coveredThrough());
        CanonicalBytes expectedBytes = WalRunControlCodec.encodeCheckpointHead(head);
        CanonicalBytes candidateBytes = WalRunControlCodec.encodeCheckpointHead(candidate);
        ControlMutationOutcome outcome = metadata.compareAndSet(headKey, Optional.of(expectedBytes), candidateBytes);
        if (outcome == ControlMutationOutcome.APPLIED
                || readControl(headKey, MAX_CONTROL_METADATA_BYTES)
                        .map(candidateBytes::equals)
                        .orElse(false)) {
            head = candidate;
            return;
        }
        throw new IllegalStateException("checkpoint publisher takeover did not converge to the exact candidate");
    }

    public synchronized WalCheckpointHeadV1 head() {
        return head;
    }

    public synchronized int queueDepth() {
        return queue.size();
    }

    public synchronized long queuedBodyBytes() {
        return queuedBodyBytes;
    }

    public synchronized void requireFinalCoverage(LaneSequenceVector terminalSequence) {
        if (!queue.isEmpty() || !head.coveredThrough().equals(terminalSequence)) {
            throw new IllegalStateException("final checkpoint head does not exactly cover the Seal terminal vector");
        }
    }

    private ArrayList<ProviderResolvedExtentRowV1> selectContiguousRows(
            List<ProviderResolvedExtentDescriptor> ordered) {
        long[] expected = head.coveredThrough().toArray();
        boolean[] exhausted = new boolean[expected.length];
        for (int index = 0; index < expected.length; index++) {
            if (expected[index] == Long.MAX_VALUE) {
                exhausted[index] = true;
            } else {
                expected[index] = Math.incrementExact(expected[index]);
            }
        }
        ArrayList<ProviderResolvedExtentRowV1> selected = new ArrayList<>();
        for (ProviderResolvedExtentDescriptor descriptor : ordered) {
            ProviderResolvedExtentRowV1 row = descriptor.row();
            int lane = row.laneId().code();
            if (!exhausted[lane] && row.laneSequence() == expected[lane]) {
                selected.add(row);
                if (row.laneSequence() == Long.MAX_VALUE) {
                    exhausted[lane] = true;
                } else {
                    expected[lane] = Math.incrementExact(expected[lane]);
                }
            }
        }
        return selected;
    }

    private static LaneSequenceVector advancedVector(
            LaneSequenceVector predecessor, List<ProviderResolvedExtentRowV1> rows) {
        LaneSequenceVector result = predecessor;
        for (ProviderResolvedExtentRowV1 row : rows) {
            result = result.with(row.laneId(), row.laneSequence());
        }
        return result;
    }

    private void removeCovered() {
        Iterator<ProviderResolvedExtentDescriptor> iterator = queue.iterator();
        while (iterator.hasNext()) {
            ProviderResolvedExtentDescriptor descriptor = iterator.next();
            if (head.coveredThrough().get(descriptor.row().laneId())
                    >= descriptor.row().laneSequence()) {
                queuedBodyBytes =
                        Math.subtractExact(queuedBodyBytes, descriptor.row().bodyLength());
                iterator.remove();
            }
        }
    }

    private void requireAdoptableConflict(WalCheckpointHeadV1 observed) {
        requireHeadForRoot(observed);
        if (observed.publisherEpoch() != head.publisherEpoch()
                || observed.pageOrdinal() < head.pageOrdinal()
                || !observed.coveredThrough().componentWiseAtLeast(head.coveredThrough())) {
            throw new IllegalStateException("checkpoint head conflict is stale, forked, or regressing");
        }
        if (observed.pageOrdinal() == head.pageOrdinal()) {
            if (!observed.pageKey().equals(head.pageKey())
                    || !observed.pageSha256().equals(head.pageSha256())
                    || !observed.coveredThrough().equals(head.coveredThrough())) {
                throw new IllegalStateException("same-ordinal checkpoint Head conflict is a fork");
            }
        } else if (objectSession == null) {
            WalCheckpointChainVerifier.Verification verified =
                    new WalCheckpointChainVerifier(metadata, root).verify(observed);
            if (head.pageOrdinal() >= 0
                    && !verified.containsExact(
                            head.pageOrdinal(),
                            head.pageKey().orElseThrow(),
                            head.pageSha256().orElseThrow())) {
                throw new IllegalStateException("checkpoint Head conflict is not a descendant of the current page");
            }
        } else {
            objectSession.verifyCheckpointChainDescendant(
                    metadata,
                    observed,
                    head.pageOrdinal(),
                    head.pageKey().orElse(""),
                    head.pageSha256().orElse(rootSha256));
        }
    }

    private void requireHeadForRoot(WalCheckpointHeadV1 value) {
        if (!value.rootSha256().equals(rootSha256) || value.shardRunEpoch() != root.shardRunEpoch()) {
            throw new IllegalArgumentException("checkpoint head belongs to a different Root/run epoch");
        }
    }

    private void reconcileImmutableOrExact(
            String key, CanonicalBytes candidate, ControlMutationOutcome outcome, long maximumCanonicalBytes) {
        if (outcome == ControlMutationOutcome.APPLIED
                || readControl(key, maximumCanonicalBytes)
                        .map(candidate::equals)
                        .orElse(false)) {
            return;
        }
        throw new IllegalStateException("immutable checkpoint record did not converge to exact candidate");
    }

    private Optional<CanonicalBytes> readControl(String key, long maximumCanonicalBytes) {
        if (objectSession != null) {
            objectSession.chargeRecoveryControlMetadata(maximumCanonicalBytes);
        }
        return metadata.get(key);
    }

    private String pageKey(long ordinal, Sha256Digest pageSha) {
        return WalRunControlKeys.checkpointPageKey(root.shardId(), root.shardRunEpoch(), ordinal, pageSha);
    }
}
