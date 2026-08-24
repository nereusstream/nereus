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
import com.nereusstream.storage.object.recovery.CumulativeRecoveryBudget;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/** Strict bounded verification of the complete physical checkpoint page chain selected by one Head. */
public final class WalCheckpointChainVerifier {
    private final CanonicalControlMetadataStore metadata;
    private final WalRunRootRecord root;
    private final Sha256Digest rootSha256;

    public WalCheckpointChainVerifier(CanonicalControlMetadataStore metadata, WalRunRootRecord root) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.root = Objects.requireNonNull(root, "root");
        this.rootSha256 = WalRunControlCodec.rootSha256(root);
    }

    public Verification verify(WalCheckpointHeadV1 head) {
        Objects.requireNonNull(head, "head");
        if (!head.rootSha256().equals(rootSha256) || head.shardRunEpoch() != root.shardRunEpoch()) {
            throw new IllegalStateException("checkpoint Head differs from the exact WalRun Root");
        }
        if (head.pageOrdinal() == -1) {
            if (!head.coveredThrough().equals(LaneSequenceVector.empty())) {
                throw new IllegalStateException("empty checkpoint Head has a non-empty vector");
            }
            return new Verification(List.of(), 0, 0, LaneSequenceVector.empty());
        }
        if (head.pageOrdinal() >= root.bounds().maxExtentCount()) {
            throw new IllegalStateException("checkpoint chain page count exceeds the Root extent bound");
        }

        ArrayList<VerifiedPage> reversePages = new ArrayList<>();
        long ordinal = head.pageOrdinal();
        Sha256Digest expectedSha = head.pageSha256().orElseThrow();
        while (true) {
            String expectedKey =
                    WalRunControlKeys.checkpointPageKey(root.shardId(), root.shardRunEpoch(), ordinal, expectedSha);
            if (ordinal == head.pageOrdinal() && !head.pageKey().orElseThrow().equals(expectedKey)) {
                throw new IllegalStateException("checkpoint Head page key differs from its exact identity");
            }
            CanonicalBytes pageBytes = metadata.get(expectedKey)
                    .orElseThrow(() -> new IllegalStateException("checkpoint page is absent: " + expectedKey));
            if (pageBytes.length() > root.checkpointPolicy().maxCanonicalPageBytes()
                    || !Sha256Digest.hash(pageBytes).equals(expectedSha)) {
                throw new IllegalStateException("checkpoint page byte cap or SHA-256 differs");
            }
            WalRunCheckpointPageV1 page =
                    WalRunControlCodec.decodeCheckpointPage(pageBytes, root.providerConfiguration());
            if (!page.rootSha256().equals(rootSha256) || page.pageOrdinal() != ordinal) {
                throw new IllegalStateException("checkpoint page Root/ordinal differs from its selected identity");
            }
            reversePages.add(new VerifiedPage(ordinal, expectedKey, expectedSha, page));
            if (ordinal == 0) {
                if (page.predecessorPageSha256().isPresent()) {
                    throw new IllegalStateException("checkpoint page zero names a predecessor");
                }
                break;
            }
            expectedSha = page.predecessorPageSha256()
                    .orElseThrow(() -> new IllegalStateException("checkpoint successor page omits predecessor SHA"));
            ordinal = Math.decrementExact(ordinal);
        }

        Collections.reverse(reversePages);
        LaneSequenceVector predecessorVector = LaneSequenceVector.empty();
        Sha256Digest predecessorSha = null;
        long extentCount = 0;
        long canonicalBodyBytes = 0;
        for (VerifiedPage verified : reversePages) {
            WalRunCheckpointPageV1 page = verified.page();
            if (verified.ordinal() == 0) {
                if (predecessorSha != null) {
                    throw new IllegalStateException("checkpoint page-zero position is not canonical");
                }
            } else if (page.predecessorPageSha256().isEmpty()
                    || !page.predecessorPageSha256().orElseThrow().equals(predecessorSha)) {
                throw new IllegalStateException("checkpoint page predecessor SHA does not form one exact chain");
            }
            LaneSequenceVector computed = predecessorVector;
            long[] next = predecessorVector.toArray();
            boolean[] exhausted = new boolean[next.length];
            for (int index = 0; index < next.length; index++) {
                if (next[index] == Long.MAX_VALUE) {
                    exhausted[index] = true;
                } else {
                    next[index] = Math.incrementExact(next[index]);
                }
            }
            for (ProviderResolvedExtentRowV1 row : page.extents()) {
                int lane = row.laneId().code();
                if (exhausted[lane] || row.laneSequence() != next[lane]) {
                    throw new IllegalStateException("checkpoint page contains a lane gap or duplicate");
                }
                if (row.directoryPrefixEnd() > root.nwg1AdmissionCaps().maxDirectoryPrefixBytes()
                        || row.bodyLength() > root.nwg1AdmissionCaps().maxCanonicalBodyBytes()) {
                    throw new IllegalStateException("checkpoint row exceeds the Root-admitted NWG1 caps");
                }
                computed = computed.with(row.laneId(), row.laneSequence());
                if (row.laneSequence() == Long.MAX_VALUE) {
                    exhausted[lane] = true;
                } else {
                    next[lane] = Math.incrementExact(next[lane]);
                }
                extentCount = Math.incrementExact(extentCount);
                canonicalBodyBytes = Math.addExact(canonicalBodyBytes, row.bodyLength());
            }
            if (!computed.equals(page.coveredThrough())) {
                throw new IllegalStateException("checkpoint page vector is not the exact contiguous row fold");
            }
            if (extentCount > root.bounds().maxExtentCount()
                    || canonicalBodyBytes > root.bounds().maxCanonicalBodyBytes()) {
                throw new IllegalStateException("checkpoint chain aggregate exceeds the Root bounds");
            }
            predecessorVector = computed;
            predecessorSha = verified.sha256();
        }
        if (!predecessorVector.equals(head.coveredThrough())) {
            throw new IllegalStateException("checkpoint Head vector differs from the complete page-chain fold");
        }
        return new Verification(reversePages, extentCount, canonicalBodyBytes, predecessorVector);
    }

    /**
     * Verifies a sealed checkpoint chain in one backwards streaming pass.  Unlike the publisher-conflict helper,
     * this path retains no page list: each immutable page proves the exact predecessor vector by subtracting its
     * locally contiguous rows from its own covered-through vector.  The caller's Root-owned budget is charged with
     * the persisted page cap before every metadata read.
     */
    public StreamingVerification verifyStreaming(WalCheckpointHeadV1 head, CumulativeRecoveryBudget recoveryBudget) {
        return verifyStreaming(head, recoveryBudget, ignored -> {}, ignored -> {});
    }

    /**
     * Streaming recovery hook. The consumer receives rows only after their page has passed exact identity/vector/cap
     * checks; callers must stage them and publish only after this method returns successfully.
     */
    public StreamingVerification verifyStreaming(
            WalCheckpointHeadV1 head,
            CumulativeRecoveryBudget recoveryBudget,
            Consumer<ProviderResolvedExtentRowV1> verifiedRowConsumer) {
        return verifyStreaming(head, recoveryBudget, verifiedRowConsumer, ignored -> {});
    }

    /** Also reports each verified page identity without retaining the page chain. */
    public StreamingVerification verifyStreaming(
            WalCheckpointHeadV1 head,
            CumulativeRecoveryBudget recoveryBudget,
            Consumer<ProviderResolvedExtentRowV1> verifiedRowConsumer,
            Consumer<VerifiedPageIdentity> verifiedPageConsumer) {
        Objects.requireNonNull(head, "head");
        Objects.requireNonNull(recoveryBudget, "recoveryBudget");
        Objects.requireNonNull(verifiedRowConsumer, "verifiedRowConsumer");
        Objects.requireNonNull(verifiedPageConsumer, "verifiedPageConsumer");
        requireHeadForRoot(head);
        if (head.pageOrdinal() == -1) {
            if (!head.coveredThrough().equals(LaneSequenceVector.empty())) {
                throw new IllegalStateException("empty checkpoint Head has a non-empty vector");
            }
            return new StreamingVerification(0, 0, 0, LaneSequenceVector.empty());
        }
        if (head.pageOrdinal() >= root.bounds().maxExtentCount()) {
            throw new IllegalStateException("checkpoint chain page count exceeds the Root extent bound");
        }

        long ordinal = head.pageOrdinal();
        Sha256Digest expectedSha = head.pageSha256().orElseThrow();
        LaneSequenceVector expectedCoverage = head.coveredThrough();
        long pageCount = 0;
        long extentCount = 0;
        long canonicalBodyBytes = 0;
        while (true) {
            String expectedKey =
                    WalRunControlKeys.checkpointPageKey(root.shardId(), root.shardRunEpoch(), ordinal, expectedSha);
            if (ordinal == head.pageOrdinal() && !head.pageKey().orElseThrow().equals(expectedKey)) {
                throw new IllegalStateException("checkpoint Head page key differs from its exact identity");
            }
            // The canonical page cap is persisted in the Root, so the full cap is committed before metadata I/O.
            recoveryBudget.chargeControlMetadata(root.checkpointPolicy().maxCanonicalPageBytes());
            CanonicalBytes pageBytes = metadata.get(expectedKey)
                    .orElseThrow(() -> new IllegalStateException("checkpoint page is absent: " + expectedKey));
            if (pageBytes.length() > root.checkpointPolicy().maxCanonicalPageBytes()
                    || !Sha256Digest.hash(pageBytes).equals(expectedSha)) {
                throw new IllegalStateException("checkpoint page byte cap or SHA-256 differs");
            }
            WalRunCheckpointPageV1 page =
                    WalRunControlCodec.decodeCheckpointPage(pageBytes, root.providerConfiguration());
            if (!page.rootSha256().equals(rootSha256) || page.pageOrdinal() != ordinal) {
                throw new IllegalStateException("checkpoint page Root/ordinal differs from its selected identity");
            }
            if (!page.coveredThrough().equals(expectedCoverage)) {
                throw new IllegalStateException("checkpoint page vector differs from its selected successor/Head");
            }

            LaneSequenceVector predecessorCoverage = subtractPageRows(page, expectedCoverage);
            pageCount = Math.incrementExact(pageCount);
            extentCount = Math.addExact(extentCount, page.extents().size());
            for (ProviderResolvedExtentRowV1 row : page.extents()) {
                if (row.directoryPrefixEnd() > root.nwg1AdmissionCaps().maxDirectoryPrefixBytes()
                        || row.bodyLength() > root.nwg1AdmissionCaps().maxCanonicalBodyBytes()) {
                    throw new IllegalStateException("checkpoint row exceeds the Root-admitted NWG1 caps");
                }
                canonicalBodyBytes = Math.addExact(canonicalBodyBytes, row.bodyLength());
                verifiedRowConsumer.accept(row);
            }
            verifiedPageConsumer.accept(new VerifiedPageIdentity(ordinal, expectedKey, expectedSha));
            if (pageCount > root.bounds().maxExtentCount()
                    || extentCount > root.bounds().maxExtentCount()
                    || canonicalBodyBytes > root.bounds().maxCanonicalBodyBytes()) {
                throw new IllegalStateException("checkpoint chain aggregate exceeds the Root bounds");
            }
            if (ordinal == 0) {
                if (page.predecessorPageSha256().isPresent()) {
                    throw new IllegalStateException("checkpoint page zero names a predecessor");
                }
                if (!predecessorCoverage.equals(LaneSequenceVector.empty())) {
                    throw new IllegalStateException("checkpoint page chain does not fold to the empty vector");
                }
                break;
            }
            expectedSha = page.predecessorPageSha256()
                    .orElseThrow(() -> new IllegalStateException("checkpoint successor page omits predecessor SHA"));
            expectedCoverage = predecessorCoverage;
            ordinal = Math.decrementExact(ordinal);
        }
        return new StreamingVerification(pageCount, extentCount, canonicalBodyBytes, head.coveredThrough());
    }

    private void requireHeadForRoot(WalCheckpointHeadV1 head) {
        if (!head.rootSha256().equals(rootSha256) || head.shardRunEpoch() != root.shardRunEpoch()) {
            throw new IllegalStateException("checkpoint Head differs from the exact WalRun Root");
        }
    }

    private static LaneSequenceVector subtractPageRows(
            WalRunCheckpointPageV1 page, LaneSequenceVector expectedCoverage) {
        long[] predecessor = expectedCoverage.toArray();
        for (int index = page.extents().size() - 1; index >= 0; index--) {
            ProviderResolvedExtentRowV1 row = page.extents().get(index);
            int lane = row.laneId().code();
            if (predecessor[lane] < 0 || predecessor[lane] != row.laneSequence()) {
                throw new IllegalStateException("checkpoint page contains a lane gap or duplicate");
            }
            predecessor[lane] = predecessor[lane] == 0 ? -1 : Math.decrementExact(predecessor[lane]);
        }
        return LaneSequenceVector.of(predecessor[0], predecessor[1], predecessor[2]);
    }

    public record VerifiedPage(long ordinal, String key, Sha256Digest sha256, WalRunCheckpointPageV1 page) {
        public VerifiedPage {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(sha256, "sha256");
            Objects.requireNonNull(page, "page");
        }
    }

    /** Minimal identity emitted by the streaming verifier for descendant-anchor checks. */
    public record VerifiedPageIdentity(long ordinal, String key, Sha256Digest sha256) {
        public VerifiedPageIdentity {
            if (ordinal < 0) {
                throw new IllegalArgumentException("verified page ordinal must be non-negative");
            }
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(sha256, "sha256");
        }
    }

    public record Verification(
            List<VerifiedPage> pages,
            long aggregateExtentCount,
            long aggregateCanonicalBodyBytes,
            LaneSequenceVector coveredThrough) {
        public Verification {
            pages = List.copyOf(pages);
            Objects.requireNonNull(coveredThrough, "coveredThrough");
        }

        public boolean containsExact(long ordinal, String key, Sha256Digest sha256) {
            return pages.stream()
                    .anyMatch(page -> page.ordinal() == ordinal
                            && page.key().equals(key)
                            && page.sha256().equals(sha256));
        }
    }

    /** Aggregate-only result for bounded recovery; no page collection is retained. */
    public record StreamingVerification(
            long pageCount,
            long aggregateExtentCount,
            long aggregateCanonicalBodyBytes,
            LaneSequenceVector coveredThrough) {
        public StreamingVerification {
            Objects.requireNonNull(coveredThrough, "coveredThrough");
        }
    }
}
